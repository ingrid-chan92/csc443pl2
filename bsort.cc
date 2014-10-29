#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "json/json.h"
#include "library.h"

using namespace std;
void convert_string_into_sort_attr(Schema *schema, string sortingAttr);
void dump_tree(leveldb::DB *db, FILE *out_fp);
bool insert_and_collision_check(leveldb::DB *db, leveldb::Slice key, leveldb::Slice value, long uniquer);


class CsvComparator : public leveldb::Comparator {
    public:
    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        string type_array;

	    string a_vals(a.ToString());
	    istringstream a_iss(a_vals);
        string a_token;
        string a_type;

        string b_vals(b.ToString());
	    istringstream b_iss(b_vals);	    
        string b_token;
        string b_type;

        string type;
        bool a_success = getline(a_iss, a_token, ',');
        bool b_success = getline(b_iss, b_token, ',');
        bool exited_from_if = false;

        while (a_success && b_success) {
            a_success = getline(a_iss, a_type, ',');
            b_success = getline(b_iss, b_type, ',');

            if(!a_success || !b_success) {
                /*in the case of two salts, it will fail here*/
                /*one salt and no salts will fail at while loop condition*/
                exited_from_if = true;
                break;
            }
               
            type = a_type;

        	if (type.compare("integer") == 0 || type.compare("long") == 0) {
	            if( atol(a_token.c_str()) < atol(b_token.c_str())){
                    return -1;
                }else if ( atol(a_token.c_str()) > atol(b_token.c_str())) {
                    return 1;
                }
            } 
            else if (type.compare("float") == 0 || type.compare("double") == 0) {
	            if (atof(a_token.c_str()) < atof(b_token.c_str())) {
                    return -1;
                } else if ( atof(a_token.c_str()) > atof(b_token.c_str())) {
                    return 1;
                }
            }           
            else {
	            // Default string comparison
	            if (a_token.compare(b_token) != 0)
                    return a_token.compare(b_token);
            }

            a_success = getline(a_iss, a_token, ',');
            b_success = getline(b_iss, b_token, ',');
	    }

        //check the salt if all else fails
        if(exited_from_if) {
            /*exited from if means both entries have salt*/
            if (atol(a_token.c_str()) < atol(b_token.c_str())) {
                return -1;
            } else if ( atol(a_token.c_str()) > atol(b_token.c_str())) {
                return 1;
            }
        } else if ((a_success && !b_success)) {
            return 1;
        } else if ((!a_success && b_success)) {
            return -1;
        }
        return 0;
    }

    const char* Name() const { return "CsvComparator"; }
    void FindShortestSeparator(std::string*, const leveldb::Slice&) const { }
    void FindShortSuccessor(std::string*) const { }
};


int main(int argc, const char* argv[]) {

	if (argc != 5) {
		cout << "ERROR: invalid input parameters!" << endl;
		cout << "Please enter <schema_file> <input_file> <out_index> <sorting_attributes>" << endl;
		exit(1);
	}

	// Do work here
    Attribute attribute;
    Schema schema; 

    CsvComparator cmp; 	
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.comparator = &cmp;
    leveldb::Status status = leveldb::DB::Open(options, "./leveldb_dir", &db);
    leveldb::Slice key;
    leveldb::Slice value;

    // Initialize args
    string schema_file(argv[1]);	
    FILE *in_fp = fopen (argv[2] , "r");
    FILE *out_fp = fopen (argv[3] , "w+");
    if (in_fp == NULL || out_fp == NULL) {
	    perror ("Error opening file");
    }		

    string sortingAttr(argv[4]);

    // Parse the schema JSON file
    // Support for std::string argument is added in C++11 so you don't have to use .c_str() if you are on that.
    Json::Value schema_val;
    Json::Reader json_reader;  
    ifstream schema_file_istream(schema_file.c_str(), ifstream::binary);
    bool successful = json_reader.parse(schema_file_istream, schema_val, false);
    if (!successful) {
        cout << "ERROR: " << json_reader.getFormatedErrorMessages() << endl;
        exit(1);
    }

    // Setup schema struct
    int schema_value_len = 0;
    int comma_len = 0;
    schema.nattrs = 0;

    // Populate attributes list
    for (u_int32_t i = 0; i < schema_val.size(); ++i) {	    
        attribute.name.assign(schema_val[i].get("name", "UTF-8" ).asString());
        attribute.length = schema_val[i].get("length", "UTF-8").asInt();
        attribute.type.assign(schema_val[i].get("type", "UTF-8").asString());
        schema.attrs.push_back(attribute);
        schema.nattrs++;    
        schema_value_len += attribute.length;
        comma_len++;
    }
    convert_string_into_sort_attr(&schema, sortingAttr);

    // Read and sort records in in_fp
	u_int32_t recordLength = get_expected_data_size(&schema);	
  	vector<Record> records;
    Record record;
	char buffer[recordLength];
	while (fgets(buffer, recordLength, in_fp) != NULL) {
		record.schema = &schema;
		record.data.assign(buffer);
   		records.push_back(record);
	}

    //inserting records into b+ tree
    string key_str, value_str;
    long uniquer = 0;
    for (u_int32_t j = 0; j < records.size(); j++) {
        key_str = "";
	    for (u_int32_t sort_i = 0; sort_i < schema.sort_attrs.size(); sort_i++) {
            key_str.append(get_attr_from_data(schema.sort_attrs[sort_i], records[j].data));
            key_str.append(",");
            key_str.append(schema.attrs[schema.sort_attrs[sort_i]].type);
            if(sort_i < schema.sort_attrs.size() - 1) {
                key_str.append(",");
            }
        }

        key = key_str;     
        value_str.assign(records[j].data);
        value = value_str; 
  
        if(insert_and_collision_check(db, key, value, uniquer)){
            uniquer++;
        }	
    }

    //writing result to file
    dump_tree(db, out_fp);

    delete db;
    fclose(in_fp);
    fclose(out_fp);

    return 0;
}

/*parse the sorting attributes into index of attributes in schema*/
void convert_string_into_sort_attr(Schema *schema, string sortingAttr) {
	string tokenizedText(sortingAttr);
	istringstream iss(tokenizedText);
	string token;

	while (getline(iss, token, ',')) {
		for (int i = 0; i < schema->nattrs; i++) {
			if (token.compare(schema->attrs[i].name) == 0) {
				// Found matching sort attribute. Record ID into sort_attrs
				schema->sort_attrs.push_back(i);
			}
		} 
	}
}

/*inserts key-value into b+ tree; add salt in case of collision*/
bool insert_and_collision_check(leveldb::DB *db, leveldb::Slice key, leveldb::Slice value, long uniquer) {
    string val;
    stringstream longstream;
    string key_str;
    bool is_collision = 0;
    
    leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &val);
    if (s.ok()) {
        longstream << ","<<uniquer;
        key_str = key.ToString();
        key_str.append(longstream.str());
        key = key_str;
        is_collision = 1;
    }
    s = db->Put(leveldb::WriteOptions(), key, value);
    return is_collision;
}   

/*traverse the b+ tree and print all key-value pairs*/
void dump_tree(leveldb::DB *db, FILE *out_fp){
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        leveldb::Slice value = it->value();
        std::string key_str = key.ToString();
        std::string val_str = value.ToString();
        //write to file
        fprintf(out_fp, "%s", val_str.c_str());
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;
}

