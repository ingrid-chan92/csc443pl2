#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "leveldb/db.h"
#include "json/json.h"
#include "library.h"

using namespace std;
void convert_string_into_sort_attr(Schema *schema, string sortingAttr);
void dump_tree(leveldb::DB *db, FILE *out_fp);
bool insert_and_collision_check(leveldb::DB *db, leveldb::Slice key, leveldb::Slice value, long uniquer);

int main(int argc, const char* argv[]) {

	if (argc != 5) {
		cout << "ERROR: invalid input parameters!" << endl;
		cout << "Please enter <schema_file> <input_file> <out_index> <sorting_attributes>" << endl;
		exit(1);
	}

	// Do work here
    Attribute attribute;
    Schema schema;  	
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    //need to specify options.comparator?
    leveldb::Status status = leveldb::DB::Open(options, "./leveldb_dir", &db);

    /*
    In the C++ namespace leveldb, we find that DB class has three important methods:
    DB::Put(WriteOptions& options, const Slice& key, const Slice& value)
    DB::Get(ReadOptions& options, const Slice& key, std::string *value)
    DB::Delete(WriteOptions& options, const Slice& key)

    leveldb::Slice key = "CSC443/WINTER";
    leveldb::Slice value = std::string("This is a course about database implementation.")
    */
    leveldb::Slice key;
    leveldb::Slice value;

    /*checkpoint 1*/
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
    
    /*checkpoint 2
    for (u_int32_t j = 0; j < schema.sort_attrs.size(); j++) {
	    	cout << schema.sort_attrs[j] << "\n";		
	}*/

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

    /*checkpoint 3
	for (u_int32_t j = 0; j < records.size(); j++) {
	//	cout << records[j].data.c_str();		
    }*/	

    //inserting records into b+ tree
    string key_str, value_str;
    long uniquer = 0;
    for (u_int32_t j = 0; j < records.size(); j++) {
        key_str = "";
	    for (u_int32_t sort_i = 0; sort_i < schema.sort_attrs.size(); sort_i++) {
            key_str.append(get_attr_from_data(schema.sort_attrs[sort_i], records[j].data));
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

/*checkpoint*/
/*inserts key-value into b+ tree; add salt in case of collision*/
bool insert_and_collision_check(leveldb::DB *db, leveldb::Slice key, leveldb::Slice value, long uniquer) {
    string val;
    stringstream longstream;
    string key_str;
    bool is_collision = 0;
    
    leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &val);
    if (s.ok()) {
        longstream << uniquer;
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
        cout << key_str << ": " << val_str << endl;
        //fprintf(out_fp, "%s", val_str.c_str);
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;
}

