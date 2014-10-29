#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <vector>
#include "library.h"
#include "json/json.h"

void print_all_records(FILE *in_fp, int recordLen, Schema schema);
void convert_string_into_sort_attr(Schema *schema, string sortingAttr);

int main(int argc, const char* argv[]) {

	Attribute attribute;
	Schema schema;  

	if (argc != 7) {
		cout << "Usage: <schema_file> <input_file> <output_file> <mem_capacity> <k> <sorting_attributes>" << endl;
		cout << "<sorting_attributes> should be comma-separated list (No spaces). E.g. attrA,attrB,attrC" << endl;
		exit(1);
	}

	// Initialize args
  	string schema_file(argv[1]);	
	FILE *in_fp = fopen (argv[2] , "r");
	FILE *out_fp = fopen (argv[3] , "w+");
  	if (in_fp == NULL || out_fp == NULL) {
		perror ("Error opening file");
	}		
	int mem_capacity = atoi(argv[4]);
	int k = atoi(argv[5]);
	string sortingAttr(argv[6]);

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

  	for (u_int32_t i = 0; i < schema_val.size(); ++i) {
	// Populate attributes list
    		attribute.name.assign(schema_val[i].get("name", "UTF-8" ).asString());
	    	attribute.length = schema_val[i].get("length", "UTF-8").asInt();
	    	attribute.type.assign(schema_val[i].get("type", "UTF-8").asString());
	    	schema.attrs.push_back(attribute);
	    	schema.nattrs++;    
	    	schema_value_len += attribute.length;
		comma_len++;

		// Print out schema
		//cout << "{schema_name : " << schema.attrs[i].name << ", schema_len : " << schema.attrs[i].length  << ", schema_type : " << schema.attrs[i].type << "}" << endl;
  	}
	convert_string_into_sort_attr(&schema, sortingAttr);

	// Initialize reusable stats	
	int recordLen = schema_value_len+comma_len+1;
	int runLength = mem_capacity / recordLen;				// Records per run
	int runSize = runLength * (recordLen - 1);		
	int numOfRuns = mem_capacity / runSize + 1;	// Number of runs
	int itMemCap = mem_capacity / (k+1);
	
	// If memory alloted to each iterator is too small to fit a record. Sort not possible
	if (itMemCap < recordLen) {
		printf("Not enough memory allocated to do msort. Consider increasing mem_capcity or decreasing k\n");
		return 1;
	}

	// Initial sort
	mk_runs(in_fp, out_fp, runLength, &schema);

	// USE THIS SOMEWHERE
	// Create array of k-iterators
	RunIterator *its[k];
	for (int i = 0; i < k; i++) {
		its[i] = new RunIterator(out_fp, 0 + (i * runSize), runLength, itMemCap, &schema);	// REPLACE 0 with the offset for startPos
	}


	// DEBUG: Test run iteartor
	while (its[0]->has_next()) {
		Record *record = its[0]->next();
		cout << "record_data 1: " << record->data << endl;
	}
	while (its[1]->has_next()) {
		Record *record = its[1]->next();
		cout << "record_data 2: " << record->data << endl;
	}
	while (its[2]->has_next()) {
		Record *record = its[2]->next();
		cout << "record_data 3: " << record->data << endl;
	}


	fclose(in_fp);
	fclose(out_fp);

  return 0;
}

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


/**
 * Debugging function for printing records. Do not run on large files
 */
void print_all_records(FILE *fp, int recordLen, Schema schema) {
  std::vector<Record> records;
	Record record;
  record.schema = &schema;

	// Set file pointer back to beginning
	fseek(fp, SEEK_SET, 0);

	// Read in all the records
  char buffer[recordLen]; //one for new-line  
  while (fgets(buffer, recordLen, fp) != NULL) {
    record.data.assign(buffer);
    records.push_back(record);
  }

  /*check value in record*/
  for (u_int32_t i = 0; i < records.size(); i++) {
    cout << "record_data : " << records[i].data;
  }
}
