#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <vector>
#include <math.h>
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
  	if (in_fp == NULL) {
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
	int runLength = mem_capacity / recordLen;	// Records per run
	int runSize = runLength * (recordLen - 1);	// Size of run			
	int itMemCap = mem_capacity / (k+1);		// Max capacity per iterator + buffer

	// Get number of runs by size of file
	fseek(in_fp, 0, SEEK_END);
	int filesize = ftell(in_fp);
	int numOfRuns = (filesize % runSize == 0) ? filesize / runSize : (filesize / runSize) + 1;	// Number of runs
	fseek(in_fp, 0, SEEK_SET);

	// If memory alloted to each iterator is too small to fit a record. Sort not possible
	if (itMemCap < recordLen) {
		printf("Not enough memory allocated to do msort. Consider increasing mem_capcity or decreasing k\n");
		return 1;
	}
	
	/* At this point, all parameters for sorting are set */
	// Initialize output files (two to alternate between write-read when merging)
	FILE *readFrom = fopen ("tmp1", "w+");
	FILE *writeTo = fopen ("tmp2" , "w+");
	string outFilename = "tmp1";		// File containing the result of the last merge

	// PASS 0: Initial sort into numOfRuns-runs
	mk_runs(in_fp, readFrom, runLength, &schema);

	// PASS 1..N: Merge sort runs	
	char *buf = (char *) malloc(itMemCap);
	while (numOfRuns > 1) {		
		int offset = 0;

		// Merge every k-runs
		for (int j = 0; j < numOfRuns; j+= k) {
			// Create array of k-iterators	
			RunIterator *its[k];
			for (int i = 0; i < k; i++) {
				its[i] = new RunIterator(readFrom, offset + (i * runSize), runLength, itMemCap, &schema);
			}	
			merge_runs(its, k, writeTo, offset, buf, itMemCap);

			// Move onto next k-runs
			offset += k * runSize;
		}		

		// Alternate read-write files (Sort alternates between two files)
		FILE *tmp = readFrom;
		readFrom = writeTo;
		writeTo = tmp;
		outFilename = (outFilename.compare("tmp1") == 0) ? "tmp2" : "tmp1"; 

		// Update stats
		runLength = runLength * k;
		runSize = runSize * k;
		numOfRuns = ceil(numOfRuns / k);

	}

	// Clean up
	free(buf);
	fclose(in_fp);
	fclose(readFrom);
	fclose(writeTo);

	// Rename the final output file into given out_file name
	rename (outFilename.c_str(), argv[3]);

	// Clean up remaining tmp files
	remove ("tmp1");
	remove ("tmp2");	

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
