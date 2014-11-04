#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <vector>
#include <math.h>
#include <sys/time.h> 
#include <ctime>
#include "library.h"
#include "json/json.h"

void print_all_records(FILE *in_fp, int recordLen, Schema schema);
void convert_string_into_sort_attr(Schema *schema, string sortingAttr);
int addAttrToSchema(Json::Value schema_val, Schema *schema, int i);
int getNumberOfInitRuns(FILE *in_fp, int maxRunSize);


int main(int argc, const char* argv[]) {
	
	timeval tim;
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
	int recordLen = 1;				// start at 1 for newline character
	schema.nattrs = 0;
  	for (u_int32_t i = 0; i < schema_val.size(); ++i) {	
	    recordLen += addAttrToSchema(schema_val, &schema, i) + 1;	// extra +1 for comma
  	}
	convert_string_into_sort_attr(&schema, sortingAttr);

	// Initialize stats	
	int maxRecPerRun = mem_capacity / recordLen;			// Records per run
	int maxRunSize = maxRecPerRun * (recordLen - 1);		// Size of run in bytes
	int numOfRuns = getNumberOfInitRuns(in_fp, maxRunSize);	// Number of runs in file
	int k = min(atoi(argv[5]), numOfRuns);					// Max number of merges per pass
	int memCap = mem_capacity / (k+1);						// Max capacity per iterator + buffer

	// Verify all parameters are valid
	if (memCap < recordLen) {
		cout << "ERROR: Not enough memory per Iterator to do msort. Increase <mem_capacity> or decrease <k>" << endl;
		return 1;
	} else if (atoi(argv[5]) == 1) {
		cout << "ERROR: <k> cannot be 1" << endl;
		return 1;
	}
	
	/* At this point, all parameters for sorting are set */

	// Initialize output files (two to alternate between write-read when merging)
	FILE *readFrom = fopen ("tmp1", "w+");
	FILE *writeTo = fopen ("tmp2" , "w+");
	string lastResult = "tmp1";		// File containing the result of the last merge

	/* Merge-sorting starts here */

	// Start the timer	
    gettimeofday(&tim, NULL);
	double before=(tim.tv_sec * 1000)+(tim.tv_usec / 1000.0);
	
	// PASS 0: Initial sort into numOfRuns-runs
	mk_runs(in_fp, readFrom, maxRecPerRun, &schema);

	// PASS 1..N: Merge sort runs	
	char *buf = (char *) malloc(memCap);
	while (numOfRuns > 1) {		
		// Position on file to read from
		int offset = 0;

		// Merge every k-runs
		for (int j = 0; j < numOfRuns; j+= k) {
			// Create array of k-iterators	
			RunIterator *its[k];
			for (int i = 0; i < k; i++) {
				its[i] = new RunIterator(readFrom, offset + (i * maxRunSize), maxRecPerRun, memCap, &schema);
			}	

			merge_runs(its, k, writeTo, offset, buf, memCap);
	
			// Free all iterators
			for (int i = 0; i < k; i++) {
				delete its[i];
			}	

			// Move onto next k-runs
			offset += k * maxRunSize;
		}		

		// Alternate read-write files (Sort alternates between two files)
		FILE *tmp = readFrom;
		readFrom = writeTo;
		writeTo = tmp;
		lastResult = (lastResult.compare("tmp1") == 0) ? "tmp2" : "tmp1"; 

		// Update stats to reflect merged runs
		maxRecPerRun = maxRecPerRun * k;
		maxRunSize = maxRunSize * k;
		numOfRuns = ceil(double(numOfRuns) / k);
		k = min (k, numOfRuns);
	}

	// Clean up
	free(buf);
	fclose(in_fp);
	fclose(readFrom);
	fclose(writeTo);

	// Rename the final output file into given out_file name
	rename (lastResult.c_str(), argv[3]);

	// Clean up remaining tmp files
	remove ("tmp1");
	remove ("tmp2");	

	/* Sorting ends here */
	// Report time taken to sort
	gettimeofday(&tim, NULL);
	double after=(tim.tv_sec * 1000)+(tim.tv_usec / 1000.0);
	printf("Time to process merge sort on file '%s' : \t%f ms\n", argv[2], after - before);

	return 0;
}

int getNumberOfInitRuns(FILE *in_fp, int maxRunSize) {
	fseek(in_fp, 0, SEEK_END);
	int numOfRuns = ceil(double(ftell(in_fp)) / maxRunSize);
	fseek(in_fp, 0, SEEK_SET);
	return numOfRuns;
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

int addAttrToSchema(Json::Value schema_val, Schema *schema, int i) {
	Attribute attribute;
	attribute.name.assign(schema_val[i].get("name", "UTF-8" ).asString());
	attribute.length = schema_val[i].get("length", "UTF-8").asInt();
	attribute.type.assign(schema_val[i].get("type", "UTF-8").asString());
	schema->attrs.push_back(attribute);
	schema->nattrs++;    
	return attribute.length;
}

