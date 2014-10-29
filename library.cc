#include <stdio.h>
#include <string.h>
#include "library.h"

bool compare_records(const Record &a, const Record &b);
std::string get_attr_from_data(int index, std::string data);
bool compare_attribute(string aAttr, string bAttr, Attribute attr);


RunIterator::RunIterator(FILE *fp, long start_pos, long run_length, long buf_size, Schema *schema) {
	// Allocate structures
	inFile = fp;
	sch = schema;
	buff = (char *) malloc(buf_size);		

	// Initialize buffer tracking data
	maxBuffSize = buf_size;
	buffPtr = 0;

	// Total records read
	recordsRead = 0;
	runLength = run_length;
	filePos = start_pos;
	recordSize = get_expected_data_size(schema);

	// Read in first chunk of data
	fseek(fp, filePos, SEEK_SET);
	buffSize = fread(buff, buffSize, 1, inFile);
	filePos += buffSize;

}

RunIterator::~RunIterator() {
	free(buff);
}

Record* RunIterator::next() {
	
	if (buffPtr >= buffSize) {
		// Finished reading from buffer. Process next chunk of data		
		fseek(inFile, filePos, SEEK_SET);
		buffSize = fread(buff, buffSize, 1, inFile);
		filePos += buffSize;
		buffPtr = 0;
	} 

	// Throw exception if nothing left to read
	if (buffSize <= 0 || recordsRead < runLength) {
		printf("RunIterator: Nothing left to read.");
		throw;
	}

	/* At the point. Buffer has data to read*/

	// Read next record from buffer
	Record *record = (Record *) malloc(recordSize);
	record->schema = sch;
	char dataBuf[recordSize];
	memcpy(dataBuf, buff + buffPtr, recordSize);
	record->data.assign(dataBuf);

	// Shift pointer to next potential record
	recordsRead += 1;
	buffPtr += recordSize; 

	return record;
	
}

bool RunIterator::has_next() {
	return buffSize != 0 && recordsRead < runLength;
}



void mk_runs(FILE *in_fp, FILE *out_fp, long run_length, Schema *schema)
{
	u_int32_t recordLength = get_expected_data_size(schema);	
  	vector<Record> records;
	char buffer[recordLength];
	size_t u_run_length = run_length;

	// Read and sort records in in_fp
	while (fgets(buffer, recordLength, in_fp) != NULL) {
		Record record;
		record.schema = schema;
		record.data.assign(buffer);
   		records.push_back(record);

		// Sort and write back runs
		if (records.size() == u_run_length) {
			sort(records.begin(), records.end(), compare_records);
			for (u_int32_t j = 0; j < records.size(); j++) {
				fprintf(out_fp, "%s", records[j].data.c_str());		
			}			
			records.clear();
		}
	}

	// Write any remaining records into out_fp
	if (records.size() > 0) {
		sort(records.begin(), records.end(), compare_records);
		for (u_int32_t j = 0; j < records.size(); j++) {
			fprintf(out_fp, "%s", records[j].data.c_str());		
		}			
		records.clear();
	}
}

void merge_runs(RunIterator* iterators[], int num_runs, FILE *out_fp,
                long start_pos, char *buf, long buf_size)
{
  // Your implementation
}

int get_expected_data_size(Schema *schema) {
	int schema_value_len = 0;
	int comma_len = 0;
	for (int i = 0; i < schema->nattrs; ++i) {
		schema_value_len += schema->attrs[i].length;
		comma_len++;
	}

	// Attribute lengths + commas in between + newline
	return schema_value_len + comma_len + 1;
}

/**
 * Comparison function for std::sort
 */
bool compare_records(const Record &a, const Record &b) {
	Schema *schema = a.schema;
	vector<int> sortAttrs = schema->sort_attrs;

	// Compare attributes of both records based on sortAttrs
	for (u_int32_t i = 0; i < sortAttrs.size(); i++) {
		string aAttr = get_attr_from_data(sortAttrs[i], a.data);
		string bAttr = get_attr_from_data(sortAttrs[i], b.data);	
		if (aAttr.compare(bAttr) != 0) {
			return compare_attribute(aAttr, bAttr, schema->attrs[sortAttrs[i]]);
		}
		// At this point, a and b are equal. Check next sort attribute
	}

	// The records are exactly the same. Default to true.
	return true;
}

/**
 *	Attribute comparison function. Compare based on attribute type
 **/
bool compare_attribute(string aAttr, string bAttr, Attribute attr) {
	string attrType = attr.type;
	if (attrType.compare("integer") == 0 || attrType.compare("long") == 0) {
		return atol(aAttr.c_str()) < atol(bAttr.c_str());
	} else if (attrType.compare("float") == 0 || attrType.compare("double") == 0) {
		return atof(aAttr.c_str()) < atof(bAttr.c_str());
	} else {
		// Default string comparison
		return aAttr.compare(bAttr) < 0;
	}
}

/** 
 * Given an index and a string of data, find the corresponding attribute
 * Attributes are separated by commas
 */
string get_attr_from_data(int index, string data) {
	string tokenizedText(data);
	istringstream iss(tokenizedText);
	string token;

	int i = 0;
	while (getline(iss, token, ',')) {
		if (i == index) {
			return token;
		} 
		i++;
	}
	
	printf("get_attr_from_data: Unknown index %d found.\n", index);
	return "";
}
