#include <stdio.h>
#include <string.h>
#include "library.h"

bool compare_records(const Record &a, const Record &b);
std::string get_attr_from_data(int index, std::string data);
bool compare_attribute(string aAttr, string bAttr, Attribute attr);
Record *getMinimumRecord(Record *minRecs[], RunIterator* its[], int num_runs);


RunIterator::RunIterator(FILE *fp, long start_pos, long run_length, long buf_size, Schema *schema) {
	// Allocate structures
	inFile = fp;
	sch = schema;
	buff = (char *) malloc(buf_size);
	memset(buff, 0, buf_size);		

	// constants
	initBuffSize = buf_size;
	recordSize = get_expected_data_size(schema) - 1;	// Subtract last comma
	runLength = run_length;
	maxBuffSizeInBytes = buf_size / recordSize; // Number of records per buffer
	
	// Non-Constants	
	totalRunLeft = run_length;
	filePos = start_pos;	
	hasNext = true;

	readRunIntoBuffer();

	// Read in current record
	currRecord = new Record;
	currRecord->schema = sch;
	char dataBuf[recordSize];
	memcpy(dataBuf, buff + buffPtr, recordSize);
	currRecord->data = string(dataBuf);

	// Shift pointer to next potential record
	buffPtr += recordSize; 
}

RunIterator::~RunIterator() {
	free(buff);
}

Record* RunIterator::next() {	
	if (!hasNext) {
		printf("RunIterator: No more records left\n");
		throw;
	}

	/* At this point. One record exists*/
	Record *prevRecord = currRecord;

	if (buffPtr >= buffSize) {
		// Finished reading from buffer. Process next chunk of data		
		readRunIntoBuffer();	
	} 

	// Nothing left to read
	if (totalRunLeft <= 0 && buffSize <= 0) {
		hasNext = false;
		return prevRecord;
	}

	/* At the point. Buffer has data to read*/

	// Read next record from buffer
	currRecord = new Record;
	currRecord->schema = sch;
	char dataBuf[recordSize];
	memcpy(dataBuf, buff + buffPtr, recordSize);
	currRecord->data = string(dataBuf);

	// Shift pointer to next potential record
	buffPtr += recordSize; 

	return prevRecord;
	
}

bool RunIterator::has_next() {
	return hasNext;
}

void RunIterator::readRunIntoBuffer() {
	long expectedRecordAmount = min(maxBuffSizeInBytes, totalRunLeft);

	// Empty buffer and read data in
	memset(buff, 0, initBuffSize);
	fseek(inFile, filePos, SEEK_SET);
	long numRead = fread(buff, recordSize, expectedRecordAmount, inFile);	

	// Set stats
	buffSize = numRead * recordSize;
	filePos += buffSize;
	totalRunLeft -= numRead;
	buffPtr = 0;	

	// Case for an incomplete/end-of-file run
	if (feof(inFile)) {
		totalRunLeft = 0;
	}	
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

void merge_runs(RunIterator* iterators[], int num_runs, FILE *out_fp, long start_pos, char *buf, long buf_size) {
	Record *next;		
	long bufPtr = 0;	

	// Empty buffer and set out file to proper start position
	fseek(out_fp, start_pos, SEEK_SET);
	memset (buf, 0, buf_size);

	// Populate minRecs with minimum records per iterator
	Record *minRecs[num_runs];
	for (int i = 0; i < num_runs; i++) {
		minRecs[i] = NULL;
		if (iterators[i]->has_next()) {
			Record *next = iterators[i]->next();
			if (!(next->data).empty()) {
				// Only record non-blank data	
				minRecs[i] = next;
			}			
		} 
	}

	// Merge runs from each iterator into buf
	while ((next = getMinimumRecord(minRecs, iterators, num_runs)) != NULL) {
		int expectedDataSize = get_expected_data_size(next->schema) - 1;
		memcpy (buf+bufPtr, next->data.c_str(), expectedDataSize);
		bufPtr += expectedDataSize;		

		if ((buf_size - bufPtr) < expectedDataSize) {
			// buffer is full. Write to file
			fprintf(out_fp, "%s", buf);				
			memset (buf, 0, buf_size);			
			bufPtr = 0;
		}
	}  

	if (bufPtr > 0) {
		// Write remaining data in file
		fprintf(out_fp, "%s", buf);
	}
}

Record *getMinimumRecord(Record *minRecs[], RunIterator* its[], int num_runs) {
	Record *min = NULL;
	int minIndex = -1;

	// Find smallest record from the top of each RunIterator
	for (int i = 0; i < num_runs; i++) {
		Record *next = minRecs[i];
		if (next != NULL && (min == NULL || compare_records(*next, *min))) {
			min = next;
			minIndex = i;
		}		
	}

	if (min != NULL) {
		// Add next record to minRecs 
		if (its[minIndex]->has_next()) {
			minRecs[minIndex] = its[minIndex]->next();
		} else {
			minRecs[minIndex] = NULL;
		}
		return min;

	} else {
		// No more records left
		return NULL;
	}
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
