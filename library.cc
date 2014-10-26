#include "library.h"

bool compare_records(const Record &a, const Record &b);
std::string get_attr_from_data(int index, std::string data);


void mk_runs(FILE *in_fp, FILE *out_fp, long run_length, Schema *schema)
{
	u_int32_t recordLength = get_expected_data_size(schema);	
  	vector<Record> records;
	char buffer[recordLength];

	// Read and sort records in in_fp
	while (fgets(buffer, recordLength, in_fp) != NULL) {
		Record record;
		record.schema = schema;
		record.data.assign(buffer);
   		records.push_back(record);

		// Sort and write back runs
		if (records.size() == run_length) {
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
			return aAttr.compare(bAttr) < 0;
		}

		// At this point, a and b are equal. Check next sort attribute
	}

	// The records are exactly the same. Default to true.
	return true;
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
