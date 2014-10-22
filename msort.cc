#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <vector>

#include "library.h"
#include "json/json.h"

using namespace std;

int main(int argc, const char* argv[]) {

  Attribute attribute;
  Schema schema;
  Record record;
  std::vector<Record> records;
  FILE *input_file;

  if (argc < 7) {
    cout << "ERROR: invalid input parameters!" << endl;
    cout << "Please enter <schema_file> <input_file> <output_file> <mem_capacity> <k> <sorting_attributes>" << endl;
    exit(1);
  }
  string schema_file(argv[1]);

  // Parse the schema JSON file
  Json::Value schema_val;
  Json::Reader json_reader;
  // Support for std::string argument is added in C++11
  // so you don't have to use .c_str() if you are on that.
  ifstream schema_file_istream(schema_file.c_str(), ifstream::binary);
  bool successful = json_reader.parse(schema_file_istream, schema_val, false);
  if (!successful) {
    cout << "ERROR: " << json_reader.getFormatedErrorMessages() << endl;
    exit(1);
  }

  // Print out the schema
  int schema_value_len = 0;
  int comma_len = 0;

  for (int i = 0; i < schema_val.size(); ++i) {
    attribute.name.assign(schema_val[i].get("name", "UTF-8" ).asString());
    attribute.length = schema_val[i].get("length", "UTF-8").asInt();
    attribute.type.assign(schema_val[i].get("type", "UTF-8").asString());
    schema.attrs.push_back(attribute);
    schema.nattrs = i;
    
    schema_value_len += attribute.length;
    comma_len++;
  }
  cout << schema_value_len <<endl;

  /*check value in schema*/
  for (int i = 0; i < schema.attrs.size(); i++) {
    cout << "{schema_name : " << schema.attrs[i].name << ", schema_len : " << schema.attrs[i].length  << ", schema_type : " << schema.attrs[i].type << "}" << endl;
  }

  input_file = fopen (argv[2] , "r");
  if (input_file == NULL) 
		perror ("Error opening input file");

  record.schema = &schema;
  char buffer[schema_value_len+comma_len+1]; //one for new-line
  
  while ( ! feof (input_file) ) {
     if (fgets(buffer, schema_value_len+comma_len+1, input_file) == NULL){
			break;
		}

    record.data.assign(buffer);
    records.push_back(record);
  }

  /*check value in record*/
  for (int i = 0; i < records.size(); i++) {
    cout << "record_data : " << records[i].data;
  }

  // Do the sort
  // Your implementation
  

  return 0;
}
