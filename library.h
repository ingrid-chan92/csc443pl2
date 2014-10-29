#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <sstream>

using namespace std;

/**
 * An attribute schema. You should probably modify
 * this to add your own fields.
 */
typedef struct {
  std::string name;
  int length;
  std::string type;
} Attribute;

/**
 * A record schema contains an array of attribute
 * schema `attrs`, as well as an array of sort-by 
 * attributes (represented as the indices of the 
 * `attrs` array).
 */
typedef struct {
  std::vector<Attribute> attrs;
  int nattrs;
  std::vector<int> sort_attrs;
  int n_sort_attrs;
} Schema;

/**
 * A record can defined as a struct with a pointer
 * to the schema and some data. 
 */
typedef struct {
  Schema* schema;
  std::string data;
} Record;

/**
 * Creates sorted runs of length `run_length` in
 * the `out_fp`.
 */
void mk_runs(FILE *in_fp, FILE *out_fp, long run_length, Schema *schema);

/**
 * Based on schema, get expected record data length
 */
int get_expected_data_size(Schema *schema);

/** 
 * Given an index and a string of data, find the corresponding attribute
 * Attributes are separated by commas
 */
string get_attr_from_data(int index, string data);

/**
 * The iterator helps you scan through a run.
 * you can add additional members as your wish
 */
class RunIterator {
  /**
   * Creates an interator using the `buf_size` to
   * scan through a run that starts at `start_pos`
   * with length `run_length`.
   */
  RunIterator(FILE *fp, long start_pos, long run_length, long buf_size,
              Schema *schema);

  /**
   * free memory
   */
  ~RunIterator();

  /**
   * reads the next record
   */
  Record* next();

  /**
   * return false if iterator reaches the end
   * of the run
   */
  bool has_next();
};

/**
 * Merge runs given by the `iterators`.
 * The number of `iterators` should be equal to the `num_runs`.
 * Write the merged runs to `out_fp` starting at position `start_pos`.
 * Cannot use more than `buf_size` of heap memory allocated to `buf`.
 */
void merge_runs(RunIterator* iterators[], int num_runs, FILE *out_fp,
                long start_pos, char *buf, long buf_size);

