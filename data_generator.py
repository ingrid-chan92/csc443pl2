import json
import sys
import random

'''
You should implement this script to generate test data for your
merge sort program.

The schema definition should be separate from the data generation
code. See example schema file `schema_example.json`.
'''

def generate_data(schema, out_file, nrecords):
  '''
  Generate data according to the `schema` given,
  and write to `out_file`.
  `schema` is an list of dictionaries in the form of:
    [ 
      {
        'name' : <attribute_name>, 
        'length' : <fixed_length>,
        ...
      },
      ...
    ]
  `out_file` is the name of the output file.
  The output file must be in csv format, with a new line
  character at the end of every record.
  '''
  print "Generating %d records" % nrecords
  #name:, length: type: distribution:

  letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

  with open(out_file, 'w') as f:
    for _ in xrange(nrecords):
        row = []
        
        for entry in range(len(schema)):    
          if schema[entry]['type'] == 'string':
            row.append(''.join([random.choice(letters) for j in range(schema[entry]['length'])]))
            
          elif (schema[entry]['type'] == 'integer'):
            dist = schema[entry]['distribution']
            row.append("%d" % random.randint(dist['min'], dist['max']))
            
          elif (schema[entry]['type'] == 'float'):
            dist = schema[entry]['distribution']
            val = random.gauss(dist['mu'], dist['sigma'])
            while (val < dist['min'] or val > dist['max']):
              val = random.gauss(dist['mu'], dist['sigma'])
            row.append( "%.2f" % val)

        print >>f, ",".join(row)

  print "Generated %d tuples" % (nrecords)


  

if __name__ == '__main__':
  import sys, json
  if not len(sys.argv) == 4:
    print "data_generator.py <schema json file> <output csv file> <# of records>"
    sys.exit(2)

  schema = json.load(open(sys.argv[1]))
  output = sys.argv[2]
  nrecords = int(sys.argv[3])

  generate_data(schema, output, nrecords)

