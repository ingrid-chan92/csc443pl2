#!/usr/bin/env python

import os
import re
import subprocess
import sys
import matplotlib.pyplot as plt
import json
import data_generator
from time import sleep

def cmd(command):
    '''Runs the given shell command and returns its standard output as a string.
    Throws an exception if the command returns anything other than 0.'''
    return subprocess.check_output(command, universal_newlines=True)


if __name__ == "__main__":

    # Build the program.
	cmd(['make'])

    # Run the program and create the plot.
	graph_value_msort_bad = [];
	graph_value_msort_good = [];
	graph_value_bsort = [];
	x=[]

	# Get 500MB file
	base = open('baseCsv', 'r')

	for i in range(1, 10):
		
		filename = 'data'+str(i)
		f = open(filename, "w")
		for j in range(0, i):
			f.write(base.read())
			base.seek(0)
		f.close()

		x.append(i * 100000)

		output = cmd(['./msort', 'testSchema', filename, 'outCsv' , str(2048), str(8), 'cgpa'])
		time = output.split()
		millisec = time[-2];
		print "Time taken : " + str(millisec)
		graph_value_msort_bad.append(float(millisec))
		sleep(1)

		output = cmd(['./msort', 'testSchema', filename, 'outCsv' , str(16384), str(4), 'cgpa'])
		time = output.split()
		millisec = time[-2];
		print "Time taken : " + str(millisec)
		graph_value_msort_good.append(float(millisec))
		sleep(1)

		output = cmd(['./bsort', 'testSchema', filename, 'outCsv', 'cgpa'])
		time = output.splitlines()
		millisec = (time[-1].split())[-1]
		print "Time taken : " + str(millisec)
		graph_value_bsort.append((float(millisec)))
		sleep(1)

		os.remove(filename)

    #print graph_value
	print len(x)
	print len(graph_value_msort_bad)

	plt.plot(x, graph_value_msort_good, 'bd-', label="Good msort")
	plt.plot(x, graph_value_msort_bad, 'rd-', label="Bad msort")
	plt.plot(x, graph_value_bsort, 'gd-', label="bsort")
	plt.legend(prop={"size": 10})
	plt.xlabel('Number of  tuples')
	plt.ylabel('Delay(ms)')
	plt.title('Experiment_5_Comparison')
	plt.savefig("Experiment_5_Comparison")

	base.close



