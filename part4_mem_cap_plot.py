#!/usr/bin/env python

import json
import data_generator
import os
import re
import subprocess
import sys
import matplotlib.pyplot as plt
from time import sleep

data_dir = "../data/context_switch"

def cmd(command):
    '''Runs the given shell command and returns its standard output as a string.
    Throws an exception if the command returns anything other than 0.'''
    #p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #print "waiting for process with PID %d" % p.pid
    #ret = os.waitpid(p.pid, 0)[1]
    #if ret != 0:
        #print p.stdout.read()
        #raise Exception('Command [%s] failed with status %d\n:%s' % (command, ret, p.stderr.read()))
    #return p.stdout.read()
    return subprocess.check_output(command, universal_newlines=True)


if __name__ == "__main__":

    # Build the program.
	cmd(['make'])

	x, y = [], []
	data_generator.generate_data(json.load(open('testSchema')), 'inBigCsv', 4000); #ALTER THIS TO DO 4GB FILE

	for memsize in range(9,13):		#ALTER THIS TO (10,18)
		mem_capacity = 2**memsize
		output = cmd(['./msort', 'testSchema', 'inBigCsv', 'outCsv', str(mem_capacity), str(16), 'start_year,cgpa'])		
		time = output.split()
		seconds = time[-2];
		x.append(memsize);
		y.append(float(seconds))
		sleep(5)

    #make sure that the threshold line draw *after* the intervals
	plt.plot(x, y, "-bd")
	plt.savefig("msort_memcap_graph")
