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
	base = open('baseCsv', 'r')
	
	# Build file by appending 500MB file 8 times
	f = open('inBigCsv', 'w')
	for i in range(0,8):
		f.write(base.read())
		base.seek(0)		
	f.close()	

	base.close()

	print 'Produced final input file'
	sleep(5)

	for j in range(3,8):
		mem_capacity = j*50
		
		print 'Running Sort with mem_capacity=' + str(mem_capacity)
		output = cmd(['./msort', 'testSchema', 'inBigCsv', 'outCsv', str(mem_capacity), str(3), 'start_year,cgpa'])		
		time = output.split()
		seconds = time[-2];
		x.append(j);
		y.append(float(seconds))

		sleep(10)

	plt.plot(x, y, "-bd")
	plt.savefig("msort_memcap_graph")

	os.remove('inBigCsv')	
