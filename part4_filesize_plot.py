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
	for fsize in range(1,9):	#ALTER THIS TO DO 4GB FILE
		filesize = fsize * 1000;	
		data_generator.generate_data(json.load(open('testSchema')), 'inBigCsv', filesize)
		output = cmd(['./msort', 'testSchema', 'inBigCsv', 'outCsv', str(1048), str(16), 'start_year,cgpa'])		
		time = output.split()
		seconds = time[-2];
		x.append(filesize)
		y.append(float(seconds))
		sleep(5)

	plt.plot(x, y, "-bd")
	plt.savefig("msort_filesize_graph")

