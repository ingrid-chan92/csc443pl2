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

	for k in range(2, 12):

		print 'Running Sort with k=' + str(k)
		output = cmd(['./msort', 'testSchema', 'baseCsv', 'outCsv', str(16384), str(k), 'start_year,cgpa'])		
		time = output.split()
		seconds = time[-2];
		print "Time taken : " + str(seconds)
		x.append(k);
		y.append(float(seconds))

		sleep(1)

	plt.plot(x, y, "-bd")
	plt.xlabel('k')
	plt.ylabel('Delay(ms)')
	plt.savefig("msort_k_graph")

