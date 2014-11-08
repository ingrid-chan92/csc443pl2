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

	# Get 500MB file
	base = open('baseCsv', 'r')

	x, y = [], []
	for fsize in range(1,10):
		# create file by appending bigCsv to new file
		filename = 'inCsv'+str(fsize)
		f = open(filename, "w")
		for i in range(0, fsize):
			f.write(base.read())
			base.seek(0)
		f.close()
		
		print 'Produced input file'
		sleep(1)

		print 'Running Sort with filesize = ' + str(fsize * 3) + 'MB'
		output = cmd(['./msort', 'testSchema', filename, 'outCsv', str(16384), str(4), 'start_year,cgpa'])		
		time = output.split()
		seconds = time[-2];
		print "Time taken : " + str(seconds)
		x.append(fsize * 100000)
		y.append(float(seconds))
		sleep(1)

		os.remove(filename)

	plt.plot(x, y, "-bd")
	plt.xlabel('Number of  tuples')
	plt.ylabel('Delay(ms)')
	plt.savefig("msort_filesize_graph")

	base.close

