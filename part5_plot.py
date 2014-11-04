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

    for i in range(1, 4):
        filesize = i * 1000;	
	data_generator.generate_data(json.load(open('schema')), 'data', filesize)
        output = cmd(['./msort', 'schema', 'data', 'res' , str(2048), str(2), 'cgpa'])
        time = output.split()
        seconds = time[-2];
        graph_value_msort_bad.append(float(seconds))
        sleep(3)

        output = cmd(['./msort', 'schema', 'data', 'res' , str(2048), str(10), 'cgpa'])
        time = output.split()
        seconds = time[-2];
        graph_value_msort_good.append(float(seconds))
        sleep(3)

        output = cmd(['./bsort', 'schema', 'data', 'res', 'cgpa'])
        time = output.splitlines()
        millisec = time[-1].split()
        graph_value_bsort.append((float(millisec[-1])/1000))
        sleep(3)

    #print graph_value
    x =range(1000,4000,1000)
    print len(x)
    print len(graph_value_msort_bad)

    plt.plot(x, graph_value_msort_good, 'bd-', label="Good msort")
    plt.plot(x, graph_value_msort_bad, 'rd-', label="Bad msort")
    plt.plot(x, graph_value_bsort, 'gd-', label="bsort")
    plt.legend(prop={"size": 10})
    plt.xlabel('Number of  tuples')
    plt.ylabel('Delay(s)')
    plt.title('Experiment 5 Comparison')
    plt.savefig("Experiment 5 Comparison")
    plt.show()
    plt.close()


