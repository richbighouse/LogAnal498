#!/usr/bin/env python
"""log_analyzer.py, by Simon and Richard
"""

import sys
import os
from os import walk
from os.path import isfile, join
from pyspark import SparkContext, SparkConf

logdir = os.getcwd() + "/logs/"

def main(argv):
	checkvalid(sys.argv)
	print(logdir)

def checkvalid(argv):
	r = range(1,10)
	if(not(argv[1] == "-q") or not(int(argv[2]) in r)):
		print "command malformed! expecting -q [1-9]"
		sys.exit()	

if __name__ == "__main__":
	main(sys.argv)


