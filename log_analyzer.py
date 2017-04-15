#!/usr/bin/env python
"""log_analyzer.py, by Simon and Richard
"""

import sys
import os
from os import walk
from os.path import isfile, join
from pyspark import SparkContext, SparkConf

logdir = os.getcwd() + "/logs/"
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

def main(argv):
	checkvalid(sys.argv)
	if(int(sys.argv[2]) == 1):
		question1(sys.argv)


def checkvalid(argv):
	r = range(1,10)
	if(not(argv[1] == "-q") or not(int(argv[2]) in r)):
		print "command malformed! expecting -q [1-9]"
		sys.exit()
	for x in xrange (3, len(argv)):
		if(not(os.path.isdir(logdir + argv[x]))):
			print "argument not a dir!"	
			sys.exit()
def question1(argv):
	print("* Q1: line counts")
	count = 0
	for x in xrange (3, len(argv)):
		count = 0
		dirname = argv[x]
		for file in os.listdir(logdir + dirname):
			text_file = sc.textFile(logdir + dirname + "/" + file)
			count += text_file.count()
		print("+ " + dirname + ": " + str(count))


if __name__ == "__main__":
	main(sys.argv)


