#!/usr/bin/env python
"""log_analyzer.py, by Simon and Richard
"""

import sys
import os
import re
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
	question = int(sys.argv[2])
	select(question)
	if(question == 1):
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

def select(x):
	if(x == 1):
		question1(sys.argv)
	elif(x == 2):
		question2(sys.argv)

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

def question2(argv):
	print("* Q2: sessions for user 'achille'")
	count = 0
	for x in xrange (3, len(argv)):
		count = 0
		dirname = argv[x]
		for file in os.listdir(logdir + dirname):
			text_file = sc.textFile(logdir + dirname + "/" + file)
			count += text_file.filter(lambda line: ("Starting Session" in line and "achille" in line)).count()
		print("+ " + dirname + ": " + str(count))

if __name__ == "__main__":
	main(sys.argv)


