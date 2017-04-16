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
	if(question == 1):
		question1(sys.argv)
	elif(question == 2):
		question2(sys.argv)
	elif(question == 3):
		question3(sys.argv)
	elif(question == 4):
		question4(sys.argv)
	elif(question == 5):
		question5(sys.argv)
	elif(question == 6):
		question6(sys.argv)


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

def question3(argv):
	print("* Q3: unique user names")
	for x in xrange (3, len(argv)):
		dirname = argv[x]
		usernames = []
		text_file = sc.textFile(logdir + dirname)
		sessions = text_file.filter(lambda line: "Starting Session" in line)
		usernames.extend(sessions.map(q3filter).collect())		
		print("+ " + dirname + ": " + str(list(set(usernames))))

def q3filter(u):
	if("Starting Session" in u):
		word_list = u.split()
		username = str(word_list[-1])[:-1]
		return username

def question4(argv):
	print("* Q4: sessions per user")
	for x in xrange (3, len(argv)):
		counts = []
		dirname = argv[x]
		text_file = sc.textFile(logdir + dirname)
		sessions = text_file.filter(lambda line: "Starting Session" in line)
		counts.extend(sessions.map(q3filter) \
					.map(lambda name: (name,1)) \
					.reduceByKey(lambda a,b: a + b).collect())
		print("+ " + dirname + ": " + str(counts))

def question5(argv):
	print("* Q5: number of errors")
	for x in xrange (3, len(argv)):
		dirname = argv[x]
		text_file = sc.textFile(logdir + dirname)
		errors = text_file.filter(q5containserror).count()
		print("+ " + dirname + ": " + str(errors))

def q5containserror(m):
	pattern = re.compile('error', re.IGNORECASE)
	return pattern.search(m)

def question6(argv):
	print("* Q6: 5 most frequent error messages")
	for x in xrange (3, len(argv)):
		dirname = argv[x]
		text_file = sc.textFile(logdir + dirname)
		errors = text_file.filter(q5containserror) \
			.map(extractMessage) \
			.reduceByKey(lambda a,b: a + b)
		print("+ " + dirname + ": " + str(errors.collect()))

def extractMessage(line):
	index = find_nth(line, ":",3)
	return line[index+1:]

def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

if __name__ == "__main__":
	main(sys.argv)

