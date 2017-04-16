#!/usr/bin/env python
"""log_analyzer.py, by Simon and Richard
"""

import sys
import os
import re
import shutil
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
	elif(question == 7):
		question7(sys.argv)
	elif(question == 8):
		question8(sys.argv)
	elif(question == 9):
		question9(sys.argv)

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
		text_file = sc.textFile(logdir + dirname)
		sessions = text_file.filter(lambda line: "Starting Session" in line)
		usernames = sessions.map(q3filter) \
			.map(lambda user: (user, dirname)) \
			.reduceByKey(lambda a,b: a).keys()	
		print("+ " + dirname + ": " + str(usernames.collect()))

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

def question6(argv):
	print("* Q6: 5 most frequent error messages")
	for x in xrange (3, len(argv)):
		dirname = argv[x]
		text_file = sc.textFile(logdir + dirname)
		errors = text_file.filter(q5containserror) \
			.map(extractSourceAndMessage) \
			.map(lambda line: (line,1)) \
			.reduceByKey(lambda a,b: a + b) \
			.takeOrdered(5, key = lambda x: -x[1])
		print("+ " + dirname + ": ")
		for e in errors:
			print("\t - ("+str(e[1])+", '"+e[0]+"')")

def question7(argv):
	print("* Q7: users who started a session on both hosts, i.e, on exactly 2 hosts")
	dirname1 = argv[3]
	dirname2 = argv[4]
	text_file1 = sc.textFile(logdir + dirname1)
	text_file2 = sc.textFile(logdir + dirname2)
	sessions1 = text_file1.filter(lambda line: "Starting Session" in line)
	sessions2 = text_file2.filter(lambda line: "Starting Session" in line)
	usernames1 = sessions1.map(q3filter) \
			.map(lambda user: (user, dirname1)) \
			.reduceByKey(lambda a,b: a).keys()
	usernames2 = sessions2.map(q3filter) \
			.map(lambda user: (user, dirname2)) \
			.reduceByKey(lambda a,b: a).keys()
	intersection = usernames1.intersection(usernames2).collect()			
	print("+ : " + str(intersection))

def question8(argv):
	print("* Q8: users who started a session on exactly one host, with host name.")
	dirname1 = argv[3]
	dirname2 = argv[4]
	text_file1 = sc.textFile(logdir + dirname1)
	text_file2 = sc.textFile(logdir + dirname2)
	sessions1 = text_file1.filter(lambda line: "Starting Session" in line)
	sessions2 = text_file2.filter(lambda line: "Starting Session" in line)
	usernames1 = sessions1.map(q3filter) \
			.map(lambda user: (user, dirname1)) \
			.reduceByKey(lambda a,b: a)
	usernames2 = sessions2.map(q3filter) \
			.map(lambda user: (user, dirname2)) \
			.reduceByKey(lambda a,b: a)
	sub1 = usernames1.subtractByKey(usernames2)
	sub2 = usernames2.subtractByKey(usernames1)
	print("+ : " + str(sub1.union(sub2).collect()))

def question9(argv):
	print("* Q9: Anonymization")
	for x in xrange (3, len(argv)):
		dirname = argv[x]
		text_file = sc.textFile(logdir + dirname)
		sessions = text_file.filter(lambda line: "Starting Session" in line)
		usernames = sessions.map(q3filter) \
				.map(lambda user: (user, 1)) \
				.reduceByKey(lambda a,b: a).sortByKey(True) \
				.keys().collect()
		mappings = []
		for i in range(0,len(usernames)):
			tup = (usernames[i], "user-"+str(i))
			mappings.append(tup)
		
		print("+ " + dirname + ": ")
		print(". User name mapping: "+str(mappings))
		filename = dirname + "-anonymized-10"
		print(". Anonymized files: " + filename)
		if(os.path.exists(logdir + filename)):
			shutil.rmtree(logdir + filename)
		anon = text_file.map(lambda u: q9replace(u,mappings)).saveAsTextFile(logdir + filename)
		
def q9replace(u, mappings):
	for i in range(0,len(mappings)):
		key, val = mappings[i]
		if(key in u.encode('utf-8')):
			new = u.replace(key, val)
			return new
	return u
		

def q3filter(u):
	if("Starting Session" in u):
		word_list = u.split()
		username = str(word_list[-1])[:-1]
		return username

def q5containserror(m):
	pattern = re.compile('error', re.IGNORECASE)
	return pattern.search(m)
		
def extractMessage(line):
	index = find_nth(line, ":",3)
	return str(line[index+1:])

def extractSourceAndMessage(line):
	index = find_nth(line, " ",4)
	return str(line[index+1:])

def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

if __name__ == "__main__":
	main(sys.argv)

