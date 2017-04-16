# LogAnal498
# Richard Grand'Maison and Simon Houle

We are using files on disk for this assignment.
Depending on your hadoop configuration, you might have to change hadoop-2.7.3/etc/hadoop/core-site.xml to remove the default value under fs.defaultFS (that way file system will be used by default and not hdfs)
http://stackoverflow.com/questions/27299923/how-to-load-local-file-in-sc-textfile-instead-of-hdfs

To be able to run this program, you have have set up your environment according to the hadoop and spark tutorials, without having configured hdfs or yarn, and including the configuration to run spark as python standalone programs

To execute the program, open a terminal and cd to the location of this readme file. The appropriate syntax is: ./log_analyzer -q <dir1> <dir2>, where:
	X is a digit between 1 and 9, inclusively
	<dir1> and <dir2> are directories located under ./logs
	
