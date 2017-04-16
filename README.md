# LogAnal498

We are using files on disk for this assignment.
Depending on your hadoop configuration, you might have to change hadoop-2.7.3/etc/hadoop/core-site.xml to remove 
the default value under fs.defaultFS (that way file system will be used by default and not hdfs)
http://stackoverflow.com/questions/27299923/how-to-load-local-file-in-sc-textfile-instead-of-hdfs
