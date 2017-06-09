#!/bin/sh

#mkdir output

#cleaning
hadoop fs -rm -r -f /user/cloudera/input
hadoop fs -rm -r -f /user/cloudera/output
#
#placing input data
hadoop fs -put input /user/cloudera/
#
#submitting JAR file
hadoop jar hadoop1 "partone/MyPair" /user/cloudera/input /user/cloudera/output

#output directory check
if [ -d "outputpair" ]; then
	rm -r -f outputpair
	mkdir outputpair
fi
#
#extracting output
hadoop fs -get /user/cloudera/output/* ./outputpair

