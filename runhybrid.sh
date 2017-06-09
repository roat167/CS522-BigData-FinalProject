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
hadoop jar hadoop1 "partone/Hybrid" /user/cloudera/input /user/cloudera/output

#outputhybrid directory check
if [ -d "outputhybrid" ]; then
	rm -r -f outputhybrid
fi
#
mkdir outputhybrid
#extracting output
hadoop fs -get /user/cloudera/output/* ./outputhybrid

