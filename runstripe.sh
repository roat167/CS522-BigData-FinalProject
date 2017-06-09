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
hadoop jar hadoop1 "partone/MyStripe" /user/cloudera/input /user/cloudera/output
#

#output directory check
if [ -d "outputstripe" ]; then
	rm -r -f outputstripe
	mkdir outputstripe
fi
#
#extracting output
hadoop fs -get /user/cloudera/output/* ./outputstripe

