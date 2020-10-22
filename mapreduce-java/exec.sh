#!/bin/bash

export CLASSPATH=:./usr/lib/hadoop/hadoop-common.jar:/usr/lib/hadoop/client/h\
adoop-mapreduce-client-core.jar 

echo Input Data:
echo
hadoop fs -cat data/*

echo
echo Starting Map Reduce :
echo
hadoop fs -rm -r  output

hadoop jar exercise.jar exercise data output


echo 
echo Map Reduce Complete :
echo Output is
echo

hadoop fs -cat output/part-r-00000





