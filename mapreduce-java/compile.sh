#!/bin/bash

javac -classpath /usr/lib/hadoop/hadoop-common.jar:/usr/lib/hadoop/client/h\
adoop-mapreduce-client-core.jar  exercise.java

jar cvf exercise.jar exercise*.class



