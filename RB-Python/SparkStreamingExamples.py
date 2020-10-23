# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Streaming
-----------------------------------------------------------------------------
"""
import os
import sys
os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
os.curdir

# Configure the environment. Set this up to the directory where
# Spark is installed
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = 'C:/Spark/spark-1.6.0-bin-hadoop2.6'

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

#Add the following paths to the system path. Please check your installation
#to make sure that these zip files actually exist. The names might change
#as versions change.
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.9-src.zip"))

#Initiate Spark context. Once this is done all other applications can run
from pyspark import SparkContext
from pyspark import SparkConf

# Optionally configure Spark Settings
conf=SparkConf()
conf.set("spark.executor.memory", "1g")
conf.set("spark.cores.max", "2")

conf.setAppName("V2 Maestros")

## Initialize SparkContext. Run only once. Otherwise you get multiple 
#Context Error.
#for streaming, create a spark context with 2 threads.
sc = SparkContext('local[2]', conf=conf)

from pyspark.streaming import StreamingContext

#............................................................................
##   Streaming with simple data
#............................................................................

vc = [[-0.1, -0.2], [0.1, 0.3], [1.1, 1.5], [0.9, 0.9]]
dvc = [sc.parallelize(i, 1) for i in vc]
ssc = StreamingContext(sc, 2)
input_stream = ssc.queueStream(dvc)

def get_output(rdd):
    print(rdd.collect())
input_stream.foreachRDD(get_output)
ssc.start()
ssc.stop

#............................................................................
##   Streaming with TCP/IP data
#............................................................................

#Create streaming context with latency of 1
streamContext = StreamingContext(sc,3)

totalLines=0
lines = streamContext.socketTextStream("localhost", 9000)


#Word count within RDD    
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint(5)

#Count lines
totalLines=0
linesCount=0
def computeMetrics(rdd):
    global totalLines
    global linesCount
    linesCount=rdd.count()
    totalLines+=linesCount
    print rdd.collect()
    print "Lines in RDD :", linesCount," Total Lines:",totalLines

lines.foreachRDD(computeMetrics)

#Compute window metrics
def windowMetrics(rdd):
    print "Window RDD size:", rdd.count()
    
windowedRDD=lines.window(6,3)
windowedRDD.foreachRDD(windowMetrics)

streamContext.start()
streamContext.stop()
print "Overall lines :", totalLines

