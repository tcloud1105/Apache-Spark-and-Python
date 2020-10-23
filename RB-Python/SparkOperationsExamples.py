# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Processing
-----------------------------------------------------------------------------
"""
import os
os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
os.curdir

#............................................................................
##   Loading Data From Files
#............................................................................

#Load the file. Lazy initialization
autoData = sc.textFile("auto-data.csv")
autoData.cache()
#Loads only now.
autoData.count()
autoData.first()
autoData.take(5)

for line in autoData.collect():
    print line

#............................................................................
##   Loading Data From a Collection
#............................................................................
collData=sc.parallelize([3,5,4,7,4])
collData.cache()
collData.count()
#............................................................................
##   Transformations
#............................................................................

#Map and create a new RDD
tsvData=autoData.map(lambda x : x.replace(",","\t"))
tsvData.take(5)

#Filter and create a new RDD
toyotaData=autoData.filter(lambda x: "toyota" in x)
toyotaData.count()

#FlatMap
words=toyotaData.flatMap(lambda line: line.split(","))
words.take(20)

#Distinct
for numbData in collData.distinct().collect():
    print numbData

#Set operations
words1 = sc.parallelize(["hello","war","peace","world"])
words2 = sc.parallelize(["war","peace","universe"])

for unions in words1.union(words2).distinct().collect():
    print unions
    
for intersects in words1.intersection(words2).collect():
    print intersects

#............................................................................
##   Actions
#............................................................................

#reduce
collData.reduce(lambda x,y: x+y)
#find the shortest line
autoData.reduce(lambda x,y: x if len(x) < len(y) else y)

#Aggregations

#Perform the same work as reduce
seqOp = (lambda x, y: (x+y))
combOp = (lambda x, y: (x+y))
collData.aggregate((0), seqOp, combOp)

#Do addition and multiplication at the same time.
#X now becomes a tuple for sequence
seqOp = (lambda x, y: (x[0]+y, x[1]*y))
#both X and Y are tuples
combOp = (lambda x, y: (x[0]+y[0], x[1]*y[1]))
collData.aggregate((0,1), seqOp, combOp)

#............................................................................
##   Functions in Spark
#............................................................................

#cleanse and transform an RDD
def cleanseRDD(autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    #convert doors to a number
    if attList[3] == "two" :
         attList[3]="2"
    else :
         attList[3]="4"
    #Convert Drive to uppercase
    attList[5] = attList[5].upper()
    return ",".join(attList)
    
cleanedData=autoData.map(cleanseRDD)
cleanedData.collect()

#Sue a function to perform reduce 
def getMPG( autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    if attList[9].isdigit() :
        return int(attList[9])
    else:
        return 0

#find average MPG-City for all cars    
autoData.reduce(lambda x,y : getMPG(x) + getMPG(y)) \
    / (autoData.count()-1)
    
#............................................................................
##   Working with Key/Value RDDs
#............................................................................

#create a KV RDD of auto Brand and Horsepower
cylData = autoData.map( lambda x: ( x.split(",")[0], \
    x.split(",")[7]))
cylData.take(5)
cylData.keys().collect()

#Remove header row
header = cylData.first()
cylHPData= cylData.filter(lambda line: line != header)

#Add a count 1 to each record and then reduce to find totals of HP and counts
brandValues=cylHPData.mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: (int(x[0]) + int(y[0]), \
    x[1] + y[1])) 
brandValues.collect()

#find average by dividing HP total by count total
brandValues.mapValues(lambda x: int(x[0])/int(x[1])). \
    collect()

#............................................................................
##   Advanced Spark : Accumulators & Broadcast Variables
#............................................................................

#function that splits the line as well as counts sedans and hatchbacks
#Speed optimization

    
#Initialize accumulator
sedanCount = sc.accumulator(0)
hatchbackCount =sc.accumulator(0)

#Set Broadcast variable
sedanText=sc.broadcast("sedan")
hatchbackText=sc.broadcast("hatchback")

def splitLines(line) :

    global sedanCount
    global hatchbackCount

    #Use broadcast variable to do comparison and set accumulator
    if sedanText.value in line:
        sedanCount +=1
    if hatchbackText.value in line:
        hatchbackCount +=1
        
    return line.split(",")


#do the map
splitData=autoData.map(splitLines)

#Make it execute the map (lazy execution)
splitData.count()
print sedanCount, hatchbackCount

#............................................................................
##   Advanced Spark : Partitions
#............................................................................
collData.getNumPartitions()

#Specify no. of partitions.
collData=sc.parallelize([3,5,4,7,4],2)
collData.cache()
collData.count()

collData.getNumPartitions()

#localhost:4040 shows the current spark instance