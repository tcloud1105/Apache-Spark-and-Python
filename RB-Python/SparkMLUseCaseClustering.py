# -*- coding: utf-8 -*-
"""
   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Machine Learning - Clustering

The input data contains samples of cars and technical / price 
information about them. The goal of this problem is to group 
these cars into 4 clusters based on their attributes

## Techniques Used

1. K-Means Clustering
2. Centering and Scaling

-----------------------------------------------------------------------------
"""
import os
os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
os.curdir

#Load the CSV file into a RDD
autoData = sc.textFile("auto-data.csv")
autoData.cache()

#Remove the first line (contains headers)
firstLine = autoData.first()
dataLines = autoData.filter(lambda x: x != firstLine)
dataLines.count()

from pyspark.sql import SQLContext,Row
sqlContext = SQLContext(sc)

import math
from pyspark.mllib.linalg import Vectors

#Convert to Local Vector.
def transformToNumeric( inputStr) :
    attList=inputStr.split(",")

    doors = 1.0 if attList[3] =="two" else 2.0
    body = 1.0 if attList[4] == "sedan" else 2.0 
       
    #Filter out columns not wanted at this stage
    values= Vectors.dense([ doors, \
                     float(body),  \
                     float(attList[7]),  \
                     float(attList[8]),  \
                     float(attList[9])  \
                     ])
    return values

autoVector = dataLines.map(transformToNumeric)
autoVector.persist()
autoVector.collect()

#Centering and scaling. To perform this every value should be subtracted
#from that column's mean and divided by its Std. Deviation.

#Perform statistical Analysis and compute mean and Std.Dev for every column
from pyspark.mllib.stat import Statistics
autoStats=Statistics.colStats(autoVector)
colMeans=autoStats.mean()
colVariance=autoStats.variance()
colStdDev=map(lambda x: math.sqrt(x), colVariance)

#place the means and std.dev values in a broadcast variable
bcMeans=sc.broadcast(colMeans)
bcStdDev=sc.broadcast(colStdDev)

def centerAndScale(inVector) :
    global bcMeans
    global bcStdDev
    
    meanArray=bcMeans.value
    stdArray=bcStdDev.value
    
    valueArray=inVector.toArray()
    retArray=[]
    for i in range(valueArray.size):
        retArray.append( (valueArray[i] - meanArray[i]) /\
            stdArray[i] )
    return Vectors.dense(retArray)
    
csAuto = autoVector.map(centerAndScale)
csAuto.collect()

#Create a Spark Data Frame
autoRows=csAuto.map( lambda f:Row(features=f))
autoDf = sqlContext.createDataFrame(autoRows)

autoDf.select("features").show(10)

from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(autoDf)
predictions = model.transform(autoDf)
predictions.collect()

#Plot the results in a scatter plot
import pandas as pd

def unstripData(instr) :
    return ( instr["prediction"], instr["features"][0], \
        instr["features"][1],instr["features"][2],instr["features"][3])
    
unstripped=predictions.map(unstripData)
predList=unstripped.collect()
predPd = pd.DataFrame(predList)

import matplotlib.pylab as plt
plt.cla()
plt.scatter(predPd[3],predPd[4], c=predPd[0])
