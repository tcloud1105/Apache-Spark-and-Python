# -*- coding: utf-8 -*-
"""
   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Machine Learning - Decision Trees

Problem Statement
*****************
The input data is the iris dataset. It contains recordings of 
information about flower samples. For each sample, the petal and 
sepal length and width are recorded along with the type of the 
flower. We need to use this dataset to build a decision tree 
model that can predict the type of flower based on the petal 
and sepal information.

## Techniques Used

1. Decision Trees 
2. Training and Testing
3. Confusion Matrix

-----------------------------------------------------------------------------
"""
import os
os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
os.curdir

#Load the CSV file into a RDD
irisData = sc.textFile("iris.csv")
irisData.persist()

#Remove the first line (contains headers)
dataLines = irisData.filter(lambda x: "Sepal" not in x)
dataLines.count()

#Convert the RDD into a Dense Vector. As a part of this exercise
#   1. Change labels to numeric ones

import math
from pyspark.mllib.linalg import Vectors

def transformToNumeric( inputStr) :
    attList=inputStr.split(",")
    
    #Set default to setosa
    irisValue=1.0
    if attList[4] == "versicolor":
        irisValue=2.0
    if attList[4] == "virginica":
        irisValue=3.0
       
    #Filter out columns not wanted at this stage
    values= Vectors.dense([ irisValue, \
                     float(attList[0]),  \
                     float(attList[1]),  \
                     float(attList[2]),  \
                     float(attList[3])  \
                     ])
    return values
    
#Change to a Vector
irisVectors = dataLines.map(transformToNumeric)
irisVectors.collect()

#Perform statistical Analysis
from pyspark.mllib.stat import Statistics
irisStats=Statistics.colStats(irisVectors)
irisStats.mean()
irisStats.variance()
irisStats.min()
irisStats.max()

Statistics.corr(irisVectors)

#Transform to a Data Frame for input to Machine Learing
#Drop columns that are not required (low correlation)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def transformToLabeledPoint(inStr) :
    attList=inStr.split(",")
    lp = ( attList[4], Vectors.dense([attList[0],attList[2],attList[3]]))
    return lp
    
irisLp = dataLines.map(transformToLabeledPoint)
irisDF = sqlContext.createDataFrame(irisLp,["label", "features"])
irisDF.select("label","features").show(10)

#Find correlations
numFeatures = irisDF.take(1)[0].features.size
labelRDD = irisDF.map(lambda lp: lp.label)
for i in range(numFeatures):
    featureRDD = irisDF.map(lambda lp: lp.features[i])
    corr = Statistics.corr(labelRDD, featureRDD, 'pearson')
    print('%d\t%g' % (i, corr))
    
#Indexing needed as pre-req for Decision Trees
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(irisDF)
td = si_model.transform(irisDF)
td.collect()

#Split into training and testing data
(trainingData, testData) = td.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()
testData.collect()

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


#Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=2, labelCol="indexed")
dtModel = dtClassifer.fit(trainingData)

dtModel.numNodes
dtModel.depth

#Predict on the test data
predictions = dtModel.transform(trainingData)
predictions.select("prediction","indexed","label","features").collect()
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="indexed",metricName="precision")
evaluator.evaluate(predictions)      

#Draw a confusion matrix
labelList=predictions.select("indexed","label").distinct().toPandas()
predictions.groupBy("indexed","prediction").count().show()

