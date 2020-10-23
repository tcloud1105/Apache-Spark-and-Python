# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Machine Learning - Linear Regression

Problem Statement
*****************
The input data set contains data about details of various car 
models. Based on the information provided, the goal is to come up 
with a model to predict Miles-per-gallon of a given model.

Techniques Used:

1. Linear Regression ( multi-variate)
2. Data Imputation - replacing non-numeric data with numeric ones
3. Variable Reduction - picking up only relevant features

-----------------------------------------------------------------------------
"""
import os
os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
os.curdir

#Load the CSV file into a RDD
autoData = sc.textFile("auto-miles-per-gallon.csv")
autoData.cache()

#Remove the first line (contains headers)
dataLines = autoData.filter(lambda x: "CYLINDERS" not in x)
dataLines.count()

#Convert the RDD into a Dense Vector. As a part of this exercise
#   1. Remove unwanted columns
#   2. Change non-numeric ( values=? ) to numeric

import math
from pyspark.mllib.linalg import Vectors

#Use default for average HP
avgHP =sc.broadcast(80.0)

def transformToNumeric( inputStr) :
    global avgHP
    attList=inputStr.split(",")
    
    #Replace ? values with a normal value
    hpValue = attList[3]
    if hpValue == "?":
        hpValue=avgHP.value
       
    #Filter out columns not wanted at this stage
    values= Vectors.dense([ float(attList[0]), \
                     float(attList[1]),  \
                     hpValue,    \
                     float(attList[5]),  \
                     float(attList[6])
                     ])
    return values

#Keep only MPG, CYLINDERS, HP,ACCELERATION and MODELYEAR
autoVectors = dataLines.map(transformToNumeric)
autoVectors.collect()

#Perform statistical Analysis
from pyspark.mllib.stat import Statistics
autoStats=Statistics.colStats(autoVectors)
autoStats.mean()
autoStats.variance()
autoStats.min()
autoStats.max()

Statistics.corr(autoVectors)

#Transform to a Data Frame for input to Machine Learing
#Drop columns that are not required (low correlation)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def transformToLabeledPoint(inStr) :
    lp = ( float(inStr[0]), Vectors.dense([inStr[1],inStr[2],inStr[4]]))
    return lp
    
autoLp = autoVectors.map(transformToLabeledPoint)
autoDF = sqlContext.createDataFrame(autoLp,["label", "features"])
autoDF.select("label","features").show(10)

#Find correlations
numFeatures = autoDF.take(1)[0].features.size
labelRDD = autoDF.map(lambda lp: float(lp.label))
for i in range(numFeatures):
    featureRDD = autoDF.map(lambda lp: lp.features[i])
    corr = Statistics.corr(labelRDD, featureRDD, 'pearson')
    print('%d\t%g' % (i, corr))

#Split into training and testing data
(trainingData, testData) = autoDF.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()

#Build the model on training data
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(maxIter=10)
lrModel = lr.fit(trainingData)

print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))

#Predict on the test data
predictions = lrModel.transform(testData)
predictions.select("prediction","label","features").show()

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="label",metricName="r2")
evaluator.evaluate(predictions)
