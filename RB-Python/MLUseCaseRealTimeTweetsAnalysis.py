# -*- coding: utf-8 -*-
"""
Created on Mon Feb 29 13:27:52 2016

@author: kumaran
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
#............................................................................
##   Building and saving the model
#............................................................................

tweetData = sc.textFile("movietweets.csv")
tweetData.collect()

tweetText=tweetData.map(lambda line: line.split(",")[1])
tweetText.collect()

from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

hashingTF = HashingTF()
tf = hashingTF.transform(tweetText)
tf.cache()
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)
tfidf.cache()
tfidf.count()

xformedData=tweetData.zip(tfidf)
xformedData.cache()
xformedData.collect()

from pyspark.mllib.regression import LabeledPoint
def convertToLabeledPoint(inVal) :
    origAttr=inVal[0].split(",")
    sentiment = 0.0 if origAttr[0] == "positive" else 1.0
    return LabeledPoint(sentiment, inVal[1])

tweetLp=xformedData.map(convertToLabeledPoint)
tweetLp.cache()
tweetLp.collect()

from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
model = NaiveBayes.train(tweetLp, 1.0)
predictionAndLabel = tweetLp.map(lambda p: \
    (float(model.predict(p.features)), float(p.label)))
predictionAndLabel.collect()

#Form confusion matrix
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
predDF = sqlContext.createDataFrame(predictionAndLabel.collect(), \
                ["prediction","label"])
predDF.groupBy("label","prediction").count().show()

#save the model
#model.save(sc,"TweetsSentimentModel")
import pickle
with open('tweetsSentiModel', 'wb') as f:
    pickle.dump(model, f)

#............................................................................
##   Getting tweets in real time and making predictions
#............................................................................

import pickle
from pyspark.mllib.classification import  NaiveBayesModel

with open('tweetsSentiModel', 'rb') as f:
    loadedModel = pickle.load(f)
    
from pyspark.streaming import StreamingContext
streamContext = StreamingContext(sc,1)

tweets = streamContext.socketTextStream("localhost", 9000)

bc_model = sc.broadcast(loadedModel)

from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF


def predictSentiment(tweetText):
    nbModel=bc_model.value
    
    hashingTF = HashingTF()
    tf = hashingTF.transform(tweetText)
    tf.cache()
    idf = IDF(minDocFreq=2).fit(tf)
    tfidf = idf.transform(tf)
    tfidf.cache()
    prediction=nbModel.predict(tfidf)
    print "Predictions for this window :"
    for i in range(0,prediction.count()):
        print prediction.collect()[i], tweetText.collect()[i]

tweets.foreachRDD(predictSentiment)

streamContext.start()
streamContext.stop()

