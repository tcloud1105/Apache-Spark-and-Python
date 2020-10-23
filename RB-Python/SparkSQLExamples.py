# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark SQL
-----------------------------------------------------------------------------
"""
import os
os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
os.curdir

#Create a SQL Context from Spark context
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#............................................................................
##   Working with Data Frames
#............................................................................

#Create a data frame from a JSON file
empDf = sqlContext.read.json("customerData.json")
empDf.show()
empDf.printSchema()

#Do SQL queries
empDf.select("name").show()
empDf.filter(empDf["age"] == 40).show()
empDf.groupBy("gender").count().show()
empDf.groupBy("deptid").\
    agg({"salary": "avg", "age": "max"}).show()

#create a data frame from a list
 deptList = [{'name': 'Sales', 'id': "100"},\
     { 'name':'Engineering','id':"200" }]
 deptDf = sqlContext.createDataFrame(deptList)
 deptDf.show()
 
#join the data frames
 empDf.join(deptDf, empDf.deptid == deptDf.id).show()
 
#cascading operations
empDf.filter(empDf["age"] >30).join(deptDf, \
        empDf.deptid == deptDf.id).\
        groupBy("deptid").\
        agg({"salary": "avg", "age": "max"}).show()

#register a data frame as table and run SQL statements against it
empDf.registerTempTable("employees")
sqlContext.sql("select * from employees where salary > 4000").show()

#to pandas data frame
empPands = empDf.toPandas()
for index, row in empPands.iterrows():
    print row["salary"]

#............................................................................
##   Working with Databases
#............................................................................
#Make sure that the spark classpaths are set appropriately in the 
#spark-defaults.conf file to include the driver files
    
demoDf = sqlContext.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/demo",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "demotable",
    user="root",
    password="").load()
    
demoDf.show()

#............................................................................
##   Creating data frames from RDD
#............................................................................

from pyspark.sql import Row
lines = sc.textFile("auto-data.csv")
#remove the first line
datalines = lines.filter(lambda x: "FUELTYPE" not in x)
datalines.count()

parts = datalines.map(lambda l: l.split(","))
autoMap = parts.map(lambda p: Row(make=p[0],\
         body=p[4], hp=int(p[7])))
autoMap.collect()

# Infer the schema, and register the DataFrame as a table.
autoDf = sqlContext.createDataFrame(autoMap)
autoDf.registerTempTable("autos")
sqlContext.sql("select * from autos where hp > 200").show()



