# -*- coding: utf-8 -*-
"""
Created on Mon Sep  7 15:28:00 2020

@author: Frank
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open("/Users/sgardziewicz/Documents/SparkCourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1',
                     errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())
# Create schema when reading u.data
schema = StructType([ \
    StructField("userID", IntegerType(), True), \
    StructField("movieID", IntegerType(), True), \
    StructField("rating", IntegerType(), True), \
    StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv(
    "file:///Users/sgardziewicz/Documents/SparkCourse/ml-100k/u.data")

moviesNames = spark.read.option("sep", "|").csv("file:///Users/sgardziewicz/Documents/SparkCourse/ml-100k/u.ITEM")
moviesNames = moviesNames.select("_c0", "_c1")
newColNames = ["movie_id", "movie_title"]
moviesNames = moviesNames.toDF(*newColNames)
moviesNames.show()

movieCounts = moviesDF.groupBy("movieID").count()

joinedDF = movieCounts.join(moviesNames, movieCounts.movieID == moviesNames.movie_id, "inner")
joinedDF = joinedDF.orderBy(func.desc("count"))
joinedDF.show()
print("^^")


# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]


lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
