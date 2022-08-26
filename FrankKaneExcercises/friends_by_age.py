from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("file:///Users/sgardziewicz/Documents/SparkCourse/fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()
people.select(people.age, people.friends)
people_selected = people.groupBy("age").avg().sort("age")
people_selected.select("age", "avg(friends)").show()



spark.stop()

