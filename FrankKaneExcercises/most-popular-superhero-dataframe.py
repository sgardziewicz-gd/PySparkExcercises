from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/sgardziewicz/Documents/SparkCourse/Marvel+Names")

lines = spark.read.text("file:///Users/sgardziewicz/Documents/SparkCourse/Marvel+Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCount = connections.agg(func.min("connections")).first()[0]
#mostPopular = connections.sort(func.col("connections").asc()).first()
mostPopular = connections.filter(connections.connections == minConnectionCount)
#mostPopular.show()
#names.show()
joined = mostPopular.join(names, "id")

print("these have only" + str(minConnectionCount) + "connections " )
joined.select("name").show()
#joinedDF = movieCounts.join(moviesNames, movieCounts.movieID == moviesNames.movie_id, "inner")

#mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name") #.first()

#print(mostPopularName[0] + " is the least popular superhero with " + str(mostPopular[1]) + " co-appearances.")

