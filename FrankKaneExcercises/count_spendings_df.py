from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

orders = spark.read.option("header", "false").option("inferSchema", "true") \
    .csv("file:///Users/sgardziewicz/Documents/SparkCourse/customer-orders.csv")
new_col_names = ["customer_id", "item_id", "amount_spent"]
orders = orders.toDF(*new_col_names)
orders = orders.select("customer_id", "amount_spent")
orders_grouped = orders.groupBy("customer_id").sum()
totalByCustomer = orders.groupBy("customer_id").agg(func.round(func.sum("amount_spent"), 2)
                                                    .alias("total_spent")).sort("total_spent").show()

orders_final = orders_grouped.select("customer_id", "sum(amount_spent)").sort("sum(amount_spent)")
orders_final.show()

# people.select(people.age, people.friends)
# people_selected = people.groupBy("age").avg().sort("age")
# people_selected.select("age", "avg(friends)").show()


spark.stop()
