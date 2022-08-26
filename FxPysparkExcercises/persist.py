import sys


class Persist:
    def __init__(self, spark):
        self.spark = spark


    def persist_data(self, df):
        try:
            df.write.option("header", "true").csv("transformed_retailstore.csv")
        except Exception as e:
            print("Error occurred while persisting data" + str(e))
            sys.exit(1)
