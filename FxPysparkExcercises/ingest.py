import logging
import logging.config


class Ingest:
    logging.config.fileConfig("../resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        logging.info("Ingesting csv")

        #customer_df = self.spark.read.csv("retailstore.csv", header=True)
        course_df = self.spark.sql("select * from fxxcoursedb.fx_course_table")
        return course_df

