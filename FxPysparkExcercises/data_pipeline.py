import logging
import logging.config
from pyspark.sql import SparkSession

import ingest
import transform
import persist


# conf.set("spark.driver.host","127.0.0.1")


class Pipeline:
    logging.config.fileConfig("../resources/configs/logging.conf")

    def run_pipeline(self):
        logging.info("Running Pipeline")
        logging.error("test error log")
        logging.debug("test debug log")

        ingest_process = ingest.Ingest(self.spark)
        df = ingest_process.ingest_data()
        df.show()

        transform_process = transform.Transform(self.spark)
        transformed_df = transform_process.transform_data(df)
        transformed_df.show()

        persist_process = persist.Persist(self.spark)
        persist_process.persist_data(df)


    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("first spark app")\
            .enableHiveSupport()\
            .getOrCreate()

    def create_hive_table(self):
        # self.spark.sql("create database if not exists fxxcoursedb")
        # self.spark.sql("create table if not exists fxxcoursedb.fx_course_table (course_id string,course_name string,author_name string,no_of_reviews string)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (1,'Java','FutureX',45)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (2,'Java','FutureXSkill',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (3,'Big Data','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (4,'Linux','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (5,'Microservices','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (6,'CMS','',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (7,'Python','FutureX','')")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (8,'CMS','Future',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (9,'Dot Net','FutureXSkill',34)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (10,'Ansible','FutureX',123)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (11,'Jenkins','Future',32)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (12,'Chef','FutureX',121)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (13,'Go Lang','',105)")
        # Treat empty strings as null
        self.spark.sql("alter table fxxcoursedb.fx_course_table set tblproperties('serialization.null.format'='')")


if __name__ == '__main__':
    logging.info("App started")
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.create_hive_table()
    pipeline.run_pipeline()
    logging.info("Done")


