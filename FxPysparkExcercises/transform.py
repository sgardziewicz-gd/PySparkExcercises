import logging


class Transform:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        logging.info("Transforming")
        df1 = df.na.fill("Unknown", ["author_name"])
        df2 = df1.na.fill("0", ["no_of_reviews"])

        return df2
