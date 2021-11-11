import logging
import os

from pyspark.sql import SparkSession

def api_load_to_silver_spark(**kwargs):
    ds = kwargs.get('ds')

    spark = SparkSession.builder \
        .master('local') \
        .appName('api_load_to_silver') \
        .getOrCreate()


    logging.info(f"Writing out_of_stock api data for {ds} to Silver")

    out_of_stock_df = spark.read \
        .json(os.path.join('/', 'datalake', 'bronze', 'out_of_stock', ds))

    out_of_stock_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'out_of_stock'),
        mode="append")

    logging.info(f"Loading out_of_stock api data for {ds} to Silver completed")