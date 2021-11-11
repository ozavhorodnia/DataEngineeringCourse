import psycopg2
import logging
import os
from datetime import datetime

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession

def load_to_bronze_spark(table, **kwargs):

    ds = kwargs.get('ds')

    pg_conn = BaseHook.get_connection('postgres_local')

    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('load_to_bronze') \
        .getOrCreate()

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")

    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)

    table_df.write \
        .option('header', True)\
        .csv(
        os.path.join('/', 'datalake', 'bronze', 'dshop', table, ds),
        mode="overwrite"
    )

    logging.info(f"Loading {table} to Bronze completed")