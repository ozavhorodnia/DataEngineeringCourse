import logging
import os

from pyspark.sql import SparkSession

def load_to_silver_spark(**kwargs):

    # ['aisles', 'clients', 'departments', 'orders', 'products']

    ds = kwargs.get('ds')

    spark = SparkSession.builder \
        .master('local') \
        .appName('load_to_silver') \
        .getOrCreate()

    # aisles
    logging.info(f"Writing table aisles to Silver")

    aisles_df = spark.read\
            .option('header', True)\
            .option('inferSchema', True)\
            .csv(os.path.join('/', 'datalake', 'bronze', 'dshop', 'aisles', ds))

    aisles_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'dshop', 'aisles'),
        mode="overwrite")

    logging.info(f"Loading aisles to Silver completed")

    # clients
    logging.info(f"Writing table clients to Silver")

    clients_df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(os.path.join('/', 'datalake', 'bronze', 'dshop', 'clients', ds))

    client_df = clients_df.drop('location_area_id')

    clients_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'dshop', 'clients'),
        mode="overwrite")

    logging.info(f"Loading clients to Silver completed")

    # departments
    logging.info(f"Writing table departments to Silver")

    departments_df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(os.path.join('/', 'datalake', 'bronze', 'dshop', 'departments', ds))

    departments_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'dshop', 'departments'),
        mode="overwrite")

    logging.info(f"Loading departments to Silver completed")

    # orders
    logging.info(f"Writing table orders to Silver")

    orders_df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(os.path.join('/', 'datalake', 'bronze', 'dshop', 'orders', ds))

    orders_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'dshop', 'orders'),
        mode="overwrite")

    logging.info(f"Loading orders to Silver completed")

    # products
    logging.info(f"Writing table products to Silver")

    products_df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(os.path.join('/', 'datalake', 'bronze', 'dshop', 'products', ds))

    products_df = products_df.drop('department_id')

    products_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'dshop', 'products'),
        mode="overwrite")

    logging.info(f"Loading products to Silver completed")




