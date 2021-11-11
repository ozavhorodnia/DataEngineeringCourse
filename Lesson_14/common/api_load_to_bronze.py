import json
import requests
import os
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession


def api_load_to_bronze_spark(**kwargs):

    ds = kwargs.get('ds')

    api_conn = BaseHook.get_connection('url_auth')
    api_out_of_stock_conn = BaseHook.get_connection('url_out_of_stock')
    headers = {'content-type': 'application/json'}
    data = {"username": api_conn.login, "password": api_conn.password}

    r = requests.post(api_conn.host, data=json.dumps(data), headers=headers)
    r.raise_for_status()
    token = "JWT " + r.json()['access_token']

    if r.status_code != 200:
        raise AirflowException("Response check returned False.")

    headers = {'content-type': 'application/json', 'Authorization': token}
    data = {"date": ds}

    r = requests.get(api_out_of_stock_conn.host, data=json.dumps(data), headers=headers)
    r.raise_for_status()
    data = r.json()

    if r.status_code != 200:
        if r.status_code == 404:
            print("Out_of_stock not found")
            return None
        else:
            raise Exception("API Failed - {0}".r.text)

    spark = SparkSession \
        .builder \
        .master('local') \
        .appName("api_load_to_silver") \
        .getOrCreate()

    logging.info(f"Writing out_of_stock api data for {ds} to Bronze")

    spark.sparkContext.parallelize(data).toDF().coalesce(1).write.mode('append').json(os.path.join('/', 'datalake', 'bronze', 'out_of_stock', ds))

    logging.info(f"Loading out_of_stock api data for {ds} to Bronze completed")

