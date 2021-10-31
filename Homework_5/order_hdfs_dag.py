import os
import psycopg2
from hdfs import InsecureClient

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook

def export_hdfs(**kwargs):

    date = kwargs['ds']
    date_dt = datetime.strptime(date, '%Y-%m-%d')

    #airflow_postgres connection host = "localhost", schema = "dshop"
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")

    #airflow hadoop connection hostname = "http://127.0.0.1:50070", login = "user"
    hadoop_conn = BaseHook.get_connection("hadoop_local")
    hostname = hadoop_conn.host
    login_name = hadoop_conn.login

    client = InsecureClient(hostname, login_name)

    # create folders for partitioning by date
    hdfs_zone_path = '/bronze'
    hdfs_project_path = os.path.join(hdfs_zone_path, 'orders')
    hdfs_year_path = os.path.join(hdfs_project_path, date_dt.strftime("%Y"))
    hdfs_month_path = os.path.join(hdfs_year_path, date_dt.strftime("%Y%m"))

    status = client.status(hdfs_zone_path, strict=False)
    if status == None:
        client.makedirs(hdfs_zone_path)

    status = client.status(hdfs_project_path, strict=False)
    if status == None:
        client.makedirs(hdfs_project_path)

    status = client.status(hdfs_year_path, strict=False)
    if status == None:
        client.makedirs(hdfs_year_path)

    status = client.status(hdfs_month_path, strict=False)
    if status == None:
        client.makedirs(hdfs_month_path)

    # copy table's data from PostgreSQL to HDFS
    sql = f"select product_id, order_date from orders where order_date = '{date}'"

    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        with client.write(os.path.join(hdfs_month_path, date + '.csv')) as csv_file:
            cursor.copy_expert(f'COPY ({sql}) TO STDOUT WITH HEADER CSV', csv_file)



dag = DAG(
    dag_id = "order_htfs_dag",
    description = "Dag to load Postgres data into hdfs",
    start_date = datetime(2021,1,1,12,30),
    end_date = datetime(2021,2,28,12,30),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id = "export_order_data",
    dag = dag,
    python_callable=export_hdfs,
    provide_context=True
)