import os
import psycopg2

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def read_pg(**kwargs):

    date = kwargs['ds']

    #airflow_postgres connection host = "localhost", schema = "dshop"
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")

    directory_path = os.path.join('/home/user/airflow/data', str(date))
    os.makedirs(directory_path, exist_ok=True)

    sql = f"select row_to_json(order_data) from (select product_id, order_date from orders where order_date = '{date}') order_data"

    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        with open(os.path.join(directory_path, date + '.json'), 'w') as json_file:
             cursor.copy_expert(f"copy ({sql}) to stdout", json_file)


dag = DAG(
    dag_id = "order_dag",
    description = "Dag to get order data from Postgres",
    start_date = datetime(2021,2,10,12,30),
    end_date = datetime(2021,2,15,12,30),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id = "export_order_data",
    dag = dag,
    python_callable=read_pg,
    provide_context=True
)