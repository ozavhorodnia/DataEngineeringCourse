from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from common.api_load_to_bronze import api_load_to_bronze_spark
from common.api_load_to_silver import api_load_to_silver_spark

dag = DAG(
    dag_id="load_out_of_stock",
    description="Load data from out_of_stock api data base to Data Lake",
    schedule_interval="@daily",
    start_date=datetime(2021, 10, 1),
    end_date=datetime(2021, 10, 15)
)

api_load_to_bronze_task = PythonOperator(
            task_id="api_load_to_bronze",
            python_callable=api_load_to_bronze_spark,
            provide_context=True,
            dag=dag
        )

api_load_to_silver_task = PythonOperator(
            task_id="api_load_to_silver",
            python_callable=api_load_to_silver_spark,
            provide_context=True,
            dag=dag
        )

api_load_to_bronze_task >> api_load_to_silver_task
