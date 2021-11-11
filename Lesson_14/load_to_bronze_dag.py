
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from common.load_to_bronze import load_to_bronze_spark
from common.load_to_silver import load_to_silver_spark


tables_to_load_to_bronze = ['aisles', 'clients', 'departments', 'orders', 'products']
load_to_bronze_tasks = []

dag = DAG(
    dag_id="load_to_bronze",
    description="Load data from PostgreSQL data base to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2021, 10, 1),
    end_date=datetime(2021, 10, 15)
)

dummy1 = DummyOperator(
    task_id='start_load_to_bronze',
    dag=dag
)


for table in tables_to_load_to_bronze:
    load_to_bronze_tasks.append(
        PythonOperator(
            task_id=f"{table}_load_to_bronze",
            python_callable=load_to_bronze_spark,
            op_kwargs={"table": table},
            provide_context=True,
            dag=dag
        )
    )

load_to_silver_task = PythonOperator(
            task_id="load_to_silver",
            python_callable=load_to_silver_spark,
            provide_context=True,
            dag=dag
        )

dummy1 >> load_to_bronze_tasks >> load_to_silver_task