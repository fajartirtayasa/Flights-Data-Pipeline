from fp_getdata_csv import get_aircraftdb, get_aircrafttypes, get_airport
from fp_transformation_csv import read_transform_load

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.hive_operator import HiveOperator

default_args = {
        'owner' : 'fatir',
        'retries' : 20,
        'retry_delay' : timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_flight_1month',
    description='DAG Flight Project (@monthly running)',
    start_date=datetime(2023,7,21)
) as dag:
    start_dag = DummyOperator(
        task_id='start_dag'
        )
    task1 = PythonOperator(
        task_id = 'get_aircraft_all',
        python_callable = get_aircraftdb
    )
    task2 = PythonOperator(
        task_id = 'get_aircraft_types',
        python_callable = get_aircrafttypes
    )
    task3 = PythonOperator(
        task_id = 'get_airport',
        python_callable = get_airport
    )
    task4 = HiveOperator(
        task_id = 'staging_in_hive',
        hql = open('/mnt/c/Users/fajar/Documents/airflow/dags/fp_hive_hql.sql', 'r').read(),
        hive_cli_conn_id = 'hive_default'
    )
    task5 = PythonOperator(
        task_id = 'read_transform_load',
        python_callable = read_transform_load
    )

    start_dag >> task1
    start_dag >> task2
    start_dag >> task3
    task1 >> task4
    task2 >> task4
    task3 >> task4
    task4 >> task5