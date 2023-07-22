from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'fatir',
    'start_date': datetime(2023, 7, 21),
    'retries': 10,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    default_args=default_args,
    dag_id='dag_flight_daily',
    description='DAG Flight Project (@daily running)'
)

# Set current time variabel
current_date = datetime.today()
# Calculate the date 2 days before the current date
begin_date = current_date - timedelta(days=2)
begin_date_string = begin_date.strftime("%d_%B_%Y")

get_daily_data = BashOperator(
    task_id = 'get_daily_data',
    bash_command = 'python3 /home/fatir/final-project/fp_get_data_api.py',
    dag=dag
)

check_db_staging = HiveOperator(
    task_id = 'create_db_staging',
    hql = open('/home/fatir/final-project/fp_create_db_staging.sql', 'r').read(),
    hive_cli_conn_id = 'hive_default',
    dag=dag
)

load_to_staging = BashOperator(
    task_id = 'load_to_staging',
    bash_command = f'hadoop fs -copyFromLocal -f /home/fatir/datalake-flight/flight_dep_data/flight_dep_data-{begin_date_string}.parquet /user/fatir/flights-data-staging/flight_table_dep/ && hadoop fs -copyFromLocal -f /home/fatir/datalake-flight/flight_arr_data/flight_arr_data-{begin_date_string}.parquet /user/fatir/flights-data-staging/flight_table_arr/',
    dag=dag
)

check_dwh = HiveOperator(
    task_id = 'check_dwh',
    hql = open('/home/fatir/final-project/fp_create_dwh.sql', 'r').read(),
    hive_cli_conn_id = 'hive_default',
    dag=dag
)

read_transform_load = BashOperator(
    task_id = 'read_transform_load',
    bash_command = 'python3 /home/fatir/final-project/fp_transformation_daily.py',
    dag = dag
)



get_daily_data >> check_db_staging >> load_to_staging >> check_dwh >> read_transform_load