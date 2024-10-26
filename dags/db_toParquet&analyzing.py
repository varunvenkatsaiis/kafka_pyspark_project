from airflow import DAG
from datetime import time, timedelta
from airflow.operators.bash import BashOperator
from airflow_dbt.operators import DbtRunOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task

default_args = {
    'start_date' : days_ago(1),
    'retries' : 1,
    'retries_delay' : timedelta(minutes=10)
}

with DAG(dag_id='parquet_analyzing' , default_args= default_args , 
            schedule_interval="*/5 * * * *",  # Every 5 minutes
            catchup=False,
            max_active_runs=1    ) as dag:
    
    
    DB_to_Parquet = BashOperator(
        task_id = 'DB_to_Parquet',
        bash_command='/bin/python /home/kali/Desktop/PROJECT-1/DB_to_storage.py'
    )

    analyzing = BashOperator(
        task_id = 'analyze_using_spark',
        bash_command='/bin/python /home/kali/Desktop/PROJECT-1/analyzing.py'
    )

    DB_to_Parquet >> analyzing


