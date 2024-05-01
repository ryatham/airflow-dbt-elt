from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 1)
}

# Define the DAG
dag = DAG(
    'src_hosts',
    default_args=default_args,
    description='DAG to run dbt src_hosts model',
    schedule_interval='@daily',
    catchup=False
)

# Define tasks for each dbt model
src_hosts_task = BashOperator(
    task_id='src_hosts_task',
    bash_command='dbt run --model src_hosts',
    dag=dag
)

src_hosts_task
