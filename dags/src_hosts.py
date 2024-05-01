from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


DBT_PROJECT_DIR = "C:\Users\gullu\Desktop\course\airflow-dbt-elt\dbt"

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
    description='DAG to run a specific dbt src_hosts model',
    schedule_interval='@daily',
    catchup=False
)

# Define a task to run a specific dbt model
src_hosts = BashOperator(
    task_id='src_hosts',
    bash_command='dbt run --model src_hosts',
    dag=dag
)

src_hosts
# Set task dependencies if needed
# For example, if you have a previous task that needs to be completed before running the specific model
# run_specific_model_task.set_upstream(earlier_task)

