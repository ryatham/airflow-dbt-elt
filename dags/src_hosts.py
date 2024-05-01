from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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

# Define Snowflake connection parameters
snowflake_conn_id = 'snow_conn_id'  # Connection ID for Snowflake in Airflow

# Define the SQL query to execute in Snowflake
snowflake_sql_query = """
    SELECT * FROM AIRBNB.DEV.SRC_HOSTS limit 10
"""

# Define the dbt command to run
dbt_command = "dbt run --select src_hosts"

# Define the DAG
dag = DAG(
    'snowflake_dbt_dag',
    default_args=default_args,
    description='DAG to connect to Snowflake and run dbt models',
    schedule_interval='@daily',
    catchup=False
)

# Define a task to execute SQL query in Snowflake
snowflake_task = SnowflakeOperator(
    task_id='snowflake_task',
    sql=snowflake_sql_query,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

# Define a task to run dbt command
dbt_task = BashOperator(
    task_id='dbt_task',
    bash_command=dbt_command,
    dag=dag
)

# Set task dependencies
snowflake_task >> dbt_task
