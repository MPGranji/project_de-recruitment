"""
DAG: recruitment_etl_batch
Description: Orchestrates the batch ETL process for the recruitment project.
Schedule: Runs every 30 minutes to synchronize data from Cassandra to MySQL.
Owner: antigravity
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks within this DAG
default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), # DAG start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,                        # Number of retries on failure
    'retry_delay': timedelta(minutes=15), # Wait time between retries
}

# Define the DAG context
with DAG(
    'recruitment_etl_batch',
    default_args=default_args,
    description='Batch ETL from Cassandra to MySQL for Recruitment Data',
    schedule_interval=timedelta(minutes=1), 
    catchup=False,                          # Skip missed runs during downtime
    tags=['recruitment', 'spark', 'etl'],
) as dag:

    # Task: Run the Spark ETL script via Docker exec
    # This triggers the ETL_Pipeline_Airflow.py script inside the 'recruitment' container
    run_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='docker exec recruitment /usr/local/spark/bin/spark-submit /home/jovyan/work/scripts/ETL_Pipeline_Airflow.py',
    )

    # Define task dependencies (Single task in this DAG)
    run_etl
