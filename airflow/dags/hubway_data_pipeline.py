from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators import (
    ClearDirectoryOperator,
    CreateDirectoryOperator
)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from helpers.year_months import get_year_months

args = {"owner": "airflow"}

dag = DAG(
    "hubway_data_pipeline",
    default_args=args,
    description="ETL Workflow for Hubway Bike Sharing KPI Calculation",
    schedule_interval='00 12 * * *',
    start_date=datetime(2019, 10, 16), 
    catchup=False, 
    max_active_runs=1
)

# create a directory for the import data (just if not exists)
create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow",
    directory="hubway_data",
    dag=dag,
)

# create a directory for the output data (just if not exists)
create_output_dir = CreateDirectoryOperator(
    task_id="create_output_dir",
    path="/home/airflow",
    directory="data_output",
    dag=dag,
)

# Should be there already the hubway_data directory, then clear it
clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/hubway_data",
    pattern="*",
    dag=dag,
)

# Should be there already the data_output directory, then clear it
clear_output_dir = ClearDirectoryOperator(
    task_id="clear_output_dir",
    directory="/home/airflow/data_output",
    pattern="*",
    dag=dag,
)