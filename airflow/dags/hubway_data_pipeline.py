from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.download_operation import KaggleDownloadOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from python.helpers.year_months_helper import get_year_months
from airflow.operators.hdfs_plugin import HdfsMkdirsFileOperator, HdfsPutFilesOperator

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

# Download the Hubway dataset from Kaggle
download_hubway_data = KaggleDownloadOperator(
    task_id="download_hubway_data",
    dataset_name="acmeyer/hubway-data",
    download_path="/home/airflow/hubway_data",
    dag=dag,
)

# Task to get year-months
get_year_months_op = PythonOperator(
    task_id='get-year-months',
    python_callable=get_year_months_task,
    provide_context=True,
    dag=dag,
)

# Task to create HDFS directories for raw data
create_hdfs_partition_raw = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-raw",
    directory="/user/hadoop/hubway_data/raw/",
    file_names="{{ task_instance.xcom_pull(task_ids='get-year-months', key='year_months') }}",  # List of year-month directories
    hdfs_conn_id="hdfs",
    dag=dag,
)

# Task to create HDFS directories for final data
create_hdfs_partition_final = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-final",
    directory="/user/hadoop/hubway_data/final/",
    file_names="{{ task_instance.xcom_pull(task_ids='get-year-months', key='year_months') }}",  # List of year-month directories
    hdfs_conn_id="hdfs",
    dag=dag,
)

# Set task dependencies
get_year_months_op >> [create_hdfs_partition_raw, create_hdfs_partition_final]

# Upload raw data to HDFS
copy_raw_to_hdfs = HdfsPutFilesOperator(
    task_id="upload-raw-to-hdfs",
    local_path="/home/airflow/hubway_data/",
    remote_path="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)