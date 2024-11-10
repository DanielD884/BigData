from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators import (
    ClearDirectoryOperator,
    CreateDirectoryOperator,
    HdfsMkdirsFileOperator,
    HdfsPutFileOperator,
    KaggleDownloadOperator,
)
from airflow.operators.python_operator import PythonOperator
from helpers.year_months_helper import get_year_months


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
    python_callable=get_year_months,
    dag=dag,
)

# Task to create HDFS directories for raw data
create_hdfs_raw_data_dir = HdfsMkdirsFileOperator(
    task_id="create_hdfs_raw_data_dir",
    directory="/user/hadoop/hubway_data/raw_data/",
    file_names="{{ task_instance.xcom_pull(task_ids='get-year-months', key='year_months') }}", 
    hdfs_conn_id="hdfs",
    dag=dag,
)

# Task to create HDFS directories for final data
create_hdfs_final_data_dir = HdfsMkdirsFileOperator(
    task_id="create_hdfs_final_data_dir",
    directory="/user/hadoop/hubway_data/final_data/",
    file_names="{{ task_instance.xcom_pull(task_ids='get-year-months', key='year_months') }}",  
    hdfs_conn_id="hdfs",
    dag=dag,
)

# Set task dependencies
get_year_months_op >> [create_hdfs_raw_data_dir, create_hdfs_final_data_dir]

# Upload raw data to HDFS
upload_raw_data = HdfsPutFileOperator(
    task_id="upload-raw-data",
    local_path="/home/airflow/hubway_data/",
    remote_path="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

# Clean raw data with python script
clean_raw_data = SparkSubmitOperator(
    task_id="clean_raw_data",
    application="/home/airflow/airflow/python/clean_raw_data.py",
    name="clean_raw_data",
    conn_id="spark",
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    application_args=[
        "--yearmonth",
        "{{ task_instance.xcom_pull(task_ids='get-year-months') }}"
    ],
    verbose=False,
    dag=dag,
)

# Set task dependencies
create_local_import_dir >> clear_local_import_dir
create_output_dir >> clear_output_dir
[clear_local_import_dir, clear_output_dir] >> download_hubway_data
download_hubway_data >> get_year_months_op
get_year_months_op >> [create_hdfs_raw_data_dir, create_hdfs_final_data_dir]
create_hdfs_raw_data_dir >> upload_raw_data
upload_raw_data >> clean_raw_data