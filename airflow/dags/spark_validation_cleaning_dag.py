from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append("/opt/airflow")

from scripts.validate_raw_data import validate_raw_data_with_spark
from scripts.transform_raw_data import transform_raw_data
import config

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="METAR_validation",
    default_args=default_args,
    description="Validate and transform METAR data on GCS",
    schedule_interval="@daily",  
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["METAR", "raw-to-bronze", "validation", "transformation"],
) as dag:

    # Task 1: Validate raw data
    t1_validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data_with_spark,
        op_kwargs={
            "bucket_name": config.BUCKET_NAME,
            "gcs_prefix": config.GCS_PREFIX,
            "state": config.STATE,
        },
    )

    # Task 2: Transform data with new schema and write to Bronze layer.
    # Note: Here we assume the validated raw data is still at the same input location.
    t2_transform_raw_data = PythonOperator(
        task_id="transform_raw_data",
        python_callable=transform_raw_data,
        op_kwargs={
            "bucket_name": config.BUCKET_NAME,
            "gcs_prefix": config.GCS_PREFIX,  
            # "input_path": "gs://dev-project-bucket-pebbles/METAR/MA/test/BAF",   
            "temp_bucket": config.TEMP_BUCKET,
        },
    )

    # Set dependency: transformation runs after validation completes.
    # t1_validate_raw_data >> 
    t2_transform_raw_data
