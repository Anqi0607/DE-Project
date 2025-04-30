from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/scripts")

from scripts.load_to_gcs import (
    write_csv_to_gcs
)
import config

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "end_date": datetime(2023, 2, 2),
}

with DAG(
    dag_id="METAR_load_raw_data_on_gcp",
    default_args=default_args,
    description="download and upload METAR weather raw data by month and station as parquet files",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["METAR", "extract", "raw"],
) as dag:
    
    t1_download_csv_to_gcs = PythonOperator(
    task_id="download_csv_to_gcs",
    python_callable=write_csv_to_gcs,
    op_kwargs={
        "state": config.STATE,
        "gcs_csv_prefix": config.GCS_CSV_PREFIX,
        "project_id": config.GCP_PROJECT_ID,
        "bucket_name": config.BUCKET_NAME,
    },
)
    #t1_download_csv_to_gcs



