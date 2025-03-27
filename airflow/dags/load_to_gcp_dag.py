from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append("/opt/airflow")

from scripts.load_to_bucket import (
    write_csv_to_local,
    convert_to_parquet,
    upload_parquet_to_gcs_with_station_structure,
    cleanup_local_files,
)
import config

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="METAR_pipeline",
    default_args=default_args,
    description="ETL pipeline for METAR weather data",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["METAR", "weather"],
) as dag:

    t1_download_csv = PythonOperator(
        task_id="download_csv",
        python_callable=write_csv_to_local,
        op_kwargs={
            "state": config.STATE,
            "startts": config.START_DATE,
            "endts": config.END_DATE,
            "output_dir": config.CSV_DIR,
        },
    )

    t2_convert_to_parquet = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
        op_kwargs={
            "csv_dir": config.CSV_DIR,
            "parquet_dir": config.PARQUET_DIR,
        },
    )

    t3_upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_parquet_to_gcs_with_station_structure,
        op_kwargs={
            "parquet_dir": config.PARQUET_DIR,
            "bucket_name": config.BUCKET_NAME,
            "gcs_prefix": config.GCS_PREFIX,
        },
    )

    t4_cleanup_local = PythonOperator(
        task_id="cleanup_local_files",
        python_callable=cleanup_local_files,
        op_kwargs={
            "csv_dir": config.CSV_DIR,
            "parquet_dir": config.PARQUET_DIR
        },
    )

    # run task in order 
    t1_download_csv >> t2_convert_to_parquet >> t3_upload_to_gcs >> t4_cleanup_local