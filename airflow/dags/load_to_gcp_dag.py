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

def download_csv_dynamic(state: str, output_dir: str, **kwargs):
    """
    Wrapper that pulls start/end date from Airflow execution context and call the real download function.
    """
    execution_date = kwargs["execution_date"]
    startts = execution_date
    endts = (execution_date + timedelta(days=32)).replace(day=1) # 1st day of next month
    print(f"Downlaoding METAR data from {startts} to {endts}")

    write_csv_to_local(state=state, startts=startts, endts=endts, output_dir=output_dir)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "end_date": datetime(2023, 2, 2),
}

with DAG(
    dag_id="METAR_pipeline",
    default_args=default_args,
    description="ETL pipeline for METAR weather data",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["METAR", "weather"],
) as dag:

    t1_download_csv = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv_dynamic,
        op_kwargs={
            "state": config.STATE,
            "output_dir": config.CSV_DIR,
        },
        provide_context=True,
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