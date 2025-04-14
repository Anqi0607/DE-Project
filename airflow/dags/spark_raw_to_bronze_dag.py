import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/scripts")

from scripts.validate_raw_data import validate_raw_data_with_spark
from scripts.transform_raw_data import transform_raw_data_dynamic
from scripts.check_bronze_data_quality import check_bronze_data_quality
from scripts.create_big_query_table import load_parquet_to_bigquery
import config

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Only backfill data for January and February 2023
    "end_date": datetime(2023, 2, 2),
}

with DAG(
    dag_id="METAR_raw_to_bronze",
    default_args=default_args,
    description="Validate and transform monthly METAR data on GCS",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["METAR", "validation", "transformation"],
) as dag:

    # Validation task: Use the Airflow context to filter data for the target month based on execution_date
    t1_validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data_with_spark,
        op_kwargs={
            "bucket_name": config.BUCKET_NAME,
            "gcs_prefix": config.GCS_PREFIX,
            "state": config.STATE,
        },
        provide_context=True,
    )

    # Transformation task: Dynamically construct the raw data path and perform the transformation
    t2_transform_raw_data = PythonOperator(
        task_id="transform_raw_data",
        python_callable=transform_raw_data_dynamic,
        provide_context=True,
    )

    # Check bronze layer data quality after transformation
    t3_check_bronze_data = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data_quality,
        provide_context=True,
    )

    #load_parquet_to_bigquery
    t4_load_bronze_table = PythonOperator(
    task_id="load_bronze_table",
    python_callable=load_parquet_to_bigquery,
    provide_context=True,
    )

    t1_validate_raw_data >> t2_transform_raw_data >> t3_check_bronze_data >> t4_load_bronze_table
    