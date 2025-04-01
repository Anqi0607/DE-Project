from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append("/opt/airflow")

from scripts.validate_raw_data import validate_raw_data_with_spark
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
    description="Validate METAR data on GCS",
    schedule_interval="@daily",  # 或根据需要设置调度周期
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["METAR", "validation"],
) as dag:

    t1_validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data_with_spark,
        op_kwargs={
            "bucket_name": config.BUCKET_NAME,
            "gcs_prefix": config.GCS_PREFIX,
            "state": config.STATE,
        },
    )

    
    t1_validate_raw_data
