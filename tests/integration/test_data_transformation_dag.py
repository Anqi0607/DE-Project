# import env first
import os
from dotenv import load_dotenv

env_file = "/opt/airflow/.env.staging"
if os.path.exists(env_file):
    # 本地调试时加载 .env.staging，
    # CI 环境里通常不会挂载这个文件，就会跳过
    load_dotenv(dotenv_path=env_file, override=True)

import pytest
from datetime import datetime, timedelta
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from google.cloud import storage

import config

def test_dag_end_to_end():
    # 加载 DAG
    dagbag = DagBag(include_examples=False)
    dag = dagbag.get_dag("METAR_raw_to_bronze")
    assert dag is not None

    # 定义执行日期
    exec_date = datetime(2023, 1, 1)

    # sample stations
    stations = ["ST01", "ST02", "ST03"]

     # ---- validate_raw_data ----
    ti_validate = TaskInstance(task=dag.get_task("validate_raw_data"), execution_date=exec_date)
    ti_validate.run(ignore_ti_state=True)
    assert ti_validate.state == State.SUCCESS

    # ---- transform_raw_data ----
    ti_transform = TaskInstance(task=dag.get_task("transform_raw_data"), execution_date=exec_date)
    ti_transform.run(ignore_ti_state=True)
    assert ti_transform.state == State.SUCCESS
    
    # assert transformed parquet file 存在了gcs bucket中的bronze folder
    # 且partition by station
    client = storage.Client()
    bucket = client.bucket(config.BUCKET_NAME)

    for station in stations:
        raw_prefix = f"{config.GCS_PREFIX}/2023/01/station={station}/"
        bronze_prefix = raw_prefix.replace("Raw", "Bronze")
        blobs = list(bucket.list_blobs(prefix=bronze_prefix))
        assert blobs, f"Expected transformed parquet files not saved in corrected folder in bucket"

        parts = [b.name for b in blobs]
        assert any(f.endswith(".parquet") for f in parts), f"Expected parquet part files not saved in gcs, but got {parts}"
    


