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
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession

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

    spark = SparkSession.builder.appName("Test Parquet in GCS").getOrCreate()

    for station in stations:
        raw_prefix = f"{config.GCS_PREFIX}/2023/01/station={station}/"
        bronze_prefix = raw_prefix.replace("Raw", "Bronze")
        blobs = list(bucket.list_blobs(prefix=bronze_prefix))
        assert blobs, f"Expected transformed parquet files not saved in corrected folder in bucket"

        parts = [b.name for b in blobs]
        assert any(f.endswith(".parquet") for f in parts), f"Expected parquet part files not saved in gcs, but got {parts}"
    
        # assert data是否被正确transform成
        parquet_path = f"gs://{config.BUCKET_NAME}/{bronze_prefix}"
        df = spark.read.parquet(parquet_path)
        # no duplicates
        assert df.count() == df.distinct().count(), "duplicates found"
        # data type
        assert dict(df.dtypes)["valid"] == "timestamp", "valid is not timestamp"
        # numeric columns
        numeric_columns = [
            "lon", "lat", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i",
            "alti", "mslp", "vsby", "gust", "skyl1", "skyl2", "skyl3", "skyl4",
            "ice_accretion_1hr", "ice_accretion_3hr", "ice_accretion_6hr",
            "peak_wind_gust", "peak_wind_drct", "peak_wind_time", "feel", "snowdepth"
        ]
        for col in numeric_columns:
            col_type = dict(df.dtypes).get(col)
            assert col_type == "double", f"Expected {col} column to be of type 'double', but got {col_type}"
        
    # ---- transform_raw_data ----
    ti_check = TaskInstance(task=dag.get_task("check_bronze_data"), execution_date=exec_date)
    ti_check.run(ignore_ti_state=True)
    assert ti_check.state == State.SUCCESS

    # ---- load_bronze_table ----
    ti_load = TaskInstance(task=dag.get_task("load_bronze_table"), execution_date=exec_date)
    ti_load.run(ignore_ti_state=True)
    assert ti_load.state == State.SUCCESS

    # assert table exists
    bq_client = bigquery.Client()
    dataset_id = f"{config.GCP_PROJECT_ID}.{config.GCP_BIGQUERY_DATASET}"
    table_id = f"{dataset_id}.bronze_table"
    try:
        table = bq_client.get_table(table_id)  # Try fetching the table
        assert table is not None, f"BigQuery table {table_id} does not exist."
    except bigquery.exceptions.NotFound:
        pytest.fail(f"BigQuery table {table_id} does not exist")
    
    # assert table schema 
    table = bq_client.get_table(table_id)
    schema = table.schema
    expected_columns = [
    ("station", "STRING"),
    ("valid", "TIMESTAMP"),
    ("lon", "FLOAT"),
    ("lat", "FLOAT"),
    ("tmpf", "FLOAT"),
    ("dwpf", "FLOAT"),
    ("relh", "FLOAT"),
    ("drct", "FLOAT"),
    ("sknt", "FLOAT"),
    ("p01i", "FLOAT"),
    ("alti", "FLOAT"),
    ("mslp", "FLOAT"),
    ("vsby", "FLOAT"),
    ("gust", "FLOAT"),
    ("skyc1", "STRING"),
    ("skyc2", "STRING"),
    ("skyc3", "STRING"),
    ("skyc4", "STRING"),
    ("skyl1", "FLOAT"),
    ("skyl2", "FLOAT"),
    ("skyl3", "FLOAT"),
    ("skyl4", "FLOAT"),
    ("wxcodes", "STRING"),
    ("ice_accretion_1hr", "FLOAT"),
    ("ice_accretion_3hr", "FLOAT"),
    ("ice_accretion_6hr", "FLOAT"),
    ("peak_wind_gust", "FLOAT"),
    ("peak_wind_drct", "FLOAT"),
    ("peak_wind_time", "FLOAT"),
    ("feel", "FLOAT"),
    ("snowdepth", "FLOAT")
]
    
    for col, dtype in expected_columns:
        assert any(field.name == col and field.field_type == dtype for field in schema), \
            f"Column {col} with expected data type {dtype} not found in BigQuery table schema"
    
    # assert data is loaded to table 
    query = f"SELECT COUNT(*) FROM `{table_id}`"
    result = bq_client.query(query).result()
    row_count = next(result).values()[0]
    
    assert row_count > 0, f"Table {table_id} has no data loaded."