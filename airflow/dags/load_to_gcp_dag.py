from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# 添加脚本所在目录
sys.path.append("/opt/airflow")

from scripts.load_to_bucket import (
    write_csv_to_local,
    convert_to_parquet,
    upload_parquet_to_gcs_with_station_structure,
    cleanup_local_files,
)
import config

def download_csv_dynamic(state: str, base_csv_dir: str, **kwargs):
    """
    从 Airflow 上下文中获取执行日期，构造独有的 CSV 文件夹（仅保留日期部分），然后下载 CSV 文件。
    """
    execution_date = kwargs["execution_date"]
    # 使用 YYYY-MM-DD 格式生成唯一文件夹名称
    unique_dir = execution_date.strftime("%Y-%m-%d")
    csv_dir = os.path.join(base_csv_dir, unique_dir)
    startts = execution_date
    endts = (execution_date + timedelta(days=32)).replace(day=1)
    print(f"Downloading METAR data from {startts} to {endts} into {csv_dir}")
    write_csv_to_local(state=state, startts=startts, endts=endts, output_dir=csv_dir)
    return csv_dir

def convert_parquet_dynamic(**kwargs):
    """
    从 XCom 中获取 CSV 文件夹路径，构造独有的 Parquet 文件夹（仅保留日期部分），然后转换 CSV 到 Parquet。
    """
    ti = kwargs["ti"]
    csv_dir = ti.xcom_pull(task_ids="download_csv")
    base_parquet_dir = config.PARQUET_DIR
    unique_dir = os.path.basename(csv_dir)  # 已经是 YYYY-MM-DD 格式
    parquet_dir = os.path.join(base_parquet_dir, unique_dir)
    print(f"Converting CSV files from {csv_dir} into Parquet files in {parquet_dir}")
    convert_to_parquet(csv_dir=csv_dir, parquet_dir=parquet_dir)
    return parquet_dir

def upload_to_gcs_dynamic(**kwargs):
    """
    从 XCom 中获取 Parquet 文件夹路径，然后上传该目录下所有文件到 GCS，
    按 station 将文件放入对应文件夹中（只保留 station 名称）。
    """
    ti = kwargs["ti"]
    parquet_dir = ti.xcom_pull(task_ids="convert_parquet")
    print(f"Uploading Parquet files from {parquet_dir} to GCS")
    upload_parquet_to_gcs_with_station_structure(
        parquet_dir=parquet_dir,
        bucket_name=config.BUCKET_NAME,
        gcs_prefix=config.GCS_PREFIX
    )
    return parquet_dir

def cleanup_dynamic(**kwargs):
    """
    从 XCom 中获取 CSV 和 Parquet 文件夹路径，然后删除这些目录。
    """
    ti = kwargs["ti"]
    csv_dir = ti.xcom_pull(task_ids="download_csv")
    parquet_dir = ti.xcom_pull(task_ids="convert_parquet")
    print(f"Cleaning up local directories: CSV: {csv_dir}, Parquet: {parquet_dir}")
    cleanup_local_files(csv_dir=csv_dir, parquet_dir=parquet_dir)

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
    tags=["METAR", "raw"],
) as dag:

    t1_download_csv = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv_dynamic,
        op_kwargs={
            "state": config.STATE,
            "base_csv_dir": config.CSV_DIR,  # 基础 CSV 目录
        },
        provide_context=True,
    )

    t2_convert_to_parquet = PythonOperator(
        task_id="convert_parquet",
        python_callable=convert_parquet_dynamic,
        provide_context=True,
    )

    t3_upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs_dynamic,
        provide_context=True,
    )

    t4_cleanup_local = PythonOperator(
        task_id="cleanup_local_files",
        python_callable=cleanup_dynamic,
        provide_context=True,
    )

    t1_download_csv >> t2_convert_to_parquet >> t3_upload_to_gcs >> t4_cleanup_local
