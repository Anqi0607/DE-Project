from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# 添加脚本所在目录
sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/scripts")

from scripts.load_to_bucket import (
    write_csv_to_local,
    convert_to_parquet,
    upload_parquet_to_gcs_with_station_structure,
    cleanup_local_files,
)
import config

def download_csv_dynamic(state: str, base_csv_dir: str, **kwargs):
    """
    根据执行日期构建形如 YYYY/MM 的目录，并下载 CSV 文件到该目录中。
    """
    execution_date = kwargs["execution_date"]
    # 构造形如 "2023/01" 的目录结构 for each monthly task
    # 以避免各个task在执行remove local files时删掉其他task的文件
    unique_dir = execution_date.strftime("%Y/%m")
    csv_dir = os.path.join(base_csv_dir, unique_dir)
    startts = execution_date
    endts = (execution_date + timedelta(days=32)).replace(day=1)
    print(f"Downloading METAR data from {startts} to {endts} into {csv_dir}")
    write_csv_to_local(state=state, startts=startts, endts=endts, output_dir=csv_dir)
    return csv_dir

def convert_parquet_dynamic(**kwargs):
    """
    从 XCom 中获取 CSV 目录，并构造对应的 Parquet 目录，保持年月结构不变，然后转换 CSV 到 Parquet。
    """
    ti = kwargs["ti"]
    csv_dir = ti.xcom_pull(task_ids="download_csv")
    base_parquet_dir = config.PARQUET_DIR
    # 得到 CSV 目录相对于 base_csv_dir 的子目录（例如 "2023/01"）
    unique_dir = os.path.relpath(csv_dir, config.CSV_DIR)
    parquet_dir = os.path.join(base_parquet_dir, unique_dir)
    print(f"Converting CSV files from {csv_dir} into Parquet files in {parquet_dir}")
    convert_to_parquet(csv_dir=csv_dir, parquet_dir=parquet_dir)
    return parquet_dir

def upload_to_gcs_dynamic(**kwargs):
    """
    从 XCom 中获取 Parquet 目录，然后上传该目录下所有文件到 GCS，
    按提取的年月和站点信息存储到路径：
      gs://<bucket>/<gcs_prefix>/<year>/<month>/<station>/<file_name>
    """
    ti = kwargs["ti"]
    parquet_dir = ti.xcom_pull(task_ids="convert_parquet")
    print(f"Uploading Parquet files from {parquet_dir} to GCS")
    upload_parquet_to_gcs_with_station_structure(
        parquet_dir=parquet_dir,
        bucket_name=config.BUCKET_NAME,
        gcs_prefix=config.GCS_PREFIX  # 例如 "METAR/MA/raw"
    )
    return parquet_dir

def cleanup_dynamic(**kwargs):
    """
    清理 XCom 中获取的 CSV 和 Parquet 目录。
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
    dag_id="METAR_extract_load_raw_data",
    default_args=default_args,
    description="download and upload METAR weather raw data by month and station as parquet files",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["METAR", "extract", "raw"],
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
