from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/scripts")

from scripts.validate_raw_data import validate_raw_data_with_spark
from scripts.transform_raw_dynamic import transform_raw_dynamic
import config

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 只回溯处理 2023 年 1 月和 2 月的数据
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

    # 验证任务：利用 Airflow 上下文传递 execution_date 来过滤当月数据
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

    # Transformation任务：动态构造 raw data 路径并转换
    # t2_transform_raw_data = PythonOperator(
    #     task_id="transform_raw_data",
    #     python_callable=transform_raw_dynamic,
    #     provide_context=True,
    # )

    # 设置任务依赖关系，根据实际需求调整
    # 如验证任务必须运行，再启用下面的依赖
    # t1_validate_raw_data >> t2_transform_raw_data
    t1_validate_raw_data
