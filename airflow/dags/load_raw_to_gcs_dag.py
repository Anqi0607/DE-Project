from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/scripts")

from scripts.download_csv_to_gcs import write_csv_to_gcs
import config

CLUSTER_NAME_TEMPLATE = "metar-cluster-{{ ds_nodash }}"


@task
def build_csv_to_parquet_job(**kwargs):
    ti = kwargs["ti"]
    execution_date = kwargs["data_interval_start"]
    ym = execution_date.strftime("%Y%m")

    # 每个月的path不一样，因此要dynamically build dataproc job的config
    input_prefix = ti.xcom_pull(task_ids="download_csv_to_gcs")
    input_path = f"gs://{config.BUCKET_NAME}/{input_prefix}"
    output_path = f"gs://{config.BUCKET_NAME}/{config.GCS_PARQUET_PREFIX}/{ym}/"

    return {
        "reference": {"project_id": config.GCP_PROJECT_ID},
        "placement": {"cluster_name": f"metar-cluster-{execution_date.strftime('%Y%m%d')}"},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{config.BUCKET_NAME}/METAR/spark-jobs/convert_csv_to_parquet_gcp.py",
            "args": [input_path, output_path],
        },
    }

@task
def build_bronze_transform_job(**kwargs):
    execution_date = kwargs["data_interval_start"]
    ym = execution_date.strftime("%Y%m")

    input_path = f"gs://{config.BUCKET_NAME}/{config.GCS_PARQUET_PREFIX}/{ym}/"
    output_path = input_path.replace("PARQUET", "Bronze-Dataproc")

    return {
        "reference": {"project_id": config.GCP_PROJECT_ID},
        "placement": {"cluster_name": f"metar-cluster-{execution_date.strftime('%Y%m%d')}"},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{config.BUCKET_NAME}/METAR/spark-jobs/transform_raw_data_gcp.py",
            "args": [input_path, output_path],
        },
    }

@task
def build_create_bq_table_job(**kwargs):
    execution_date = kwargs["data_interval_start"]
    ym = execution_date.strftime("%Y%m")

    parquet_prefix = config.GCS_PARQUET_PREFIX
    bronze_prefix = parquet_prefix.replace("PARQUET", "Bronze-Dataproc")
    input_path = f"gs://{config.BUCKET_NAME}/{bronze_prefix}/{ym}/"
    return {
        "reference": {"project_id": config.GCP_PROJECT_ID},
        "placement": {"cluster_name": f"metar-cluster-{execution_date.strftime('%Y%m%d')}"},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{config.BUCKET_NAME}/METAR/spark-jobs/create_bq_table_gcp.py",
            "args": [input_path, config.GCP_PROJECT_ID, config.GCP_BIGQUERY_DATASET, config.TEMP_BUCKET],
        },
    }

cluster_config = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_size_gb": 50
        },
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_size_gb": 100
        },
    },
    "gce_cluster_config": {
        "zone_uri": "us-central1-f",
        "service_account": "staging-terraform-sa@just-camera-454714-e8.iam.gserviceaccount.com"
    }
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "end_date": datetime(2023, 2, 2),
}

with DAG(
    dag_id="METAR_load_raw_data_on_GCP",
    default_args=default_args,
    description="download and upload METAR weather raw data by month and station as parquet files",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["METAR", "extract", "raw", "Dataproc"],
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

    t2_create_cluster = DataprocCreateClusterOperator(
    task_id="create_dataproc_cluster",
    project_id=config.GCP_PROJECT_ID,
    region=config.GCP_REGION,
    cluster_name=CLUSTER_NAME_TEMPLATE,
    cluster_config=cluster_config,
    )

    t3_spark_job_config = build_csv_to_parquet_job()

    t4_submit_convert_spark = DataprocSubmitJobOperator(
        task_id="submit_convert_spark_job",
        job=t3_spark_job_config,
        region=config.GCP_REGION,
        project_id=config.GCP_PROJECT_ID,
    )

    t5_transform_job_config = build_bronze_transform_job()

    t6_submit_transform_spark = DataprocSubmitJobOperator(
            task_id="submit_transform_spark_job",
            job=t5_transform_job_config,
            region=config.GCP_REGION,
            project_id=config.GCP_PROJECT_ID,
        )

    t7_table_job_config = build_create_bq_table_job()

    t8_submit_table_spark = DataprocSubmitJobOperator(
        task_id="submit_table_spark_job",
        job=t7_table_job_config,
        region=config.GCP_REGION,
        project_id= config.GCP_PROJECT_ID,
    )

    t9_delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=config.GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME_TEMPLATE,
        region=config.GCP_REGION,
        trigger_rule="all_done",  # 即使任务失败也删除
    )

    (
    t1_download_csv_to_gcs 
    >> t2_create_cluster 
    >> t3_spark_job_config 
    >> t4_submit_convert_spark
    >> t5_transform_job_config 
    >> t6_submit_transform_spark
    >> t7_table_job_config 
    >> t8_submit_table_spark 
    >> t9_delete_cluster
)

