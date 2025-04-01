from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="spark_submit_test",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    spark_submit_job = SparkSubmitOperator(
        task_id="spark_submit_job",
        application="/opt/airflow/dags/wordcount.py",  # 你的 PySpark 脚本路径
        conn_id="spark_default",
        conf={
            "spark.pyspark.driver.python": "python3",
            "spark.pyspark.python": "python3"
        },
        verbose=True,
        dag=dag,
    )
