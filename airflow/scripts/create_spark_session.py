# spark_session.py
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from dotenv import load_dotenv

# 加载 .env 文件中的环境变量
load_dotenv()

def get_spark_session(app_name="DefaultApp", master=None, temp_bucket="", extra_conf=None):
    """
    创建并返回一个 SparkSession 对象。
    
    参数：
      app_name: 应用名称。
      master: Spark master 设置（如果为 None，则从环境变量 SPARK_MASTER 中获取，默认为 local[*]）。
      temp_bucket: （可选）Spark 使用的临时 GCS bucket。
      extra_conf: （可选）字典，包含额外的 Spark 配置项，用于覆盖默认配置。
      
    返回：
      SparkSession 对象。
    """
    # 如果 master 没有传入，则从环境变量中获取
    if master is None:
        master = os.getenv("SPARK_MASTER", "local[*]")
    
    credentials_location = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_location:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS 环境变量未设置。")
    
    conf = SparkConf() \
        .setMaster(master) \
        .setAppName(app_name) \
        .set("spark.jars", "/opt/airflow/lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.driver.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.executor.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
        .set("spark.sql.parquet.enableVectorizedReader", "false") \
        .set("spark.driver.memory", "4g") \
        .set("spark.executor.memory", "4g")
    
    if temp_bucket:
        conf.set("temporaryGcsBucket", temp_bucket)
    
    if extra_conf:
        for key, value in extra_conf.items():
            conf.set(key, value)
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "30")
    
    # 设置 Hadoop 配置以支持 GCS
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    
    return spark
