# spark_session.py
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

def get_spark_session(app_name="DefaultApp", master=None, temp_bucket="", extra_conf=None):
    """
    Create and return a SparkSession object.
    
    Parameters:
      app_name: Application name.
      master: Spark master setting (if None, it will be obtained from the SPARK_MASTER environment variable, default is local[*]).
      temp_bucket: (Optional) Temporary GCS bucket used by Spark.
      extra_conf: (Optional) A dictionary containing additional Spark configurations to override the default settings.
      
    Returns:
      A SparkSession object.
    """
    # If master is not provided, get it from the environment variable
    if master is None:
        master = os.getenv("SPARK_MASTER", "local[*]")
    
    credentials_location = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_location:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
    
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
    
    # Set Hadoop configuration to support GCS
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    
    return spark
