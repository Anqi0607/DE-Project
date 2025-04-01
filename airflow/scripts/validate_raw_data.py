import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from google.cloud import storage
from pyspark.sql.utils import AnalysisException

def validate_raw_data_with_spark(bucket_name: str, gcs_prefix: str, state: str):
    """
    Uses PySpark to read raw Parquet files from GCS and validates them.

    Validation checks include:
      1. Whether the file can be successfully read.
      2. Checking file size via GCS metadata (must not be 0).
      3. Consistency of the number and order of columns.
      4. Field type checks (e.g., timestamp).
      5. Total record count (must have at least one record).
      6. Null value ratio for each column.

    Args:
        bucket_name (str): Name of the GCS bucket.
        gcs_prefix (str): GCS path prefix (e.g., "METAR").
        state (str): State code, e.g., "ACK" or "IA".
    """
    # Path to the credentials file; update according to your environment
    credentials_location = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Create Spark configuration using a locally provided gcs-connector jar
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName("Validate METAR Raw Data") \
        .set("spark.driver.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.executor.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    
    # Create SparkContext and SparkSession
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
    
    # Set Hadoop configurations for GCS
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    
    # Use the google-cloud-storage client to list files
    client = storage.Client()  # Ensure GOOGLE_APPLICATION_CREDENTIALS or the provided keyfile is effective
    base_path = f"{gcs_prefix}"
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=base_path))
    
    if not blobs:
        raise ValueError(f"No files found under {base_path}.")

    # Group files by station (assumes the station name is the second-to-last folder in the path)
    files_by_station = {}
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            parts = blob.name.split("/")
            station = parts[-2]
            files_by_station.setdefault(station, []).append(blob)
    
    reference_schema = None
    for station, blob_list in files_by_station.items():
        print(f"\nValidating station: {station}")
        for blob in blob_list:
            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            print(f"Validating file: {gcs_uri}")

            # 1. Check file size using GCS metadata
            if blob.size == 0:
                raise ValueError(f"File is empty: {blob.name}")

            # 2. Try to read the Parquet file with Spark
            try:
                df = spark.read.parquet(gcs_uri)
            except AnalysisException as e:
                raise RuntimeError(f"Failed to read: {gcs_uri}, error: {e}")

            # 3. Verify record count (must have at least one record)
            record_count = df.count()
            if record_count < 1:
                raise ValueError(f"No records in file: {gcs_uri}")
            print(f"Record count: {record_count}")

            # 4. Calculate and print the null ratio for each column
            for col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                ratio = null_count / record_count
                print(f"Column '{col}': {null_count} nulls, ratio: {ratio:.2%}")

            # 5. Verify column consistency by comparing the schema
            if reference_schema is None:
                reference_schema = df.schema
            else:
                if df.schema != reference_schema:
                    raise ValueError(f"Schema mismatch in file: {gcs_uri}")

    print("\nAll raw data validations passed!")

