import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from google.cloud import storage
from pyspark.sql.utils import AnalysisException

def compare_schemas(ref_schema, curr_schema):
    """
    Compare two schemas and return detailed differences as a string.
    """
    lines = []
    lines.append("Schema mismatch detected!")
    lines.append("Reference schema:")
    lines.append(str(ref_schema))
    lines.append("Current schema:")
    lines.append(str(curr_schema))
    
    # Build dictionaries mapping field names to field objects for both schemas.
    ref_fields = {field.name: field for field in ref_schema.fields}
    curr_fields = {field.name: field for field in curr_schema.fields}
    
    # Fields that exist only in the reference schema.
    only_in_ref = set(ref_fields.keys()) - set(curr_fields.keys())
    # Fields that exist only in the current schema.
    only_in_curr = set(curr_fields.keys()) - set(ref_fields.keys())
    
    if only_in_ref:
        lines.append("Fields only in reference schema: " + ", ".join(only_in_ref))
    if only_in_curr:
        lines.append("Fields only in current schema: " + ", ".join(only_in_curr))
    
    # Compare data types for common fields.
    common_fields = set(ref_fields.keys()).intersection(set(curr_fields.keys()))
    for field_name in common_fields:
        ref_type = ref_fields[field_name].dataType
        curr_type = curr_fields[field_name].dataType
        if ref_type != curr_type:
            lines.append(f"Field '{field_name}' type mismatch: reference type = {ref_type}, current type = {curr_type}")
    
    return "\n".join(lines)

def validate_raw_data_with_spark(bucket_name: str, gcs_prefix: str, state: str):
    """
    Use PySpark to read raw Parquet files from GCS and validate them.

    Validations include:
      1. Checking if the file can be successfully read.
      2. Checking file size via GCS metadata (must not be 0).
      3. Consistency of column count and order.
      4. Field type validations (e.g., timestamp).
      5. Total record count (must have at least one record).
      6. Null value ratio for each column.

    Args:
        bucket_name (str): The name of the GCS bucket.
        gcs_prefix (str): The GCS path prefix (e.g., "METAR").
        state (str): State code, e.g., "ACK" or "IA".
    """
    # Get credentials file path from the environment variable.
    credentials_location = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_location:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    # Get the log directory from the environment variable LOG_DIR; default to /opt/airflow/logs.
    log_dir = os.environ.get("LOG_DIR", "/opt/airflow/logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    mismatch_log_file = os.path.join(log_dir, "schema_mismatch.log")
    
    # Clear the log file.
    with open(mismatch_log_file, "w") as f:
        f.write("Schema Mismatch Log\n\n")
    
    # Create Spark configuration, load the GCS connector JAR, and set GCS authentication parameters.
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName("Validate METAR Raw Data") \
        .set("spark.jars", "/opt/airflow/lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.driver.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.executor.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
    
    # Create SparkContext and SparkSession.
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
    
    # Set Hadoop configuration to support GCS access.
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    
    # Use the google-cloud-storage client to list files.
    client = storage.Client()  # Ensure credentials are properly configured.
    base_path = f"{gcs_prefix}"
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=base_path))
    
    if not blobs:
        raise ValueError(f"No files found under path {base_path}.")
    
    # Group files by station (assuming the station name is the second-to-last folder in the path).
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

            # 1. Check file size using GCS metadata.
            if blob.size == 0:
                print(f"File is empty: {blob.name}")
                continue  # Skip file if empty.

            # 2. Try to read the Parquet file with Spark.
            try:
                print(f"Attempting to read file: {gcs_uri}")
                df = spark.read.parquet(gcs_uri)
            except AnalysisException as e:
                print(f"Failed to read file: {gcs_uri}, error: {e}")
                continue  # Skip file if reading fails.

            # 3. Validate record count (must have at least one record).
            record_count = df.count()
            if record_count < 1:
                print(f"No records found in file: {gcs_uri}")
                continue
            print(f"Record count: {record_count}")

            # 4. Calculate and print the null ratio for each column.
            for col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                ratio = null_count / record_count
                print(f"Column '{col}': {null_count} nulls, ratio: {ratio:.2%}")

            # 5. Validate schema consistency.
            if reference_schema is None:
                reference_schema = df.schema
            else:
                if df.schema != reference_schema:
                    mismatch_message = compare_schemas(reference_schema, df.schema)
                    # Write the schema mismatch information to the log file.
                    with open(mismatch_log_file, "a") as log_file:
                        log_file.write(f"File: {gcs_uri}\n")
                        log_file.write(mismatch_message + "\n\n")
                    print(f"Schema mismatch in file {gcs_uri} recorded in log.")
    
    print("\nRaw data validation completed!")
    print(f"Please check the schema mismatch log: {mismatch_log_file}")
