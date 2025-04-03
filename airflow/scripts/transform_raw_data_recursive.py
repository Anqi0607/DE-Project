import os
import json
import logging
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import types
from pyspark.sql import functions as F

def transform_station_data(station_input_path: str, station_output_path: str, temp_bucket: str = "") -> dict:
    """
    Process data for a single station folder. The cleaning steps include:
      1. Replace "M" values with null in all string columns;
      2. Cast specified numeric columns (numeric_columns) from string to double;
      3. Remove duplicates and drop the "metar" column;
      4. Convert the "valid" column to a timestamp with format "yyyy-MM-dd HH:mm", 
         setting any future timestamps to null;
      5. Write the result to station_output_path.
    
    Returns a dictionary with the processing result, including the station name,
    the initial record count, the cleaned record count, and the final schema (in JSON format).
    """
    credentials_location = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    
    conf = SparkConf() \
        .setMaster("local[*]") \
        .setAppName("Transform Station Raw Data") \
        .set("spark.jars", "/opt/airflow/lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.driver.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.executor.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
        .set("spark.sql.parquet.enableVectorizedReader", "false")
    if temp_bucket:
        conf.set("temporaryGcsBucket", temp_bucket)
    
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
    
    try:
        df = spark.read.option("recursiveFileLookup", "true").parquet(station_input_path)
    except Exception as e:
        print(f"Error reading Parquet files for station [{station_input_path}]: {e}")
        sc.stop()
        raise

    initial_count = df.count()
    print(f"Station [{station_input_path}] initial record count: {initial_count}")
    
    # Replace "M" with null in all string-type columns
    for field in df.schema.fields:
        if isinstance(field.dataType, types.StringType):
            df = df.withColumn(field.name, F.when(F.col(field.name) == "M", None)
                                            .otherwise(F.col(field.name)))
            
    numeric_columns = [
        "lat", "lon", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i", "alti", "mslp", "vsby", "gust",
        "skyl1", "skyl2", "skyl3", "skyl4",
        "ice_accretion_1hr", "ice_accretion_3hr", "ice_accretion_6hr",
        "peak_wind_gust", "peak_wind_drct", "peak_wind_time", "feel",
        "snowdepth"
    ]
    # Cast specified columns to double
    for col_name in numeric_columns:
        df = df.withColumn(col_name, F.col(col_name).cast("double"))
    
    # Remove duplicates, drop the "metar" column, convert "valid" to timestamp and set future timestamps to null
    df = df.dropDuplicates() \
           .drop("metar") \
           .withColumn("valid", F.to_timestamp(F.col("valid"), "yyyy-MM-dd HH:mm")) \
           .withColumn("valid", F.when(F.col("valid") > F.current_timestamp(), None)
                                     .otherwise(F.col("valid")))
    
    cleaned_count = df.count()
    print(f"Station [{station_input_path}] cleaned record count: {cleaned_count}")
    
    print(f"Station [{station_input_path}] final schema:")
    df.printSchema()
    final_schema = df.schema.json()
    
    # Write the transformed data to station_output_path
    df.write.mode("overwrite").parquet(station_output_path)
    print(f"Transformed data for station [{station_input_path}] written to {station_output_path}")
    
    sc.stop()
    
    station = station_input_path.rstrip("/").split("/")[-1]
    result = {
        "station": station,
        "initial_count": initial_count,
        "cleaned_count": cleaned_count,
        "final_schema": final_schema
    }
    return result

def transform_raw_data(bucket_name: str, gcs_prefix: str, output_prefix: str = "", temp_bucket: str = ""):
    """
    Read all files under the specified gcs_prefix in the bucket,
    group them by station, process each station individually, and log the processing results in JSON format.
    
    Args:
      bucket_name (str): Name of the GCS bucket (e.g., "dev-project-bucket-pebbles")
      gcs_prefix (str): Data prefix (e.g., "METAR/MA/raw")
      output_prefix (str): Output prefix (e.g., "METAR/MA/bronze"; if empty, it will be auto-replaced)
      temp_bucket (str): (Optional) Temporary GCS bucket used during Spark processing.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(client.list_blobs(bucket, prefix=gcs_prefix))
    if not blobs:
        raise ValueError(f"No files found under path {gcs_prefix} in bucket {bucket_name}.")
    
    files_by_station = {}
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            parts = blob.name.split("/")
            station = parts[-2]
            files_by_station.setdefault(station, []).append(blob.name)
    
    print("Stations found:", list(files_by_station.keys()))
    
    if not output_prefix:
        output_prefix = gcs_prefix.replace("test", "test_bronze")
    print("output_prefix:", output_prefix)
    
    results = []
    for station, file_list in files_by_station.items():
        station_input_path = f"gs://{bucket_name}/{gcs_prefix}/{station}"
        station_output_path = f"gs://{bucket_name}/{output_prefix}/{station}"
        print(f"Processing station [{station}]")
        result = transform_station_data(station_input_path, station_output_path, temp_bucket=temp_bucket)
        results.append(result)
    
    # About logging:
    # If run job on GCP, we can use the Logging module to write the transform result into a location
    # and use Cloud Logging to monitor and analyze 

    # Use logging module to log the processing results to a log file
    log_dir = os.environ.get("LOG_DIR", "/opt/airflow/logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    results_log_file = os.path.join(log_dir, "raw_to_bronze.log")
    
    # Configure logger
    logger = logging.getLogger("ProcessingResultsLogger")
    logger.setLevel(logging.INFO)
    # Create file handler
    fh = logging.FileHandler(results_log_file)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    # Avoid adding duplicate handlers
    if not logger.handlers:
        logger.addHandler(fh)
    
    logger.info("Processing Results Log")
    logger.info(json.dumps(results, indent=2))
    
    print(f"All processing results have been logged to {results_log_file}")
