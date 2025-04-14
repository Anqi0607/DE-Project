# load_parquet_to_bq.py

import os
from pyspark.sql import SparkSession
import config
from .create_spark_session import get_spark_session

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("GCP_BIGQUERY_DATASET")

def load_parquet_to_bigquery(**kwargs):
    """
    Reads data from the specified Parquet file path and writes the data to a BigQuery internal table.
    
    Parameters:
      - input_path: The full GCS path to the data (e.g., gs://<BUCKET_NAME>/<GCS_PREFIX>/2023/01).
                    This path is usually obtained from the XCom of the previous task.
    
    Returns:
      - target_table: The full name of the BigQuery internal table that the data was written to,
                      in the format "{GCP_PROJECT_ID}:{GCP_BIGQUERY_DATASET}.bronze_table".
    
    Notes:
      - The write is performed using the Spark-BigQuery Connector, so you need to include the proper
        dependency package when submitting the Spark job.
      - Spark will use config.TEMP_BUCKET as the temporary GCS bucket.
    """
    ti = kwargs["ti"]
    input_path = ti.xcom_pull(task_ids="transform_raw_data")

    print(f"Reading Parquet files from: {input_path}")
    
    # Create a SparkSession using the pre-defined method
    # will need temp bucket in order to write into big query
    spark = get_spark_session(app_name="Load Bronze Data to BQ Table", temp_bucket=config.TEMP_BUCKET)
    
    # Read the Parquet files (assuming that the data format is correct and no additional transformation is needed)
    df = spark.read.parquet(input_path)
    
    # Define the target BigQuery table with the full name in the format: {GCP_PROJECT_ID}:{GCP_BIGQUERY_DATASET}.bronze_table
    target_table = f"{PROJECT_ID}:{DATASET}.bronze_table"
    print(f"Writing data to BigQuery table: {target_table}")
    
    # Write the data into a BigQuery internal table using the append mode
    # will need temp bucket in order to write into big query
    df.write.format("bigquery") \
        .option("table", target_table) \
        .option("temporaryGcsBucket", config.TEMP_BUCKET) \
        .mode("append") \
        .save()
    
    print("Data successfully loaded into BigQuery.")
    
    spark.stop()
    return target_table
