from datetime import datetime
import os
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from .create_spark_session import get_spark_session
import config

def transform_raw_data_dynamic(**kwargs):
    """
    Construct raw data and bronze output directories based on the execution date,
    and read Parquet files to perform data cleaning and transformation:
      - For example, for an execution date of 2023-01-01,
        the input path is constructed as:
          gs://{BUCKET_NAME}/{GCS_PREFIX}/2023/01
      - The output path is obtained by replacing "Raw" with "Bronze" in GCS_PREFIX,
        e.g., gs://{BUCKET_NAME}/METAR/MA/Bronze/2023/01

    Data cleaning steps include:
      - Removing duplicate records;
      - Dropping unnecessary columns (e.g., metar);
      - Converting the 'valid' column to a timestamp (format "yyyy-MM-dd HH:mm")
        and setting any future timestamps to null;
      - Casting numeric columns (initially read as strings) to double;
      - Repartitioning the data by 'station' and writing the result partitioned by station.
      
    Returns the output path of the transformed data.
    """
    # Get execution_date from the context (using data_interval_start instead of execution_date)
    execution_date = kwargs["data_interval_start"]
    unique_dir = execution_date.strftime("%Y/%m")
    
    # Construct input and output paths
    raw_dir = f"gs://{config.BUCKET_NAME}/{config.GCS_PREFIX}"
    input_path = os.path.join(raw_dir, unique_dir)
    output_path = input_path.replace("Raw", "Bronze")
    
    print(f"Transforming data from {input_path} to {output_path}")
    
    # Define schema: all fields are initially read as strings
    schema = StructType([
        StructField("station", StringType(), True),
        StructField("valid", StringType(), True),
        StructField("lon", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("tmpf", StringType(), True),
        StructField("dwpf", StringType(), True),
        StructField("relh", StringType(), True),
        StructField("drct", StringType(), True),
        StructField("sknt", StringType(), True),
        StructField("p01i", StringType(), True),
        StructField("alti", StringType(), True),
        StructField("mslp", StringType(), True),
        StructField("vsby", StringType(), True),
        StructField("gust", StringType(), True),
        StructField("skyc1", StringType(), True),
        StructField("skyc2", StringType(), True),
        StructField("skyc3", StringType(), True),
        StructField("skyc4", StringType(), True),
        StructField("skyl1", StringType(), True),
        StructField("skyl2", StringType(), True),
        StructField("skyl3", StringType(), True),
        StructField("skyl4", StringType(), True),
        StructField("wxcodes", StringType(), True),
        StructField("ice_accretion_1hr", StringType(), True),
        StructField("ice_accretion_3hr", StringType(), True),
        StructField("ice_accretion_6hr", StringType(), True),
        StructField("peak_wind_gust", StringType(), True),
        StructField("peak_wind_drct", StringType(), True),
        StructField("peak_wind_time", StringType(), True),
        StructField("feel", StringType(), True),
        StructField("snowdepth", StringType(), True)
    ])
    
    # Create a SparkSession
    spark = get_spark_session(app_name="Transform Raw Data", temp_bucket=config.TEMP_BUCKET)
    
    try:
        df = spark.read \
            .schema(schema) \
            .option("recursiveFileLookup", "true") \
            .parquet(input_path)
    except Exception as e:
        print(f"Error reading Parquet files: {e}")
        spark.stop()
        raise

    # We already know that M refers to missing value so convert to null
    df = df.replace("M", None)

    # Convert the 'valid' column to timestamp (assuming format "yyyy-MM-dd HH:mm")
    df = df.withColumn("valid", F.to_timestamp(F.col("valid"), "yyyy-MM-dd HH:mm"))
    
    # Explicitly cast numeric columns to double
    numeric_columns = [
        "lon", "lat", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i",
        "alti", "mslp", "vsby", "gust", "skyl1", "skyl2", "skyl3", "skyl4",
        "ice_accretion_1hr", "ice_accretion_3hr", "ice_accretion_6hr",
        "peak_wind_gust", "peak_wind_drct", "peak_wind_time", "feel", "snowdepth"
    ]
    for col in numeric_columns:
        df = df.withColumn(col, F.col(col).cast("double"))
    
    # Data cleaning: remove duplicate records, drop unnecessary columns (e.g., metar),
    # and clean the 'valid' column by setting future timestamps to null.
    df = df.dropDuplicates() \
           .drop("metar") \
           .withColumn("valid", F.when(F.col("valid") > F.current_timestamp(), None)
                       .otherwise(F.col("valid")))
    
    # Repartition the data by 'station'
    df = df.repartition("station")
    
    # Write the result partitioned by 'station'
    df.write.mode("overwrite") \
          .partitionBy("station") \
          .parquet(output_path)
    
    print(f"Transformed data written to {output_path}")
    spark.stop()
    
    return output_path
