import sys
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

if len(sys.argv) != 3:
    print("Usage: convert_csv_to_parquet_gcp.py <input_csv_dir> <output_parquet_dir>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

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
spark = SparkSession.builder.appName("Transform Raw Data").getOrCreate()
try:
    df = spark.read \
        .schema(schema) \
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

