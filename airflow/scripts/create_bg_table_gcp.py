import sys
from pyspark.sql import SparkSession

input_path = sys.argv[1]
project_id = sys.argv[2]
dataset = sys.argv[3]
temp_bucket = sys.argv[4]

spark = SparkSession.builder.appName("Creat BQ table").getOrCreate()

try:
    df = spark.read.parquet(input_path)
except Exception as e:
    print(f"Error reading Parquet files: {e}")
    spark.stop()
    raise

target_table = f"{project_id}:{dataset}.bronze_table_dataproc"
print(f"Writing data to BigQuery table: {target_table}")

# Write the data into a BigQuery internal table using the append mode
# will need temp bucket in order to write into big query
df.write.format("bigquery") \
    .option("table", target_table) \
    .option("temporaryGcsBucket", temp_bucket) \
    .mode("append") \
    .save()

print("Data successfully loaded into BigQuery.")

spark.stop()

