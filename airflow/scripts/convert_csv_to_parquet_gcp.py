import sys
import os
import glob
from pyspark.sql import SparkSession

if len(sys.argv) != 3:
    print("Usage: convert_csv_to_parquet_gcp.py <input_csv_dir> <output_parquet_dir>")
    sys.exit(1)

input_csv_dir = sys.argv[1]
output_parquet_dir = sys.argv[2]

spark = SparkSession.builder.appName("CSV_to_Parquet_Spark").getOrCreate()

# 使用 Spark 的 GCS 通配符读取 CSV 文件
csv_files_path = os.path.join(input_csv_dir, "*.csv")
df = spark.read.option("header", "true").option("inferSchema", "false").csv(csv_files_path)

# 所有字段转 string
for col_name in df.columns:
    df = df.withColumn(col_name, df[col_name].cast("string"))

# 写入 GCS Parquet（合并一个文件）
df.coalesce(1).write.mode("overwrite").parquet(output_parquet_dir)

spark.stop()