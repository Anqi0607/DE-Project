from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

df = spark.range(10)
print(df.collect())
spark.stop()
