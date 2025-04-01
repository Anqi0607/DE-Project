from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("simple-count").getOrCreate()

# 创建一个包含 1 到 10 的列表并并行化为 RDD
data = spark.sparkContext.parallelize(range(1, 11))

# 显示总数
print("Count:", data.count())

# 显示平方数
squares = data.map(lambda x: x * x).collect()
print("Squares:", squares)

spark.stop()
