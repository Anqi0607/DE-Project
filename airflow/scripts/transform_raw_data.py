# transform_raw_data.py
import os
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from create_spark_session import get_spark_session  # 使用公共的 SparkSession 创建函数

def transform_raw_data(input_path: str, output_path: str = "", temp_bucket: str = ""):
    """
    读取 Parquet 文件，对数据进行清洗：
      - 去除重复记录；
      - 删除不需要的列（如 metar）；
      - 将 valid 字段先转换为 timestamp（假定格式 "yyyy-MM-dd HH:mm"），再将未来的时间置为 null；
      - 将数值列（原本以字符串读取）转换为 double；
      - 根据 station 重新分区，并按 station 分区写出结果。
      
    如果未指定 output_path，则将 input_path 中的 "/raw" 替换为 "/bronze"。
    
    参数：
      input_path: 原始数据存储路径（例如：gs://bucket/METAR/MA/raw/2023/01）
      output_path: 转换后数据输出路径（例如：gs://bucket/METAR/MA/bronze/2023/01）
      temp_bucket: （可选）Spark 处理时使用的临时 GCS bucket。
    """
    if not output_path:
        output_path = input_path.replace("/raw", "/bronze")
    
    # 使用公共模块创建 SparkSession
    spark = get_spark_session(app_name="Transform Raw Data", temp_bucket=temp_bucket)
    
    # 定义 schema：所有字段先以字符串方式读取
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
    
    try:
        df = spark.read \
            .schema(schema) \
            .option("recursiveFileLookup", "true") \
            .parquet(input_path)
    except Exception as e:
        print(f"读取 Parquet 文件时发生错误：{e}")
        spark.stop()
        raise
    
    # 将 valid 字段转换为 timestamp（假定格式 "yyyy-MM-dd HH:mm"）
    df = df.withColumn("valid", F.to_timestamp(F.col("valid"), "yyyy-MM-dd HH:mm"))
    
    # 显式将数值列转换为 double
    numeric_columns = [
        "lon", "lat", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i",
        "alti", "mslp", "vsby", "gust", "skyl1", "skyl2", "skyl3", "skyl4",
        "ice_accretion_1hr", "ice_accretion_3hr", "ice_accretion_6hr",
        "peak_wind_gust", "peak_wind_drct", "peak_wind_time", "feel", "snowdepth"
    ]
    for col in numeric_columns:
        df = df.withColumn(col, F.col(col).cast("double"))
    
    # 数据清洗：去重、删除不需要的列（例如 metar）、清洗 valid 列（将未来时间置为 null）
    df = df.dropDuplicates() \
           .drop("metar") \
           .withColumn("valid", F.when(F.col("valid") > F.current_timestamp(), None)
                       .otherwise(F.col("valid")))
    
    df = df.repartition("station")
    
    df.write.mode("overwrite") \
          .partitionBy("station") \
          .parquet(output_path)
    
    print(f"转换后的数据已写入 {output_path}")
    spark.stop()
