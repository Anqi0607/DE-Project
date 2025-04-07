from datetime import datetime
import os
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from create_spark_session import get_spark_session
import config

def transform_raw_data_dynamic(**kwargs):
    """
    根据执行日期构造 raw data 和 bronze 输出目录，
    并读取 Parquet 文件进行数据清洗与转换：
      - 输入路径例如：gs://{BUCKET_NAME}/{GCS_PREFIX}/2023/01
      - 输出路径将 GCS_PREFIX 中的 "test" 替换为 "bronze"，例如：
           gs://{BUCKET_NAME}/METAR/MA/bronze/2023/01

    数据清洗步骤包括：
      - 去除重复记录；
      - 删除不需要的列（如 metar）；
      - 将 valid 字段转换为 timestamp（格式 "yyyy-MM-dd HH:mm"），并将未来时间置为 null；
      - 将数值列（原本以字符串读取）转换为 double；
      - 根据 station 重新分区，并按 station 分区写出数据。
      
    返回转换后的输出路径。
    """
    execution_date = kwargs["data_interval_start"]
    unique_dir = execution_date.strftime("%Y/%m")
    
    # 构造输入路径和输出路径
    raw_dir = f"gs://{config.BUCKET_NAME}/{config.GCS_PREFIX}"
    input_path = os.path.join(raw_dir, unique_dir)
    output_path = input_path.replace("Raw", "Bronze")
    
    print(f"Transforming data from {input_path} to {output_path}")
    
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
    
    # 创建 SparkSession
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
    
    # 转换 valid 字段为 timestamp
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
    
    # 根据 station 重新分区
    df = df.repartition("station")
    
    # 写出结果，按 station 分区
    df.write.mode("overwrite") \
          .partitionBy("station") \
          .parquet(output_path)
    
    print(f"Transformed data written to {output_path}")
    spark.stop()
    
    return output_path
