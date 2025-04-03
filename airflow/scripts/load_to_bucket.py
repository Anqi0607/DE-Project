import os
import json
import glob
import shutil
import requests
import io
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError
from typing import Iterator, Optional
import pandas as pd
from google.cloud import storage

from pyspark.sql import SparkSession
from pyspark import SparkConf

SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

def get_stations_from_network(state: str) -> list[str]:
    """
    获取指定州 ASOS 网络的站点列表。

    参数：
      state (str): 两位州缩写，例如 'IA', 'NY'

    返回：
      List[str]: 站点ID列表
    """
    stations = []
    network = f"{state}_ASOS"
    uri = f"https://mesonet.agron.iastate.edu/geojson/network/{network}.geojson"
    
    try:
        with urlopen(uri) as response:
            jdict = json.load(response)
        for site in jdict["features"]:
            stations.append(site["properties"]["sid"])
    except URLError as e:
        print(f"failed to fetch data for network {network} : {e}")
    except KeyError as e:
        print(f"Unexpected format in response: missing key {e}")
    return stations

def download_data(url: str, station: str, failed_stations: Optional[list] = None) -> Iterator[pd.DataFrame]:
    """
    从给定 URL 下载 CSV 数据，跳过注释行并分块解析。
    如果下载或解析失败，则将站点添加到 failed_stations 中。

    参数：
      url (str): CSV 数据下载 URL
      station (str): 站点ID（用于日志记录）
      failed_stations (list, optional): 用于收集下载失败的站点ID

    Yields:
      pd.DataFrame: CSV 数据块
    """
    try:
        response = requests.get(url)
        if not response.ok:
            raise ValueError(f"Bad response: HTTP {response.status_code}")
        content = response.text.strip()
        # 检查是否返回 HTML（错误页）
        if "<html" in content.lower() or "<!doctype" in content.lower():
            raise ValueError("HTML response received (likely error page)")
        # 去除注释行
        lines = [line for line in content.splitlines() if not line.startswith("#")]
        if len(lines) <= 1:
            raise ValueError("CSV content has no data rows")
        cleaned = "\n".join(lines)
        for chunk in pd.read_csv(io.StringIO(cleaned), chunksize=100000):
            print(f"Loaded chunk for {station}: {len(chunk)} rows × {len(chunk.columns)} cols")
            yield chunk
    except Exception as e:
        print(f"Failed to download or parse data for station {station}: {e}")
        if failed_stations is not None:
            failed_stations.append(station)
        return

def write_csv_to_local(state: str, startts: datetime, endts: datetime, output_dir: str = "csv"):
    """
    获取指定州所有 ASOS 站点的数据，并以 CSV 分块文件形式保存在本地。

    参数：
      state (str): 两位州缩写（如 "IA"）
      startts (datetime): 起始时间（UTC）
      endts (datetime): 截止时间（UTC）
      output_dir (str): 保存 CSV 文件的目录
    """
    os.makedirs(output_dir, exist_ok=True)
    service = (
        SERVICE
        + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"
        + startts.strftime("year1=%Y&month1=%m&day1=%d&")
        + endts.strftime("year2=%Y&month2=%m&day2=%d&")
    )
    stations = get_stations_from_network(state)
    if not stations:
        print(f"No stations found for state: {state}")
        return
    failed_stations = []
    for station in stations:
        uri = f"{service}&station={station}"
        print(f"Downloading: {station}")
        try:
            for i, chunk in enumerate(download_data(uri, station, failed_stations)):
                start_str = startts.strftime("%Y%m%d")
                end_str = endts.strftime("%Y%m%d")
                filename = f"{station}_{start_str}_{end_str}_chunk{i}.csv"
                filepath = os.path.join(output_dir, filename)
                chunk.to_csv(filepath, index=False)
                print(f"Saved: {filename}")
        except Exception as e:
            print(f"Unexpected error while processing {station}: {e}")
            failed_stations.append(station)
    if failed_stations:
        print("\nFailed to process the following stations:")
        for station in failed_stations:
            print(f" - {station}")
    else:
        print("\nAll stations processed successfully!")

def get_spark_session(app_name="CSV_to_Parquet", master="local[*]", temp_bucket=""):
    """
    初始化 SparkSession，并配置 GCS 连接以及禁用 Parquet 字典编码。
    """
    credentials_location = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_location:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS 环境变量未设置。")
    conf = SparkConf() \
        .setMaster(master) \
        .setAppName(app_name) \
        .set("spark.jars", "/opt/airflow/lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.driver.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.executor.extraClassPath", "/opt/airflow/lib/*") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
        .set("spark.sql.parquet.enableVectorizedReader", "false") \
        .set("spark.sql.parquet.enable.dictionary", "false")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # 配置 Hadoop 以支持 GCS
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    return spark

def convert_to_parquet(csv_dir: str, parquet_dir: str):
    """
    用 Spark 将 csv_dir 目录下的所有 CSV 文件转换成 Parquet 文件，
    并将所有列强制转换为 string 类型。转换时采用 coalesce(1)
    合并到一个分区写出，这样每个 CSV 对应的 Parquet 目录中只有一个 part 文件。
    
    参数：
      csv_dir (str): 存放 CSV 文件的本地目录
      parquet_dir (str): 输出 Parquet 文件的目录
    """
    spark = get_spark_session(app_name="CSV_to_Parquet_Spark")
    os.makedirs(parquet_dir, exist_ok=True)
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_dir}")
        spark.stop()
        return
    for csv_file in csv_files:
        try:
            # 读取 CSV，所有列均以字符串加载
            df = spark.read.option("header", "true") \
                           .option("inferSchema", "false") \
                           .csv(csv_file)
            # 显式 cast 每一列为 string
            for col_name in df.columns:
                df = df.withColumn(col_name, df[col_name].cast("string"))
            file_name = os.path.basename(csv_file).replace(".csv", ".parquet")
            parquet_path = os.path.join(parquet_dir, file_name)
            # 使用 coalesce(1) 将数据合并到一个分区写出
            df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
            print(f"Converted: {csv_file} -> {parquet_path}")
        except Exception as e:
            print(f"Failed to convert {csv_file}: {e}")
    spark.stop()

def upload_parquet_to_gcs_with_station_structure(parquet_dir: str, bucket_name: str, gcs_prefix: str = ""):
    """
    将 parquet_dir 目录下的 Parquet 目录上传到 GCS，
    如果遇到 Spark 写出的 Parquet 目录（文件夹），查找其中唯一的 part 文件，
    并将上传的目标对象名称设置为目录名称（例如 PVC_20230101_20230201_chunk0.parquet），
    同时将文件放入 GCS 中对应 station 文件夹下（station 为文件名前第一个下划线前部分）。
    
    参数：
      parquet_dir (str): 存放 Parquet 文件或目录的本地目录
      bucket_name (str): GCS bucket 名称
      gcs_prefix (str): GCS 路径前缀（例如 'asos/md'）
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    entries = glob.glob(os.path.join(parquet_dir, "*.parquet"))
    if not entries:
        print(f"No Parquet entries found in {parquet_dir}")
        return

    for entry in entries:
        # 如果是目录（Spark 写出的 Parquet 目录）
        if os.path.isdir(entry):
            base_name = os.path.basename(entry)
            station = base_name.split("_")[0]
            # 查找目录下唯一的 part 文件（假设只有一个）
            part_files = glob.glob(os.path.join(entry, "part-*"))
            if part_files:
                local_file = part_files[0]
                # 将上传后的文件命名为目录名称
                file_name = base_name
                blob_path = os.path.join(gcs_prefix, station, file_name).replace("\\", "/")
                try:
                    blob = bucket.blob(blob_path)
                    blob.upload_from_filename(local_file)
                    print(f"Uploaded file: {local_file} -> gs://{bucket_name}/{blob_path}")
                except Exception as e:
                    print(f"Failed to upload {local_file}: {e}")
            else:
                print(f"No part file found in directory {entry}")
        else:
            # 如果是单个文件，则按原有逻辑上传
            try:
                file_name = os.path.basename(entry)
                station = file_name.split("_")[0]
                blob_path = os.path.join(gcs_prefix, station, file_name).replace("\\", "/")
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(entry)
                print(f"Uploaded: {entry} -> gs://{bucket_name}/{blob_path}")
            except Exception as e:
                print(f"Failed to upload {entry}: {e}")

def cleanup_local_files(csv_dir: str, parquet_dir: str):
    """
    删除指定 CSV 和 Parquet 文件目录下的所有文件。

    参数：
      csv_dir (str): CSV 文件所在目录
      parquet_dir (str): Parquet 文件所在目录
    """
    for folder in [csv_dir, parquet_dir]:
        try:
            if os.path.exists(folder):
                shutil.rmtree(folder)
                print(f"Deleted folder: {folder}")
            else:
                print(f"Folder not found: {folder}")
        except Exception as e:
            print(f"Failed to delete {folder}: {e}")
