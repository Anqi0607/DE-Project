import os
import glob
import re
import requests
import io
import json
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError
from typing import Iterator, Optional
import pandas as pd
from google.cloud import storage
from create_spark_session import get_spark_session  # 复用公共 SparkSession 创建函数

SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

def get_stations_from_network(state: str) -> list[str]:
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
    try:
        response = requests.get(url)
        if not response.ok:
            raise ValueError(f"Bad response: HTTP {response.status_code}")
        content = response.text.strip()
        if "<html" in content.lower() or "<!doctype" in content.lower():
            raise ValueError("HTML response received (likely error page)")
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
    os.makedirs(output_dir, exist_ok=True)
    # 构造查询 URL
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
                # 使用 YYYYMM 格式生成文件名
                ym = startts.strftime("%Y%m")
                filename = f"{station}_{ym}_chunk{i}.csv"
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

def convert_to_parquet(csv_dir: str, parquet_dir: str):
    spark = get_spark_session(app_name="CSV_to_Parquet_Spark")
    os.makedirs(parquet_dir, exist_ok=True)
    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_dir}")
        spark.stop()
        return
    for csv_file in csv_files:
        try:
            df = spark.read.option("header", "true") \
                           .option("inferSchema", "false") \
                           .csv(csv_file)
            for col_name in df.columns:
                df = df.withColumn(col_name, df[col_name].cast("string"))
            file_name = os.path.basename(csv_file).replace(".csv", ".parquet")
            parquet_path = os.path.join(parquet_dir, file_name)
            df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
            print(f"Converted: {csv_file} -> {parquet_path}")
        except Exception as e:
            print(f"Failed to convert {csv_file}: {e}")
    spark.stop()

def upload_parquet_to_gcs_with_station_structure(parquet_dir: str, bucket_name: str, gcs_prefix: str = ""):
    """
    将 parquet_dir 目录下的 Parquet 文件上传到 GCS，
    根据文件名（格式：STATION_YYYYMM_chunkX.parquet）提取年份、月份及站点，
    然后将文件上传到路径：
      gs://<bucket>/<gcs_prefix>/<year>/<month>/<station>/<file_name>
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    entries = glob.glob(os.path.join(parquet_dir, "*.parquet"))
    if not entries:
        print(f"No Parquet entries found in {parquet_dir}")
        return

    pattern = re.compile(r"^([A-Z0-9]+)_([0-9]{6}).*\.parquet$", re.IGNORECASE)
    for entry in entries:
        if os.path.isdir(entry):
            base_name = os.path.basename(entry)
            match = pattern.match(base_name)
            if match:
                station = match.group(1)
                ym = match.group(2)
                year = ym[:4]
                month = ym[4:6]
                blob_path = os.path.join(gcs_prefix, year, month, station, base_name).replace("\\", "/")
            else:
                station = base_name.split("_")[0]
                blob_path = os.path.join(gcs_prefix, station, base_name).replace("\\", "/")
            part_files = glob.glob(os.path.join(entry, "part-*"))
            if part_files:
                local_file = part_files[0]
                try:
                    blob = bucket.blob(blob_path)
                    blob.upload_from_filename(local_file)
                    print(f"Uploaded file: {local_file} -> gs://{bucket_name}/{blob_path}")
                except Exception as e:
                    print(f"Failed to upload {local_file}: {e}")
            else:
                print(f"No part file found in directory {entry}")
        else:
            file_name = os.path.basename(entry)
            match = pattern.match(file_name)
            if match:
                station = match.group(1)
                ym = match.group(2)
                year = ym[:4]
                month = ym[4:6]
                blob_path = os.path.join(gcs_prefix, year, month, station, file_name).replace("\\", "/")
            else:
                station = file_name.split("_")[0]
                blob_path = os.path.join(gcs_prefix, station, file_name).replace("\\", "/")
            try:
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(entry)
                print(f"Uploaded: {entry} -> gs://{bucket_name}/{blob_path}")
            except Exception as e:
                print(f"Failed to upload {entry}: {e}")

def cleanup_local_files(csv_dir: str, parquet_dir: str):
    for folder in [csv_dir, parquet_dir]:
        try:
            if os.path.exists(folder):
                shutil.rmtree(folder)
                print(f"Deleted folder: {folder}")
            else:
                print(f"Folder not found: {folder}")
        except Exception as e:
            print(f"Failed to delete {folder}: {e}")
