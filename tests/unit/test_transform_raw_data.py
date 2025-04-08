import os
import glob
import json
from datetime import datetime
import pytest
import io
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# 从 package 中导入 transform_raw_data_dynamic 函数
from metar_etl.transform_raw_data import transform_raw_data_dynamic
import metar_etl.transform_raw_data as trd  # 用于修改 config 参数

# -------------------------------------------------------------------
# Dummy function to simulate Spark reading a Parquet file
# -------------------------------------------------------------------
def dummy_read_parquet(uri, spark):
    """
    Dummy function to simulate reading a Parquet file with Spark.
    Ignores the passed uri and returns a dummy DataFrame that
    contains all expected columns.
    """
    # 构造一个 dummy DataFrame，其中包含两个数据行，所有列均按预期返回（部分字段设为 "M" 模拟缺失）
    data = [
        (
            "TEST1", "2023-01-01 12:00", "100.0", "50.0", "25.0", "20.0",
            "80", "180", "10", "0", "29.92", "1013", "10", "5",
            "CLR", "", "", "", "5.0", "5.0", "5.0", "5.0", "RA",
            "M", "M", "M", "M", "M", "M", "25", "0"
        ),
        (
            "TEST1", "2023-01-01 12:00", "M", "M", "M", "M",
            "M", "M", "M", "M", "M", "M", "M", "M",
            "CLR", "", "", "", "M", "M", "M", "M", "RA",
            "M", "M", "M", "M", "M", "M", "M", "M"
        )
    ]
    columns = [
        "station", "valid", "lon", "lat", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i",
        "alti", "mslp", "vsby", "gust", "skyc1", "skyc2", "skyc3", "skyc4",
        "skyl1", "skyl2", "skyl3", "skyl4", "wxcodes", "ice_accretion_1hr",
        "ice_accretion_3hr", "ice_accretion_6hr", "peak_wind_gust", "peak_wind_drct",
        "peak_wind_time", "feel", "snowdepth"
    ]
    return spark.createDataFrame(data, columns)

# -------------------------------------------------------------------
# Dummy DataFrameReader and DummySparkSession
# -------------------------------------------------------------------
class DummyDataFrameReader:
    def __init__(self, spark):
        self.spark = spark
        self._schema = None
        self._options = {}
    def schema(self, schema):
        self._schema = schema
        return self
    def option(self, key, value):
        self._options[key] = value
        return self
    def parquet(self, uri):
        # 直接调用 dummy_read_parquet，返回 dummy DataFrame
        return dummy_read_parquet(uri, self.spark)

class DummySparkSession:
    """
    Dummy SparkSession，用于模拟 Spark 操作。
    内部使用真正的 SparkSession，但将 read 属性替换为 DummyDataFrameReader，
    以避免实际调用 gs:// 路径读取。
    """
    def __init__(self):
        self._spark = SparkSession.builder.master("local[*]").appName("DummySparkSession").getOrCreate()
        self._dummy_reader = DummyDataFrameReader(self._spark)
    @property
    def read(self):
        return self._dummy_reader
    def stop(self):
        self._spark.stop()

class DummyDataFrameWriter:
    """
    Dummy DataFrameWriter，用于覆盖 DataFrame.write 操作，不进行实际写入。
    """
    def mode(self, mode):
        return self
    def partitionBy(self, col):
        return self
    def parquet(self, path):
        print("Dummy write called with path:", path)

# -------------------------------------------------------------------
# Dummy Storage Client (与前面的保持不变)
# -------------------------------------------------------------------
class DummyBlob:
    def __init__(self, name, size):
        self.name = name
        self.size = size

class DummyBucket:
    def __init__(self):
        self.blobs = []
    def blob(self, blob_path):
        return DummyBlob(blob_path, 100)

class DummyStorageClient:
    def __init__(self, blobs):
        self._blobs = blobs
    def bucket(self, bucket_name):
        dummy_bucket = DummyBucket()
        dummy_bucket.blobs = self._blobs
        return dummy_bucket
    def list_blobs(self, bucket, prefix):
        return [b for b in self._blobs if b.name.startswith(prefix)]

def dummy_urlopen(uri):
    dummy_data = {
        "features": [
            {"properties": {"sid": "TEST1"}},
            {"properties": {"sid": "TEST2"}}
        ]
    }
    class DummyResponse:
        def __init__(self, text):
            self.text = text
        def read(self):
            return self.text.encode("utf-8")
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    return DummyResponse(json.dumps(dummy_data))

def dummy_requests_get(url):
    class DummyReqResponse:
        def __init__(self, text):
            self.text = text
            self.ok = True
            self.status_code = 200
    text = "# This is a comment\ncol1,col2\n1,2\n3,4\n"
    return DummyReqResponse(text)

def dummy_download_data(url, station, failed_stations=None):
    data = "col1,col2\n1,2\n3,4\n"
    df = pd.read_csv(io.StringIO(data))
    yield df

def dummy_get_stations_from_network(state: str):
    return ["TEST1", "TEST2"]

# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------
@pytest.fixture
def dummy_execution_date():
    """Return a fixed data_interval_start date for testing."""
    return datetime(2023, 1, 1)

@pytest.fixture
def dummy_storage_client():
    """Return a DummyStorageClient with one dummy blob."""
    dummy_blobs = [
        DummyBlob("METAR/2023/01/TEST1_202301_chunk0.parquet", 100)
    ]
    return DummyStorageClient(dummy_blobs)

@pytest.fixture
def dummy_spark(monkeypatch):
    """
    Return an instance of DummySparkSession.
    同时，用 DummyDataFrameWriter 覆盖 DataFrame.write 属性，避免实际写入文件。
    """
    ds = DummySparkSession()
    monkeypatch.setattr(DataFrame, "write", property(lambda self: DummyDataFrameWriter()))
    yield ds
    ds.stop()

# -------------------------------------------------------------------
# Unit Test for transform_raw_data_dynamic
# -------------------------------------------------------------------
def test_transform_raw_data_dynamic(monkeypatch, dummy_execution_date, dummy_spark):
    """
    Test transform_raw_data_dynamic:
      - Simulate config parameters.
      - Verify that the output_path is constructed correctly based on data_interval_start.
      - Use dummy SparkSession to simulate reading and writing operations.
    """
    # 模拟 config 参数：假设 GCS_PREFIX 原始值为 "Raw/TEST"，转换后将 "Raw" 替换为 "Bronze"
    monkeypatch.setattr(trd.config, "BUCKET_NAME", "dummy_bucket")
    monkeypatch.setattr(trd.config, "GCS_PREFIX", "Raw/TEST")
    monkeypatch.setattr(trd.config, "TEMP_BUCKET", "dummy_temp_bucket")
    
    # 构造上下文，使用 key "data_interval_start"
    context = {"data_interval_start": dummy_execution_date}
    
    # 预期：
    # raw_dir = gs://dummy_bucket/Raw/TEST
    # unique_dir = "2023/01"
    # input_path = gs://dummy_bucket/Raw/TEST/2023/01
    # output_path = input_path.replace("Raw", "Bronze") => gs://dummy_bucket/Bronze/TEST/2023/01
    expected_output = "gs://dummy_bucket/Bronze/TEST/2023/01"
    
    # 替换 get_spark_session，使其返回 dummy_spark
    monkeypatch.setattr("metar_etl.transform_raw_data.get_spark_session", lambda app_name, temp_bucket="": dummy_spark)
    
    # 调用 transform_raw_data_dynamic
    output_path_result = transform_raw_data_dynamic(**context)
    
    # 验证返回的 output_path 是否符合预期
    assert output_path_result == expected_output, f"Expected {expected_output}, got {output_path_result}"
