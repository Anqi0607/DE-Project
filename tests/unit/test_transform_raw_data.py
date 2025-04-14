import os
import json
from datetime import datetime
import pytest
import io
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# 导入 transform_raw_data_dynamic 函数
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
    # 构造一个 dummy DataFrame，其中包含两行数据，用于测试数据转换各个步骤
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
        # 调用 dummy_read_parquet 返回 DataFrame
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
        self.dummy_writer = None  # 用于保存写入时捕获的 DummyDataFrameWriter
    # 把method包装成属性访问方式，使得可以通过instance.read来调用（无需.read()）来模拟spark.read
    @property
    def read(self):
        return self._dummy_reader
    def stop(self):
        self._spark.stop()

class DummyDataFrameWriter:
    """
    Dummy DataFrameWriter，用于覆盖 DataFrame.write 操作，不进行实际写入，
    同时保存待写入的 DataFrame 到 self.df 供测试验证。
    """
    def __init__(self):
        self.df = None

    def mode(self, mode):
        return self

    def partitionBy(self, col):
        return self

    def parquet(self, path):
        print("Dummy write called with path:", path)
        # 此处不进行实际写入操作
        return

# -------------------------------------------------------------------
# Dummy Storage Client（保留原来的实现）
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

# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------
@pytest.fixture
def dummy_execution_date():
    """Return a fixed data_interval_start date for testing."""
    return datetime(2023, 1, 1)

@pytest.fixture
def dummy_storage_client():
    """Return a DummyStorageClient with一个 dummy blob."""
    dummy_blobs = [
        DummyBlob("METAR/2023/01/TEST1_202301_chunk0.parquet", 100)
    ]
    return DummyStorageClient(dummy_blobs)

@pytest.fixture
def dummy_spark(monkeypatch):
    """
    Return an instance of DummySparkSession.
    同时，利用 monkeypatch 替换 DataFrame.write 属性，使其返回 DummyDataFrameWriter，
    并将写入的 DataFrame 绑定到 writer.df 以供后续验证。
    """
    ds = DummySparkSession()

    # 定义 fake_write 函数，把当前 DataFrame 对象绑定到 DummyDataFrameWriter 中
    def fake_write(self):
        writer = DummyDataFrameWriter()
        writer.df = self  # 这里 self 指当前 DataFrame 对象
        ds.dummy_writer = writer  # 保存到 DummySparkSession 对象上，方便后续测试使用
        return writer

    # 替换 DataFrame.write 属性为自定义的 fake_write 返回的 DummyDataFrameWriter 对象
    monkeypatch.setattr(DataFrame, "write", property(lambda self: fake_write(self)))
    yield ds
    ds.stop()

# -------------------------------------------------------------------
# Unit Test for transform_raw_data_dynamic
# -------------------------------------------------------------------
def test_transform_raw_data_dynamic(monkeypatch, dummy_execution_date, dummy_spark):
    """
    测试 transform_raw_data_dynamic：
      - 模拟 config 参数，验证输出路径构造是否正确。
      - 验证数据转换后写入的 DataFrame 是否符合预期。
    """
    # 模拟 config 参数：假设 GCS_PREFIX 原始值为 "Raw/TEST"，转换后将 "Raw" 替换为 "Bronze"
    monkeypatch.setattr(trd.config, "BUCKET_NAME", "dummy_bucket")
    monkeypatch.setattr(trd.config, "GCS_PREFIX", "Raw/TEST")
    monkeypatch.setattr(trd.config, "TEMP_BUCKET", "dummy_temp_bucket")
    
    # 构造上下文，使用 key "data_interval_start"
    context = {"data_interval_start": dummy_execution_date}
    
    # 预期输出路径
    expected_output = "gs://dummy_bucket/Bronze/TEST/2023/01"
    
    # 替换 get_spark_session 为 dummy_spark
    monkeypatch.setattr("metar_etl.transform_raw_data.get_spark_session", lambda app_name, temp_bucket="": dummy_spark)
    
    # 为防止 transform_raw_data_dynamic 调用 spark.stop() 后关闭 SparkContext，
    # 覆盖 dummy_spark.stop() 为不执行任何操作
    monkeypatch.setattr(dummy_spark, "stop", lambda: None)
    
    # 调用 transform_raw_data_dynamic
    output_path_result = transform_raw_data_dynamic(**context)
    
    # 验证路径构造是否符合预期
    assert output_path_result == expected_output, f"Expected {expected_output}, got {output_path_result}"
    
    # 此时转换过程已调用，写入操作触发 Fake DataFrameWriter，将 DataFrame 保存到 ds.dummy_writer.df 中
    dummy_writer = dummy_spark.dummy_writer
    transformed_df = dummy_writer.df  # 获取捕获到的 DataFrame
    assert transformed_df is not None, "No DataFrame was written."
    
    # 将转换后的 DataFrame 转为 Python 对象列表进行比较
    result_data = [row.asDict() for row in transformed_df.collect()]
    
    # 构造预期结果，根据 transform_raw_data_dynamic 的处理逻辑。
    # 注意：预期结果中的 valid 字段将是一个 datetime 对象，且数字字段 cast 为 double，"M" 已转换为 None。
    expected_data = [
        {
            "station": "TEST1",
            "valid": datetime.strptime("2023-01-01 12:00", "%Y-%m-%d %H:%M"),
            "lon": 100.0, "lat": 50.0, "tmpf": 25.0, "dwpf": 20.0,
            "relh": 80.0, "drct": 180.0, "sknt": 10.0, "p01i": 0.0,
            "alti": 29.92, "mslp": 1013.0, "vsby": 10.0, "gust": 5.0,
            "skyc1": "CLR", "skyc2": "", "skyc3": "", "skyc4": "",
            "skyl1": 5.0, "skyl2": 5.0, "skyl3": 5.0, "skyl4": 5.0,
            "wxcodes": "RA",
            "ice_accretion_1hr": None, "ice_accretion_3hr": None, "ice_accretion_6hr": None,
            "peak_wind_gust": None, "peak_wind_drct": None, "peak_wind_time": None,
            "feel": 25.0, "snowdepth": 0.0
        },
        {
            "station": "TEST1",
            "valid": datetime.strptime("2023-01-01 12:00", "%Y-%m-%d %H:%M"),
            "lon": None, "lat": None, "tmpf": None, "dwpf": None,
            "relh": None, "drct": None, "sknt": None, "p01i": None,
            "alti": None, "mslp": None, "vsby": None, "gust": None,
            "skyc1": "CLR", "skyc2": "", "skyc3": "", "skyc4": "",
            "skyl1": None, "skyl2": None, "skyl3": None, "skyl4": None,
            "wxcodes": "RA",
            "ice_accretion_1hr": None, "ice_accretion_3hr": None, "ice_accretion_6hr": None,
            "peak_wind_gust": None, "peak_wind_drct": None, "peak_wind_time": None,
            "feel": None, "snowdepth": None
        }
    ]
    
    # assert result_data == expected_data
    # 逐行比较转换后 DataFrame 的结果与预期结果
    for res_row, exp_row in zip(result_data, expected_data):
        for key, exp_value in exp_row.items():
            assert res_row[key] == exp_value, f"Mismatch in column '{key}': expected {exp_value}, got {res_row[key]}"
