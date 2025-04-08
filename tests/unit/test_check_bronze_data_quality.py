import os
import glob
import json
from datetime import datetime
import pytest
import io
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# 从 package 中导入待测试的函数
from metar_etl.check_bronze_data_quality import check_bronze_data_quality
import metar_etl.check_bronze_data_quality as cbq  # 用于修改依赖

# -------------------------------------------------------------------
# Dummy Functions and Classes for Spark Simulation
# -------------------------------------------------------------------
def dummy_read_parquet(uri, spark):
    """
    Dummy function to simulate reading a Parquet file with Spark.
    It ignores the passed uri and returns a dummy DataFrame created using the provided SparkSession.
    The returned DataFrame contains all expected columns.
    """
    # 构造一个 dummy DataFrame，包含所有预期列，一行数据（便于统计记录数和验证输出）
    data = [(
        "TEST", "2023-01-01 12:00", "100", "50", "25", "20", "80", "180", "10", "0",
        "29.92", "1013", "10", "5", "CLR", "", "", "", "5", "5", "5", "5", "RA",
        "1", "1", "1", "1", "1", "1", "25", "0"
    )]
    columns = [
        "station", "valid", "lon", "lat", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i",
        "alti", "mslp", "vsby", "gust", "skyc1", "skyc2", "skyc3", "skyc4",
        "skyl1", "skyl2", "skyl3", "skyl4", "wxcodes", "ice_accretion_1hr",
        "ice_accretion_3hr", "ice_accretion_6hr", "peak_wind_gust", "peak_wind_drct",
        "peak_wind_time", "feel", "snowdepth"
    ]
    # 调用 dummy_spark 的 createDataFrame 方法
    return spark.createDataFrame(data, columns)

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
        return dummy_read_parquet(uri, self.spark)

class DummySparkSession:
    """
    Dummy SparkSession to simulate Spark operations.
    内部创建真正的 SparkSession，但将 read 属性替换为 DummyDataFrameReader，
    并提供 createDataFrame 方法代理到底层 SparkSession。
    """
    def __init__(self):
        self._spark = SparkSession.builder.master("local[*]").appName("DummySparkSession").getOrCreate()
        self._dummy_reader = DummyDataFrameReader(self._spark)
    @property
    def read(self):
        return self._dummy_reader
    def createDataFrame(self, data, schema):
        return self._spark.createDataFrame(data, schema)
    def stop(self):
        self._spark.stop()

class DummyDataFrameWriter:
    """
    Dummy DataFrameWriter to override the write operation (avoid actual file I/O).
    """
    def mode(self, mode):
        return self
    def partitionBy(self, col):
        return self
    def parquet(self, path):
        print("Dummy write called with path:", path)

# -------------------------------------------------------------------
# Dummy Storage Client to simulate GCS interactions (保持不变)
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
# Dummy TI for Airflow XCom simulation
# -------------------------------------------------------------------
class DummyTI:
    def __init__(self, output_path):
        self._output_path = output_path
    def xcom_pull(self, task_ids):
        return self._output_path

# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------
@pytest.fixture
def dummy_execution_date():
    """Return a fixed data_interval_start date for testing."""
    return datetime(2023, 1, 1)

@pytest.fixture
def dummy_output_path():
    """
    Return a dummy output path.
    Here it can be an arbitrary string since we override SparkSession.read.parquet.
    """
    return "dummy_output_path"

@pytest.fixture
def dummy_ti(dummy_output_path):
    """Return a dummy TI object for simulating Airflow XCom."""
    return DummyTI(dummy_output_path)

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
# Unit Test for check_bronze_data_quality
# -------------------------------------------------------------------
def test_check_bronze_data_quality_valid(monkeypatch, dummy_ti, dummy_spark):
    """
    Test check_bronze_data_quality when a valid dummy DataFrame is returned.
    The dummy DataFrame will be generated via dummy_read_parquet and includes one row.
    """
    # 替换 get_spark_session，使其返回我们的 dummy_spark
    monkeypatch.setattr("metar_etl.check_bronze_data_quality.get_spark_session", lambda app_name: dummy_spark)
    
    # 覆盖 dummy_spark.read.parquet，使其返回我们构造的 dummy DataFrame。
    monkeypatch.setattr(dummy_spark.read, "parquet", lambda uri: dummy_read_parquet(uri, dummy_spark))
    
    # 构造 Airflow 上下文 kwargs，包含 dummy TI 对象
    context = {"ti": dummy_ti}
    
    # 调用 check_bronze_data_quality 并捕获输出
    # 这里直接调用函数，观察是否正常执行。若需要验证输出，可以增加 capsys 参数捕获输出。
    check_bronze_data_quality(**context)
    
    # 如果执行无异常，即认为测试通过
