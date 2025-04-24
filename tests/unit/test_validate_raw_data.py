import os
import sys
import glob
import json
from datetime import datetime
import pytest
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from scripts.validate_raw_data import validate_raw_data_with_spark, compare_schemas

# -------------------------------------------------------------------
# Dummy Classes and Helper Functions for Simulation
# -------------------------------------------------------------------

class DummyBlob:
    """Dummy blob to simulate a file on GCS."""
    def __init__(self, name, size):
        self.name = name
        self.size = size

class DummyBucket:
    def __init__(self):
        self.blobs = []
    def blob(self, blob_path):
        # 返回一个 dummy blob 对象（这里只做简单模拟）
        return DummyBlob(blob_path, 100)

class DummyStorageClient:
    # 传入的test case中blobs应该是一个由DummyBlob组成的list
    def __init__(self, blobs):
        self._blobs = blobs
    def bucket(self, bucket_name):
        dummy_bucket = DummyBucket()
        dummy_bucket.blobs = self._blobs
        return dummy_bucket
    def list_blobs(self, bucket, prefix):
        return [b for b in self._blobs if b.name.startswith(prefix)]

# -------------------------------------------------------------------
# Dummy SparkSession to override real Spark operations
# -------------------------------------------------------------------
class DummyDataFrameReader:
    def parquet(self, uri):
        """
        Simulate reading a Parquet file.
        Ignores the passed uri and returns a dummy DataFrame.
        """
        data = [
            ("TEST1", "2023-01-01 12:00", "1.0", "M"),
            ("TEST1", "2023-01-01 12:00", None, "3.0")
        ]
        columns = ["station", "valid", "num1", "num2"]
        # 使用本地 SparkSession 来创建 DataFrame（避免调用真实的 GCS 逻辑）
        spark = SparkSession.builder.master("local[*]").appName("DummyReadParquet").getOrCreate()
        df = spark.createDataFrame(data, columns)
        return df

class DummySparkSession:
    def __init__(self):
        self._read = DummyDataFrameReader()
    @property
    def read(self):
        return self._read
    def stop(self):
        pass

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
def dummy_spark():
    """
    Return a DummySparkSession instance.
    """
    return DummySparkSession()

# -------------------------------------------------------------------
# Unit Test Functions
# -------------------------------------------------------------------

# to cover all the logical branches of the validate_raw_data_with_spark functions
def test_validate_raw_data_no_blobs(monkeypatch, dummy_execution_date):
    """
    Test that validate_raw_data_with_spark raises ValueError when no blobs are found.
    """
    dummy_client = DummyStorageClient([])
    monkeypatch.setattr("scripts.validate_raw_data.storage.Client", lambda: dummy_client)
    
    with pytest.raises(ValueError, match="No files found under path"):
        validate_raw_data_with_spark("dummy_bucket", "METAR",
                                     data_interval_start=dummy_execution_date)

def test_validate_raw_data_valid(monkeypatch, dummy_execution_date, dummy_storage_client, dummy_spark, capsys):
    """
    Test validate_raw_data_with_spark when a dummy blob is found and a dummy DataFrame is returned.
    """
    # 准备一个 DummyBlob，其大小为 0 来模拟空文件
    dummy_blob_empty = DummyBlob("METAR/2023/01/empty_file.parquet", 0)
    # 准备一个 DummyBlob，其大小正常但后续通过 DummySparkSession 返回数据，记录数为 2
    dummy_blob_valid = DummyBlob("METAR/2023/01/valid_file.parquet", 100)
    
    # 构造一个包含这两个场景的 blob 列表
    dummy_blobs = [dummy_blob_empty, dummy_blob_valid]
    dummy_client = DummyStorageClient(dummy_blobs)
    monkeypatch.setattr("scripts.validate_raw_data.storage.Client", lambda: dummy_client)
    
    # 使用 DummySparkSession (需要确保其 parquet() 方法返回一个固定 DataFrame)
    dummy_spark = DummySparkSession()
    monkeypatch.setattr("scripts.validate_raw_data.get_spark_session", lambda app_name: dummy_spark)
    
    # 调用待测试函数
    validate_raw_data_with_spark("dummy_bucket", "METAR", data_interval_start=dummy_execution_date)
    
    # Capture output to check for expected messages.
    captured = capsys.readouterr().out

    assert "File is empty:" in captured
    # 断言中检查各列的 null 值比率输出是否正确
    assert "Record count: 2" in captured
    # 针对 num1 列的断言
    assert "Column 'num1': 1 nulls (or 'M'), ratio: 50.00%" in captured
    # 针对 num2 列的断言
    assert "Column 'num2': 1 nulls (or 'M'), ratio: 50.00%" in captured

def test_compare_schemas():
    """
    Test the compare_schemas function with different schemas.
    """
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema1 = StructType([
        StructField("station", StringType(), True),
        StructField("tmpf", DoubleType(), True),
        StructField("test1", DoubleType(), True),

    ])
    schema2 = StructType([
        StructField("station", StringType(), True),
        StructField("tmpf", StringType(), True),
        StructField("test2", DoubleType(), True),
    ])

    diff = compare_schemas(schema1, schema2)
    
    expected_ref_diff = "Fields only in reference schema: test1"
    expected_curr_diff = "Fields only in current schema: test2"
    expected_type_diff = "Field 'tmpf' type mismatch: reference type = DoubleType(), current type = StringType()"

    assert expected_ref_diff in diff
    assert expected_curr_diff in diff
    assert expected_type_diff in diff
