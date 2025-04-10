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


from metar_etl.validate_raw_data import validate_raw_data_with_spark, compare_schemas

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
    def __init__(self, blobs):
        self._blobs = blobs
    def bucket(self, bucket_name):
        dummy_bucket = DummyBucket()
        dummy_bucket.blobs = self._blobs
        return dummy_bucket
    def list_blobs(self, bucket, prefix):
        return [b for b in self._blobs if b.name.startswith(prefix)]

def dummy_urlopen(uri):
    """Simulate urlopen returning JSON data."""
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
    """Simulate requests.get returning a simple CSV text."""
    class DummyReqResponse:
        def __init__(self, text):
            self.text = text
            self.ok = True
            self.status_code = 200
    text = "# This is a comment\ncol1,col2\n1,2\n3,4\n"
    return DummyReqResponse(text)

def dummy_download_data(url, station, failed_stations=None):
    """Simulate download_data by returning a generator yielding a DataFrame."""
    data = "col1,col2\n1,2\n3,4\n"
    df = pd.read_csv(io.StringIO(data))
    yield df

def dummy_get_stations_from_network(state: str):
    """Simulate get_stations_from_network returning a fixed station list."""
    return ["TEST1", "TEST2"]

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
def test_validate_raw_data_no_blobs(monkeypatch, dummy_execution_date):
    """
    Test that validate_raw_data_with_spark raises ValueError when no blobs are found.
    """
    dummy_client = DummyStorageClient([])
    monkeypatch.setattr("metar_etl.validate_raw_data.storage.Client", lambda: dummy_client)
    
    with pytest.raises(ValueError, match="No files found under path"):
        validate_raw_data_with_spark("dummy_bucket", "METAR", "DummyState",
                                     data_interval_start=dummy_execution_date)

def test_validate_raw_data_valid(monkeypatch, dummy_execution_date, dummy_storage_client, dummy_spark, capsys):
    """
    Test validate_raw_data_with_spark when a dummy blob is found and a dummy DataFrame is returned.
    """
    # Replace storage.Client with dummy_storage_client.
    monkeypatch.setattr("metar_etl.validate_raw_data.storage.Client", lambda: dummy_storage_client)
    # Replace get_spark_session with a lambda that returns our DummySparkSession.
    monkeypatch.setattr("metar_etl.validate_raw_data.get_spark_session", lambda app_name: dummy_spark)
    
    # Call the function
    validate_raw_data_with_spark("dummy_bucket", "METAR", "DummyState", data_interval_start=dummy_execution_date)
    
    # Capture output to check for expected messages.
    captured = capsys.readouterr().out
    assert "Record count:" in captured
    assert "Raw data validation completed!" in captured

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
