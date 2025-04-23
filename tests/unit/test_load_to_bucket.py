# tests/unit/test_load_to_bucket.py
import os
import io
import sys
import glob
import json
from datetime import datetime
import tempfile
import pytest
import pandas as pd

# 将项目根目录下安装的 package 导入
from scripts.load_to_bucket import (
    get_stations_from_network,
    download_data,
    write_csv_to_local,
    convert_to_parquet,
    upload_parquet_to_gcs_with_station_structure,
    cleanup_local_files
)
from google.cloud import storage

# -------------------------------------------------------------------
# Dummy objects and helper functions to simulate external dependencies
# -------------------------------------------------------------------

class DummyResponse:
    """Dummy response for simulating urlopen."""
    def __init__(self, text):
        self.text = text
    def read(self):
        # return bytes
        return self.text.encode("utf-8")
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

def dummy_urlopen(uri):
    """Simulate urlopen returning JSON data."""
    dummy_data = {
        "features": [
            {"properties": {"sid": "TEST1"}},
            {"properties": {"sid": "TEST2"}}
        ]
    }
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
    """Simulate download_data by returning a generator yielding a DataFrame with two rows."""
    data = "col1,col2\n1,2\n3,4\n"
    df = pd.read_csv(io.StringIO(data))
    yield df

def dummy_get_stations_from_network(state: str):
    """Simulate get_stations_from_network returning a fixed station list."""
    return ["TEST1", "TEST2"]

# Dummy classes for simulating GCS upload
class DummyBlob:
    def __init__(self, name):
        self.name = name
        self.uploaded = False
    def upload_from_filename(self, filename):
        self.uploaded = True

class DummyBucket:
    def __init__(self):
        self.blobs = {}
    def blob(self, blob_path):
        dummy = DummyBlob(blob_path)
        self.blobs[blob_path] = dummy
        return dummy

class DummyStorageClient:
    def __init__(self, blobs=None):
        if blobs is None:
            blobs = []
        self.bucket_obj = DummyBucket()
        self.bucket_obj.blobs = blobs
    def bucket(self, bucket_name):
        return self.bucket_obj
    def list_blobs(self, bucket, prefix):
        return []

# -------------------------------------------------------------------
# Fixtures for temporary directories if needed
# -------------------------------------------------------------------

@pytest.fixture
def temp_csv_dir(tmp_path):
    """Create a temporary CSV directory with a sample CSV file for testing."""
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    sample_csv = csv_dir / "TEST_202301_chunk0.csv"
    sample_data = "col1,col2\n1,2\n3,4\n"
    sample_csv.write_text(sample_data)
    return str(csv_dir)

@pytest.fixture
def temp_parquet_dir(tmp_path):
    """Create a temporary Parquet directory."""
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()
    return str(parquet_dir)

# -------------------------------------------------------------------
# Unit test functions
# -------------------------------------------------------------------

def test_get_stations_from_network(monkeypatch):
    # Replace urlopen with dummy_urlopen within the package metar_etl.load_to_bucket
    monkeypatch.setattr("scripts.load_to_bucket.urlopen", dummy_urlopen)
    stations = get_stations_from_network("DummyState")
    assert stations == ["TEST1", "TEST2"]

def test_download_data(monkeypatch):
    # Replace requests.get with dummy_requests_get
    monkeypatch.setattr("scripts.load_to_bucket.requests.get", dummy_requests_get)
    chunks = list(download_data("http://dummy.url", "DummyStation"))
    assert len(chunks) > 0, "Should return at least one DataFrame chunk"
    df = chunks[0]
    assert "col1" in df.columns and "col2" in df.columns
    # number of rows should be 2
    assert df.shape[0] == 2

def test_write_csv_to_local(tmp_path, monkeypatch):
    """
    Test write_csv_to_local's path construction and file writing logic.
    Use a temporary directory to simulate the output directory.
    """
    monkeypatch.setattr("scripts.load_to_bucket.get_stations_from_network", dummy_get_stations_from_network)
    monkeypatch.setattr("scripts.load_to_bucket.download_data", dummy_download_data)
    
    state = "DummyState"
    startts = datetime(2023, 1, 1)
    endts = datetime(2023, 1, 2)
    output_dir = str(tmp_path / "csv")
    os.makedirs(output_dir, exist_ok=True)
    write_csv_to_local(state, startts, endts, output_dir)
    
    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    assert len(csv_files) > 0, "Expected CSV files in the output directory."

    expected_files = {"TEST1_202301_chunk0.csv", "TEST2_202301_chunk0.csv"}
    assert expected_files.issubset(set(csv_files)), f"Expected files {expected_files}, but got: {csv_files}" 

def test_convert_to_parquet(temp_csv_dir, temp_parquet_dir):

    convert_to_parquet(temp_csv_dir, temp_parquet_dir)
    parquet_files = glob.glob(os.path.join(temp_parquet_dir, "*.parquet"))
    assert len(parquet_files) > 0, "Parquet file should be created after conversion."

    expected_file = "TEST_202301_chunk0.parquet"
    actual_files = [os.path.basename(f) for f in parquet_files]
    assert expected_file in actual_files, f"Expected {expected_file} in {actual_files}"

def test_upload_parquet_to_gcs(monkeypatch, tmp_path):
    """
    Test the upload_parquet_to_gcs_with_station_structure function
    by checking that DummyBlob objects have their uploaded attribute set to True.
    """
    # 创建一个临时目录模拟 Parquet 文件目录
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()
    # 创建一个 dummy parquet 文件（内容不重要，仅用来触发上传）
    dummy_parquet = parquet_dir / "TEST_202301_chunk0.parquet"
    dummy_parquet.write_text("dummy parquet content")
    
    # 创建一个 DummyStorageClient 实例，并保存到变量 dummy_storage
    dummy_storage = DummyStorageClient([])
    
    # 替换 storage.Client 使其返回我们的 dummy_storage
    monkeypatch.setattr("scripts.load_to_bucket.storage.Client", lambda: dummy_storage)
    
    bucket_name = "dummy_bucket"
    gcs_prefix = "prefix"
    
    # 调用上传函数，上传过程中会调用 dummy_storage.bucket(...).blob(...) 生成 DummyBlob 对象，
    # 并且 DummyBlob.upload_from_filename() 会将 uploaded 设为 True。
    upload_parquet_to_gcs_with_station_structure(str(parquet_dir), bucket_name, gcs_prefix)
    
    # 获取 DummyBucket 对象，并检查 blobs 是否被设置为 uploaded=True
    bucket = dummy_storage.bucket(bucket_name)
    # 遍历 bucket.blobs，确保每个 DummyBlob 的 uploaded 属性都为 True
    for blob in bucket.blobs:
        assert blob.uploaded, f"Blob '{blob.name}' was not marked as uploaded."

def test_cleanup_local_files(tmp_path):
    """
    Test cleanup_local_files by creating temporary directories and files,
    then verifying that they are deleted successfully.
    """
    csv_dir = tmp_path / "csv"
    parquet_dir = tmp_path / "parquet"
    csv_dir.mkdir()
    parquet_dir.mkdir()
    (csv_dir / "dummy.txt").write_text("dummy")
    (parquet_dir / "dummy.txt").write_text("dummy")
    
    cleanup_local_files(str(csv_dir), str(parquet_dir))
    
    assert not os.path.exists(str(csv_dir)), "CSV directory should be deleted."
    assert not os.path.exists(str(parquet_dir)), "Parquet directory should be deleted."