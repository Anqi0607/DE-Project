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
from metar_etl.load_to_bucket import (
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
        # 返回编码后的文本
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
    def __init__(self):
        self.bucket_obj = DummyBucket()
    def bucket(self, bucket_name):
        return self.bucket_obj
    def list_blobs(self, bucket, prefix):
        return []  # For unit tests, we simulate no listed blobs

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
    monkeypatch.setattr("metar_etl.load_to_bucket.urlopen", dummy_urlopen)
    stations = get_stations_from_network("DummyState")
    assert stations == ["TEST1", "TEST2"]

def test_download_data(monkeypatch):
    # Replace requests.get with dummy_requests_get
    monkeypatch.setattr("metar_etl.load_to_bucket.requests.get", dummy_requests_get)
    chunks = list(download_data("http://dummy.url", "DummyStation"))
    assert len(chunks) > 0, "Should return at least one DataFrame chunk"
    df = chunks[0]
    assert "col1" in df.columns and "col2" in df.columns
    assert df.shape[0] == 2

def test_write_csv_to_local(tmp_path, monkeypatch):
    """
    Test write_csv_to_local's path construction and file writing logic.
    Use a temporary directory to simulate the output directory.
    """
    monkeypatch.setattr("metar_etl.load_to_bucket.get_stations_from_network", dummy_get_stations_from_network)
    monkeypatch.setattr("metar_etl.load_to_bucket.download_data", dummy_download_data)
    
    state = "DummyState"
    startts = datetime(2023, 1, 1)
    endts = datetime(2023, 1, 2)
    output_dir = str(tmp_path / "csv")
    os.makedirs(output_dir, exist_ok=True)
    write_csv_to_local(state, startts, endts, output_dir)
    
    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    assert len(csv_files) > 0, "Expected CSV files in the output directory."

def test_convert_to_parquet(temp_csv_dir, temp_parquet_dir):
    from metar_etl.load_to_bucket import convert_to_parquet
    convert_to_parquet(temp_csv_dir, temp_parquet_dir)
    parquet_files = glob.glob(os.path.join(temp_parquet_dir, "*.parquet"))
    assert len(parquet_files) > 0, "Parquet file should be created after conversion."

def test_upload_parquet_to_gcs(monkeypatch, tmp_path):
    """
    Test upload_parquet_to_gcs_with_station_structure function by simulating GCS upload.
    Uses DummyStorageClient to mock the behavior.
    """
    # Create a temporary directory to simulate the Parquet file directory.
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()
    dummy_parquet = parquet_dir / "TEST_202301_chunk0.parquet"
    dummy_parquet.write_text("dummy parquet content")
    
    # Replace storage.Client with a lambda returning DummyStorageClient
    monkeypatch.setattr("metar_etl.load_to_bucket.storage.Client", lambda: DummyStorageClient())
    
    bucket_name = "dummy_bucket"
    gcs_prefix = "prefix"
    upload_parquet_to_gcs_with_station_structure(str(parquet_dir), bucket_name, gcs_prefix)
    # Since DummyStorageClient does not simulate listing or further checking, we assume no exceptions means success.

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
    
    from metar_etl.load_to_bucket import cleanup_local_files
    cleanup_local_files(str(csv_dir), str(parquet_dir))
    
    assert not os.path.exists(str(csv_dir)), "CSV directory should be deleted."
    assert not os.path.exists(str(parquet_dir)), "Parquet directory should be deleted."
