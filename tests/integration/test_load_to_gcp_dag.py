import os
import shutil
from datetime import datetime
import glob
import pandas as pd
import pytest
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State

import scripts.load_to_bucket as lb
import config


# 用来收集所有被“上传”到 GCS 的本地文件路径
# 实际存储为 (blob_path, local_path)
uploaded_files = []

@pytest.fixture
def patch_config_and_external(tmp_path, monkeypatch):
    # 1) 创建临时 CSV 和 Parquet 目录
    csv_dir = tmp_path / "csv"
    parquet_dir = tmp_path / "parquet"
    csv_dir.mkdir()
    parquet_dir.mkdir()

    # 2) Monkey-patch 配置
    monkeypatch.setattr(config, "CSV_DIR", str(csv_dir))
    monkeypatch.setattr(config, "PARQUET_DIR", str(parquet_dir))
    monkeypatch.setattr(config, "BUCKET_NAME", "fake-bucket")
    monkeypatch.setattr(config, "GCS_PREFIX", "fake-prefix")

    # 3) Mock get_stations_from_network → 固定返回一个站点
    monkeypatch.setattr(lb, "get_stations_from_network", lambda state: ["AAA"])

    # 4) Mock download_data → 生成一个小 DataFrame，写成 CSV
    def fake_download_data(url, station, failed_stations=None):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        yield df
    monkeypatch.setattr(lb, "download_data", fake_download_data)

    # 5) Mock GCS 客户端，将上传的文件路径记录到 uploaded_files

    # Blob为一个可以对blob（bucket里的文件path）进行操作的对象
    # 因此它需要一个upload_from_filename method
    class DummyBlob:
        def __init__(self, blob_name):
            # 记录远端对象名
            self.blob_name = blob_name
        def upload_from_filename(self, local_path):
            # 将 (远端对象名, 本地文件路径) 记录下来
            uploaded_files.append((self.blob_name, local_path))

    class DummyBucket:
        def blob(self, blob_path):
            # blob_path 是 DAG 中拼好的远端存储路径
            return DummyBlob(blob_path)

    monkeypatch.setattr(
        "google.cloud.storage.Client",
        # create 一个 class C，它有一个 bucket(self, b) 方法，调用时返回 DummyBucket()
        # () 表示立即实例化这个 class C
        lambda: type("C", (), {"bucket": lambda self, b: DummyBucket()})()
    )

    cfg = {"csv_dir": str(csv_dir), 
           "parquet_dir": str(parquet_dir), 
           "uploaded": uploaded_files}

    yield cfg

    # 清理临时目录
    # yield 之后的 code 会在 test 结束后再执行
    shutil.rmtree(str(csv_dir))
    shutil.rmtree(str(parquet_dir))


def test_dag_end_to_end(patch_config_and_external):
    cfg = patch_config_and_external
    # 1) 加载 DAG
    dagbag = DagBag(include_examples=False)
    dag = dagbag.get_dag("METAR_extract_load_raw_data")
    assert dag is not None

    # 2) 定义执行日期
    exec_date = datetime(2023, 1, 1)

    # ---- download_csv ----
    ti_download = TaskInstance(task=dag.get_task("download_csv"), execution_date=exec_date)
    # ignore task 的状态 e.g. 已 successfully run 过，强制重新 run
    ti_download.run(ignore_ti_state=True)
    assert ti_download.state == State.SUCCESS

    # 下载阶段应在 CSV_DIR/2023/01 里生成 .csv 文件
    csv_subdir = os.path.join(cfg["csv_dir"], "2023", "01")
    assert os.path.isdir(csv_subdir), f"Expected directory {csv_subdir}"
    files = os.listdir(csv_subdir)
    assert any(f.startswith("AAA_202301_") and f.endswith(".csv")
               for f in files), \
        f"No CSV files with prefix AAA_202301_ in {csv_subdir}, found {files}"

    # ---- convert_parquet ----
    ti_convert = TaskInstance(task=dag.get_task("convert_parquet"), execution_date=exec_date)
    ti_convert.run(ignore_ti_state=True)
    assert ti_convert.state == State.SUCCESS

    # 转换阶段应在 PARQUET_DIR/2023/01 下生成 .parquet 目录
    parquet_subdir = os.path.join(cfg["parquet_dir"], "2023", "01")
    assert os.path.isdir(parquet_subdir), f"Expected directory {parquet_subdir}"
    parquet_entries = os.listdir(parquet_subdir)
    # 至少有一个以 .parquet 结尾的目录
    assert any(entry.startswith("AAA_202301_") and entry.endswith(".parquet") 
               for entry in parquet_entries), \
        f"No parquet directories found in {parquet_subdir}"

    # ---- upload_to_gcs ----
    ti_upload = TaskInstance(task=dag.get_task("upload_to_gcs"), execution_date=exec_date)
    ti_upload.run(ignore_ti_state=True)
    assert ti_upload.state == State.SUCCESS

    # 上传阶段应至少调用一次 upload_from_filename
    assert cfg["uploaded"], "No files were uploaded to GCS"

    # 并且校验远端 blob_path 和本地路径
    expected_basename = "AAA_202301_chunk0.parquet"
    expected_partquet_subdir = os.path.join(cfg["parquet_dir"], "2023", "01", expected_basename)
    part_files = glob.glob(os.path.join(expected_partquet_subdir, "part-*.parquet"))
    assert part_files, f"No part file found in {parquet_subdir}"
    expected_local = part_files[0]

    for blob_name, local_path in cfg["uploaded"]:
        assert blob_name == f"fake-prefix/2023/01/AAA/{expected_basename}",f"Expected parquet not uploaded"
        assert local_path.startswith(parquet_subdir), f"Local path {local_path} not under {parquet_subdir}"
        assert os.path.exists(local_path), f"Local file {local_path} missing"
        assert local_path == expected_local, f"Expected upload of {expected_local}, but got {local_path}"
    # ---- cleanup_local_files ----
    ti_cleanup = TaskInstance(task=dag.get_task("cleanup_local_files"), execution_date=exec_date)
    ti_cleanup.run(ignore_ti_state=True)
    assert ti_cleanup.state == State.SUCCESS

    # cleanup 阶段应删除具体的“2023/01”子目录
    assert not os.path.exists(csv_subdir), f"{csv_subdir} should have been removed"
    assert not os.path.exists(parquet_subdir), f"{parquet_subdir} should have been removed"