# import env first
import os
from dotenv import load_dotenv

env_file = "/opt/airflow/.env.staging"
if os.path.exists(env_file):
    # 本地调试时加载 .env.staging，
    # CI 环境里通常不会挂载这个文件，就会跳过
    load_dotenv(dotenv_path=env_file, override=True)

import shutil
from datetime import datetime, timedelta
import glob
import pandas as pd
import pytest
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from google.cloud import storage

import scripts.load_to_bucket as lb
import config

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

    # 3) Mock get_stations_from_network → 固定返回一个站点
    monkeypatch.setattr(lb, "get_stations_from_network", lambda state: ["ST01", "ST02", "ST03"])

    # 4) Mock download_data → 生成一个小 DataFrame，写成 CSV
    def fake_download_data(url, station, failed_stations=None):
        sample_data = {
            "station": ["ST01", "ST02", "ST03", "ST02"],
            "valid": [
                "2023-01-01 12:00", 
                "2023-01-01 12:30", 
                "2023-01-01 13:00", 
                (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
            ],
            "lon": ["100.0", "110.0", "M", "120.0"],
            "lat": ["50.0", "55.0", "60.0", "M"],
            "tmpf": ["22.0", 24.0, 20.0, "M"],
            "dwpf": ["18.0", "M", 16.0, 20.0],
            "relh": [85.0, "M", 90.0, 80.0],
            "drct": [180, 90, 270, "M"],
            "sknt": [10, 15, 5, "M"],
            "p01i": [0.1, 0.2, 0.0, "M"],
            "alti": [1013, 1015, 1012, "M"],
            "mslp": [1011, 1013, 1010, "M"],
            "vsby": [10, 8, 12, "M"],
            "gust": [15, 20, 10, "M"],
            "skyc1": ["CLR", "FEW", "SCT", "BKN"],
            "skyc2": ["FEW", "SCT", "BKN", "OVC"],
            "skyc3": ["BKN", "OVC", "CLR", "FEW"],
            "skyc4": ["OVC", "FEW", "SCT", "CLR"],
            "skyl1": [0.0, 0.0, 0.0, 0.0],
            "skyl2": [1000, 1500, 1200, "M"],
            "skyl3": [3000, 3500, 4000, "M"],
            "skyl4": [5000, 6000, 7000, "M"],
            "wxcodes": ["RA", "SN", "TS", "M"],
            "ice_accretion_1hr": [0.0, 0.1, 0.0, "M"],
            "ice_accretion_3hr": [0.2, 0.3, 0.0, "M"],
            "ice_accretion_6hr": [0.5, 0.4, 0.0, "M"],
            "peak_wind_gust": [30, 35, 40, "M"],
            "peak_wind_drct": [180, 90, 270, "M"],
            "peak_wind_time": [
                "2023-01-01 12:10", 
                "2023-01-01 12:40", 
                "2023-01-01 13:05", 
                (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M")
            ], 
            "feel": [22, 25, 19, "M"],
            "snowdepth": [0.0, 0.0, "M", 0.0]
        }
        df = pd.DataFrame(sample_data)
        yield df
    monkeypatch.setattr(lb, "download_data", fake_download_data)

    cfg = {"csv_dir": str(csv_dir), 
           "parquet_dir": str(parquet_dir)
           }

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
    stations = lb.get_stations_from_network(config.STATE)
    csv_subdir = os.path.join(cfg["csv_dir"], "2023", "01")
    assert os.path.isdir(csv_subdir), f"Expected directory {csv_subdir}"
    files = os.listdir(csv_subdir)

    for station in stations:
        csv_prefix = f"{station}_202301_"
        assert any(f.startswith(csv_prefix) and f.endswith(".csv")
               for f in files), \
        f"No CSV files with prefix {csv_prefix} in {csv_subdir}, found {files}"

    # ---- convert_parquet ----
    ti_convert = TaskInstance(task=dag.get_task("convert_parquet"), execution_date=exec_date)
    ti_convert.run(ignore_ti_state=True)
    assert ti_convert.state == State.SUCCESS

    # 转换阶段应在 PARQUET_DIR/2023/01 下生成 .parquet 目录
    parquet_subdir = os.path.join(cfg["parquet_dir"], "2023", "01")
    assert os.path.isdir(parquet_subdir), f"Expected directory {parquet_subdir}"

    for station in stations:
        expected_basename = f"{station}_202301_chunk0.parquet"
        expected_parquet_dir = os.path.join(parquet_subdir, expected_basename)
        assert os.path.isdir(expected_parquet_dir)

        # 目录里至少含一个 part-*.parquet
        parts = glob.glob(os.path.join(expected_parquet_dir, "part-*.parquet"))
        assert parts, f"No part file in {expected_parquet_dir}"

    # ---- upload_to_gcs ----
    ti_upload = TaskInstance(task=dag.get_task("upload_to_gcs"), execution_date=exec_date)
    ti_upload.run(ignore_ti_state=True)
    assert ti_upload.state == State.SUCCESS

    client = storage.Client()
    bucket = client.bucket(config.BUCKET_NAME)

    for station in stations:
        blob_prefix = f"{config.GCS_PREFIX}/2023/01/{station}"
        blobs = list(bucket.list_blobs(prefix=blob_prefix))

        names = [b.name for b in blobs]
        expected_blob = f"{config.GCS_PREFIX}/2023/01/{station}/{station}_202301_chunk0.parquet"
        assert expected_blob in names, f"{expected_blob} not found in staging bucket; got {names}"

    # ---- cleanup_local_files ----
    ti_cleanup = TaskInstance(task=dag.get_task("cleanup_local_files"), execution_date=exec_date)
    ti_cleanup.run(ignore_ti_state=True)
    assert ti_cleanup.state == State.SUCCESS

    # cleanup 阶段应删除具体的“2023/01”子目录
    assert not os.path.exists(csv_subdir), f"{csv_subdir} should have been removed"
    assert not os.path.exists(parquet_subdir), f"{parquet_subdir} should have been removed"