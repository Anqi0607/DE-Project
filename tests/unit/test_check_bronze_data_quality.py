import pytest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import metar_etl.check_bronze_data_quality as cbq

# 修改 dummy_read_parquet，使其返回问题数据
def dummy_read_parquet(uri, spark):
    """
    Dummy function simulating reading a Parquet file.
    Returns a dummy DataFrame containing quality issues:
      - Duplicate records.
      - Some null values in critical columns.
      - Schema issue: 'valid' column remains as StringType.
    """
    # 构造问题数据：包含2行，其中存在重复记录，并且 'valid' 列有 null 值。
    data = [
        (
            "TEST", "2023-01-01 12:00", "100", "50", "25", "20",
            "80", "180", "10", "0", "29.92", "1013", "10", "5",
            "CLR", "CLR2", "CLR3", "CLR4", "5", "5", "5", "5", "RA",
            "1", "1", "1", "1", "1", "1", "25", "0"
        ),
        (
            "TEST", "2023-01-01 12:00", "100", "50", "25", "20",
            "80", "180", "10", "0", "29.92", "1013", "10", "5",
            "CLR", "CLR2", "CLR3", "CLR4", "5", "5", "5", "5", "RA",
            "1", "1", "1", "1", "1", "1", "25", "0"
        ),
        (
            "TEST", None, "100", "50", "25", "20",
            "80", "180", "10", "0", "29.92", "1013", "10", "5",
            "CLR", "CLR2", "CLR3", "CLR4", "5", "5", "5", "5", "RA",
            "1", "1", "1", "1", "1", "1", "25", "0"
        )
    ]
    columns = [
        "station", "valid", "lon", "lat", "tmpf", "dwpf", "relh", "drct",
        "sknt", "p01i", "alti", "mslp", "vsby", "gust", "skyc1", "skyc2",
        "skyc3", "skyc4", "skyl1", "skyl2", "skyl3", "skyl4", "wxcodes",
        "ice_accretion_1hr", "ice_accretion_3hr", "ice_accretion_6hr",
        "peak_wind_gust", "peak_wind_drct", "peak_wind_time", "feel",
        "snowdepth"
    ]
    return spark.createDataFrame(data, columns)

# Dummy TI 用于模拟 Airflow XCom
class DummyTI:
    def __init__(self, output_path):
        self._output_path = output_path
    def xcom_pull(self, task_ids):
        return self._output_path

# DummySparkSession 和 DummyDataFrameReader 维持原有实现，但确保 dummy_read_parquet 被调用
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

# Fixture: dummy_spark，使用真实 SparkSession（或 DummySparkSession）构造一个简单的环境
@pytest.fixture
def dummy_spark(monkeypatch):
    ds = DummySparkSession()
    # use yield instead of return to execute ds.stop() after yield
    yield ds
    ds.stop()

# Fixture: dummy_ti，模拟 Airflow XCom 返回的输出路径
@pytest.fixture
def dummy_ti():
    return DummyTI("dummy_output_path")

# 测试 check_bronze_data_quality，验证各项数据质量问题是否被检测到
def test_check_bronze_data_quality_quality(monkeypatch, dummy_spark, dummy_ti, capsys):
    """
    构造一个 Dummy DataFrame 存在以下问题：
      - 数据中含重复记录（两行数据不同之处只是 valid 列其中一行为 null）。
      - 'valid' 列预期应为 TimestampType，但实际上为 StringType，同时有 50% 的 null 值。
    通过调用 check_bronze_data_quality，检测是否能在输出中发现：
      - 总记录数输出应为 2。
      - 存在重复记录（如果 distinct 行数小于总记录数）。
      - Schema 警告：提示 'valid' 的数据类型错误。
      - 关键列（'valid'）null 比例高的警告。
    """
    # 用 monkeypatch 替换 SparkSession 读取 Parquet 时返回我们的问题 DataFrame
    monkeypatch.setattr(dummy_spark.read, "parquet", lambda uri: dummy_read_parquet(uri, dummy_spark))
    
    # 覆盖 get_spark_session 使得 check_bronze_data_quality 使用 dummy_spark
    monkeypatch.setattr(cbq, "get_spark_session", lambda app_name: dummy_spark)
    
    # 构造上下文: 模拟 Airflow 的 TI 对象，使得 xcom_pull 返回一个 dummy 输出路径（此处值无关紧要）
    context = {"ti": dummy_ti}
    
    # 调用 check_bronze_data_quality
    cbq.check_bronze_data_quality(**context)
    
    # 捕获输出
    captured = capsys.readouterr().out
    # 检查总记录数输出：预期 2 行数据
    assert "Total records: 3" in captured, "Should report 3 records."
    
    # 检查重复记录警告：由于存在两行重复（station, lon, lat, tmpf, dwpf 相同），distinct 可能输出警告
    assert "Warning: Duplicate records exist" in captured, "Should warn about duplicate records."
    
    # 检查 schema 警告：'valid' 列预期为 TimestampType，但此处为 StringType，应输出提示
    assert "expected TimestampType" in captured, "Should warn that 'valid' column type is incorrect."
    
    # 检查关键字段的 null 比例：'valid' 列有1/3条记录为 null,应超过20%，输出警告
    assert "High null ratio in critical column valid" in captured, "Should warn about high null ratio in 'valid' column."
