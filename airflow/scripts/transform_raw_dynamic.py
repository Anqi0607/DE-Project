# transform_raw_dynamic.py
from datetime import datetime
import os
from scripts.transform_raw_data import transform_raw_data
import config

def transform_raw_dynamic(**kwargs):
    """
    根据执行日期构造 raw data 和 bronze 输出目录，例如：
      对于执行日期 2023-01-01，
      构造输入路径为：gs://{BUCKET_NAME}/{GCS_PREFIX}/2023/01
      输出路径则将 GCS_PREFIX 中的 "test" 替换为 "bronze",
      如：gs://{BUCKET_NAME}/METAR/MA/bronze/2023/01
      
      然后调用 transform_raw_data 进行转换。
    """
    execution_date = kwargs["execution_date"]
    unique_dir = execution_date.strftime("%Y/%m")
    raw_dir = f"gs://{config.BUCKET_NAME}/{config.GCS_PREFIX}"
    input_path = os.path.join(raw_dir, unique_dir)
    output_path = input_path.replace("test", "bronze")
    print(f"Transforming data from {input_path} to {output_path}")
    transform_raw_data(input_path=input_path, output_path=output_path, temp_bucket=config.TEMP_BUCKET)
    return output_path
