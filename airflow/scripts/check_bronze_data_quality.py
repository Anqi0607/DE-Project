from pyspark.sql import functions as F
from .create_spark_session import get_spark_session

def check_bronze_data_quality(**kwargs):
    """
    Perform data quality checks on Bronze layer data.
    
    Validations include:
      1. Check if the dataset is empty.
      2. Check for duplicate records.
      3. Verify that the actual schema matches the expected schema:
           - "station" should be StringType
           - "valid" should be TimestampType
           - Expected numeric columns (e.g., "lon", "lat", "tmpf", "dwpf", "relh", "drct", "sknt", "p01i",
             "alti", "mslp", "vsby", "gust", "skyl1", "skyl2", "skyl3", "skyl4",
             "ice_accretion_1hr", "ice_accretion_3hr", "ice_accretion_6hr",
             "peak_wind_gust", "peak_wind_drct", "peak_wind_time", "feel", "snowdepth")
             should be DoubleType;
      4. Check the null ratio for critical columns (e.g., "station", "valid").
    
    The output_path is obtained from the XCom of the previous task (task_id="transform_raw_data").
    
    Args:
      **kwargs: Airflow context variables.
    """
    # Retrieve output_path from XCom of the previous task
    ti = kwargs["ti"]
    output_path = ti.xcom_pull(task_ids="transform_raw_data")
    
    # Create SparkSession for data quality check
    spark = get_spark_session(app_name="Bronze Data Quality Check")
    
    try:
        # Read the Bronze layer data
        df = spark.read.parquet(output_path)
    except Exception as e:
        print(f"Error reading Bronze data from {output_path}: {e}")
        spark.stop()
        raise
    
    # 1. Check if the dataset is empty
    record_count = df.count()
    if record_count == 0:
        print(f"Data quality check failed: No records found in {output_path}!")
        spark.stop()
        return
    else:
        print(f"Total records: {record_count}")
    
    # 2. Check for duplicate records by comparing distinct count with total count
    distinct_count = df.distinct().count()
    if distinct_count < record_count:
        print(f"Warning: Duplicate records exist. Distinct count: {distinct_count}, Total count: {record_count}")
    else:
        print("No duplicate records found.")
    
    # 3. Verify that the actual schema matches the expected schema.
    # Expected schema: key is column name, value is expected data type (only the type name)
    expected_schema = {
        "station": "StringType",
        "valid": "TimestampType",
        "lon": "DoubleType",
        "lat": "DoubleType",
        "tmpf": "DoubleType",
        "dwpf": "DoubleType",
        "relh": "DoubleType",
        "drct": "DoubleType", 
        "sknt": "DoubleType",
        "p01i": "DoubleType",
        "alti": "DoubleType",
        "mslp": "DoubleType",
        "vsby": "DoubleType",
        "gust": "DoubleType",
        "skyc1": "StringType",  # Keep original string
        "skyc2": "StringType",
        "skyc3": "StringType",
        "skyc4": "StringType",
        "skyl1": "DoubleType",
        "skyl2": "DoubleType",
        "skyl3": "DoubleType",
        "skyl4": "DoubleType",
        "wxcodes": "StringType",
        "ice_accretion_1hr": "DoubleType",
        "ice_accretion_3hr": "DoubleType",
        "ice_accretion_6hr": "DoubleType",
        "peak_wind_gust": "DoubleType",
        "peak_wind_drct": "DoubleType",
        "peak_wind_time": "DoubleType",  # Based on transformation, should be double
        "feel": "DoubleType",
        "snowdepth": "DoubleType"
    }
    actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
    for col, exp_type in expected_schema.items():
        if col in actual_schema:
            if not actual_schema[col].startswith(exp_type):
                print(f"Data type warning: Column {col} is {actual_schema[col]}, expected {exp_type}.")
        else:
            print(f"Missing data: Column {col} not found in the dataset.")
    
    # 4. Check the null ratio for critical columns (e.g., station and valid)
    critical_columns = ["station", "valid"]
    # Construct aggregation expressions to compute null counts in one pass
    agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in critical_columns]
    null_counts = df.agg(*agg_exprs).collect()[0].asDict()
    
    for col in critical_columns:
        null_count = null_counts[col]
        ratio = null_count / record_count
        print(f"Column '{col}': {null_count} nulls, ratio: {ratio:.2%}")
        if ratio > 0.2:
            print(f"Warning: High null ratio in critical column {col}: {ratio:.2%}")
    
    print("\nBronze data quality check completed!")
    spark.stop()
