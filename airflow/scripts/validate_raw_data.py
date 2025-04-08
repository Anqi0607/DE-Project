import os
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import sum, when, col
from google.cloud import storage
from .create_spark_session import get_spark_session

def compare_schemas(ref_schema, curr_schema):
    """
    Compare two schemas and return detailed differences as a string.
    """
    lines = []
    lines.append("Schema mismatch detected!")
    lines.append("Reference schema:")
    lines.append(str(ref_schema))
    lines.append("Current schema:")
    lines.append(str(curr_schema))
    
    # Build dictionaries mapping field names to field objects for both schemas.
    ref_fields = {field.name: field for field in ref_schema.fields}
    curr_fields = {field.name: field for field in curr_schema.fields}
    
    # Fields that exist only in the reference schema.
    only_in_ref = set(ref_fields.keys()) - set(curr_fields.keys())
    # Fields that exist only in the current schema.
    only_in_curr = set(curr_fields.keys()) - set(ref_fields.keys())
    
    if only_in_ref:
        lines.append("Fields only in reference schema: " + ", ".join(only_in_ref))
    if only_in_curr:
        lines.append("Fields only in current schema: " + ", ".join(only_in_curr))
    
    # Compare data types for common fields.
    common_fields = set(ref_fields.keys()).intersection(set(curr_fields.keys()))
    for field_name in common_fields:
        ref_type = ref_fields[field_name].dataType
        curr_type = curr_fields[field_name].dataType
        if ref_type != curr_type:
            lines.append(f"Field '{field_name}' type mismatch: reference type = {ref_type}, current type = {curr_type}")
    
    return "\n".join(lines)

def validate_raw_data_with_spark(bucket_name: str, gcs_prefix: str, state: str, **kwargs):
    """
    Use PySpark to read raw Parquet files from GCS and validate them on a monthly basis.
    
    Validations include:
      1. Checking if the file can be successfully read.
      2. Checking file size via GCS metadata (must not be 0).
      3. Checking total record count (must have at least one record).
      4. Calculating the null value ratio for each column (null or "M").
      5. Schema consistency across files.
    
    The function filters files based on the execution_date provided by Airflow.
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        gcs_prefix (str): The GCS path prefix (e.g., "METAR").
        state (str): State code, e.g., "ACK" or "IA".
    """
    # Get execution_date from the context (passed by Airflow)
    execution_date = kwargs.get("data_interval_start")
    target_year = execution_date.strftime("%Y")
    target_month = execution_date.strftime("%m")
    
    # Get the path of the credentials file
    credentials_location = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_location:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    # Create SparkSession using get_spark_session (assumed to have optimized settings)
    spark = get_spark_session(app_name="Validate METAR Raw Data")
    
    # Use the google-cloud-storage client to list files under the target year/month directory.
    client = storage.Client()  # Ensure credentials are properly configured
    bucket = client.bucket(bucket_name)
    prefix = f"{gcs_prefix}/{target_year}/{target_month}/"
    blobs = list(client.list_blobs(bucket, prefix=prefix))

    if not blobs:
        raise ValueError(f"No files found under path {prefix}.")

    monthly_blobs = [blob for blob in blobs if blob.name.endswith(".parquet")]

    reference_schema = None
    # Validate all files for the target month
    for blob in monthly_blobs:
        gcs_uri = f"gs://{bucket_name}/{blob.name}"
        print(f"\nValidating file: {gcs_uri}")

        # 1. Check file size (must not be 0)
        if blob.size == 0:
            print(f"File is empty: {blob.name}")
            continue

        # 2. Attempt to read the Parquet file using Spark
        try:
            print(f"Attempting to read file: {gcs_uri}")
            df = spark.read.parquet(gcs_uri)
        except AnalysisException as e:
            print(f"Failed to read file: {gcs_uri}, error: {e}")
            continue

        # Cache DataFrame to avoid repeated scans
        df.cache()

        # 3. Check record count (must have at least one record)
        record_count = df.count()
        if record_count < 1:
            print(f"No records found in file: {gcs_uri}")
            continue
        print(f"Record count: {record_count}")

        # 4. Calculate and print the null value ratio for each column using a single aggregation
        agg_exprs = [
            sum(when(col(c).isNull() | (col(c) == "M"), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]
        agg_result = df.agg(*agg_exprs).collect()[0].asDict()
        for c in df.columns:
            null_count = agg_result[c]
            ratio = null_count / record_count
            print(f"Column '{c}': {null_count} nulls (or 'M'), ratio: {ratio:.2%}")

        # 5. Validate schema consistency (using the first successfully read file as the reference)
        if reference_schema is None:
            reference_schema = df.schema
        else:
            if df.schema != reference_schema:
                mismatch_message = compare_schemas(reference_schema, df.schema)
                print(f"Schema mismatch in file {gcs_uri}:\n{mismatch_message}\n")

    print("\nRaw data validation completed!")
