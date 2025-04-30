import os
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Pipeline Parameters
STATE = "MA"

CSV_DIR = "csv"
PARQUET_DIR = "parquet"

GCP_REGION = "us-central1"
BUCKET_NAME = os.getenv("GCP_GCS_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_BIGQUERY_DATASET = os.getenv("GCP_BIGQUERY_DATASET")
TEMP_BUCKET = "de-zoomcamp-project-tem-bucket-pebbles"

GCS_PREFIX = f"METAR/{STATE}/Raw"
GCS_CSV_PREFIX = f"METAR/{STATE}/CSV"
GCS_PARQUET_PREFIX = f"METAR/{STATE}/PARQUET"