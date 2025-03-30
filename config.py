import os
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Pipeline Parameters
STATE = "MA"

CSV_DIR = "csv"
PARQUET_DIR = "parquet"

BUCKET_NAME = os.getenv("GCP_GCS_BUCKET")
GCS_PREFIX = "METAR/{STATE}/raw"
