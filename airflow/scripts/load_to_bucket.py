import os
import json
import glob
import shutil
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError
from typing import Iterator
import pandas as pd
from google.cloud import storage

SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

def get_stations_from_network(state: str) -> list[str]:
    """
    Get a list of station IDs from a given state's ASOS network.

    Args:
        state (str): Two-letter state abbreviation, e.g., 'IA', 'NY'

    Returns:
        List[str]: List of station IDs (sids) in the network

    References:
        https://github.com/akrherz/iem/blob/main/scripts/asos/iem_scraper_example.py
    """
    
    stations = []
    network = f"{state}_ASOS"
    uri = f"https://mesonet.agron.iastate.edu/geojson/network/{network}.geojson"
    
    try:
        with urlopen(uri) as response:
            jdict = json.load(response)
        for site in jdict["features"]:
            stations.append(site["properties"]["sid"])
    except URLError as e:
        print(f"failed to fetch data for network {network} : {e}")
    except KeyError as e:
        print(f"Unexpected format in response: missing key {e}")

    return stations

def download_data(url:str) -> Iterator[pd.DataFrame]:
    """
    Download CSV data in chunks and return an iterator of DataFrames.

    Args:
        url (str): URL of the CSV file.

    Returns:
        Iterator[pd.DataFrame]: An iterator over chunks of the CSV file.
    """
    for chunk in pd.read_csv(url, chunksize=100000):
        yield chunk
    


def write_csv_to_local(state: str, startts: datetime, endts: datetime, output_dir: str = "csv"):
    """
    Fetch data for all stations in a given state's ASOS network and save them as chunked CSV files
    in a flat directory. Station names are included in the file name.

    Args:
        state (str): Two-letter state abbreviation (e.g., "IA")
        startts (datetime): Start timestamp (UTC)
        endts (datetime): End timestamp (UTC)
        output_dir (str): Directory to save the CSV files
    """
    os.makedirs(output_dir, exist_ok=True)

    service = SERVICE + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"
    service += startts.strftime("year1=%Y&month1=%m&day1=%d&")
    service += endts.strftime("year2=%Y&month2=%m&day2=%d&")

    stations = get_stations_from_network(state)
    if not stations:
        print(f"No stations found for state: {state}")
        return

    for station in stations:
        uri = f"{service}&station={station}"
        print(f"Downloading: {station}")

        try:
            for i, chunk in enumerate(download_data(uri)):
                filename = f"{station}_{startts:%Y%m%d}_{endts:%Y%m%d}_chunk{i}.csv"
                filepath = os.path.join(output_dir, filename)
                chunk.to_csv(filepath, index=False)
                print(f"Saved: {filename}")
        except Exception as e:
            print(f"Failed to process station {station}: {e}")


def convert_to_parquet(csv_dir: str, parquet_dir: str):
    """
    Convert all CSV files in a flat directory to Parquet format.

    Args:
        csv_dir (str): Directory containing CSV files.
        parquet_dir (str): Directory to save Parquet files.
    """
    os.makedirs(parquet_dir, exist_ok=True)

    csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_dir}")
        return

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            file_name = os.path.basename(csv_file).replace(".csv", ".parquet")
            parquet_path = os.path.join(parquet_dir, file_name)
            df.to_parquet(parquet_path, index=False)
            print(f"Converted: {csv_file}")
        except Exception as e:
            print(f"Failed to convert {csv_file}: {e}")


def upload_parquet_to_gcs_with_station_structure(parquet_dir: str, bucket_name: str, gcs_prefix: str = ""):
    """
    Upload .parquet files from a flat local directory to GCS, using the station name from filename
    to create subfolders in the GCS bucket.

    Args:
        parquet_dir (str): Local directory containing .parquet files.
        bucket_name (str): GCS bucket name.
        gcs_prefix (str): Optional prefix for GCS path (e.g., 'asos/md')
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
    if not parquet_files:
        print(f"No Parquet files found in {parquet_dir}")
        return

    for local_path in parquet_files:
        try:
            file_name = os.path.basename(local_path)
            station = file_name.split("_")[0]  # Extract station name
            blob_path = os.path.join(gcs_prefix, station, file_name).replace("\\", "/") # for windows compatibility

            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            print(f"Uploaded: {local_path} → gs://{bucket_name}/{blob_path}")
        except Exception as e:
            print(f"Failed to upload {local_path}: {e}")

def cleanup_local_files(csv_dir: str, parquet_dir: str):
    """
    Delete all files in the given CSV and Parquet directories.

    Args:
        csv_dir (str): Path to the CSV directory.
        parquet_dir (str): Path to the Parquet directory.
    """
    for folder in [csv_dir, parquet_dir]:
        try:
            if os.path.exists(folder):
                shutil.rmtree(folder)
                print(f"Deleted folder: {folder}")
            else:
                print(f"Folder not found: {folder}")
        except Exception as e:
            print(f"Failed to delete {folder}: {e}")   
