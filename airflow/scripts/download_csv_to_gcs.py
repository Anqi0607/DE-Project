import os
import glob
import re
import requests
import io
import json
import shutil
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.error import URLError
from typing import Iterator, Optional
import pandas as pd
from google.cloud import storage

SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

def get_stations_from_network(state: str) -> list[str]:
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

def download_data(url: str, station: str, failed_stations: Optional[list] = None) -> Iterator[pd.DataFrame]:
    try:
        response = requests.get(url)
        if not response.ok:
            raise ValueError(f"Bad response: HTTP {response.status_code}")
        content = response.text.strip()
        if "<html" in content.lower() or "<!doctype" in content.lower():
            raise ValueError("HTML response received (likely error page)")
        # Remove any comment lines from the response
        lines = [line for line in content.splitlines() if not line.startswith("#")]
        if len(lines) <= 1:
            raise ValueError("CSV content has no data rows")
        # Join the lines into a complete string so that pandas can read it
        cleaned = "\n".join(lines)
        # Simulate a file from the string for pandas to read
        for chunk in pd.read_csv(io.StringIO(cleaned), chunksize=100000):
            print(f"Loaded chunk for {station}: {len(chunk)} rows × {len(chunk.columns)} cols")
            # This function is a generator, processing and returning one chunk at a time 
            # without loading the entire CSV file into memory.
            yield chunk
    except Exception as e:
        print(f"Failed to download or parse data for station {station}: {e}")
        if failed_stations is not None:
            failed_stations.append(station)
        return

def write_csv_to_gcs(
    state: str,
    gcs_csv_prefix: str,
    project_id: str,
    bucket_name: str,
    **kwargs,
) -> str:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    # 通过 execution_date 构造时间区间
    execution_date = kwargs["data_interval_start"]
    startts = execution_date
    endts = (execution_date + timedelta(days=32)).replace(day=1)
    ym = startts.strftime("%Y%m")

    stations = get_stations_from_network(state)
    if not stations:
        print(f"No stations found for state: {state}")
        return ""

    failed_stations = []
    for station in stations:
        uri = (
            SERVICE
            + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"
            + startts.strftime("year1=%Y&month1=%m&day1=%d&")
            + endts.strftime("year2=%Y&month2=%m&day2=%d&")
            + f"&station={station}"
        )
        print(f"Downloading: {station}")
        try:
            for i, chunk in enumerate(download_data(uri, station, failed_stations)):
                file_name = f"{station}_{ym}_chunk{i}.csv"
                blob_path = f"{gcs_csv_prefix}/{ym}/{file_name}"
                buffer = io.StringIO()
                chunk.to_csv(buffer, index=False)
                buffer.seek(0)
                blob = bucket.blob(blob_path)
                blob.upload_from_file(buffer, content_type="text/csv")
                print(f"Uploaded: {blob_path}")
        except Exception as e:
            print(f"Unexpected error while processing {station}: {e}")
            failed_stations.append(station)

    if failed_stations:
        print("\nFailed to process the following stations:")
        for station in failed_stations:
            print(f" - {station}")
    else:
        print("\nAll stations processed successfully!")

    print(f"\nGCS CSV directory for this run: {gcs_csv_prefix}/{ym}/")

    # 返回整个年月路径（供下游 XCom 使用）
    return f"{gcs_csv_prefix}/{ym}/"
