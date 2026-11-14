import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

                                                                                                                       
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_PATH = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_ROOT_PATH)

from utils.s3_upload import save_parquet_to_s3

                                  
PROJECT_ROOT = Path(__file__).parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_FILE)

                
GBFS_BASE = "https://gbfs.urbansharing.com/oslobysykkel.no"
ENDPOINTS = {
    "system_information": f"{GBFS_BASE}/system_information.json",
    "station_information": f"{GBFS_BASE}/station_information.json",
    "station_status": f"{GBFS_BASE}/station_status.json",
}

                                   
CLIENT_IDENTIFIER = os.getenv("BYSYKKEL_CLIENT", "traffic-citymonitor")


def fetch_gbfs_data(feed_name, url):
    """Fetch a GBFS feed and store as JSON + Parquet."""
    headers = {"Client-Identifier": CLIENT_IDENTIFIER}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"{feed_name} -> HTTP {response.status_code}")
            return
    except Exception as e:
        print(f"{feed_name} -> Connection error: {e}")
        return

    data = response.json()

    try:
                                                                    
        if "stations" in data.get("data", {}):
            df = pd.json_normalize(data["data"]["stations"])
        elif "data" in data and isinstance(data["data"], dict):
            df = pd.json_normalize(data["data"])
        else:
            df = pd.json_normalize(data)

        df["last_updated"] = data.get("last_updated", None)

                             
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
        parquet_s3_key = f"oslobysykkel/{today}/{feed_name}_{timestamp}.parquet"

        save_parquet_to_s3(df, parquet_s3_key)

        print(f"{feed_name}: {len(df)} records saved to S3")
    except Exception as e:
        print(f"{feed_name} -> Could not convert to parquet: {e}")


def run_once():
    """One collection pass over all GBFS feeds."""
    for feed_name, url in ENDPOINTS.items():
        fetch_gbfs_data(feed_name, url)


STATUS_INTERVAL = 15  # station_status ttl at the source
STATUS_CALLS_PER_INVOCATION = 4  # invoked once a minute -> 4 x 15 s
STATIC_FEEDS_EVERY_MIN = 10  # station_information / system_information rarely change


def lambda_handler(event, context):
    """Invoked every minute: station_status every 15 s, static feeds every 10 min."""
    start = time.monotonic()
    if datetime.now().minute % STATIC_FEEDS_EVERY_MIN == 0:
        fetch_gbfs_data("system_information", ENDPOINTS["system_information"])
        fetch_gbfs_data("station_information", ENDPOINTS["station_information"])
    for i in range(STATUS_CALLS_PER_INVOCATION):
        wait = start + i * STATUS_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        fetch_gbfs_data("station_status", ENDPOINTS["station_status"])
    return {"status": "ok"}


def run_collector(interval=180):
    """Run continuous collector every `interval` seconds (default 1 minute)."""
    print("Starting Oslo Bysykkel GBFS collector...\n")
    while True:
        start = datetime.now()
        print(f"New cycle at {start.strftime('%Y-%m-%d %H:%M:%S')}")

        run_once()

        duration = (datetime.now() - start).total_seconds()
        print(f"Cycle completed in {duration:.1f}s")
        print(f"Sleeping {interval}s before next pull...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_collector(interval=180)
