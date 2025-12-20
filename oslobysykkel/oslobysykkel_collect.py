import os
import time
import json
import requests
import pandas as pd
from datetime import datetime

# Automatically determine BASE_DIR: use mnt path on server, local path otherwise
if os.path.exists("/mnt/volume_ams3_01"):
    BASE_DIR = "/mnt/volume_ams3_01/oslobysykkel_data"
else:
BASE_DIR = "oslobysykkel_data"
os.makedirs(BASE_DIR, exist_ok=True)

# GBFS base URLs
GBFS_BASE = "https://gbfs.urbansharing.com/oslobysykkel.no"
ENDPOINTS = {
    "system_information": f"{GBFS_BASE}/system_information.json",
    "station_information": f"{GBFS_BASE}/station_information.json",
    "station_status": f"{GBFS_BASE}/station_status.json",
}

# Identification header (mandatory)
CLIENT_IDENTIFIER = os.getenv("BYSYKKEL_CLIENT", "traffic-citymonitor")


def get_daily_dir():
    """Return or create directory for today's data."""
    today = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(BASE_DIR, today)
    os.makedirs(daily_dir, exist_ok=True)
    return daily_dir


def save_json(data, path):
    """Save JSON data to a file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
    daily_dir = get_daily_dir()
    timestamp = datetime.now().strftime("%H%M%S")
    json_path = os.path.join(daily_dir, f"{feed_name}_{timestamp}.json")
    parquet_path = json_path.replace(".json", ".parquet")

    save_json(data, json_path)

    try:
        # Most GBFS data is under data -> stations or data -> system
        if "stations" in data.get("data", {}):
            df = pd.json_normalize(data["data"]["stations"])
        elif "data" in data and isinstance(data["data"], dict):
            df = pd.json_normalize(data["data"])
        else:
            df = pd.json_normalize(data)

        df["last_updated"] = data.get("last_updated", None)
        df.to_parquet(parquet_path, index=False)
        print(f"{feed_name}: {len(df)} records saved -> {parquet_path}")
    except Exception as e:
        print(f"{feed_name} -> Could not convert to parquet: {e}")


def run_collector(interval=180):
    """Run continuous collector every `interval` seconds (default 1 minute)."""
    print("Starting Oslo Bysykkel GBFS collector...\n")
    while True:
        start = datetime.now()
        print(f"New cycle at {start.strftime('%Y-%m-%d %H:%M:%S')}")

        for feed_name, url in ENDPOINTS.items():
            fetch_gbfs_data(feed_name, url)

        duration = (datetime.now() - start).total_seconds()
        print(f"Cycle completed in {duration:.1f}s")
        print(f"Sleeping {interval}s before next pull...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_collector(interval=180)
