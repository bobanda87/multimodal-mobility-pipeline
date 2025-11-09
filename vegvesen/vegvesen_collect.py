import os
import time
import json
import requests
import xmltodict
import pandas as pd
from datetime import datetime
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = "vegvesen_data"
# BASE_DIR = "/mnt/volume_ams3_01/vegvesen_data"
os.makedirs(BASE_DIR, exist_ok=True)

USERNAME = os.getenv("VEGVESEN_USER")
PASSWORD = os.getenv("VEGVESEN_PASS")

FEEDS = {
    "traffic_situations": "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetSituation/pullsnapshotdata",
    "weather_data": "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetMeasuredWeatherData/pullsnapshotdata",
    "travel_times": "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetTravelTimeData/pullsnapshotdata"
}


def get_daily_dir():
    today = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(BASE_DIR, today)
    os.makedirs(daily_dir, exist_ok=True)
    return daily_dir


def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def ensure_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def clean_nested(obj):
    if isinstance(obj, dict):
        return {k: clean_nested(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        if all(isinstance(i, (str, int, float, bool, type(None))) for i in obj):
            return ",".join(map(str, obj))
        else:
            return [clean_nested(i) for i in obj]
    else:
        return obj


def extract_situations(data):
    try:
        container = data.get("ns2:messageContainer", {})
        payload = container.get("ns2:payload", {})
        situations = ensure_list(payload.get("ns12:situation", []))

        for s in situations:
            rec = s.get("ns12:situationRecord")
            s["ns12:situationRecord"] = ensure_list(rec)
        return situations
    except Exception as e:
        print(f"Error extracting situations: {e}")
        return []


def flatten_records(situations):
    records = []
    for s in situations:
        base = {k: v for k, v in s.items() if k != "ns12:situationRecord"}
        for rec in s.get("ns12:situationRecord", []):
            rec_clean = clean_nested(rec)
            base_clean = clean_nested(base)
            combined = {**base_clean, **rec_clean}
            records.append(combined)
    return records


def extract_weather_measurements(data):
    try:
        container = data.get("ns2:messageContainer", {})
        payload = container.get("ns2:payload", {})
        measurements = ensure_list(payload.get("ns10:siteMeasurements", []))
        cleaned = [clean_nested(m) for m in measurements]
        return cleaned
    except Exception as e:
        print(f"Error extracting weather measurements: {e}")
        return []


def fetch_datex_data(feed_name, url):
    if not USERNAME or not PASSWORD:
        print("Missing VEGVESEN_USER or VEGVESEN_PASS environment variables.")
        return None

    try:
        response = requests.get(url, auth=HTTPBasicAuth(
            USERNAME, PASSWORD), timeout=60)
        if response.status_code != 200:
            print(f"{feed_name} -> HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"{feed_name} -> Connection error: {e}")
        return None

    data = xmltodict.parse(response.text)
    daily_dir = get_daily_dir()
    timestamp = datetime.now().strftime("%H%M%S")
    json_path = os.path.join(daily_dir, f"{feed_name}_{timestamp}.json")
    parquet_path = json_path.replace(".json", ".parquet")

    save_json(data, json_path)

    try:
        if feed_name == "traffic_situations":
            situations = extract_situations(data)
            if not situations:
                print(f"{feed_name}: No situation records found.")
                return
            records = flatten_records(situations)
            df = pd.json_normalize(records, sep=".")

        elif feed_name == "weather_data":
            measurements = extract_weather_measurements(data)
            if not measurements:
                print(f"{feed_name}: No weather measurements found.")
                return
            df = pd.json_normalize(measurements, sep=".")

        else:
            flat = clean_nested(data)
            df = pd.json_normalize(flat, sep=".")

        df.to_parquet(parquet_path, index=False)
        print(f"{feed_name}: {len(df)} records saved -> {parquet_path}")
    except Exception as e:
        print(f"{feed_name} -> Could not convert to parquet: {e}")


def run_collector(interval=180):
    print("Starting Statens Vegvesen DATEX collector...\n")
    while True:
        start = datetime.now()
        print(f"New cycle at {start.strftime('%Y-%m-%d %H:%M:%S')}")
        fetch_datex_data("traffic_situations", FEEDS["traffic_situations"])
        fetch_datex_data("travel_times", FEEDS["travel_times"])
        fetch_datex_data("weather_data", FEEDS["weather_data"])
        duration = (datetime.now() - start).total_seconds()
        print(f"Cycle completed in {duration:.1f}s")
        print(f"Sleeping {interval}s before next pull...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_collector(interval=180)
