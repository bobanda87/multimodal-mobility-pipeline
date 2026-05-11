import os
import sys
import time
import requests
import xmltodict
import pandas as pd
from datetime import datetime
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from pathlib import Path

                                                                                                           
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.s3_upload import save_json_to_s3, save_parquet_to_s3

                                  
PROJECT_ROOT = Path(__file__).parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_FILE)

USERNAME = os.getenv("VEGVESEN_USER")
PASSWORD = os.getenv("VEGVESEN_PASS")

FEEDS = {
    "traffic_situations": "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetSituation/pullsnapshotdata",
    "weather_data": "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetMeasuredWeatherData/pullsnapshotdata",
    "travel_times": "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/GetTravelTimeData/pullsnapshotdata",
                                                                                                
    "weather_site_table": (
        "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/"
        "GetMeasurementWeatherSiteTable/pullsnapshotdata"
    ),
}


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

        elif feed_name == "weather_site_table":
                                                                                        
                                                                                
            try:
                container = data.get("ns2:messageContainer", {})
                payload = container.get("ns2:payload", {})
                site_table = payload.get("ns10:measurementSiteTable") or {}
                sites = ensure_list(site_table.get("ns10:measurementSite", []))
            except Exception:
                sites = []
            if not sites:
                print(f"{feed_name}: No measurementSite records found.")
                return
            records = [clean_nested(s) for s in sites]
            df = pd.json_normalize(records, sep=".")

        else:
            flat = clean_nested(data)
            df = pd.json_normalize(flat, sep=".")

                             
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
                                                                                 
        if feed_name == "weather_site_table":
            json_s3_key = "vegvesen/reference/weather_site_table.json"
            parquet_s3_key = "vegvesen/reference/weather_site_table.parquet"
        else:
            json_s3_key = f"vegvesen/{today}/{feed_name}_{timestamp}.json"
            parquet_s3_key = f"vegvesen/{today}/{feed_name}_{timestamp}.parquet"

        save_json_to_s3(data, json_s3_key)
        save_parquet_to_s3(df, parquet_s3_key)

        print(f"{feed_name}: {len(df)} records saved to S3")
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
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "site-table":
                                                                            
                                                                 
        fetch_datex_data("weather_site_table", FEEDS["weather_site_table"])
    else:
        run_collector(interval=180)
