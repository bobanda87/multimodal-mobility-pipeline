"""
Avinor flight data collector (departures only, past window).

Uses XmlFeed v1.0: https://partner.avinor.no/en/services/flight-data/
- airport (required), direction=D (departures), TimeFrom / TimeTo (hours).
- Avinor: retrieve every 3 minutes; cache data; do not let end-users hit the API directly.
- Status codes: A=Arrived, C=Cancelled, D=Departed, E=New time, N=New info.
- Times in UTC (ISO 8601).
"""
import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET

                                                                                                     
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from concurrent.futures import ThreadPoolExecutor

from utils.s3_upload import save_parquet_to_s3

HEADERS = {"User-Agent": "AvinorDataCollector/1.0 (+https://avinor.no)"}


def fetch_airports():
    url = "https://asrv.avinor.no/airportNames/v1.0"
    response = requests.get(url, headers=HEADERS, timeout=30)
    if response.status_code != 200:
        print(f"airportNames -> HTTP {response.status_code}")
        return []
    text_clean = response.text.replace(
        'xmlns="http://www.avinor.no/xmlfeed/airportnames/v1.0"', "")
    root = ET.fromstring(text_clean)
    airports = []
    for ap in root.findall(".//airportName"):
        code = ap.attrib.get("code")
        name = ap.attrib.get("name")
        if code and len(code) == 3:
            airports.append({"code": code, "name": name})
    return airports


def fetch_flight_data(airport_code):
                                                                                            
                                                                                       
    url = (
        f"https://asrv.avinor.no/XmlFeed/v1.0?airport={airport_code}"
        f"&direction=D&TimeFrom=24&TimeTo=0"
    )
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"{airport_code} -> HTTP {response.status_code}")
            return None
        root = ET.fromstring(response.text)
        flights = []
        for flight in root.findall(".//flight"):
            record = {}
            for child in flight:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                record[tag] = (child.text or "").strip() or None
                for attr, val in child.attrib.items():
                    record[f"{tag}_{attr}"] = val
            record.update(flight.attrib)
            if "status_code" in record:
                record["status"] = record["status_code"]
            if "delayed" not in record:
                record["delayed"] = None
            flights.append(record)

        if not flights:
            print(f"{airport_code}: no flights")
            return None

                             
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
        parquet_s3_key = f"avinor/{today}/{airport_code}_{timestamp}.parquet"

        df = pd.DataFrame(flights)
        save_parquet_to_s3(df, parquet_s3_key)

        print(f"{airport_code}: {len(flights)} flights saved to S3")
        return flights
    except Exception as e:
        print(f"{airport_code}: error {e}")
        return None


_airports_cache = None


def run_once(max_workers=1):
    """One collection pass over all airports (airport list is cached across warm Lambda invocations)."""
    global _airports_cache
    if not _airports_cache:
        _airports_cache = fetch_airports()
        print(f"Fetched {len(_airports_cache)} airports.\n")
    codes = [ap["code"] for ap in _airports_cache]
    if max_workers <= 1:
        for code in codes:
            fetch_flight_data(code)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            list(pool.map(fetch_flight_data, codes))


def lambda_handler(event, context):
    run_once(max_workers=32)
    return {"status": "ok"}


def run_collector(interval=180):
    print("Starting Avinor flight data collector...\n")
    while True:
        start_time = datetime.now()
        print(f"New cycle at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        run_once()
        duration = (datetime.now() - start_time).total_seconds()
        print(f"Cycle completed in {duration:.1f}s")
        print(f"Sleeping {interval}s before next pull...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_collector(interval=180)
