import time
import requests
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
from utils.s3_upload import save_json_to_s3, save_parquet_to_s3

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
    url = f"https://asrv.avinor.no/XmlFeed/v1.0?airport={airport_code}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"{airport_code} -> HTTP {response.status_code}")
            return None
        root = ET.fromstring(response.text)
        flights = []
        for flight in root.findall(".//flight"):
            record = {child.tag: child.text for child in flight}
            record.update(flight.attrib)
            flights.append(record)
        if not flights:
            print(f"{airport_code}: no flights")
            return None

        # Save directly to S3
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
        json_s3_key = f"avinor/{today}/{airport_code}_{timestamp}.json"
        parquet_s3_key = f"avinor/{today}/{airport_code}_{timestamp}.parquet"

        df = pd.DataFrame(flights)
        save_json_to_s3(flights, json_s3_key)
        save_parquet_to_s3(df, parquet_s3_key)

        print(f"{airport_code}: {len(flights)} flights saved to S3")
        return flights
    except Exception as e:
        print(f"{airport_code}: error {e}")
        return None


def run_collector(interval=180):
    print("Starting Avinor flight data collector...\n")
    airports = fetch_airports()
    print(f"Fetched {len(airports)} airports.\n")
    while True:
        start_time = datetime.now()
        print(f"New cycle at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        for ap in airports:
            fetch_flight_data(ap["code"])
        duration = (datetime.now() - start_time).total_seconds()
        print(f"Cycle completed in {duration:.1f}s")
        print(f"Sleeping {interval}s before next pull...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_collector(interval=180)
