"""
Avinor flight data collector (departures only, past window).

Uses XmlFeed v1.0: https://partner.avinor.no/en/services/flight-data/
- airport (required), direction=D (departures), TimeFrom / TimeTo (hours).
- Avinor: retrieve every 3 minutes; cache data; do not let end-users hit the API directly.
- Status codes: A=Arrived, C=Cancelled, D=Departed, E=New time, N=New info.
- Times in UTC (ISO 8601).
"""
import json
import os
import sys
import threading
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

from utils.callstats import CallStats
from utils.s3_upload import raise_if_failed_saves, reset_failed_saves, save_parquet_to_s3

HEADERS = {"User-Agent": "AvinorDataCollector/1.0 (+https://avinor.no)"}

# The airport list has about 2000 codes, almost all without flights. Asking for every one every 3 minutes
# from 32 threads made the server close the connections: 99% of requests failed, OSL included. Only the
# airports that had flights in earlier data are polled (regenerate the file with refresh_active_airports.py).
ACTIVE_AIRPORTS_FILE = os.path.join(SCRIPT_DIR, "active_airports.json")
WORKERS = int(os.getenv("AVINOR_WORKERS", "2"))
PAUSE_S = float(os.getenv("AVINOR_PAUSE_S", "0.7"))  # each thread waits this long after a request; ~141 airports take about a minute
BUDGET_S = int(os.getenv("AVINOR_BUDGET_S", "140"))  # no new request after this many seconds (the Lambda timeout is 170)
RETRIES = 3  # per airport, on a closed connection or a 5xx answer
MAX_FAILED_SHARE = 0.25  # more airports than this failing makes the invocation fail

CALLS = CallStats()  # successful and failed calls, printed at the end of every invocation
_stats_lock = threading.Lock()
STATS = {"saved": 0, "no_flights": 0, "failed": 0, "skipped": 0}


def _count(kind):
    with _stats_lock:
        STATS[kind] += 1


def _get(url):
    """
    GET with a few retries and a short growing pause when the server drops the connection or answers 5xx.
    Returns the response with .attempts set; raises ConnectionError (with .error and .attempts) after the last try.
    """
    last = None
    for attempt in range(RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code < 500:
                response.attempts = attempt + 1
                return response
            last = f"HTTP {response.status_code}"
        except requests.exceptions.RequestException as e:
            last = f"{type(e).__name__}"
        if attempt < RETRIES - 1:
            time.sleep(1.0 + attempt)  # 1 s, then 2 s: even a full outage stays far below the Lambda timeout
    error = ConnectionError(f"{last} after {RETRIES} attempts")
    error.error, error.attempts = last, RETRIES
    raise error


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
        if code and len(code) == 3 and code.isalpha():  # the list has entries like '88J' that always answer HTTP 400
            airports.append({"code": code, "name": name})
    return airports


def fetch_flight_data(airport_code):
                                                                                            
                                                                                       
    url = (
        f"https://asrv.avinor.no/XmlFeed/v1.0?airport={airport_code}"
        f"&direction=D&TimeFrom=24&TimeTo=0"
    )
    started = time.time()
    try:
        response = _get(url)
        latency, retries = time.time() - started, response.attempts - 1
        if response.status_code != 200:
            print(f"{airport_code} -> HTTP {response.status_code}")
            _count("failed")
            CALLS.record("avinor", "failed", latency, f"HTTP {response.status_code}", retries)
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
            _count("no_flights")
            CALLS.record("avinor", "empty", latency, retries=retries)
            return None

                             
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
        parquet_s3_key = f"avinor/{today}/{airport_code}_{timestamp}.parquet"

        df = pd.DataFrame(flights)
        save_parquet_to_s3(df, parquet_s3_key)

        print(f"{airport_code}: {len(flights)} flights saved to S3")
        _count("saved")
        CALLS.record("avinor", "ok", latency, retries=retries)
        return flights
    except Exception as e:
        print(f"{airport_code}: error {e}")
        _count("failed")
        CALLS.record("avinor", "failed", time.time() - started, getattr(e, "error", type(e).__name__),
                     getattr(e, "attempts", 1) - 1)
        return None


_airports_cache = None


def run_once(max_workers=1):
    """One collection pass over all airports (airport list is cached across warm Lambda invocations)."""
    global _airports_cache
    if not _airports_cache:
        if os.path.exists(ACTIVE_AIRPORTS_FILE):
            with open(ACTIVE_AIRPORTS_FILE) as f:
                _airports_cache = [{"code": c, "name": None} for c in json.load(f)["airports"]]
            print(f"Using {len(_airports_cache)} active airports.\n")
        else:
            _airports_cache = fetch_airports()
            print(f"Fetched {len(_airports_cache)} airports.\n")
    codes = [ap["code"] for ap in _airports_cache]  # busiest first, so a cycle that is cut short still has them
    deadline = time.monotonic() + BUDGET_S

    def paced(code):
        if time.monotonic() > deadline:
            _count("skipped")
            CALLS.record("avinor", "skipped")
            return
        fetch_flight_data(code)
        time.sleep(PAUSE_S)

    if max_workers <= 1:
        for code in codes:
            paced(code)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            list(pool.map(paced, codes))


def lambda_handler(event, context):
    global CALLS
    reset_failed_saves()
    CALLS = CallStats()
    for key in STATS:
        STATS[key] = 0
    run_once(max_workers=WORKERS)
    CALLS.emit()
    total = sum(STATS.values())
    print(f"avinor: {STATS['saved']} airports saved, {STATS['no_flights']} without flights, "
          f"{STATS['failed']} failed, {STATS['skipped']} skipped (out of time)")
    bad = STATS["failed"] + STATS["skipped"]
    if total and bad / (total + STATS["skipped"]) > MAX_FAILED_SHARE:
        raise RuntimeError(f"{STATS['failed']} failed and {STATS['skipped']} skipped of {total + STATS['skipped']} airports")
    raise_if_failed_saves()
    return {"status": "ok", **STATS}


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
