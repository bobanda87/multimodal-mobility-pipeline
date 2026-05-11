"""
Entur SIRI ET (Estimated Timetable) collector, plus GTFS-RT vehicle-positions and alerts.

- SIRI ET: departure time and per-stop delays (replaces trip-updates).
- Vehicle-positions and alerts: from GTFS-RT API (same as entur_collect).

Entur rate limit: 4 requests per minute. Loop rotates SIRI ET (2/min), vehicle-positions (1/min), alerts (1/min).
"""

import os
import sys
import time
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from google.transit import gtfs_realtime_pb2

                                                              
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.s3_upload import save_json_to_s3, save_parquet_to_s3

                                        
OPERATORS = [
    "AKT", "ATB", "AVI", "BNR", "BRA", "FIN", "FLT", "GJB", "GOA", "INN",
    "KOL", "MOR", "NBU", "NOR", "NSB", "OST", "RUT", "SJN", "SKY", "SOF",
    "TEL", "TRO", "VKT", "VOT", "VYB", "VYG", "VYX",
]

SIRI_NS = {"siri": "http://www.siri.org.uk/siri"}
HEADERS = {"ET-Client-Name": "entur-multimodal-analytics"}
ET_BASE_URL = "https://api.entur.io/realtime/v1/rest/et"
GTFS_RT_BASE_URL = "https://api.entur.io/realtime/v1/gtfs-rt"


def _parse_iso_to_unix(iso_str: str | None) -> int | None:
    """Parse ISO 8601 datetime to Unix timestamp (seconds)."""
    if not iso_str or not iso_str.strip():
        return None
    try:
                                                  
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return None


def _iso_to_gtfs_time(iso_str: str | None) -> str | None:
    """From ISO datetime take time part as HH:MM:SS (or H:MM:SS for GTFS start_time)."""
    if not iso_str or not iso_str.strip():
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%H:%M:%S")
    except (ValueError, TypeError):
        return None


def _iso_to_gtfs_date(iso_str: str | None) -> str | None:
    """From ISO datetime take date as YYYYMMDD."""
    if not iso_str or not iso_str.strip():
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y%m%d")
    except (ValueError, TypeError):
        return None


def _delay_seconds(aimed_iso: str | None, expected_iso: str | None) -> int | None:
    """Delay in seconds: expected - aimed. Positive = late."""
    if not aimed_iso or not expected_iso:
        return None
    try:
        aimed = datetime.fromisoformat(aimed_iso.replace("Z", "+00:00"))
        expected = datetime.fromisoformat(expected_iso.replace("Z", "+00:00"))
        return int((expected - aimed).total_seconds())
    except (ValueError, TypeError):
        return None


def _text(el: ET.Element | None) -> str | None:
    if el is None:
        return None
    t = (el.text or "").strip()
    return t if t else None


def fetch_siri_et(operator: str, max_size: int = 1500) -> list[dict] | None:
    """
    Fetch SIRI ET for one operator. Returns a list of dicts compatible with trip-updates:
    id, type, trip_id, route_id, timestamp, start_date, start_time, stop_time_updates
    (stop_id, arrival_delay, departure_delay), plus direction_ref, operator_ref, monitored,
    data_source, and per-stop stop_point_name, destination_display, arrival_status,
    departure_status.
    """
    url = f"{ET_BASE_URL}?datasetId={operator}&maxSize={max_size}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code != 200:
            print(f"{operator} siri_et -> HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"{operator} siri_et -> Connection error: {e}")
        return None

    root = ET.fromstring(response.content)
    decoded = []

    for journey in root.findall(".//siri:EstimatedVehicleJourney", SIRI_NS):
        framed = journey.find("siri:FramedVehicleJourneyRef", SIRI_NS)
        dated_ref = framed.find("siri:DatedVehicleJourneyRef", SIRI_NS) if framed is not None else None
        data_frame_ref = framed.find("siri:DataFrameRef", SIRI_NS) if framed is not None else None

        trip_id = _text(dated_ref)
        if not trip_id:
            continue

        line_ref = journey.find("siri:LineRef", SIRI_NS)
        route_id = _text(line_ref) or None
        recorded_el = journey.find("siri:RecordedAtTime", SIRI_NS)
        recorded_iso = _text(recorded_el)
        timestamp = _parse_iso_to_unix(recorded_iso)

        start_date = _iso_to_gtfs_date(_text(data_frame_ref)) if data_frame_ref is not None else None
                                                                    
        calls_el = journey.find("siri:EstimatedCalls", SIRI_NS)
        first_aimed_dep = None
        stop_time_updates = []

        if calls_el is not None:
            for call in calls_el.findall("siri:EstimatedCall", SIRI_NS):
                stop_ref = call.find("siri:StopPointRef", SIRI_NS)
                stop_id = _text(stop_ref)
                aimed_arr = _text(call.find("siri:AimedArrivalTime", SIRI_NS))
                expected_arr = _text(call.find("siri:ExpectedArrivalTime", SIRI_NS))
                aimed_dep = _text(call.find("siri:AimedDepartureTime", SIRI_NS))
                expected_dep = _text(call.find("siri:ExpectedDepartureTime", SIRI_NS))

                if first_aimed_dep is None and aimed_dep:
                    first_aimed_dep = aimed_dep

                arrival_delay = _delay_seconds(aimed_arr, expected_arr)
                departure_delay = _delay_seconds(aimed_dep, expected_dep)

                rec = {
                    "stop_id": stop_id,
                    "arrival_delay": arrival_delay,
                    "departure_delay": departure_delay,
                }
                stop_name = _text(call.find("siri:StopPointName", SIRI_NS))
                if stop_name is not None:
                    rec["stop_point_name"] = stop_name
                dest = _text(call.find("siri:DestinationDisplay", SIRI_NS))
                if dest is not None:
                    rec["destination_display"] = dest
                arr_status = _text(call.find("siri:ArrivalStatus", SIRI_NS))
                if arr_status is not None:
                    rec["arrival_status"] = arr_status
                dep_status = _text(call.find("siri:DepartureStatus", SIRI_NS))
                if dep_status is not None:
                    rec["departure_status"] = dep_status
                stop_time_updates.append(rec)

        start_time = _iso_to_gtfs_time(first_aimed_dep) if first_aimed_dep else None

        entry = {
            "id": trip_id,
            "type": "siri_et",
            "trip_id": trip_id,
            "route_id": route_id,
            "timestamp": timestamp,
            "start_date": start_date,
            "start_time": start_time,
            "stop_time_updates": stop_time_updates,
        }
        direction_ref = _text(journey.find("siri:DirectionRef", SIRI_NS))
        if direction_ref is not None:
            entry["direction_ref"] = direction_ref
        operator_ref = _text(journey.find("siri:OperatorRef", SIRI_NS))
        if operator_ref is not None:
            entry["operator_ref"] = operator_ref
        monitored = _text(journey.find("siri:Monitored", SIRI_NS))
        if monitored is not None:
            entry["monitored"] = monitored
        data_source = _text(journey.find("siri:DataSource", SIRI_NS))
        if data_source is not None:
            entry["data_source"] = data_source

        decoded.append(entry)

    return decoded


def fetch_gtfs_rt_feed(operator: str, feed_type: str) -> list[dict] | None:
    """
    Fetch GTFS-RT vehicle-positions or alerts for one operator.
    feed_type: "vehicle-positions" or "alerts".
    Saves to S3 as entur/{date}/{operator}_{feed_type}_{time}.json/parquet.
    """
    url = f"{GTFS_RT_BASE_URL}/{feed_type}?datasource={operator}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"{operator} {feed_type} -> HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"{operator} {feed_type} -> Connection error: {e}")
        return None

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    decoded = []
    for entity in feed.entity:
        entry = {"id": entity.id}
        if entity.HasField("vehicle"):
            veh = entity.vehicle
            entry["type"] = "vehicle_position"
            entry["trip_id"] = veh.trip.trip_id if veh.trip else None
            entry["latitude"] = veh.position.latitude if veh.position else None
            entry["longitude"] = veh.position.longitude if veh.position else None
            entry["timestamp"] = veh.timestamp
        elif entity.HasField("alert"):
            alert = entity.alert
            entry["type"] = "alert"
            entry["cause"] = str(alert.cause)
            entry["effect"] = str(alert.effect)
        decoded.append(entry)

    if decoded:
        today = datetime.now().strftime("%Y-%m-%d")
        ts = datetime.now().strftime("%H%M%S")
                                                                                                 
        json_s3_key = f"entur/{today}/{operator}_{feed_type}_{ts}.json"
        parquet_s3_key = f"entur/{today}/{operator}_{feed_type}_{ts}.parquet"
        df = pd.DataFrame(decoded)
        save_json_to_s3(decoded, json_s3_key)
        save_parquet_to_s3(df, parquet_s3_key)
        print(f"  {operator} {feed_type}: {len(decoded)} records -> S3")
    return decoded


def run_collector(interval_seconds: int = 60):
    """
    Main loop: every interval_seconds do one request (4 per minute).
    Rotates: SIRI ET (2/min), vehicle-positions (1/min), alerts (1/min), round-robin operators.
    Each run saves a NEW file (timestamp in name); multiple files per day per type are intentional (time-series snapshots).
    """
    print(
        "Starting Entur collector: SIRI ET + vehicle-positions + alerts "
        "(4 requests/min, round-robin).\n"
    )
    slot = 0
    while True:
        start_time = datetime.now()
        operator = OPERATORS[(slot // 4) % len(OPERATORS)]
        kind = slot % 4                                                       

        if kind in (0, 1):
            print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] SIRI ET {operator}...")
            decoded = fetch_siri_et(operator)
            if decoded:
                today = datetime.now().strftime("%Y-%m-%d")
                ts = datetime.now().strftime("%H%M%S")
                json_s3_key = f"entur/{today}/{operator}_siri_et_{ts}.json"
                parquet_s3_key = f"entur/{today}/{operator}_siri_et_{ts}.parquet"
                df = pd.DataFrame(decoded)
                save_json_to_s3(decoded, json_s3_key)
                save_parquet_to_s3(df, parquet_s3_key)
                print(f"  {operator} siri_et: {len(decoded)} journeys -> S3")
            else:
                print(f"  {operator} siri_et: no data")
        elif kind == 2:
            print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] vehicle-positions {operator}...")
            fetch_gtfs_rt_feed(operator, "vehicle-positions")
        else:
            print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] alerts {operator}...")
            fetch_gtfs_rt_feed(operator, "alerts")

        slot += 1
        duration = (datetime.now() - start_time).total_seconds()
        sleep_for = max(0, interval_seconds - duration)
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    run_collector(interval_seconds=60)
