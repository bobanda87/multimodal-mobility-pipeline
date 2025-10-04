import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2

BASE_DIR = "entur_data"
OPERATORS = [
    "AKT", "ATB", "AVI", "BNR", "BRA", "FIN", "FLT", "GJB", "GOA", "INN",
    "KOL", "MOR", "NBU", "NOR", "NSB", "OST", "RUT", "SJN", "SKY", "SOF",
    "TEL", "TRO", "VKT", "VOT", "VYB", "VYG", "VYX"
]
FEEDS = {
    "trip_updates": "trip-updates",
    "vehicle_positions": "vehicle-positions",
    "alerts": "alerts"
}
HEADERS = {"ET-Client-Name": "entur-multimodal-analytics"}
os.makedirs(BASE_DIR, exist_ok=True)


def get_daily_dir():
    today = datetime.now().strftime("%Y-%m-%d")
    daily_dir = os.path.join(BASE_DIR, today)
    os.makedirs(daily_dir, exist_ok=True)
    return daily_dir


def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_entur_data(operator, feed_type):
    url = f"https://api.entur.io/realtime/v1/gtfs-rt/{feed_type}?datasource={operator}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"⚠️  {operator} {feed_type} -> HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"❌  {operator} {feed_type} -> Connection error: {e}")
        return None

    daily_dir = get_daily_dir()
    timestamp = datetime.now().strftime("%H%M%S")
    filename_pb = os.path.join(
        daily_dir, f"{operator}_{feed_type}_{timestamp}.pb")

    with open(filename_pb, "wb") as f:
        f.write(response.content)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    decoded = []
    for entity in feed.entity:
        entry = {"id": entity.id}
        if entity.HasField("trip_update"):
            trip = entity.trip_update
            entry["type"] = "trip_update"
            entry["trip_id"] = trip.trip.trip_id
            entry["route_id"] = trip.trip.route_id
            entry["timestamp"] = trip.timestamp
            entry["stop_time_updates"] = [
                {
                    "stop_id": stu.stop_id,
                    "arrival_delay": getattr(stu.arrival, "delay", None),
                    "departure_delay": getattr(stu.departure, "delay", None)
                }
                for stu in trip.stop_time_update
            ]
        elif entity.HasField("vehicle"):
            veh = entity.vehicle
            entry["type"] = "vehicle_position"
            entry["trip_id"] = veh.trip.trip_id
            entry["latitude"] = veh.position.latitude
            entry["longitude"] = veh.position.longitude
            entry["timestamp"] = veh.timestamp
        elif entity.HasField("alert"):
            alert = entity.alert
            entry["type"] = "alert"
            entry["cause"] = str(alert.cause)
            entry["effect"] = str(alert.effect)
        decoded.append(entry)

    json_filename = filename_pb.replace(".pb", ".json")
    save_json(decoded, json_filename)

    if decoded:
        df = pd.DataFrame(decoded)
        parquet_filename = json_filename.replace(".json", ".parquet")
        df.to_parquet(parquet_filename, index=False)

    print(f"✅  {operator} {feed_type}: {len(decoded)} records saved.")
    return decoded


def run_collector(interval=900):
    print("🚀 Starting full Entur GTFS-RT collector...\n")
    while True:
        start_time = datetime.now()
        print(f"⏰ New cycle at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        for operator in OPERATORS:
            for name, feed in FEEDS.items():
                fetch_entur_data(operator, feed)
        duration = (datetime.now() - start_time).total_seconds()
        print(f"✅ Cycle completed in {duration:.1f}s")
        print(f"⏱️ Sleeping {interval}s before next pull...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_collector(interval=900)
