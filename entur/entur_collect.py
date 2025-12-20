import time
import requests
import pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from utils.s3_upload import save_json_to_s3, save_parquet_to_s3

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


def fetch_entur_data(operator, feed_type):
    url = f"https://api.entur.io/realtime/v1/gtfs-rt/{feed_type}?datasource={operator}"
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

    # Save directly to S3
    if decoded:
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%H%M%S")
        json_s3_key = f"entur/{today}/{operator}_{feed_type}_{timestamp}.json"
        parquet_s3_key = f"entur/{today}/{operator}_{feed_type}_{timestamp}.parquet"

        df = pd.DataFrame(decoded)
        save_json_to_s3(decoded, json_s3_key)
        save_parquet_to_s3(df, parquet_s3_key)

    print(f"{operator} {feed_type}: {len(decoded)} records saved to S3.")
    return decoded


def run_collector(interval=180):
    print("Starting full Entur GTFS-RT collector...\n")
    while True:
        start_time = datetime.now()
        print(f"New cycle at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        for operator in OPERATORS:
            for name, feed in FEEDS.items():
                fetch_entur_data(operator, feed)
        duration = (datetime.now() - start_time).total_seconds()
        print(f"Cycle completed in {duration:.1f}s")
        print(f"Sleeping {interval}s before next pull...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_collector(interval=180)
