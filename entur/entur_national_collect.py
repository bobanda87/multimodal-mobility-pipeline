"""
Entur national GTFS-RT collector: vehicle-positions and alerts for all of Norway in one request each.

The per-operator collector (entur_siri_et_collect.py) polls one operator per minute, so an operator is
refreshed about every 108 minutes. The national feeds (no `datasource` parameter) return every
operator at once, so a position is at most a few seconds old at the source.

Raw files go to their own source folder, entur_national/<date>/, so the normalization of the per-operator
files (entur/) never sees them:
  entur_national/<date>/vehicle-positions_<HHMMSS>.parquet   every poll (default twice a minute)
  entur_national/<date>/alerts_<HHMMSS>.parquet              every 5 minutes

The vehicle feed lists every vehicle Entur has ever seen recently, most of them parked and silent for
hours. Only vehicles whose own timestamp is at most VEHICLE_MAX_AGE_S seconds old are kept (default 600,
0 keeps all), which drops roughly half of the rows.

Request budget: 2 polls/min + 1 alerts/5 min next to the 1 request/min of the SIRI ET collector, which
stays under the 4 requests/min the per-operator collector documents (not confirmed in Entur's docs).
"""

import json
import os
import sys
import time
from datetime import datetime

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.callstats import CallStats
from utils.s3_upload import raise_if_failed_saves, reset_failed_saves, save_parquet_to_s3

GTFS_RT_URL = "https://api.entur.io/realtime/v1/gtfs-rt"
HEADERS = {"ET-Client-Name": "entur-multimodal-analytics"}
POLLS_PER_MINUTE = int(os.getenv("NATIONAL_POLLS_PER_MINUTE", "2"))
VEHICLE_MAX_AGE_S = int(os.getenv("VEHICLE_MAX_AGE_S", "600"))
ALERTS_EVERY_MIN = 5

_VP = gtfs_realtime_pb2.VehiclePosition


def _name(enum, message, field):
    """Enum name of an optional field, None when the producer did not set it."""
    return enum.Name(getattr(message, field)) if message.HasField(field) else None


def decode_vehicle_positions(feed, max_age_s: int = VEHICLE_MAX_AGE_S, now: int | None = None) -> pd.DataFrame:
    now = int(time.time()) if now is None else now
    rows = []
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        ts = v.timestamp if v.HasField("timestamp") else None
        if max_age_s and (ts is None or now - ts > max_age_s):
            continue
        pos, trip = v.position, v.trip
        has_pos = v.HasField("position")
        rows.append({
            "entity_id": e.id,
            "vehicle_id": v.vehicle.id or None,
            "vehicle_label": v.vehicle.label or None,
            "trip_id": trip.trip_id or None,
            "route_id": trip.route_id or None,
            "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
            "start_date": trip.start_date or None,
            "latitude": pos.latitude if has_pos else None,
            "longitude": pos.longitude if has_pos else None,
            "bearing": pos.bearing if has_pos and pos.HasField("bearing") else None,
            "speed_ms": pos.speed if has_pos and pos.HasField("speed") else None,
            "current_status": _name(_VP.VehicleStopStatus, v, "current_status"),
            "stop_id": v.stop_id or None,
            "congestion_level": _name(_VP.CongestionLevel, v, "congestion_level"),
            "occupancy_status": _name(_VP.OccupancyStatus, v, "occupancy_status"),
            "vehicle_timestamp": ts,
            "feed_timestamp": feed.header.timestamp,
            "fetched_at": now,
        })
    df = pd.DataFrame(rows)
    for col in ("direction_id", "vehicle_timestamp"):
        if col in df:
            df[col] = _int64(df[col])
    return df


_INT64_MAX = 2**63 - 1


def _int64(series: pd.Series) -> pd.Series:
    """Nullable int64. Protobuf uint64 values do not cast safely, and a producer can send a value that
    does not fit at all (seen in an alert's active_period.start); that becomes null."""
    return pd.array([None if pd.isna(x) or int(x) > _INT64_MAX else int(x) for x in series], dtype="Int64")


def _text(translated) -> str | None:
    return translated.translation[0].text if translated.translation else None


def decode_alerts(feed, now: int | None = None) -> pd.DataFrame:
    now = int(time.time()) if now is None else now
    rows = []
    for e in feed.entity:
        if not e.HasField("alert"):
            continue
        a = e.alert
        period = a.active_period[0] if a.active_period else None
        informed = [
            {k: v for k, v in {"agency_id": i.agency_id, "route_id": i.route_id, "stop_id": i.stop_id,
                               "trip_id": i.trip.trip_id}.items() if v}
            for i in a.informed_entity
        ]
        rows.append({
            "entity_id": e.id,
            "cause": _name(gtfs_realtime_pb2.Alert.Cause, a, "cause"),
            "effect": _name(gtfs_realtime_pb2.Alert.Effect, a, "effect"),
            "header_text": _text(a.header_text),
            "description_text": _text(a.description_text),
            "active_start": period.start if period is not None and period.HasField("start") else None,
            "active_end": period.end if period is not None and period.HasField("end") else None,
            "n_active_periods": len(a.active_period),
            "informed_entities": json.dumps(informed, ensure_ascii=False),
            "feed_timestamp": feed.header.timestamp,
            "fetched_at": now,
        })
    df = pd.DataFrame(rows)
    for col in ("active_start", "active_end"):
        if col in df:
            df[col] = _int64(df[col])
    return df


CALLS = CallStats()  # successful and failed calls, printed at the end of every invocation


def fetch_feed(feed_type: str):
    """GET one national GTFS-RT feed; None on any failure (recorded in CALLS)."""
    name = f"entur_{feed_type.replace('-', '_')}"
    started = time.time()
    try:
        response = requests.get(f"{GTFS_RT_URL}/{feed_type}", headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"national {feed_type} -> HTTP {response.status_code}")
            CALLS.record(name, "failed", time.time() - started, f"HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"national {feed_type} -> connection error: {e}")
        CALLS.record(name, "failed", time.time() - started, type(e).__name__)
        return None
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    CALLS.record(name, "ok", time.time() - started)
    return feed


def collect(feed_type: str) -> int:
    """Fetch, decode and store one feed; returns the number of rows written."""
    feed = fetch_feed(feed_type)
    if feed is None:
        return 0
    df = decode_vehicle_positions(feed) if feed_type == "vehicle-positions" else decode_alerts(feed)
    if df.empty:
        print(f"national {feed_type}: no rows")
        CALLS.record(f"entur_{feed_type.replace('-', '_')}_rows", "empty")
        return 0
    today = datetime.now().strftime("%Y-%m-%d")
    stamp = datetime.now().strftime("%H%M%S")
    save_parquet_to_s3(df, f"entur_national/{today}/{feed_type}_{stamp}.parquet")
    print(f"national {feed_type}: {len(df)} of {len(feed.entity)} entities -> S3")
    return len(df)


def lambda_handler(event, context):
    """Invoked every minute: alerts every 5th minute, vehicle positions POLLS_PER_MINUTE times."""
    global CALLS
    reset_failed_saves()
    CALLS = CallStats()
    start = time.monotonic()
    if datetime.now().minute % ALERTS_EVERY_MIN == 0:
        try:
            collect("alerts")
        except Exception as e:  # alerts are secondary, never let them block the vehicle polls
            print(f"national alerts failed: {e}")
    for i in range(POLLS_PER_MINUTE):
        wait = start + i * 60 / POLLS_PER_MINUTE - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            collect("vehicle-positions")
        except Exception as e:  # one failed poll must not skip the next one
            print(f"national vehicle-positions poll {i} failed: {e}")
    CALLS.emit()
    raise_if_failed_saves()
    return {"status": "ok"}


if __name__ == "__main__":
    while True:
        started = time.monotonic()
        lambda_handler({}, None)
        time.sleep(max(0, 60 - (time.monotonic() - started)))
