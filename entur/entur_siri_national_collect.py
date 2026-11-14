"""
Entur national SIRI ET collector: every operator in one pass, once a minute.

The per-operator collector (entur_siri_et_collect.py) asks for one operator per request, drops every
journey that has no FramedVehicleJourneyRef (all of VYG, BNR and GOA, 17% of the country) and reads only
EstimatedCalls. This one asks for rest/et without datasetId, follows MoreData until the set is complete,
and keeps both estimated and recorded calls (the recorded ones carry the actual times of stops already
passed).

How a pass works (measured against the live API):
* A fixed requestorId makes Entur remember what was already sent. The first request (or one after the
  session expired, between 65 and 150 s without a request) returns the whole set in 3 pages; the requests
  after it return only what changed, about a quarter of the journeys, in 1 page. So a pass costs 1
  request, 3 when the session had expired. SIRI Lite GET is limited to 4 requests per minute.
* Pages are fetched back to back; a journey that changes in between shows up on two pages, so
  journeys are deduplicated on (DataSource, journey id, date), keeping the newest RecordedAtTime.

Raw files go to entur_national-style folder entur_siri_national/<date>/, two tables per pass:
  journeys_<HHMMSS>.parquet   one row per journey (what changed since the previous pass)
  calls_<HHMMSS>.parquet      one row per stop call of those journeys, estimated or recorded
"""

import io
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.s3_upload import save_parquet_to_s3

URL = "https://api.entur.io/realtime/v1/rest/et"
HEADERS = {"ET-Client-Name": "entur-multimodal-analytics"}
# One value for every invocation: Entur keys the "what did I already send" session on it
REQUESTOR_ID = os.getenv("SIRI_REQUESTOR_ID", "b8a4f8d8-5d7e-4c3a-9a31-3d0a6f0f8e10")
MAX_SIZE = 1500
MAX_PAGES = 8
FULL_SNAPSHOT_JOURNEYS = 3000  # a pass with at least this many journeys is the whole set, not a delta

_NS = "{http://www.siri.org.uk/siri}"


def _text(el, tag):
    child = el.find(_NS + tag)
    return child.text.strip() if child is not None and child.text else None


def _epoch(value):
    return int(datetime.fromisoformat(value).timestamp()) if value else None


def _bool(value):
    return None if value is None else value.lower() == "true"


def _call(call_el, call_type):
    aimed_arr, aimed_dep = _epoch(_text(call_el, "AimedArrivalTime")), _epoch(_text(call_el, "AimedDepartureTime"))
    exp_arr, exp_dep = _epoch(_text(call_el, "ExpectedArrivalTime")), _epoch(_text(call_el, "ExpectedDepartureTime"))
    act_arr, act_dep = _epoch(_text(call_el, "ActualArrivalTime")), _epoch(_text(call_el, "ActualDepartureTime"))
    arr = act_arr if act_arr is not None else exp_arr
    dep = act_dep if act_dep is not None else exp_dep
    return {
        "call_type": call_type,
        "order": int(_text(call_el, "Order") or 0),
        "stop_point_ref": _text(call_el, "StopPointRef"),
        "stop_point_name": _text(call_el, "StopPointName"),
        "aimed_arrival": aimed_arr, "expected_arrival": exp_arr, "actual_arrival": act_arr,
        "aimed_departure": aimed_dep, "expected_departure": exp_dep, "actual_departure": act_dep,
        "arrival_delay_s": arr - aimed_arr if arr is not None and aimed_arr is not None else None,
        "departure_delay_s": dep - aimed_dep if dep is not None and aimed_dep is not None else None,
        "arrival_status": _text(call_el, "ArrivalStatus"),
        "departure_status": _text(call_el, "DepartureStatus"),
        "cancellation": _bool(_text(call_el, "Cancellation")),
        "destination_display": _text(call_el, "DestinationDisplay"),
        "visit_number": int(_text(call_el, "VisitNumber")) if _text(call_el, "VisitNumber") else None,
    }


def _journey_id(journey):
    """(id, how): the id sits in FramedVehicleJourneyRef for most operators, directly in the journey for others."""
    framed = journey.find(_NS + "FramedVehicleJourneyRef")
    if framed is not None and _text(framed, "DatedVehicleJourneyRef"):
        return _text(framed, "DatedVehicleJourneyRef"), _text(framed, "DataFrameRef"), "framed"
    if _text(journey, "DatedVehicleJourneyRef"):
        return _text(journey, "DatedVehicleJourneyRef"), None, "dated"
    if _text(journey, "EstimatedVehicleJourneyCode"):
        return _text(journey, "EstimatedVehicleJourneyCode"), None, "code"
    return None, None, None


def parse_page(content: bytes, journeys: dict) -> int:
    """Add the journeys of one page to `journeys` {key: (journey row, [call rows])}; returns rows seen."""
    seen = 0
    for _, el in ET.iterparse(io.BytesIO(content), events=("end",)):
        if el.tag != _NS + "EstimatedVehicleJourney":
            continue
        seen += 1
        trip_id, frame, how = _journey_id(el)
        if trip_id:
            datasource = _text(el, "DataSource")
            recorded = _epoch(_text(el, "RecordedAtTime"))
            key = (datasource, trip_id, frame)
            known = journeys.get(key)
            if known is None or (recorded or 0) >= (known[0]["recorded_at"] or 0):
                calls = [_call(c, "recorded") for c in el.findall(f"{_NS}RecordedCalls/{_NS}RecordedCall")]
                calls += [_call(c, "estimated") for c in el.findall(f"{_NS}EstimatedCalls/{_NS}EstimatedCall")]
                journeys[key] = ({
                    "datasource": datasource, "trip_id": trip_id, "id_kind": how, "data_frame_ref": frame,
                    "line_ref": _text(el, "LineRef"), "published_line_name": _text(el, "PublishedLineName"),
                    "direction_ref": _text(el, "DirectionRef"), "operator_ref": _text(el, "OperatorRef"),
                    "vehicle_ref": _text(el, "VehicleRef"), "vehicle_mode": _text(el, "VehicleMode"),
                    "origin_name": _text(el, "OriginName"), "destination_name": _text(el, "DestinationName"),
                    "monitored": _bool(_text(el, "Monitored")), "cancellation": _bool(_text(el, "Cancellation")),
                    "extra_journey": _bool(_text(el, "ExtraJourney")),
                    "is_complete_stop_sequence": _bool(_text(el, "IsCompleteStopSequence")),
                    "prediction_inaccurate": _bool(_text(el, "PredictionInaccurate")),
                    "occupancy": _text(el, "Occupancy"), "recorded_at": recorded,
                    "n_recorded": sum(1 for c in calls if c["call_type"] == "recorded"),
                    "n_estimated": sum(1 for c in calls if c["call_type"] == "estimated"),
                }, calls)
        el.clear()
    return seen


def fetch_pass(requestor_id: str = REQUESTOR_ID):
    """All pages of one pass. Returns (journeys {key: (row, calls)}, pages, rows_seen, complete)."""
    journeys, pages, rows = {}, 0, 0
    while pages < MAX_PAGES:
        try:
            response = requests.get(URL, params={"maxSize": MAX_SIZE, "requestorId": requestor_id},
                                    headers=HEADERS, timeout=60)
        except Exception as e:
            print(f"siri national page {pages + 1} -> connection error: {e}")
            return journeys, pages, rows, False
        if response.status_code != 200:
            print(f"siri national page {pages + 1} -> HTTP {response.status_code}")
            return journeys, pages, rows, False
        pages += 1
        rows += parse_page(response.content, journeys)
        if b"<MoreData>true" not in response.content:
            return journeys, pages, rows, True
    print(f"siri national: still MoreData after {MAX_PAGES} pages, stopping")
    return journeys, pages, rows, False


def to_frames(journeys: dict, fetched_at: int, pages: int, rows: int, complete: bool):
    jrows, crows = [], []
    for (datasource, trip_id, frame), (jrow, calls) in journeys.items():
        jrow = {**jrow, "fetched_at": fetched_at}
        jrows.append(jrow)
        for c in calls:
            crows.append({"datasource": datasource, "trip_id": trip_id, "data_frame_ref": frame,
                          "journey_recorded_at": jrow["recorded_at"], "fetched_at": fetched_at, **c})
    jdf, cdf = pd.DataFrame(jrows), pd.DataFrame(crows)
    for df in (jdf, cdf):
        df["pass_pages"], df["pass_rows"], df["pass_complete"] = pages, rows, complete
        df["full_snapshot"] = rows >= FULL_SNAPSHOT_JOURNEYS
    return jdf, cdf


def collect() -> dict:
    started = time.time()
    fetched_at = int(started)
    journeys, pages, rows, complete = fetch_pass()
    if not journeys:
        print("siri national: nothing collected")
        return {"journeys": 0, "pages": pages}
    jdf, cdf = to_frames(journeys, fetched_at, pages, rows, complete)
    today, stamp = datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%H%M%S")
    save_parquet_to_s3(jdf, f"entur_siri_national/{today}/journeys_{stamp}.parquet")
    save_parquet_to_s3(cdf, f"entur_siri_national/{today}/calls_{stamp}.parquet")
    print(f"siri national: {pages} page(s), {rows} rows -> {len(jdf)} journeys, {len(cdf)} calls "
          f"({'full' if rows >= FULL_SNAPSHOT_JOURNEYS else 'delta'}{'' if complete else ', INCOMPLETE'}), {time.time() - started:.1f}s")
    return {"journeys": len(jdf), "calls": len(cdf), "pages": pages}


def lambda_handler(event, context):
    return {"status": "ok", **collect()}


if __name__ == "__main__":
    while True:
        started = time.monotonic()
        collect()
        time.sleep(max(0, 60 - (time.monotonic() - started)))
