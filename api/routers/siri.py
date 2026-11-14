from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import db, files as s3_files
from ..deps import Bucket, TimeRange

router = APIRouter(prefix="/siri", tags=["public transport (SIRI ET, all operators)"])

MAX_RANGE_HOURS = 72  # one file per pass and minute: wider ranges open thousands of files


def _now() -> datetime:
    return datetime.now(timezone.utc)


@router.get("/journeys/latest")
def latest_journeys(
    minutes: int = Query(60, ge=1, le=360, description="Journeys that changed within this many minutes"),
    datasource: str | None = Query(None, description="Operator, e.g. RUT"),
    line_ref: str | None = Query(None, description="e.g. RUT:Line:31"),
    limit: int = Query(500, ge=1, le=5000),
):
    """
    Latest known state of every journey that changed in the last `minutes` minutes.

    Each pass holds only the journeys that changed since the previous one, so a journey that has not
    changed (e.g. one that has not started) is not listed.
    """
    end = _now()
    start = end - timedelta(minutes=minutes)
    files = s3_files.siri_national_files("journeys", start, end)
    if not files:
        return []
    sql = """
        SELECT datasource, trip_id, data_frame_ref, line_ref, published_line_name, vehicle_mode, vehicle_ref,
               origin_name, destination_name, monitored, cancellation, extra_journey, n_recorded, n_estimated,
               to_timestamp(recorded_at) AS recorded_at, to_timestamp(fetched_at) AS fetched_at
        FROM read_parquet(?, union_by_name = true)
        WHERE fetched_at >= ? AND fetched_at < ?
    """
    params = [files, int(start.timestamp()), int(end.timestamp())]
    if datasource:
        sql += " AND datasource = ?"
        params.append(datasource.upper())
    if line_ref:
        sql += " AND line_ref = ?"
        params.append(line_ref)
    sql += """
        QUALIFY row_number() OVER (PARTITION BY datasource, trip_id, data_frame_ref
                                   ORDER BY recorded_at DESC, fetched_at DESC) = 1
        ORDER BY recorded_at DESC LIMIT ?
    """
    params.append(limit)
    return db.query(sql, params)


@router.get("/journeys/{trip_id:path}/stops")
def journey_stops(
    trip_id: str,
    hours: int = Query(24, ge=1, le=48, description="How far back to look for passes"),
    date: str | None = Query(None, description="Operating date (DataFrameRef) of the journey, YYYY-MM-DD"),
    latest: bool = Query(False, description="One row per stop (the newest pass) instead of every pass"),
    limit: int = Query(5000, ge=1, le=20000),
):
    """
    How the delay of one journey developed stop by stop: every pass in which the journey changed,
    with the estimated delay at each stop and, once the vehicle has passed it, the recorded (actual) delay.
    """
    end = _now()
    start = end - timedelta(hours=hours)
    files = s3_files.siri_national_files("calls", start, end)
    if not files:
        return []
    sql = """
        SELECT datasource, trip_id, data_frame_ref, "order", stop_point_ref, stop_point_name, call_type,
               to_timestamp(fetched_at) AS fetched_at,
               to_timestamp(aimed_arrival) AS aimed_arrival, to_timestamp(actual_arrival) AS actual_arrival,
               arrival_delay_s, departure_delay_s, arrival_status, departure_status, cancellation
        FROM read_parquet(?, union_by_name = true)
        WHERE trip_id = ? AND fetched_at >= ? AND fetched_at < ?
    """
    params = [files, trip_id, int(start.timestamp()), int(end.timestamp())]
    if date:
        sql += " AND data_frame_ref = ?"
        params.append(date)
    if latest:
        sql += ' QUALIFY row_number() OVER (PARTITION BY data_frame_ref, "order" ORDER BY fetched_at DESC) = 1'
    sql += ' ORDER BY data_frame_ref, "order", fetched_at LIMIT ?'
    params.append(limit)
    rows = db.query(sql, params)
    if not rows:
        raise HTTPException(status_code=404, detail="No pass in the requested window mentions this journey")
    return rows


@router.get("/delays")
def delays(
    rng: TimeRange = Depends(),
    bucket: Bucket = "hour",
    datasource: str | None = Query(None, description="Operator, e.g. RUT"),
):
    """
    Realised arrival delay (seconds) per operator and time bucket: the recorded calls, i.e. stops the
    vehicle has actually passed, bucketed by the actual arrival time. Each stop of each journey counts once.
    """
    if rng.end - rng.start > timedelta(hours=MAX_RANGE_HOURS):
        raise HTTPException(status_code=422, detail=f"range is limited to {MAX_RANGE_HOURS} hours")
    files = s3_files.siri_national_files("calls", rng.start, rng.end)
    if not files:
        return []
    sql = f"""
        WITH stops AS (
            SELECT datasource, trip_id, data_frame_ref, "order",
                   arg_max(arrival_delay_s, fetched_at) AS arrival_delay_s,
                   arg_max(actual_arrival, fetched_at) AS actual_arrival
            FROM read_parquet(?, union_by_name = true)
            WHERE call_type = 'recorded' AND actual_arrival IS NOT NULL AND arrival_delay_s IS NOT NULL
              AND actual_arrival >= ? AND actual_arrival < ?
    """
    params = [files, int(rng.start.timestamp()), int(rng.end.timestamp())]
    if datasource:
        sql += " AND datasource = ?"
        params.append(datasource.upper())
    sql += f"""
            GROUP BY datasource, trip_id, data_frame_ref, "order"
        )
        SELECT date_trunc('{bucket}', to_timestamp(actual_arrival)) AS ts, datasource,
               count(*) AS stops, count(DISTINCT trip_id) AS trips,
               avg(arrival_delay_s) AS avg_delay_s,
               quantile_cont(arrival_delay_s, 0.9) AS p90_delay_s,
               avg((arrival_delay_s > 300)::INT) AS share_over_5min
        FROM stops GROUP BY 1, 2 ORDER BY 1, 2
    """
    return db.query(sql, params)
