from fastapi import APIRouter, Depends, Query

from .. import db, files as s3_files
from ..deps import retry_once_on_missing_file, Bucket, TimeRange

router = APIRouter(prefix="/flights", tags=["aviation"])


@router.get("/delays")
@retry_once_on_missing_file
def delays(
    rng: TimeRange = Depends(),
    bucket: Bucket = "day",
    airport: str | None = Query(None, description="IATA code, e.g. OSL"),
):
    """Flights and delays per airport and time bucket.

    A flight is counted once (latest status per flight_id and schedule_time), so repeated
    status updates and files overlapping across date folders do not inflate the counts.
    """
    files = s3_files.normalized_files("avinor", rng.start, rng.end)
    if not files:
        return []
    sql = f"""
        WITH f AS (
            SELECT flight_id, airport, schedule_time,
                   arg_max(delay_minutes, status_time) AS delay_minutes,
                   min(timestamp) AS ts
            FROM read_parquet(?, union_by_name = true)
            WHERE timestamp >= ? AND timestamp < ?
    """
    params = [files, rng.start, rng.end]
    if airport:
        sql += " AND airport = ?"
        params.append(airport.upper())
    sql += f"""
            GROUP BY flight_id, airport, schedule_time
        )
        SELECT date_trunc('{bucket}', ts) AS ts, airport,
               count(*) AS flights,
               count(*) FILTER (WHERE delay_minutes > 0) AS delayed_flights,
               avg(delay_minutes) FILTER (WHERE delay_minutes > 0) AS avg_delay_minutes
        FROM f
        GROUP BY 1, 2 ORDER BY 1, 2
    """
    return db.query(sql, params)
