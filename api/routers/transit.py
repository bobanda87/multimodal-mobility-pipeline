from fastapi import APIRouter, Depends, Query

from .. import db, files as s3_files
from ..deps import retry_once_on_missing_file, Bucket, TimeRange

router = APIRouter(prefix="/transit", tags=["public transport"])


@router.get("/delays")
@retry_once_on_missing_file
def delays(
    rng: TimeRange = Depends(),
    bucket: Bucket = "hour",
    route_id: str | None = Query(None),
):
    """Public transport arrival delay (minutes) per time bucket, from Entur SIRI ET."""
    files = s3_files.normalized_files("entur_siri_et", rng.start, rng.end)
    if not files:
        return []
    sql = f"""
        SELECT date_trunc('{bucket}', to_timestamp(timestamp)) AS ts,
               avg(max_arrival_delay_sec) / 60.0 AS avg_delay_min,
               quantile_cont(max_arrival_delay_sec, 0.9) / 60.0 AS p90_delay_min,
               count(DISTINCT trip_id) AS trips
        FROM read_parquet(?, union_by_name = true)
        WHERE timestamp >= ? AND timestamp < ?
    """
    # some early files carry timestamp = 0 (1970-01-01); the lower bound excludes them
    params = [files,
              int(rng.start.timestamp()), int(rng.end.timestamp())]
    if route_id:
        sql += " AND route_id = ?"
        params.append(route_id)
    sql += " GROUP BY 1 ORDER BY 1"
    return db.query(sql, params)
