from fastapi import APIRouter, Depends, Query

from .. import db, files as s3_files
from ..deps import Bucket, TimeRange

router = APIRouter(prefix="/bikes", tags=["micromobility"])


@router.get("/availability")
def availability(
    rng: TimeRange = Depends(),
    bucket: Bucket = "hour",
    station: str | None = Query(None, description="`location` value of one station"),
):
    """Average bikes and free docks per time bucket (all stations, or one)."""
    files = s3_files.normalized_files("oslobysykkel", rng.start, rng.end)
    if not files:
        return []
    sql = f"""
        SELECT date_trunc('{bucket}', timestamp) AS ts,
               avg(num_bikes_available) AS bikes_avg,
               avg(num_docks_available) AS docks_avg,
               count(DISTINCT location) AS stations,
               count(*) AS observations
        FROM read_parquet(?, union_by_name = true)
        WHERE timestamp >= ? AND timestamp < ?
    """
    # oslobysykkel timestamps are stored without a time zone
    params = [files,
              rng.start.replace(tzinfo=None), rng.end.replace(tzinfo=None)]
    if station:
        sql += " AND location = ?"
        params.append(station)
    sql += " GROUP BY 1 ORDER BY 1"
    return db.query(sql, params)
