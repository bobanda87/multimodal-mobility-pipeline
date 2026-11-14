from fastapi import APIRouter, Depends, Query

from .. import db, files as s3_files
from ..deps import retry_once_on_missing_file, Bucket, TimeRange

router = APIRouter(prefix="/weather", tags=["infrastructure"])


@router.get("/observations")
@retry_once_on_missing_file
def observations(
    rng: TimeRange = Depends(),
    bucket: Bucket = "hour",
    location: str | None = Query(None, description="Weather station `location` value"),
):
    """Average road weather measurements per time bucket."""
    files = s3_files.normalized_files("vegvesen", rng.start, rng.end)
    if not files:
        return []
    sql = f"""
        SELECT date_trunc('{bucket}', timestamp) AS ts,
               avg(air_temperature) AS air_temperature,
               avg(road_temperature) AS road_temperature,
               avg(precipitation_intensity) AS precipitation_intensity,
               avg(wind_speed) AS wind_speed,
               count(DISTINCT location) AS stations
        FROM read_parquet(?, union_by_name = true)
        WHERE timestamp >= ? AND timestamp < ?
    """
    params = [files, rng.start, rng.end]
    if location:
        sql += " AND location = ?"
        params.append(location)
    sql += " GROUP BY 1 ORDER BY 1"
    return db.query(sql, params)
