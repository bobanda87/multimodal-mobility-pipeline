import os
import sys

                                                                                           
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.s3_upload import (
    save_parquet_to_s3,
    list_parquet_files_from_s3,
    read_parquet_from_s3,
    load_normalized_files,
    mark_files_as_normalized
)

                                                                                        
try:
    import certifi
    if not os.environ.get("SSL_CERT_FILE"):
        os.environ["SSL_CERT_FILE"] = certifi.where()
    if not os.environ.get("AWS_CA_BUNDLE"):
        os.environ["AWS_CA_BUNDLE"] = certifi.where()
except ImportError:
    pass

from glob import glob
import re
import polars as pl
import pandas as pd
from datetime import datetime, timezone


def get_latest_parquet(data_dir, pattern="*.parquet"):
    """Find the most recently modified Parquet file in a directory.

    Args:
        data_dir: Directory to search in
        pattern: Optional pattern to filter files (e.g., "*vehicle-positions*.parquet")
    """
    files = glob(os.path.join(data_dir, "**", pattern), recursive=True)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def get_all_parquet_files(data_dir, pattern="*.parquet"):
    """Find all Parquet files in a directory (legacy function for local files).

    Args:
        data_dir: Directory to search in
        pattern: Optional pattern to filter files (e.g., "*vehicle-positions*.parquet")

    Returns:
        List of parquet file paths, sorted by modification time (oldest first)
    """
    files = glob(os.path.join(data_dir, "**", pattern), recursive=True)
    if not files:
        return []
    return sorted(files, key=os.path.getmtime)


def get_all_parquet_files_from_s3(source: str, pattern: str = "*.parquet", with_metadata: bool = False, date_prefix: str | None = None):
    """Find all Parquet files from S3 for a specific source.

    Args:
        source: Data source name (e.g., "avinor", "oslobysykkel", "vegvesen", "entur" for SIRI ET)
        pattern: Optional pattern to filter files (e.g., "*vehicle-positions*.parquet")
        with_metadata: If True, return list of dicts with 'key' and 'last_modified'
        date_prefix: If set (e.g. "2026-01-29"), list only files under that date folder (only today's files).

    Returns:
        List of S3 keys (full paths), sorted by LastModified time (oldest first),
        or list of dicts if with_metadata=True
    """
    return list_parquet_files_from_s3(source, pattern, exclude_normalized=True, with_metadata=with_metadata, date_prefix=date_prefix)


def group_files_by_date(files_with_metadata: list) -> dict:
    """
    Group files by date based on their S3 key path or last_modified timestamp.

    Args:
        files_with_metadata: List of dicts with 'key' and 'last_modified'

    Returns:
        Dictionary mapping date strings (YYYY-MM-DD) to lists of file keys
    """
    from collections import defaultdict
    from datetime import datetime

    files_by_date = defaultdict(list)

    for file_info in files_with_metadata:
        key = file_info['key']
        last_modified = file_info['last_modified']

                                                                                                      
        date_str = None
        parts = key.split('/')
                                                  
        for part in parts:
            if len(part) == 10 and part.count('-') == 2:
                try:
                                                
                    datetime.strptime(part, '%Y-%m-%d')
                    date_str = part
                    break
                except ValueError:
                    continue

                                                    
        if date_str is None:
            if isinstance(last_modified, datetime):
                date_str = last_modified.strftime('%Y-%m-%d')
            else:
                                                
                date_str = last_modified.date().isoformat()

        files_by_date[date_str].append(key)

    return dict(files_by_date)


def _unify_avinor_schemas(raw_dfs: list[pl.DataFrame]) -> list[pl.DataFrame]:
    """Align Avinor raw DataFrames so pl.concat(how='diagonal') does not raise SchemaError (String vs Null etc.)."""
    if not raw_dfs:
        return raw_dfs
    all_cols = set()
    for df in raw_dfs:
        all_cols.update(df.columns)
    unified = {}
    for col in all_cols:
        best = pl.Null
        for df in raw_dfs:
            if col not in df.columns:
                continue
            dt = df.schema[col]
            if dt == pl.Null or (str(dt).startswith("Unknown") or "Unknown" in str(dt)):
                continue
            if dt == pl.String or (hasattr(pl, "Utf8") and str(dt) == "Utf8"):
                best = pl.String
                break
            if "Datetime" in str(dt):
                best = pl.Datetime("us")
                break
            if dt == pl.Date:
                best = pl.Date
                break
            if dt in (pl.Float64, pl.Float32):
                best = pl.Float64
                break
            if dt in (pl.Int64, pl.Int32, pl.UInt32, pl.UInt64):
                if best != pl.Float64:
                    best = pl.Int64
            if dt == pl.Boolean:
                best = pl.Boolean
                break
        if best == pl.Null:
            best = pl.String
        unified[col] = best
    out = []
    for df in raw_dfs:
        exprs = []
        for col, dtype in unified.items():
            if col in df.columns:
                exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
            else:
                exprs.append(pl.lit(None).cast(dtype).alias(col))
        out.append(df.select(exprs))
    return out


def _datetime_expr_to_utc(df: pl.DataFrame, col: str) -> pl.Expr:
    """Ujednačava Polars Datetime na UTC (naivni -> replace_time_zone, već sa TZ -> convert)."""
    dtype = df.schema.get(col)
    c = pl.col(col)
    if dtype is None:
        return c
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            return c.dt.replace_time_zone("UTC")
        return c.dt.convert_time_zone("UTC")
    return c


                                                                   
def normalize_avinor(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Avinor flight data: unified schema + status_time, delayed, delay_minutes, uniqueID (bez odbacivanja podataka)."""
    if "status" not in df.columns:
        df = df.with_columns([pl.lit(None).cast(pl.String).alias("status")])
    else:
        df = df.with_columns(
            [pl.col("status").cast(pl.String).alias("status")])

    df = df.with_columns([
        pl.col("schedule_time")
        .str.replace("Z", "+00:00")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
        .alias("timestamp")
    ])

                                                    
    if "status_time" in df.columns:
        df = df.with_columns([
            pl.col("status_time")
            .str.replace("Z", "+00:00")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .alias("status_time_dt")
        ])
    else:
        df = df.with_columns(
            [pl.lit(None).cast(pl.Datetime("us", time_zone="UTC")).alias("status_time_dt")])

                                                                             
                                                                                  
    df = df.with_columns([
        _datetime_expr_to_utc(df, "timestamp").alias("timestamp"),
        _datetime_expr_to_utc(df, "status_time_dt").alias("status_time_dt"),
    ])

                                                                      
    df = df.with_columns([
        pl.when(pl.col("status_time_dt").is_not_null())
        .then((pl.col("status_time_dt") - pl.col("timestamp")).dt.total_seconds() / 60)
        .otherwise(None)
        .cast(pl.Float64)
        .alias("delay_minutes")
    ])

                                                                                                        
    out_cols = [
        pl.col("airline") if "airline" in df.columns else pl.lit(
            None).cast(pl.String).alias("airline"),
        pl.col("flight_id") if "flight_id" in df.columns else pl.lit(
            None).cast(pl.String).alias("flight_id"),
        pl.col("airport"),
        pl.col("schedule_time") if "schedule_time" in df.columns else pl.lit(
            None).cast(pl.String).alias("schedule_time"),
        pl.col("airport").alias("location"),
        pl.col("status").alias("metric_value"),
        pl.col("timestamp"),
        pl.col("arr_dep").alias("metric_name"),
        pl.lit("avinor").alias("source"),
        pl.col("status_time_dt").alias("status_time"),
        pl.col("delayed") if "delayed" in df.columns else pl.lit(
            None).cast(pl.String).alias("delayed"),
        pl.col("delay_minutes"),
    ]
    if "uniqueID" in df.columns:
        out_cols.append(pl.col("uniqueID").cast(pl.String))
    else:
        out_cols.append(pl.lit(None).cast(pl.String).alias("uniqueID"))

    return df.select(out_cols)


                                                                   
def normalize_oslobysykkel(df: pl.DataFrame, station_capacity: dict | None = None) -> pl.DataFrame:
    """Normalize Oslo Bysykkel station status data. Zadržava sva korisna polja (bez odbacivanja).

    Args:
        df: DataFrame with station_status data (GBFS: station_id, is_installed, is_renting,
            is_returning, last_reported, num_vehicles_available, num_bikes_available,
            num_docks_available, vehicle_types_available)
        station_capacity: Optional dict mapping station_id to capacity
    """
    df = df.with_columns([
        pl.from_epoch(pl.col("last_reported"),
                      time_unit="s").alias("timestamp")
    ])

    if station_capacity:
        capacity_df = pl.DataFrame({
            "station_id": list(station_capacity.keys()),
            "capacity": list(station_capacity.values())
        })
        df = df.join(capacity_df, on="station_id", how="left")
        df = df.with_columns([
            (pl.col("capacity") - pl.col("num_bikes_available")).alias("bikes_in_use")
        ])
    else:
        df = df.with_columns([
            pl.col("num_docks_available").alias("bikes_in_use")
        ])
        if "capacity" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("capacity"))

                                                                                                                
    out = [
        pl.col("station_id").alias("location"),
        pl.col("bikes_in_use").cast(
            pl.Float64, strict=False).alias("metric_value"),
        pl.col("num_bikes_available"),
        pl.col("num_docks_available"),
        pl.col("is_renting"),
        pl.col("is_returning"),
        pl.col("timestamp"),
        pl.lit("oslobysykkel").alias("source"),
        pl.col("capacity"),
        pl.col("is_installed") if "is_installed" in df.columns else pl.lit(
            None).cast(pl.Boolean).alias("is_installed"),
        pl.col("num_vehicles_available") if "num_vehicles_available" in df.columns else pl.lit(
            None).cast(pl.Int64).alias("num_vehicles_available"),
    ]
    return df.select(out)


                                                                   
def _weather_struct_path(expr, *path: str):
    """Follow struct path; return null if any step is null (safe for optional nested structs)."""
    for p in path:
        expr = expr.struct.field(p)
    return expr


def normalize_vegvesen(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Vegvesen weather data: road/air temp, precipitation, wind, humidity, dew point.
    Keeps one row per measurement type per site per time; metric_value for backward compat.
    Parquet stores ns10:physicalQuantity as List[Struct] per row; after explode we use struct field access.
    """
    if "ns10:physicalQuantity" not in df.columns:
        return pl.DataFrame()
    df = df.explode("ns10:physicalQuantity")

                                                                                             
    time_col = "ns10:measurementTimeDefault.ns10:timeValue"
    if time_col not in df.columns:
        return pl.DataFrame()
    df = df.with_columns([
        pl.col(time_col).str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False).alias("timestamp")
    ])

    pq = pl.col("ns10:physicalQuantity")
    inner = pq.struct.field("ns10:physicalQuantity")
    bd = inner.struct.field("ns10:basicData")

                                                                                              
    def _safe_float(expr):
        return expr.cast(pl.Float64, strict=False)

    df = df.with_columns([
        _safe_float(_weather_struct_path(bd, "ns10:roadSurfaceConditionMeasurements",
                    "roadSurfaceTemperature", "temperature")).alias("road_temperature"),
        _safe_float(_weather_struct_path(bd, "ns10:temperature",
                    "airTemperature", "temperature")).alias("air_temperature"),
        _safe_float(_weather_struct_path(bd, "ns10:precipitationDetail", "precipitationIntensity",
                    "millimetresPerHourIntensity")).alias("precipitation_intensity"),
        _safe_float(_weather_struct_path(bd, "ns10:wind",
                    "windSpeed", "windSpeed")).alias("wind_speed"),
        _safe_float(_weather_struct_path(bd, "ns10:wind", "windDirectionBearing",
                    "directionBearing")).alias("wind_direction_bearing"),
        _safe_float(_weather_struct_path(bd, "ns10:wind",
                    "maximumWindSpeed", "windSpeed")).alias("max_wind_speed"),
        _safe_float(_weather_struct_path(bd, "ns10:humidity",
                    "relativeHumidity", "percentage")).alias("relative_humidity"),
        _safe_float(_weather_struct_path(bd, "ns10:temperature",
                    "dewPointTemperature", "temperature")).alias("dew_point_temperature"),
    ])
                                                             
    try:
        df = df.with_columns(inner.struct.field(
            "@xsi:type").alias("measurement_type"))
    except Exception:
        df = df.with_columns(pl.lit(None).cast(
            pl.String).alias("measurement_type"))

                                                               
    any_value = (
        pl.col("road_temperature").is_not_null()
        | pl.col("air_temperature").is_not_null()
        | pl.col("precipitation_intensity").is_not_null()
        | pl.col("wind_speed").is_not_null()
        | pl.col("wind_direction_bearing").is_not_null()
        | pl.col("max_wind_speed").is_not_null()
        | pl.col("relative_humidity").is_not_null()
        | pl.col("dew_point_temperature").is_not_null()
    )
    df = df.filter(any_value)

    df = df.with_columns(
        pl.coalesce(
            pl.col("road_temperature"),
            pl.col("air_temperature"),
            pl.col("precipitation_intensity"),
            pl.col("wind_speed"),
            pl.col("max_wind_speed"),
            pl.col("relative_humidity"),
            pl.col("dew_point_temperature"),
        ).alias("metric_value")
    )

                                                                                                
    loc_col = "ns10:measurementSiteReference.@id"
    loc_expr = pl.col(loc_col) if loc_col in df.columns else pl.lit(
        None).cast(pl.String)
    metric_name_expr = pq.struct.field("@index")

    return df.select([
        loc_expr.alias("location"),
        metric_name_expr.alias("metric_name"),
        pl.col("metric_value"),
        pl.col("timestamp"),
        pl.lit("vegvesen").alias("source"),
        pl.col("road_temperature"),
        pl.col("air_temperature"),
        pl.col("precipitation_intensity"),
        pl.col("wind_speed"),
        pl.col("wind_direction_bearing"),
        pl.col("max_wind_speed"),
        pl.col("relative_humidity"),
        pl.col("dew_point_temperature"),
        pl.col("measurement_type"),
    ])


def normalize_vegvesen_travel_times(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Vegvesen travel_times (Datex2 ElaboratedDataPublication).
    Output: location_id, period_start, period_end, free_flow_travel_time_sec,
    free_flow_speed_kmh, traffic_status, source.
    """
    pq_col = "ns2:messageContainer.ns2:payload.ns10:physicalQuantity"
    if pq_col not in df.columns:
        return pl.DataFrame()
    df = df.explode(pq_col)
                                                                                  
    loc_struct = pl.col(pq_col).struct.field(
        "ns10:pertinentLocation").struct.field("ns8:predefinedLocationReference")
    location_id = loc_struct.struct.field("@id")
    bd = pl.col(pq_col).struct.field("ns10:basicData")
    period_struct = bd.struct.field(
        "ns10:measurementOrCalculationTime").struct.field("ns10:period")
    period_start_str = period_struct.struct.field("startOfPeriod")
    period_end_str = period_struct.struct.field("endOfPeriod")
    free_flow_travel_time_sec = bd.struct.field("ns10:freeFlowTravelTime").struct.field(
        "ns10:duration").cast(pl.Float64, strict=False)
    free_flow_speed_kmh = bd.struct.field("ns10:freeFlowSpeed").struct.field(
        "speed").cast(pl.Float64, strict=False)
    traffic_status = bd.struct.field(
        "ns10:trafficStatus").struct.field("ns10:trafficStatusValue")
    df = df.with_columns([
        location_id.alias("location_id"),
        period_start_str.str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False).alias("period_start"),
        period_end_str.str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False).alias("period_end"),
        free_flow_travel_time_sec.alias("free_flow_travel_time_sec"),
        free_flow_speed_kmh.alias("free_flow_speed_kmh"),
        traffic_status.alias("traffic_status"),
    ])
    return df.select([
        pl.col("location_id"),
        pl.col("period_start"),
        pl.col("period_end"),
        pl.col("free_flow_travel_time_sec"),
        pl.col("free_flow_speed_kmh"),
        pl.col("traffic_status"),
        pl.lit("vegvesen").alias("source"),
    ])


def normalize_vegvesen_traffic_situations(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Vegvesen traffic_situations (DATEX GetSituation): broj i tip događaja po vremenu i lokaciji.
    Raw parquet je flatten situation records (json_normalize). Izvlači timestamp, tip situacije i lokaciju
    (ako postoji), agregira po minuti (i po tipu/lokaciji). Output: date, minute, situation_type, location_id, event_count, source.
    """
    if df.height == 0:
        return pl.DataFrame()
    cols_lower = [c.lower() for c in df.columns]
                                                                            
    time_col = None
    for needle in ["creationtime", "overallstarttime", "timevalue", "startofperiod"]:
        for i, cl in enumerate(cols_lower):
            if needle in cl:
                time_col = df.columns[i]
                break
        if time_col is not None:
            break
    if time_col is None:
        return pl.DataFrame()
                                                          
    type_col = None
    for needle in ["situationtype", "reporttype", "situationrecordtype"]:
        for i, cl in enumerate(cols_lower):
            if needle in cl:
                type_col = df.columns[i]
                break
        if type_col is not None:
            break
                                                                           
    location_col = None
    for needle in ["locationreference", "alertclocation", "countynumber", "locationid", "predefinedlocation", "@id"]:
        for i, cl in enumerate(cols_lower):
            if needle in cl and "location" in cl or needle in cl:
                                                                             
                location_col = df.columns[i]
                if "location" in cl or "county" in cl or "alert" in cl:
                    break
        if location_col is not None:
            break
                                                  
    ts = df[time_col]
    if ts.dtype == pl.Utf8 or str(ts.dtype) == "String":
        ts = ts.str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
    elif ts.dtype != pl.Datetime:
        ts = ts.cast(pl.Datetime("us"), strict=False)
    base = df.with_columns([
        ts.dt.replace_time_zone("UTC").dt.truncate("1m").alias("minute"),
        pl.col(type_col).cast(pl.Utf8).fill_null("unknown").alias(
            "situation_type") if type_col else pl.lit("unknown").alias("situation_type"),
        pl.col(location_col).cast(pl.Utf8).fill_null("").alias(
            "location_id") if location_col else pl.lit("").alias("location_id"),
    ])
                                                                      
    group_cols = ["minute", "situation_type"]
    if location_col and base["location_id"].n_unique() > 1:
        group_cols.append("location_id")
    else:
        base = base.drop("location_id")
        base = base.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("location_id"))
    out = base.group_by(group_cols).agg(pl.len().alias("event_count"))
    if "location_id" not in out.columns:
        out = out.with_columns(pl.lit(None).cast(pl.Utf8).alias("location_id"))
    out = out.with_columns([
        pl.col("minute").dt.strftime("%Y-%m-%d").alias("date"),
        pl.lit("vegvesen").alias("source"),
    ])
    return out.select(["date", "minute", "situation_type", "location_id", "event_count", "source"])


def normalize_entur_trip_updates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize Entur trip-updates or SIRI ET: extract per-trip delay from stop_time_updates.
    Accepts both GTFS-RT trip-updates and SIRI ET parquet (same structure: trip_id, route_id,
    timestamp, start_date, start_time, stop_time_updates with arrival_delay/departure_delay).
    Handles: start_time all null (cast to String); struct without arrival_delay/departure_delay.
    Output schema: trip_id, route_id, timestamp, start_date, start_time, departure_timestamp
    (Unix s), max_arrival_delay_sec, max_departure_delay_sec.
    """
    if "stop_time_updates" not in df.columns or df.height == 0:
        return pl.DataFrame()
                                                                                                     
    if "start_time" in df.columns:
        df = df.with_columns(pl.col("start_time").cast(
            pl.Utf8, strict=False).fill_null(""))
    base_cols = ["trip_id", "route_id", "timestamp", "stop_time_updates"]
    optional = []
    if "start_date" in df.columns:
        optional.append("start_date")
    if "start_time" in df.columns:
        optional.append("start_time")
    select_cols = base_cols + optional
    exploded = df.select(select_cols).explode("stop_time_updates")
                                                                          
                                                               
    stu = pl.col("stop_time_updates")
    schema_stu = df.schema.get("stop_time_updates")
    if schema_stu is None:
        struct_fields = []
    elif isinstance(schema_stu, pl.Struct):
        struct_fields = [f.name for f in schema_stu.fields]
    elif isinstance(schema_stu, pl.List):
        inner = getattr(schema_stu, "inner", schema_stu)
        struct_fields = [f.name for f in inner.fields] if isinstance(
            inner, pl.Struct) else []
    else:
        struct_fields = []
    has_arrival = "arrival_delay" in struct_fields
    has_departure = "departure_delay" in struct_fields
    delay_cols = []
    if has_arrival:
        delay_cols.append(stu.struct.field(
            "arrival_delay").alias("arrival_delay"))
    else:
        delay_cols.append(pl.lit(None).cast(pl.Int64).alias("arrival_delay"))
    if has_departure:
        delay_cols.append(stu.struct.field(
            "departure_delay").alias("departure_delay"))
    else:
        delay_cols.append(pl.lit(None).cast(pl.Int64).alias("departure_delay"))
    exploded = exploded.with_columns(delay_cols).drop("stop_time_updates")
    exploded = exploded.with_columns([
        pl.col("arrival_delay").fill_null(0).cast(pl.Int64),
        pl.col("departure_delay").fill_null(0).cast(pl.Int64),
    ])
    group_cols = ["trip_id", "route_id", "timestamp"] + optional
    out = exploded.group_by(group_cols).agg([
        pl.col("arrival_delay").max().alias("max_arrival_delay_sec"),
        pl.col("departure_delay").max().alias("max_departure_delay_sec"),
    ])
                                                                                         
    if "start_date" in out.columns and "start_time" in out.columns:
                                                                                  
        valid_ts = out["start_time"].str.len_chars() > 0
                                                                                 
        parts = out["start_time"].str.split_exact(":", 3)
        hours = parts.struct.field("field_0").cast(
            pl.Int32, strict=False).fill_null(0)
        minutes = parts.struct.field("field_1").cast(
            pl.Int32, strict=False).fill_null(0)
        seconds = parts.struct.field("field_2").cast(
            pl.Int32, strict=False).fill_null(0)
        base_date = out["start_date"].cast(pl.Utf8).str.strptime(
            pl.Date, "%Y%m%d", strict=False)
        base_dt = base_date.cast(pl.Datetime("us"))
        departure_dt = base_dt + \
            pl.duration(hours=hours, minutes=minutes, seconds=seconds)
        out = out.with_columns(
            pl.when(valid_ts)
            .then(departure_dt.dt.epoch("s").cast(pl.Int64))
            .otherwise(pl.lit(None).cast(pl.Int64))
            .alias("departure_timestamp")
        )
    else:
        out = out.with_columns(pl.lit(None).cast(
            pl.Int64).alias("departure_timestamp"))

                                                                                                   
                                                                                                            
                                                                  
                                                                                                        
                                            
    if "start_date" in out.columns:
        svc_date = (
            pl.col("start_date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, "%Y%m%d", strict=False)
        )
        svc_midnight_utc = (
            svc_date.cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .dt.epoch("s")
            .cast(pl.Int64)
        )
        dep = pl.col("departure_timestamp")
        out = out.with_columns(
            pl.when(dep.is_not_null() & (dep.cast(pl.Int64, strict=False).fill_null(0) > 0))
            .then(dep.cast(pl.Int64, strict=False))
            .when(svc_date.is_not_null())
            .then(svc_midnight_utc)
            .otherwise(dep.cast(pl.Int64, strict=False))
            .alias("departure_timestamp")
        )
    out = out.with_columns(
        pl.when(pl.col("timestamp").is_null() | (pl.col("timestamp").cast(pl.Int64, strict=False).fill_null(0) == 0))
        .then(pl.col("departure_timestamp").cast(pl.Int64, strict=False))
        .otherwise(pl.col("timestamp").cast(pl.Int64, strict=False))
        .alias("timestamp")
    )
    return out


_ENTUR_MIN_PLAUSIBLE_UNIX = 946684800


def entur_snapshot_epoch_from_s3_key(date_str: str, s3_key: str) -> int | None:
    """
    Iz imena Entur raw fajla (npr. RUT_trip-updates_132528.parquet) izvlači HHMMSS snimka
    i spaja sa datumom foldera na S3 (YYYY-MM-DD) u UTC epoch sekunde.

    Koristi se za trip-updates bez RecordedAtTime/start_date: raspodela kroz dan umesto
    jedne ponoći za ceo batch, radi preklapanja sa minut-rasterom vremena u analizi.
    """
    base = os.path.basename(s3_key)
    m = re.search(r"_(\d{6})\.parquet$", base, flags=re.IGNORECASE)
    if not m:
        return None
    hhmmss = m.group(1)
    try:
        h = int(hhmmss[0:2])
        mi = int(hhmmss[2:4])
        sec = int(hhmmss[4:6])
    except ValueError:
        return None
    if h > 23 or mi > 59 or sec > 59:
        return None
    day = datetime.strptime(date_str, "%Y-%m-%d").replace(
        tzinfo=timezone.utc, hour=h, minute=mi, second=sec, microsecond=0
    )
    return int(day.timestamp())


def fill_entur_bad_timestamps_with_epoch(df: pl.DataFrame, fallback_epoch: int) -> pl.DataFrame:
    """Za redove bez plauzibilnog timestamp/departure_timestamp, postavi oba na fallback_epoch."""
    if df.height == 0:
        return df
    fe = pl.lit(int(fallback_epoch)).cast(pl.Int64)
    _ts_ok = pl.col("timestamp").cast(
        pl.Float64, strict=False).fill_null(0) > _ENTUR_MIN_PLAUSIBLE_UNIX
    _dep_ok = pl.col("departure_timestamp").cast(
        pl.Float64, strict=False).fill_null(0) > _ENTUR_MIN_PLAUSIBLE_UNIX
    out = df.with_columns(
        pl.when(_dep_ok)
        .then(pl.col("departure_timestamp").cast(pl.Int64, strict=False))
        .otherwise(fe)
        .alias("departure_timestamp")
    ).with_columns(
        pl.when(_ts_ok)
        .then(pl.col("timestamp").cast(pl.Int64, strict=False))
        .otherwise(pl.col("departure_timestamp"))
        .alias("timestamp")
    )
    return out


NORMALIZERS = {
    "avinor": normalize_avinor,
    "oslobysykkel": normalize_oslobysykkel,
    "vegvesen": normalize_vegvesen,
    "vegvesen_travel_times": normalize_vegvesen_travel_times,
    "vegvesen_traffic_situations": normalize_vegvesen_traffic_situations,
}


def _filter_dates_to_interval(sorted_dates: list[str], start_date: str | None, end_date: str | None) -> list[str]:
    """Keep only dates in [start_date, end_date] (inclusive)."""
    if start_date is not None:
        sorted_dates = [d for d in sorted_dates if d >= start_date]
    if end_date is not None:
        sorted_dates = [d for d in sorted_dates if d <= end_date]
    return sorted_dates


def run_normalization(
    only_today: bool = True,
    start_date: str | None = None,
    end_date: str | None = None,
    re_run_all: bool = False,
    only_entur_delays: bool = False,
):
    """Run normalization for all available data sources from S3.

    Args:
        only_today: If True and no interval given, process only files from today. If False and no interval, process all dates.
        start_date: Start of interval (YYYY-MM-DD). If set, only dates >= start_date are processed.
        end_date: End of interval (YYYY-MM-DD). If set, only dates <= end_date are processed.
        When start_date or end_date is set, only_today is ignored and files are listed for the whole source, then filtered by this interval.
        re_run_all: If True, process all raw files (ignore tracking list). Use for full re-normalization from scratch.
        only_entur_delays: If True, skip avinor/oslo/vegvesen and only run Entur SIRI ET + trip-updates normalization.
    """
    print("Starting normalization process...\n")
    print("Reading files from S3 bucket...\n")
    today_str = datetime.now().strftime("%Y-%m-%d")
    use_interval = start_date is not None or end_date is not None
    date_prefix = None
    if use_interval:
        print(
            f"Processing files in interval: {start_date or '(any)'} to {end_date or '(any)'}\n")
    elif only_today:
        date_prefix = today_str
        print(f"Processing only files from today: {today_str}\n")
    if re_run_all:
        print("Re-run all: ignoring normalized tracking – processing all raw files.\n")

                                                                                                               
                                                                                                          
    sources = ["avinor", "oslobysykkel", "vegvesen",
               "vegvesen_travel_times", "vegvesen_traffic_situations"]

    if only_entur_delays:
        print(
            "Mode: samo Entur (SIRI ET + trip-updates) — ostali izvori se preskaču.\n")

    if not only_entur_delays:
        for source in sources:
                                                  
            station_capacity = None                               
            if source == "oslobysykkel":
                                                                     
                parquet_files_metadata = get_all_parquet_files_from_s3(
                    source, "*station_status*.parquet", with_metadata=True,
                    date_prefix=date_prefix)

                                                                      
                station_capacity = {}
                info_files = get_all_parquet_files_from_s3(
                    source, "*station_information*.parquet",
                    date_prefix=date_prefix)
                if info_files:
                                                                                                            
                    latest_info_key = info_files[-1]
                    try:
                        info_df = read_parquet_from_s3(latest_info_key)
                        if info_df is not None and ("station_id" in info_df.columns and
                                                    "capacity" in info_df.columns):
                                                                    
                            for row in info_df.iter_rows(named=True):
                                station_capacity[row["station_id"]] = (
                                    row["capacity"])
                            print(
                                f"  Loaded capacity for {len(station_capacity)} "
                                f"stations from {os.path.basename(latest_info_key)}")
                    except Exception as e:
                        print(
                            f"  Warning: Could not load station capacity: {e}")
                        station_capacity = None
                else:
                    station_capacity = None
            elif source == "vegvesen":
                parquet_files_metadata = get_all_parquet_files_from_s3(
                    source, "*weather_data*.parquet", with_metadata=True,
                    date_prefix=date_prefix)
            elif source == "vegvesen_travel_times":
                                                       
                parquet_files_metadata = get_all_parquet_files_from_s3(
                    "vegvesen", "*travel_times*.parquet", with_metadata=True,
                    date_prefix=date_prefix)
            elif source == "vegvesen_traffic_situations":
                parquet_files_metadata = get_all_parquet_files_from_s3(
                    "vegvesen", "*traffic_situations*.parquet", with_metadata=True,
                    date_prefix=date_prefix)
            else:
                parquet_files_metadata = get_all_parquet_files_from_s3(
                    source, with_metadata=True,
                    date_prefix=date_prefix)

            if not parquet_files_metadata:
                print(f"{source}: No parquet files found on S3.")
                continue

                                                                               
            normalized_files = set() if re_run_all else load_normalized_files(source)
            files_to_process_metadata = [
                f for f in parquet_files_metadata
                if f["key"] not in normalized_files
            ]
            if not files_to_process_metadata:
                print(f"{source}: All files already normalized. Skipping.\n")
                continue

                                 
            files_by_date = group_files_by_date(files_to_process_metadata)
            sorted_dates = sorted(files_by_date.keys())
            if use_interval:
                sorted_dates = _filter_dates_to_interval(
                    sorted_dates, start_date, end_date)
            elif only_today:
                sorted_dates = [d for d in sorted_dates if d == today_str]
            if not sorted_dates:
                if only_today and not use_interval:
                    print(f"{source}: No files from today ({today_str}). Skipping.\n")
                else:
                    print(f"{source}: No files in selected interval. Skipping.\n")
                continue

            print(f"{source}: Found {len(files_to_process_metadata)} files to process across {len(sorted_dates)} days")
            print(f"{source}: Processing by date (oldest first)...\n")

                                         
            total_processed = 0
            total_errors = 0
            all_newly_normalized = []

            for date_str in sorted_dates:
                files_for_date = files_by_date[date_str]
                print(f"{source}: Processing {date_str} ({len(files_for_date)} files)")

                all_normalized = []
                processed = 0
                errors = 0
                newly_normalized = []                                             

                                                                                                                        
                if source == "avinor" and files_for_date:
                    raw_dfs = []
                    n_total = len(files_for_date)
                    for idx, s3_key in enumerate(files_for_date):
                        if (idx + 1) % 100 == 0 or idx == 0:
                            print(
                                f"  Avinor {date_str}: loading {idx + 1}/{n_total} ...")
                        df = read_parquet_from_s3(s3_key)
                        if df is None:
                            errors += 1
                            continue
                        raw_dfs.append(df)
                    if raw_dfs:
                                                                                                  
                        raw_dfs = _unify_avinor_schemas(raw_dfs)
                        combined_raw = pl.concat(raw_dfs, how="diagonal")
                        if "uniqueID" in combined_raw.columns:
                            n_before = combined_raw.height
                            combined_raw = combined_raw.unique(
                                subset=["uniqueID"], keep="last")
                            n_after = combined_raw.height
                            if n_before > n_after:
                                print(
                                    f"  Avinor {date_str}: deduplicated {n_before} -> {n_after} unique flights (by uniqueID)")
                        df_norm = NORMALIZERS[source](combined_raw)
                        if df_norm.height > 0:
                            all_normalized = [df_norm]
                            processed = len(raw_dfs)
                            newly_normalized = list(files_for_date)
                            print(
                                f"  ✓ {date_str}: {df_norm.height} records from {len(raw_dfs)} files (Avinor, one row per uniqueID)")
                else:
                    for s3_key in files_for_date:
                        try:
                            df = read_parquet_from_s3(s3_key)
                            if df is None:
                                errors += 1
                                print(
                                    f"  ✗ {os.path.basename(s3_key)}: Error reading from S3")
                                continue

                                                                              
                            if source == "oslobysykkel" and station_capacity:
                                df_norm = NORMALIZERS[source](
                                    df, station_capacity=station_capacity)
                            else:
                                df_norm = NORMALIZERS[source](df)

                            if df_norm.height > 0:
                                all_normalized.append(df_norm)
                                processed += 1
                                                     
                                newly_normalized.append(s3_key)
                                print(
                                    f"  ✓ {os.path.basename(s3_key)}: {df_norm.height} records")
                            else:
                                print(
                                    f"  - {os.path.basename(s3_key)}: empty, skipping")
                        except Exception as e:
                            errors += 1
                            print(
                                f"  ✗ {os.path.basename(s3_key)}: Error -> {str(e)[:80]}")

                if all_normalized:
                                                                                 
                                                             
                    first_schema = all_normalized[0].schema

                                                                   
                    normalized_aligned = []
                    for df in all_normalized:
                                                                       
                        df_aligned = df.select([
                            pl.col(col).cast(first_schema[col]) if col in df.columns
                            else pl.lit(None).cast(first_schema[col]).alias(col)
                            for col in first_schema.keys()
                        ])
                        normalized_aligned.append(df_aligned)

                                                                    
                    combined_df = pl.concat(normalized_aligned)

                                                                                                        
                    if source == "vegvesen" and "timestamp" in combined_df.columns:
                        n_before = combined_df.height
                        combined_df = combined_df.filter(
                            pl.col("timestamp").dt.strftime("%Y-%m-%d") == date_str
                        )
                        n_after = combined_df.height
                        if n_before > n_after:
                            print(
                                f"  Vegvesen {date_str}: filtered to date "
                                f"({n_before} -> {n_after} rows)"
                            )
                                                                                     
                    if source == "vegvesen_traffic_situations" and "minute" in combined_df.columns:
                        n_before = combined_df.height
                        combined_df = combined_df.filter(
                            pl.col("minute").dt.strftime("%Y-%m-%d") == date_str
                        )
                        n_after = combined_df.height
                        if n_before > n_after:
                            print(
                                f"  Vegvesen traffic_situations {date_str}: filtered to date "
                                f"({n_before} -> {n_after} rows)"
                            )

                                                           
                    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                    parquet_s3_key = f"normalized/{date_str}/{source}_normalized_{timestamp}.parquet"

                                                                      
                    df_pandas = combined_df.to_pandas()
                    save_parquet_to_s3(df_pandas, parquet_s3_key)

                                                                         
                    if newly_normalized:
                        mark_files_as_normalized(source, newly_normalized)
                        all_newly_normalized.extend(newly_normalized)

                    total_processed += processed
                    total_errors += errors

                    print(
                        f"  ✓ {date_str}: Saved to S3 -> {parquet_s3_key} "
                        f"({combined_df.height} total records from {processed} files)")
                    if errors > 0:
                        print(f"  ⚠ {date_str}: {errors} errors")
                    print()

                                     
            if all_newly_normalized:
                print(
                    f"{source}: Summary - Processed {total_processed} files across {len(sorted_dates)} days")
                print(
                    f"{source}: Marked {len(all_newly_normalized)} files as normalized in tracking file\n")
            else:
                print(
                    f"{source}: No valid data to save (processed: {total_processed}, errors: {total_errors})\n")

                                                                                                                   
    source = "entur_siri_et"
    siri_et_metadata = get_all_parquet_files_from_s3(
        "entur", "*_siri_et_*.parquet", with_metadata=True,
        date_prefix=date_prefix)
    trip_updates_metadata = get_all_parquet_files_from_s3(
        "entur", "*trip-updates*.parquet", with_metadata=True,
        date_prefix=date_prefix)
                                                                                                   
    seen_keys = set()
    parquet_files_metadata = []
    for f in (siri_et_metadata or []) + (trip_updates_metadata or []):
        if f["key"] not in seen_keys:
            seen_keys.add(f["key"])
            parquet_files_metadata.append(f)
    parquet_files_metadata.sort(key=lambda x: x["key"])
    if not parquet_files_metadata:
        print(f"{source}: No SIRI ET or trip-updates parquet files found on S3.\n")
    else:
        normalized_files = set() if re_run_all else load_normalized_files(source)
        files_to_process_metadata = [
            f for f in parquet_files_metadata if f["key"] not in normalized_files
        ]
        if not files_to_process_metadata:
            print(
                f"{source}: All SIRI ET / trip-updates files already normalized. Skipping.\n")
        else:
            files_by_date = group_files_by_date(files_to_process_metadata)
            sorted_dates = sorted(files_by_date.keys())
            if use_interval:
                sorted_dates = _filter_dates_to_interval(
                    sorted_dates, start_date, end_date)
            elif only_today:
                sorted_dates = [d for d in sorted_dates if d == today_str]
            if not sorted_dates:
                if only_today and not use_interval:
                    print(
                        f"{source}: No files from today ({today_str}). Skipping.\n")
                else:
                    print(
                        f"{source}: No SIRI ET / trip-updates files in selected interval. Skipping.\n")
            elif sorted_dates:
                if only_today and not use_interval:
                    print(
                        f"{source}: Found {len(files_to_process_metadata)} SIRI ET / trip-updates files for today ({today_str})\n")
                else:
                    print(
                        f"{source}: Found {len(files_to_process_metadata)} SIRI ET / trip-updates files across {len(sorted_dates)} days\n")
                all_newly_normalized = []
                for date_str in sorted_dates:
                    files_for_date = files_by_date[date_str]
                    all_normalized = []
                    newly_normalized = []
                    raw_zero_ts = 0
                    raw_total = 0
                    for s3_key in files_for_date:
                        try:
                            df = read_parquet_from_s3(s3_key)
                            if df is None:
                                continue
                            if "timestamp" in df.columns:
                                raw_total += df.height
                                raw_zero_ts += df.filter(
                                    pl.col("timestamp") == 0).height
                            df_norm = normalize_entur_trip_updates(df)
                            if df_norm.height > 0:
                                snap_epoch = entur_snapshot_epoch_from_s3_key(
                                    date_str, s3_key)
                                if snap_epoch is not None:
                                    df_norm = fill_entur_bad_timestamps_with_epoch(
                                        df_norm, snap_epoch)
                                all_normalized.append(df_norm)
                                newly_normalized.append(s3_key)
                        except Exception as e:
                            print(f"  ✗ {os.path.basename(s3_key)}: {e}")
                    if all_normalized:
                        first_schema = all_normalized[0].schema
                        aligned = [
                            d.select([pl.col(c).cast(first_schema[c]) if c in d.columns else pl.lit(
                                None).cast(first_schema[c]).alias(c) for c in first_schema.keys()])
                            for d in all_normalized
                        ]
                        combined_df = pl.concat(aligned)
                                                                                                           
                        _path_epoch = (
                            pl.lit(date_str)
                            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                            .cast(pl.Datetime("us"))
                            .dt.replace_time_zone("UTC")
                            .dt.epoch("s")
                            .cast(pl.Int64)
                        )
                        _ts_ok = pl.col("timestamp").cast(
                            pl.Float64, strict=False).fill_null(0) > _ENTUR_MIN_PLAUSIBLE_UNIX
                        _dep_ok = pl.col("departure_timestamp").cast(
                            pl.Float64, strict=False).fill_null(0) > _ENTUR_MIN_PLAUSIBLE_UNIX
                        combined_df = combined_df.with_columns(
                            pl.when(_dep_ok)
                            .then(pl.col("departure_timestamp").cast(pl.Int64, strict=False))
                            .otherwise(_path_epoch)
                            .alias("departure_timestamp")
                        )
                        combined_df = combined_df.with_columns(
                            pl.when(_ts_ok)
                            .then(pl.col("timestamp").cast(pl.Int64, strict=False))
                            .otherwise(pl.col("departure_timestamp"))
                            .alias("timestamp")
                        )
                                                                                                                                     
                        dedup_cols = ["trip_id", "route_id"]
                        if "departure_timestamp" in combined_df.columns and combined_df["departure_timestamp"].null_count() < combined_df.height:
                            dedup_cols.append("departure_timestamp")
                        else:
                            dedup_cols.append("timestamp")
                        n_before_et = combined_df.height
                        combined_df = combined_df.unique(
                            subset=dedup_cols, keep="last")
                        n_after_et = combined_df.height
                        if n_before_et > n_after_et:
                            print(
                                f"  Entur SIRI ET {date_str}: deduplicated {n_before_et} -> {n_after_et} unique trips (by {dedup_cols})")
                        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                        parquet_s3_key = f"normalized/{date_str}/entur_siri_et_normalized_{timestamp}.parquet"
                        save_parquet_to_s3(
                            combined_df.to_pandas(), parquet_s3_key)
                        mark_files_as_normalized(source, newly_normalized)
                        all_newly_normalized.extend(newly_normalized)
                        msg = f"  ✓ {date_str}: entur_siri_et -> {parquet_s3_key} ({combined_df.height} trip-delay records)"
                        if raw_total > 0:
                            pct = 100.0 * raw_zero_ts / raw_total
                            msg += f" | raw: {raw_zero_ts}/{raw_total} ({pct:.0f}%) timestamp=0"
                        print(msg)
                if all_newly_normalized:
                    print(
                        f"{source}: Marked {len(all_newly_normalized)} files as normalized.\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Normalize raw parquet files from S3 and upload normalized results to S3."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Start of date interval to normalize (inclusive). If set, only dates >= this are processed.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="End of date interval to normalize (inclusive). If set, only dates <= this are processed.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all dates on S3 (no date filter).",
    )
    parser.add_argument(
        "--re-run-all",
        action="store_true",
        help="Ignore normalized tracking: process all raw files (full re-normalization from scratch).",
    )
    parser.add_argument(
        "--only-entur-delays",
        action="store_true",
        help="Preskoči sve izvore osim Entur (SIRI ET + trip-updates). Koristi sa --start-date/--end-date ili --all.",
    )
    args = parser.parse_args()

    only_today = True
    start_date = None
    end_date = None
    if args.all:
        only_today = False
    else:
        start_date = args.start_date
        end_date = args.end_date
        if start_date is not None or end_date is not None:
            only_today = False

    if args.only_entur_delays and not args.all and not (
        args.start_date or args.end_date
    ):
        parser.error(
            "--only-entur-delays zahteva --start-date i/ili --end-date, ili --all."
        )

    run_normalization(
        only_today=only_today,
        start_date=start_date,
        end_date=end_date,
        re_run_all=args.re_run_all,
        only_entur_delays=args.only_entur_delays,
    )
