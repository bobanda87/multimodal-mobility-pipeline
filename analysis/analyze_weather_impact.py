from utils.s3_upload import (
    S3_PARQUET_PATH,
    list_normalized_parquet_files_from_s3,
    read_parquet_from_s3,
)
import polars as pl
from pathlib import Path
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

                                                  
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def analyze_weather_impact(
    normalized_dir: str = "normalized_data",
    city_bbox: dict | None = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    period_days: Optional[int] = None,
    weekday_only: bool = False,
    weekend_only: bool = False,
    read_from_s3: bool = True,
    hour_start: Optional[int] = None,
    hour_end: Optional[int] = None,
):
    """
    Analyze correlation between weather conditions and:
    - Oslo Bysykkel availability
    - Public transport activity (Entur)
    - Flight delays (Avinor)

    Args:
        normalized_dir: Local directory containing normalized parquet files (used when read_from_s3=False)
        city_bbox: Bounding box for filtering data (default: Oslo area)
        start_date: Start date for filtering data (datetime object). If None and period_days is set,
                    calculates from end_date backwards.
        end_date: End date for filtering data (datetime object). If None, uses current time.
        period_days: Number of days to analyze. If set, calculates start_date from end_date.
        weekday_only: If True, keep only weekdays (Mon–Fri) in the analysis.
        weekend_only: If True, keep only weekend days (Sat–Sun) in the analysis.
        read_from_s3: If True (default), load normalized files from S3; otherwise from normalized_dir.

    Returns:
        Dictionary with correlation results and basic interpretation.
    """
    if city_bbox is None:
        city_bbox = {
            "lat_min": 59.85,
            "lat_max": 60.00,
            "lon_min": 10.60,
            "lon_max": 10.85,
        }

    source_data = {}

    if read_from_s3:
                                       
        file_keys = list_normalized_parquet_files_from_s3()
        if not file_keys:
            raise RuntimeError(
                "No normalized parquet files found on S3.\n"
                "Please run 'python normalize_data.py' first to create normalized files on S3."
            )
                                                                                            
                                                                                        
        if start_date is not None and end_date is not None:
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)
            ds = start_date.strftime("%Y-%m-%d")
            de = end_date.strftime("%Y-%m-%d")

            def _norm_folder_date(key: str) -> str | None:
                parts = key.split("/")
                try:
                    i = parts.index("normalized")
                    d = parts[i + 1]
                    if len(d) == 10 and d[4] == "-" and d[7] == "-":
                        return d
                except (ValueError, IndexError):
                    return None
                return None

            filtered = [
                k for k in file_keys
                if (d := _norm_folder_date(k)) is not None and ds <= d <= de
            ]
            if filtered:
                print(
                    f"Loading {len(filtered)} normalized files from S3 "
                    f"(date folders {ds}..{de}, filtered from {len(file_keys)} total)..."
                )
                file_keys = filtered
            else:
                print(
                    f"Warning: no S3 keys under normalized/ in [{ds}, {de}]; loading all {len(file_keys)} files."
                )
        else:
            print(f"Loading {len(file_keys)} normalized files from S3...")
        for s3_key in file_keys:
            try:
                df = read_parquet_from_s3(s3_key)
                if df is None:
                    print(
                        f"Warning: Could not read {os.path.basename(s3_key)} from S3")
                    continue
                                                                                                          
                source = None
                key_basename = os.path.basename(s3_key)
                if "avinor" in key_basename and "entur_siri_et" not in key_basename:
                    source = "avinor"
                elif "entur_siri_et" in key_basename:
                    source = "entur_siri_et"
                elif "oslobysykkel" in key_basename:
                    source = "oslobysykkel"
                elif "vegvesen_traffic_situations" in key_basename:
                    source = "vegvesen_traffic_situations"
                elif "vegvesen_travel_times" in key_basename:
                    source = "vegvesen_travel_times"
                elif "vegvesen" in key_basename:
                    source = "vegvesen"
                else:
                    if "source" in df.columns:
                        sources = df["source"].unique().to_list()
                        if sources:
                            source = sources[0]
                if source:
                    if source not in source_data:
                        source_data[source] = []
                    source_data[source].append(df)
            except Exception as e:
                print(
                    f"Warning: Could not load {os.path.basename(s3_key)}: {e}")
    else:
                                   
        if not os.path.isabs(normalized_dir):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            normalized_dir = os.path.join(project_root, normalized_dir)
        Path(normalized_dir).mkdir(parents=True, exist_ok=True)
        files = list(Path(normalized_dir).glob("*_normalized_*.parquet"))
        if not files:
            abs_path = os.path.abspath(normalized_dir)
            raise RuntimeError(
                f"No normalized parquet files found in {abs_path}\n"
                f"Please run 'python normalize_data.py' first or use read_from_s3=True."
            )
        print(f"Loading {len(files)} normalized files from local directory...")
        for f in files:
            try:
                df = pl.read_parquet(f)
                source = None
                if "avinor" in f.name and "entur_siri_et" not in f.name:
                    source = "avinor"
                elif "entur_siri_et" in f.name:
                    source = "entur_siri_et"
                elif "oslobysykkel" in f.name:
                    source = "oslobysykkel"
                elif "vegvesen_traffic_situations" in f.name:
                    source = "vegvesen_traffic_situations"
                elif "vegvesen_travel_times" in f.name:
                    source = "vegvesen_travel_times"
                elif "vegvesen" in f.name:
                    source = "vegvesen"
                else:
                    if "source" in df.columns:
                        sources = df["source"].unique().to_list()
                        if sources:
                            source = sources[0]
                if source:
                    if source not in source_data:
                        source_data[source] = []
                    source_data[source].append(df)
            except Exception as e:
                print(f"Warning: Could not load {f.name}: {e}")

    if not source_data:
        raise RuntimeError("No valid dataframes loaded")

                                                                                   
    combined_by_source = {}
    for source, dfs in source_data.items():
        if dfs:
            if len(dfs) > 1:
                                                                    
                all_cols: dict[str, pl.DataType] = {}
                for df in dfs:
                    for col, dt in df.schema.items():
                        if col not in all_cols or str(all_cols[col]) == "Null":
                            all_cols[col] = dt
                        elif str(dt) != "Null" and all_cols[col] != dt:
                                                                                   
                            a, b = all_cols[col], dt
                            if {a, b} <= {pl.Int64, pl.Float64, pl.Int32, pl.Float32}:
                                all_cols[col] = pl.Float64
                            elif pl.String in (a, b):
                                all_cols[col] = pl.String
                aligned = []
                for df in dfs:
                    exprs = []
                    for col, dtype in all_cols.items():
                        if col in df.columns:
                            exprs.append(pl.col(col).cast(dtype, strict=False))
                        else:
                            exprs.append(pl.lit(None).cast(dtype).alias(col))
                    aligned.append(df.select(exprs))
                combined_by_source[source] = pl.concat(aligned, how='diagonal')
            else:
                combined_by_source[source] = dfs[0]

                                           
    if start_date is not None or end_date is not None or period_days is not None:
                              
        if end_date is None:
            end_date = datetime.now()

        if period_days is not None:
                                                          
            start_date = end_date - timedelta(days=period_days)

        if start_date is None:
                                                                       
            start_date = datetime(1970, 1, 1)                   

        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        print("\nFiltering data by time period:")
        print(f"  Start: {start_date}")
        print(f"  End: {end_date}")
        print(f"  Period: {(end_date - start_date).days} days")
        if weekday_only:
            print("  Subset: weekdays only (Mon–Fri)")
        elif weekend_only:
            print("  Subset: weekend only (Sat–Sun)")
        print()

                                                                             
        for source in combined_by_source.keys():
            df = combined_by_source[source]
            original_count = df.height
            filter_ts = None
            if "timestamp" in df.columns:
                                                                                                                            
                if source == "entur_siri_et":
                                                                                           
                    ts_dtype = df.schema["timestamp"]
                    if ts_dtype == pl.Datetime:
                        df = df.with_columns(
                            pl.col("timestamp").dt.replace_time_zone(
                                "UTC").alias("timestamp")
                        )
                    elif ts_dtype in (pl.Int64, pl.UInt64, pl.Int32, pl.UInt32):
                        df = df.with_columns(
                            pl.from_epoch(pl.col("timestamp"), time_unit="s")
                            .dt.replace_time_zone("UTC")
                            .alias("timestamp"),
                        )
                    else:
                        tc = pl.col("timestamp").cast(pl.Float64)
                        df = df.with_columns(
                            pl.when(tc > 1e15)
                            .then(pl.from_epoch((tc / 1e6).cast(pl.Int64), time_unit="s"))
                            .when(tc > 1e12)
                            .then(pl.from_epoch((tc / 1e3).cast(pl.Int64), time_unit="s"))
                            .otherwise(pl.from_epoch(tc.cast(pl.Int64), time_unit="s"))
                            .dt.replace_time_zone("UTC")
                            .alias("timestamp")
                        )
                    if "departure_timestamp" in df.columns:
                        df = df.with_columns([
                            pl.from_epoch(pl.col("departure_timestamp"), time_unit="s").dt.replace_time_zone(
                                "UTC").alias("_dep_ts")
                        ])
                                                                                                              
                        filter_ts = pl.coalesce(
                            pl.col("_dep_ts"), pl.col("timestamp"))
                    else:
                        filter_ts = pl.col("timestamp")
                else:
                    df = df.with_columns([
                        pl.col("timestamp").dt.replace_time_zone(
                            "UTC").alias("timestamp")
                    ])
                    filter_ts = pl.col("timestamp")
            elif source == "vegvesen_travel_times" and "period_start" in df.columns:
                df = df.with_columns(
                    pl.col("period_start").dt.replace_time_zone("UTC").alias("period_start")
                )
                filter_ts = pl.col("period_start")
            elif source == "vegvesen_traffic_situations" and "minute" in df.columns:
                df = df.with_columns(
                    pl.col("minute").dt.replace_time_zone("UTC").alias("minute")
                )
                filter_ts = pl.col("minute")

            if filter_ts is not None:
                df = df.filter(
                    (filter_ts >= start_date) &
                    (filter_ts <= end_date)
                )
                                                                           
                if weekday_only:
                    df = df.filter(filter_ts.dt.weekday() <= 5)
                elif weekend_only:
                    df = df.filter(filter_ts.dt.weekday() >= 6)
                filtered_count = df.height
                combined_by_source[source] = df
                print(f"  {source}: {original_count:,} -> {filtered_count:,} records "
                      f"({filtered_count/original_count*100:.1f}% retained)")
            else:
                if source not in ("vegvesen_travel_times", "vegvesen_traffic_situations"):
                    print(
                        f"  Warning: {source} has no 'timestamp' column, skipping time filter")

                                                                      

                                                                             
    location_to_zone = None
    try:
        from analysis.vegvesen_zones import load_location_to_zone
        s3_key = (
            f"{S3_PARQUET_PATH.rstrip('/')}/vegvesen/reference/"
            "vegvesen_location_to_zone.parquet"
        )
        location_to_zone = load_location_to_zone(from_s3=True, s3_key=s3_key)
        if location_to_zone is None:
            location_to_zone = load_location_to_zone()
        if location_to_zone is not None:
            print(f"Location->zone loaded: {location_to_zone.height} stations")
        else:
            print("Tip: run 'python analysis/build_vegvesen_location_to_zone.py' and upload to S3 (or save to analysis/reference/) for 'correlations by zone'.")
    except Exception:
        location_to_zone = None

                                                                                                  
                                                                                         
    weather = None
    weather_by_zone = None
    if "vegvesen" in combined_by_source:
        vegvesen_df = combined_by_source["vegvesen"]
        print(f"Vegvesen data loaded: {vegvesen_df.height} records")
        print(f"Vegvesen columns: {vegvesen_df.columns}")

        if location_to_zone is not None and "location" in vegvesen_df.columns:
            vegvesen_df = vegvesen_df.join(
                location_to_zone, on="location", how="left")
            n_with_zone = vegvesen_df.filter(
                pl.col("zone").is_not_null()).height
            print(
                f"  Joined zone: {n_with_zone} records with zone (deo grada)")

        use_road_temp = "road_temperature" in vegvesen_df.columns
        if use_road_temp:
            temp_col = pl.col("road_temperature").cast(
                pl.Float64, strict=False)
        elif "metric_value" in vegvesen_df.columns:
            temp_col = pl.col("metric_value").cast(pl.Float64, strict=False)
        else:
            print(
                "Warning: Neither road_temperature nor metric_value found. "
                f"Available: {vegvesen_df.columns}"
            )
            use_road_temp = False
            temp_col = None

                                                                                   
        optional_weather_cols = [
            "air_temperature",
            "precipitation_intensity",
            "wind_speed",
            "max_wind_speed",
            "relative_humidity",
        ]

        if temp_col is not None:
            base = vegvesen_df.with_columns([
                pl.col("timestamp")
                .dt.replace_time_zone("UTC")
                .dt.truncate("1m")
                .alias("minute"),
                temp_col.alias("temperature"),
            ]).filter(
                (pl.col("temperature").is_not_null())
                & (pl.col("temperature").is_finite())
                & (pl.col("temperature") >= -50)
                & (pl.col("temperature") <= 50)
            )
            if not use_road_temp:
                base = base.filter(pl.col("temperature") != 0)

            if base.height > 0:
                                                                                          
                agg_exprs = [pl.col("temperature").mean().alias(
                    "road_temperature")]
                for c in optional_weather_cols:
                    if c in base.columns:
                        agg_exprs.append(
                            pl.col(c).cast(
                                pl.Float64, strict=False).mean().alias(c)
                        )
                weather = base.group_by("minute").agg(agg_exprs)
                print(
                    f"Weather data aggregated: {weather.height} time buckets "
                    f"(source: {'road_temperature' if use_road_temp else 'metric_value'})"
                )
                min_max = weather.select(
                    pl.col("minute").min().alias("min_minute"),
                    pl.col("minute").max().alias("max_minute"),
                )
                print("  Weather minute range:", min_max)

                                                            
                if "zone" in base.columns and base.filter(pl.col("zone").is_not_null()).height > 0:
                    agg_by_zone = [
                        pl.col("temperature").mean().alias("road_temperature")]
                    for c in optional_weather_cols:
                        if c in base.columns:
                            agg_by_zone.append(
                                pl.col(c).cast(
                                    pl.Float64, strict=False).mean().alias(c)
                            )
                    weather_by_zone = (
                        base.filter(pl.col("zone").is_not_null())
                        .group_by(["minute", "zone"])
                        .agg(agg_by_zone)
                    )
                    print(
                        f"  Weather by zone (deo grada): "
                        f"{weather_by_zone.height} rows, zones: "
                        f"{weather_by_zone['zone'].unique().to_list()}"
                    )
            else:
                print(
                    "Warning: No valid temperature values after filtering "
                    "(check range -50..50 °C)"
                )
                if not use_road_temp:
                    print(
                        f"Sample metric_value: "
                        f"{vegvesen_df.select('metric_value').head(5)}"
                    )
                weather = None

                                                           
                                                                                 
                                                                                                
    bysykkel = None
    if "oslobysykkel" in combined_by_source:
        oslobysykkel_df = combined_by_source["oslobysykkel"]
        print(f"Oslo Bysykkel raw data: {oslobysykkel_df.height} records")

        bysykkel = (
            oslobysykkel_df
            .with_columns([
                                                         
                pl.col("timestamp")
                .dt.replace_time_zone("UTC")
                .dt.truncate("1m")
                .alias("minute"),
                pl.col("metric_value")
                .cast(pl.Float64, strict=False)
                .alias("bikes_in_use")
            ])
            .filter(pl.col("bikes_in_use").is_not_null())
            .group_by("minute")
            .agg(pl.col("bikes_in_use").sum().alias("bikes_in_use"))
        )
        print(f"Oslo Bysykkel aggregated: {bysykkel.height} time buckets")
        if bysykkel.height > 0:
            print(
                f"Bysykkel minute range: {bysykkel.select([pl.col('minute').min().alias('min_minute'), pl.col('minute').max().alias('max_minute')])}")

                             
    avinor = None
    if "avinor" in combined_by_source:
        avinor_df = combined_by_source["avinor"]
        avinor = (
            avinor_df
            .with_columns([
                                                         
                pl.col("timestamp")
                .dt.replace_time_zone("UTC")
                .dt.truncate("1m")
                .alias("minute")
            ])
            .group_by("minute")
            .agg(pl.len().alias("flight_activity"))
            .select(["minute", "flight_activity"])
        )

                                                        
    if weather is None or weather.height == 0:
        raise RuntimeError("No weather data available for analysis")

    combined = weather

    if bysykkel is not None and bysykkel.height > 0:
        combined = combined.join(bysykkel, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("bikes_available")])

                                                                                
    pt_activity_minute = None
    if "entur_siri_et" in combined_by_source:
        entur_df = combined_by_source["entur_siri_et"]
        if entur_df.height > 0:
            if "departure_timestamp" in entur_df.columns and entur_df.filter(pl.col("departure_timestamp").is_not_null() & (pl.col("departure_timestamp") > 0)).height > 0:
                pt_activity_minute = (
                    entur_df.filter(pl.col("departure_timestamp").is_not_null() & (pl.col("departure_timestamp") > 0))
                    .with_columns(pl.from_epoch(pl.col("departure_timestamp"), time_unit="s").dt.replace_time_zone("UTC").dt.truncate("1m").alias("minute"))
                    .group_by("minute")
                    .agg(pl.len().alias("public_transport_activity"))
                )
            elif "timestamp" in entur_df.columns:
                ts = entur_df["timestamp"]
                if ts.dtype in (pl.Int64, pl.Float64):
                    pt_activity_minute = (
                        entur_df.filter(pl.col("timestamp").is_not_null() & (pl.col("timestamp") > 0))
                        .with_columns(pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.replace_time_zone("UTC").dt.truncate("1m").alias("minute"))
                        .group_by("minute")
                        .agg(pl.len().alias("public_transport_activity"))
                    )
                else:
                    pt_activity_minute = (
                        entur_df.with_columns(pl.col("timestamp").dt.truncate("1m").alias("minute"))
                        .group_by("minute")
                        .agg(pl.len().alias("public_transport_activity"))
                    )
    if pt_activity_minute is not None and pt_activity_minute.height > 0:
        combined = combined.join(pt_activity_minute, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("public_transport_activity")])

    if avinor is not None and avinor.height > 0:
        combined = combined.join(avinor, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("flight_activity")])

                                                                                            
                                                                                    
    delay_minute = None
    if "entur_siri_et" in combined_by_source:
        delay_df = combined_by_source["entur_siri_et"]
        if delay_df.height > 0 and "timestamp" in delay_df.columns:
                                                                                     
                                                                                                            
            ts_dtype = delay_df.schema["timestamp"]
            if ts_dtype == pl.Datetime:
                ts_sec = pl.col("timestamp").dt.replace_time_zone(
                    "UTC").dt.epoch("s").cast(pl.Int64)
            elif ts_dtype in (pl.Int64, pl.UInt64, pl.Int32, pl.UInt32):
                ts_sec = pl.col("timestamp").cast(pl.Int64)
            else:
                tc = pl.col("timestamp").cast(pl.Float64)
                ts_sec = (
                    pl.when(tc > 1e15)
                    .then((tc / 1e6).cast(pl.Int64))
                    .when(tc > 1e12)
                    .then((tc / 1e3).cast(pl.Int64))
                    .otherwise(tc.cast(pl.Int64))
                )
            if "departure_timestamp" in delay_df.columns:
                dep = pl.col("departure_timestamp").cast(pl.Int64)
                agg_ts_sec = pl.when(dep.is_not_null() & (dep > 0)).then(
                    dep).otherwise(ts_sec).alias("_agg_ts_sec")
            else:
                agg_ts_sec = ts_sec.alias("_agg_ts_sec")
            delay_df = delay_df.with_columns(agg_ts_sec).filter(
                (pl.col("_agg_ts_sec") >= 946684800)
                & (pl.col("_agg_ts_sec") <= 4102444800)
            )
            if delay_df.height > 0:
                delay_minute = (
                    delay_df
                    .with_columns([
                        pl.from_epoch(pl.col("_agg_ts_sec"),
                                      time_unit="s").alias("ts"),
                    ])
                    .with_columns([
                        pl.col("ts").dt.replace_time_zone(
                            "UTC").dt.truncate("1m").alias("minute"),
                    ])
                    .group_by("minute")
                    .agg(
                        pl.col("max_departure_delay_sec").mean().alias(
                            "mean_delay_sec"),
                        pl.col("max_arrival_delay_sec").mean().alias(
                            "mean_arrival_delay_sec"),
                    )
                )
                combined = combined.join(delay_minute.select(
                    ["minute", "mean_delay_sec"]), on="minute", how="left")
                delay_minute = delay_minute                      
            else:
                combined = combined.with_columns(
                    [pl.lit(None).alias("mean_delay_sec")])
        else:
            combined = combined.with_columns(
                [pl.lit(None).alias("mean_delay_sec")])
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("mean_delay_sec")])

                                                                                       
    if "vegvesen_travel_times" in combined_by_source:
        tt_df = combined_by_source["vegvesen_travel_times"]
        if tt_df.height > 0 and "period_start" in tt_df.columns:
            tt_minute = (
                tt_df.with_columns(pl.col("period_start").dt.truncate("1m").alias("minute"))
                .group_by("minute")
                .agg(
                    pl.col("free_flow_travel_time_sec").mean().alias("mean_travel_time_sec"),
                    pl.col("free_flow_speed_kmh").mean().alias("mean_speed_kmh"),
                )
            )
            combined = combined.join(tt_minute, on="minute", how="left")

                                                            
    if "vegvesen_traffic_situations" in combined_by_source:
        sit_df = combined_by_source["vegvesen_traffic_situations"]
        if sit_df.height > 0 and "minute" in sit_df.columns:
            sit_minute = (
                sit_df.group_by("minute")
                .agg(pl.col("event_count").sum().alias("traffic_incident_count"))
            )
            combined = combined.join(sit_minute, on="minute", how="left")

                                                                                              
    activity_cols = []
    if "bikes_in_use" in combined.columns:
        activity_cols.append("bikes_in_use")
    elif "bikes_available" in combined.columns:
        activity_cols.append("bikes_available")
    if "public_transport_activity" in combined.columns:
        activity_cols.append("public_transport_activity")
    if "flight_activity" in combined.columns:
        activity_cols.append("flight_activity")
    if activity_cols:
        combined = combined.with_columns([
            pl.col(c).fill_null(0).alias(f"_fill_{c}") for c in activity_cols
        ])
        scales = []
        for c in activity_cols:
            lo = combined.select(pl.col(f"_fill_{c}").min())[0, 0]
            hi = combined.select(pl.col(f"_fill_{c}").max())[0, 0]
            lo = lo if lo is not None else 0.0
            hi = hi if hi is not None else 0.0
            if hi > lo:
                scales.append(
                    ((pl.col(f"_fill_{c}") - lo) / (hi - lo)).alias(f"_s_{c}"))
            else:
                scales.append(pl.lit(0.0).alias(f"_s_{c}"))
        combined = combined.with_columns(scales)
        combined = combined.with_columns([
            pl.sum_horizontal([f"_s_{c}" for c in activity_cols]).alias(
                "multimodal_activity")
        ])
        combined = combined.drop(
            [f"_fill_{c}" for c in activity_cols] + [f"_s_{c}" for c in activity_cols])

                                                                
    if hour_start is not None and hour_end is not None:
        combined = combined.filter(
            (pl.col("minute").dt.hour() >= hour_start)
            & (pl.col("minute").dt.hour() < hour_end)
        )
        if combined.height > 0:
            print(f"  Part of day {hour_start:02d}-{hour_end:02d} (UTC): {combined.height} minutes")

                                                                    
                                                                      
    correlations = {}
    if combined.height > 1:
        bikes_col = "bikes_in_use" if "bikes_in_use" in combined.columns else "bikes_available"
        if bikes_col in combined.columns:
            bikes_data = combined.filter(
                (pl.col("road_temperature").is_not_null())
                & (pl.col(bikes_col).is_not_null())
            )
            print(
                f"Bikes data overlap: {bikes_data.height} records with both temperature and bikes")
            if bikes_data.height > 1:
                corr_bikes = bikes_data.select(
                    pl.corr("road_temperature", bikes_col)
                )[0, 0]
                correlations["temperature_vs_bysykkel"] = (
                    corr_bikes if corr_bikes is not None and (
                        corr_bikes == corr_bikes) else None
                )
            else:
                correlations["temperature_vs_bysykkel"] = None
        else:
            correlations["temperature_vs_bysykkel"] = None

        if "public_transport_activity" in combined.columns:
                                                                           
            transport_data = combined.filter(
                (pl.col("road_temperature").is_not_null())
                & (pl.col("public_transport_activity").is_not_null())
            )
            print(
                f"Transport data overlap: {transport_data.height} records with both temperature and transport")
            if transport_data.height > 1:
                corr_transport = transport_data.select(
                    pl.corr("road_temperature", "public_transport_activity")
                )[0, 0]
                                                                                    
                correlations["temperature_vs_public_transport"] = (
                    corr_transport if corr_transport is not None and (
                        corr_transport == corr_transport) else None
                )
            else:
                correlations["temperature_vs_public_transport"] = None
        else:
            correlations["temperature_vs_public_transport"] = None

        if "flight_activity" in combined.columns:
                                                                        
            flight_data = combined.filter(
                (pl.col("road_temperature").is_not_null())
                & (pl.col("flight_activity").is_not_null())
            )
            if flight_data.height > 1:
                corr_flights = flight_data.select(
                    pl.corr("road_temperature", "flight_activity")
                )[0, 0]
                                                                                    
                correlations["temperature_vs_flight_activity"] = (
                    corr_flights if corr_flights is not None and (
                        corr_flights == corr_flights) else None
                )
            else:
                correlations["temperature_vs_flight_activity"] = None
        else:
            correlations["temperature_vs_flight_activity"] = None

        if "multimodal_activity" in combined.columns:
            mm_data = combined.filter(
                (pl.col("road_temperature").is_not_null()) & (
                    pl.col("multimodal_activity").is_not_null())
            )
            if mm_data.height > 1:
                corr_mm = mm_data.select(
                    pl.corr("road_temperature", "multimodal_activity"))[0, 0]
                correlations["temperature_vs_multimodal"] = (
                    corr_mm if corr_mm is not None and (
                        corr_mm == corr_mm) else None
                )
            else:
                correlations["temperature_vs_multimodal"] = None
        else:
            correlations["temperature_vs_multimodal"] = None

                                                                               
        if "mean_delay_sec" in combined.columns:
            delay_data = combined.filter(
                (pl.col("road_temperature").is_not_null()) & (
                    pl.col("mean_delay_sec").is_not_null())
            )
            if delay_data.height > 1:
                corr_delay = delay_data.select(
                    pl.corr("road_temperature", "mean_delay_sec"))[0, 0]
                correlations["temperature_vs_delay"] = (
                    corr_delay if corr_delay is not None and (
                        corr_delay == corr_delay) else None
                )
            else:
                correlations["temperature_vs_delay"] = None
        else:
            correlations["temperature_vs_delay"] = None

        if "mean_travel_time_sec" in combined.columns:
            d = combined.filter(pl.col("road_temperature").is_not_null() & pl.col("mean_travel_time_sec").is_not_null())
            c = d.select(pl.corr("road_temperature", "mean_travel_time_sec"))[0, 0] if d.height > 1 else None
            correlations["temperature_vs_travel_time"] = c if c is not None and (c == c) else None
        else:
            correlations["temperature_vs_travel_time"] = None
        if "traffic_incident_count" in combined.columns:
            d = combined.filter(pl.col("road_temperature").is_not_null() & pl.col("traffic_incident_count").is_not_null())
            c = d.select(pl.corr("road_temperature", "traffic_incident_count"))[0, 0] if d.height > 1 else None
            correlations["temperature_vs_traffic_incidents"] = c if c is not None and (c == c) else None
        else:
            correlations["temperature_vs_traffic_incidents"] = None

                                                                                               
        for pred_col, prefix in [
            ("precipitation_intensity", "precipitation"),
            ("wind_speed", "wind_speed"),
            ("relative_humidity", "relative_humidity"),
        ]:
            if pred_col not in combined.columns:
                for k in ["bysykkel", "public_transport", "flight_activity", "multimodal", "delay"]:
                    correlations[f"{prefix}_vs_{k}"] = None
                continue
            non_null = combined.filter(pl.col(pred_col).is_not_null()).height
            if non_null < 2:
                for k in ["bysykkel", "public_transport", "flight_activity", "multimodal", "delay"]:
                    correlations[f"{prefix}_vs_{k}"] = None
                continue
            bikes_col = "bikes_in_use" if "bikes_in_use" in combined.columns else "bikes_available"
            if bikes_col in combined.columns:
                d = combined.filter(pl.col(pred_col).is_not_null() & pl.col(bikes_col).is_not_null())
                c = d.select(pl.corr(pred_col, bikes_col))[0, 0] if d.height > 1 else None
                correlations[f"{prefix}_vs_bysykkel"] = c if c is not None and (c == c) else None
            else:
                correlations[f"{prefix}_vs_bysykkel"] = None
            if "public_transport_activity" in combined.columns:
                d = combined.filter(pl.col(pred_col).is_not_null() & pl.col("public_transport_activity").is_not_null())
                c = d.select(pl.corr(pred_col, "public_transport_activity"))[0, 0] if d.height > 1 else None
                correlations[f"{prefix}_vs_public_transport"] = c if c is not None and (c == c) else None
            else:
                correlations[f"{prefix}_vs_public_transport"] = None
            if "flight_activity" in combined.columns:
                d = combined.filter(pl.col(pred_col).is_not_null() & pl.col("flight_activity").is_not_null())
                c = d.select(pl.corr(pred_col, "flight_activity"))[0, 0] if d.height > 1 else None
                correlations[f"{prefix}_vs_flight_activity"] = c if c is not None and (c == c) else None
            else:
                correlations[f"{prefix}_vs_flight_activity"] = None
            if "multimodal_activity" in combined.columns:
                d = combined.filter(pl.col(pred_col).is_not_null() & pl.col("multimodal_activity").is_not_null())
                c = d.select(pl.corr(pred_col, "multimodal_activity"))[0, 0] if d.height > 1 else None
                correlations[f"{prefix}_vs_multimodal"] = c if c is not None and (c == c) else None
            else:
                correlations[f"{prefix}_vs_multimodal"] = None
            if "mean_delay_sec" in combined.columns:
                d = combined.filter(pl.col(pred_col).is_not_null() & pl.col("mean_delay_sec").is_not_null())
                c = d.select(pl.corr(pred_col, "mean_delay_sec"))[0, 0] if d.height > 1 else None
                correlations[f"{prefix}_vs_delay"] = c if c is not None and (c == c) else None
            else:
                correlations[f"{prefix}_vs_delay"] = None
    else:
        correlations = {
            "temperature_vs_bysykkel": None,
            "temperature_vs_public_transport": None,
            "temperature_vs_flight_activity": None,
            "temperature_vs_multimodal": None,
            "temperature_vs_delay": None,
            "temperature_vs_travel_time": None,
            "temperature_vs_traffic_incidents": None,
        }
        for prefix in ["precipitation", "wind_speed", "relative_humidity"]:
            for k in ["bysykkel", "public_transport", "flight_activity", "multimodal", "delay"]:
                correlations[f"{prefix}_vs_{k}"] = None

                    
    interpretation = {}
    if correlations["temperature_vs_bysykkel"] is not None:
        corr_val = correlations["temperature_vs_bysykkel"]
        interpretation["bysykkel"] = (
            "Negative correlation suggests reduced bike usage during cold conditions."
            if corr_val < -0.3
            else "Weak or no clear relationship detected."
        )
    else:
        interpretation["bysykkel"] = "Insufficient data for analysis."

    if correlations["temperature_vs_public_transport"] is not None:
        corr_val = correlations["temperature_vs_public_transport"]
        interpretation["public_transport"] = (
            "Negative correlation may indicate reduced service or mobility under bad weather."
            if corr_val < -0.3
            else "Public transport appears resilient to temperature changes."
        )
    else:
        interpretation["public_transport"] = "Insufficient data for analysis."

    if correlations["temperature_vs_flight_activity"] is not None:
        corr_val = correlations["temperature_vs_flight_activity"]
        interpretation["aviation"] = (
            "Positive correlation indicates increased activity during colder conditions."
            if corr_val > 0.3
            else "No strong temperature impact on flight activity detected."
        )
    else:
        interpretation["aviation"] = "Insufficient data for analysis."

    if correlations.get("temperature_vs_multimodal") is not None:
        corr_val = correlations["temperature_vs_multimodal"]
        interpretation["multimodal"] = (
            "Positive: higher temperature associated with higher combined (bike+PT+flight) activity."
            if corr_val > 0.2
            else "Negative: colder conditions associated with higher combined activity."
            if corr_val < -0.2
            else "Weak linear relationship between temperature and combined multimodal activity."
        )
    else:
        interpretation["multimodal"] = "Insufficient data for multimodal index."

    if correlations.get("temperature_vs_delay") is not None:
        corr_val = correlations["temperature_vs_delay"]
        interpretation["delay"] = (
            "Positive: higher temperature associated with higher mean delay (worse conditions)."
            if corr_val > 0.2
            else "Negative: colder conditions associated with higher delay."
            if corr_val < -0.2
            else "Weak linear relationship between temperature and PT delay."
        )
    else:
        interpretation["delay"] = "No delay data (run normalize_data to include Entur SIRI ET)."

                                                                       
    correlations_by_zone = {}
    if weather_by_zone is not None and weather_by_zone.height > 0 and "zone" in weather_by_zone.columns:
        zones = weather_by_zone["zone"].unique().to_list()
        for z in zones:
            zone_weather = weather_by_zone.filter(pl.col("zone") == z).select(
                pl.col("minute"),
                pl.col("road_temperature").alias("temp_zone"),
            )
            merged = combined.join(zone_weather, on="minute", how="inner")
            cz = {}
            if "bikes_in_use" in merged.columns:
                d = merged.filter(pl.col("temp_zone").is_not_null() & pl.col("bikes_in_use").is_not_null())
                if d.height > 1:
                    c = d.select(pl.corr("temp_zone", "bikes_in_use"))[0, 0]
                    cz["temp_vs_bysykkel"] = float(c) if c is not None and c == c else None
            if "mean_delay_sec" in merged.columns:
                d = merged.filter(pl.col("temp_zone").is_not_null() & pl.col("mean_delay_sec").is_not_null())
                if d.height > 1:
                    c = d.select(pl.corr("temp_zone", "mean_delay_sec"))[0, 0]
                    cz["temp_vs_delay"] = float(c) if c is not None and c == c else None
            if cz:
                correlations_by_zone[str(z)] = cz

                                                                                                             
    regression_result = None
    if "mean_delay_sec" in combined.columns:
        reg_cols_full = ["road_temperature"]
        for c in ["precipitation_intensity", "wind_speed", "relative_humidity"]:
            if c in combined.columns:
                reg_cols_full.append(c)
        reg_cols = reg_cols_full
        reg_df = combined.select(["mean_delay_sec"] + reg_cols).drop_nulls()
        if reg_df.height <= 10 and "road_temperature" in combined.columns:
            reg_cols = ["road_temperature"]
            reg_df = combined.select(["mean_delay_sec", "road_temperature"]).drop_nulls()
        if reg_df.height > 10 and reg_df["mean_delay_sec"].null_count() == 0:
            try:
                import numpy as np
                y = reg_df["mean_delay_sec"].to_numpy().astype(float)
                X = reg_df.select(reg_cols).to_numpy().astype(float)
                from numpy.linalg import lstsq
                ones = np.ones((X.shape[0], 1))
                X_with_const = np.hstack([ones, X])
                coeffs, residuals, rank, s = lstsq(X_with_const, y, rcond=None)
                ss_res = np.atleast_1d(residuals).sum()
                ss_tot = ((y - y.mean()) ** 2).sum()
                r2 = (1 - ss_res / ss_tot) if ss_tot > 0 else None
                regression_result = {
                    "coefficients": {"intercept": float(coeffs[0]) if coeffs is not None and len(coeffs) > 0 else None},
                    "r_squared": float(r2) if r2 is not None and r2 == r2 else None,
                    "predictors": reg_cols,
                }
                for i, col in enumerate(reg_cols):
                    if coeffs is not None and i + 1 < len(coeffs):
                        regression_result["coefficients"][col] = float(coeffs[i + 1])
            except Exception:
                regression_result = None

                                                                                               
    delay_note = (
        "Delay data: Entur SIRI ET (Estimated Timetable) provides arrival_delay/departure_delay. "
        "Analysis uses normalized entur_siri_et, aggregated by minute, with temperature_vs_delay correlation."
    )

                                                    
    date_range = None
    if weather is not None and weather.height > 0:
        date_range = {
            "start": weather.select(pl.col("minute").min())[0, 0],
            "end": weather.select(pl.col("minute").max())[0, 0],
        }

                                                                       
    temperature_summary = None
    if "road_temperature" in combined.columns:
        temp_valid = combined.filter(pl.col(
            "road_temperature").is_not_null() & pl.col("road_temperature").is_finite())
        if temp_valid.height > 0:
            t = temp_valid.select(pl.col("road_temperature")).to_series()
            temperature_summary = {
                "min_c": round(float(t.min()), 1),
                "max_c": round(float(t.max()), 1),
                "mean_c": round(float(t.mean()), 1),
                "std_c": round(float(t.std()), 1) if temp_valid.height > 1 else 0.0,
                "n_readings": temp_valid.height,
            }

    return {
        "correlations": correlations,
        "interpretation": interpretation,
        "sample_size_minutes": combined.height,
        "date_range": date_range,
        "temperature_summary": temperature_summary,
        "delay_note": delay_note,
        "data_summary": {
            "weather_records": weather.height if weather is not None else 0,
            "bysykkel_records": bysykkel.height if (bysykkel is not None and bysykkel.height > 0) else 0,
            "entur_siri_et_records": delay_minute.height if (delay_minute is not None and delay_minute.height > 0) else 0,
            "avinor_records": avinor.height if (avinor is not None and avinor.height > 0) else 0,
        },
        "weather_by_zone": weather_by_zone,
        "correlations_by_zone": correlations_by_zone,
        "regression_delay": regression_result,
        "combined_df": combined,
    }


def compare_periods(
    periods: list[dict],
    normalized_dir: str = "normalized_data",
    city_bbox: dict | None = None,
    read_from_s3: bool = True,
) -> dict:
    """
    Compare weather impact analysis across multiple time periods.

    Args:
        periods: List of period dictionaries, each with:
                - "name": str - Name/label for this period (e.g., "Week 1", "January")
                - "start_date": Optional[datetime] - Start date
                - "end_date": Optional[datetime] - End date
                - "period_days": Optional[int] - Number of days (alternative to dates)
        normalized_dir: Local directory for normalized files (used when read_from_s3=False)
        city_bbox: Bounding box for filtering data
        read_from_s3: If True (default), load normalized files from S3

    Returns:
        Dictionary with comparison results for each period
    """
    comparison_results = {}

    for period in periods:
        name = period.get("name", "Unknown Period")
        print(f"\n{'='*60}")
        print(f"Analyzing period: {name}")
        print(f"{'='*60}")

        try:
            results = analyze_weather_impact(
                normalized_dir=normalized_dir,
                city_bbox=city_bbox,
                start_date=period.get("start_date"),
                end_date=period.get("end_date"),
                period_days=period.get("period_days"),
                weekday_only=period.get("weekday_only", False),
                weekend_only=period.get("weekend_only", False),
                read_from_s3=read_from_s3,
            )
            comparison_results[name] = results
        except Exception as e:
            print(f"Error analyzing period {name}: {e}")
            comparison_results[name] = {"error": str(e)}

    return comparison_results


                                                                             
                                                                  
                                                        
                                                                             

                                                                 
                                                                                     
PERIODS_THREE_PHASES = [
    {
        "name": "Pre-Christmas (Dec 20–26)",
        "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2025, 12, 26, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "Turn of year (Dec 27 – Jan 2)",
        "start_date": datetime(2025, 12, 27, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 2, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "Early January (Jan 3–11)",
        "start_date": datetime(2026, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 11, 23, 59, 59, tzinfo=timezone.utc),
    },
]

                                                           
                                                                              
PERIODS_TWO_WEEKS = [
    {
        "name": "Week 1 (Dec 20–26)",
        "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2025, 12, 26, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "Week 2 (Dec 27 – Jan 2)",
        "start_date": datetime(2025, 12, 27, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 2, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "Week 3 (Jan 3–9)",
        "start_date": datetime(2026, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 9, 23, 59, 59, tzinfo=timezone.utc),
    },
]

                                                   
                                                                          
PERIODS_DEC_VS_JAN = [
    {
        "name": "December (Dec 20–31)",
        "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "January (Jan 1–11)",
        "start_date": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 11, 23, 59, 59, tzinfo=timezone.utc),
    },
]

                                                       
PERIODS_FOUR_WINDOWS = [
    {"name": "Window 1 (Dec 20–25)", "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
     "end_date": datetime(2025, 12, 25, 23, 59, 59, tzinfo=timezone.utc)},
    {"name": "Window 2 (Dec 26–31)", "start_date": datetime(2025, 12, 26, 0, 0, 0, tzinfo=timezone.utc),
     "end_date": datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)},
    {"name": "Window 3 (Jan 1–5)", "start_date": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
     "end_date": datetime(2026, 1, 5, 23, 59, 59, tzinfo=timezone.utc)},
    {"name": "Window 4 (Jan 6–11)", "start_date": datetime(2026, 1, 6, 0, 0, 0, tzinfo=timezone.utc),
     "end_date": datetime(2026, 1, 11, 23, 59, 59, tzinfo=timezone.utc)},
]

                                                                       
                                                                   
PERIODS_WEEKDAY_VS_WEEKEND = [
    {
        "name": "Weekdays (Mon–Fri)",
        "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 11, 23, 59, 59, tzinfo=timezone.utc),
        "weekday_only": True,
    },
    {
        "name": "Weekend (Sat–Sun)",
        "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 11, 23, 59, 59, tzinfo=timezone.utc),
        "weekend_only": True,
    },
]

                                                                         
                                                        
PERIODS_JAN30_FEB16 = [
    {
        "name": "Week 1 (Jan 30 – Feb 5)",
        "start_date": datetime(2026, 1, 30, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 2, 5, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "Week 2 (Feb 6 – Feb 12)",
        "start_date": datetime(2026, 2, 6, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 2, 12, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "Week 3 (Feb 13 – Feb 16)",
        "start_date": datetime(2026, 2, 13, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 2, 16, 23, 59, 59, tzinfo=timezone.utc),
    },
]

                                                                         
PERIODS_WA_WB_WC = [
    {
        "name": "W-A (2025-12-20 – 2026-01-04)",
        "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 4, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "W-B (2026-01-31 – 2026-02-07)",
        "start_date": datetime(2026, 1, 31, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 2, 7, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "W-C (2026-03-07 – 2026-03-14)",
        "start_date": datetime(2026, 3, 7, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 3, 14, 23, 59, 59, tzinfo=timezone.utc),
    },
]

                                                                                                              
                                                             
PERIODS_WA1_WA2_WB_WC = [
    {
        "name": "W-A1 (2025-12-20 – 2025-12-27)",
        "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2025, 12, 27, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "W-A2 (2025-12-28 – 2026-01-04)",
        "start_date": datetime(2025, 12, 28, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 4, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "W-B (2026-01-31 – 2026-02-07)",
        "start_date": datetime(2026, 1, 31, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 2, 7, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name": "W-C (2026-03-07 – 2026-03-14)",
        "start_date": datetime(2026, 3, 7, 0, 0, 0, tzinfo=timezone.utc),
        "end_date": datetime(2026, 3, 14, 23, 59, 59, tzinfo=timezone.utc),
    },
]


def save_weather_impact_plots(results: dict, output_dir: Optional[str] = None) -> None:
    """
    Snimi scatter, time series i heatmap korelacija (zahteva matplotlib).
    results mora sadržati 'combined_df' (Polars DataFrame sa minute, road_temperature, bikes_in_use, mean_delay_sec, itd.).
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib nije instaliran; preskačem vizualizacije.")
        return
    combined = results.get("combined_df")
    if combined is None or combined.height == 0:
        return
    out = output_dir or os.path.join(PROJECT_ROOT, "analysis", "output")
    Path(out).mkdir(parents=True, exist_ok=True)
    df = combined.to_pandas()

                                          
    if "road_temperature" in df.columns and "bikes_in_use" in df.columns:
        sub = df[["road_temperature", "bikes_in_use"]].dropna()
        if len(sub) > 10:
            fig, ax = plt.subplots()
            ax.scatter(sub["road_temperature"], sub["bikes_in_use"], alpha=0.3, s=5)
            ax.set_xlabel("Road temperature (°C)")
            ax.set_ylabel("Bikes in use")
            ax.set_title("Temperature vs Oslo Bysykkel usage")
            fig.savefig(os.path.join(out, "scatter_temp_vs_bikes.png"), dpi=100, bbox_inches="tight")
            plt.close(fig)

                                            
    if "road_temperature" in df.columns and "mean_delay_sec" in df.columns:
        sub = df[["road_temperature", "mean_delay_sec"]].dropna()
        if len(sub) > 10:
            fig, ax = plt.subplots()
            ax.scatter(sub["road_temperature"], sub["mean_delay_sec"], alpha=0.3, s=5)
            ax.set_xlabel("Road temperature (°C)")
            ax.set_ylabel("Mean delay (s)")
            ax.set_title("Temperature vs PT delay")
            fig.savefig(os.path.join(out, "scatter_temp_vs_delay.png"), dpi=100, bbox_inches="tight")
            plt.close(fig)

                                                                     
    if "minute" in df.columns and "road_temperature" in df.columns:
        ts_df = df[["minute", "road_temperature"]].dropna(subset=["road_temperature"])
        if "mean_delay_sec" in df.columns:
            ts_df = ts_df.merge(df[["minute", "mean_delay_sec"]], on="minute", how="left")
        if len(ts_df) > 5000:
            ts_df = ts_df.sample(n=5000, random_state=42).sort_values("minute")
        if len(ts_df) > 10:
            fig, ax1 = plt.subplots(figsize=(10, 4))
            ax1.plot(ts_df["minute"], ts_df["road_temperature"], color="C0", alpha=0.7, label="Temperature (°C)")
            ax1.set_ylabel("Road temperature (°C)", color="C0")
            ax2 = ax1.twinx()
            if "mean_delay_sec" in ts_df.columns and ts_df["mean_delay_sec"].notna().any():
                ax2.plot(ts_df["minute"], ts_df["mean_delay_sec"], color="C1", alpha=0.7, label="Mean delay (s)")
                ax2.set_ylabel("Mean delay (s)", color="C1")
            ax1.set_xlabel("Time")
            ax1.set_title("Time series: temperature and PT delay")
            fig.savefig(os.path.join(out, "timeseries_temp_delay.png"), dpi=100, bbox_inches="tight")
            plt.close(fig)

                        
    plot_cols = [c for c in ["road_temperature", "bikes_in_use", "mean_delay_sec", "flight_activity", "public_transport_activity"] if c in df.columns]
    if len(plot_cols) >= 2:
        corr_mat = df[plot_cols].corr()
        fig, ax = plt.subplots(figsize=(8, 6))
        im = ax.imshow(corr_mat, cmap="RdBu_r", vmin=-1, vmax=1)
        ax.set_xticks(range(len(plot_cols)))
        ax.set_yticks(range(len(plot_cols)))
        ax.set_xticklabels(plot_cols, rotation=45, ha="right")
        ax.set_yticklabels(plot_cols)
        for i in range(len(plot_cols)):
            for j in range(len(plot_cols)):
                ax.text(j, i, f"{corr_mat.iloc[i, j]:.2f}", ha="center", va="center", fontsize=8)
        plt.colorbar(im, ax=ax, label="Correlation")
        ax.set_title("Correlation matrix")
        fig.savefig(os.path.join(out, "correlation_heatmap.png"), dpi=100, bbox_inches="tight")
        plt.close(fig)
    print(f"Plots saved to {out}")


def print_comparison_table(comparison_results: dict) -> None:
    """Print a summary table of correlations and temperature conditions across periods (for MDPI tables)."""
    col_w = 28
    period_w = 32
    metrics = [
        "temperature_vs_bysykkel",
        "temperature_vs_public_transport",
        "temperature_vs_flight_activity",
        "temperature_vs_multimodal",
        "temperature_vs_delay",
    ]
    short_labels = ["Bysykkel", "PT", "Flight", "Multimodal", "Delay"]
    period_names = list(comparison_results.keys())
    valid_periods = [p for p in period_names if isinstance(
        comparison_results[p], dict) and "correlations" in comparison_results[p]]
    if not valid_periods:
        print("No valid period results to display.")
        return
    total_w = period_w + len(metrics) * col_w
    print("\n" + "=" * total_w)
    print("COMPARATIVE CORRELATIONS (for manuscript table)")
    print("=" * total_w)
    header = "Period".ljust(period_w) + "".join(s.ljust(col_w)
                                                for s in short_labels)
    print(header)
    print("-" * total_w)
    for name in valid_periods:
        res = comparison_results[name]
        corr = res.get("correlations", {})
        row = name[: period_w - 1].ljust(period_w)
        for m in metrics:
            v = corr.get(m)
            row += (f"{v:.3f}" if v is not None else "N/A").ljust(col_w)
        print(row)
    print("=" * total_w)

                                                      
    print("\n" + "=" * total_w)
    print("ROAD SURFACE TEMPERATURE (°C) BY PERIOD")
    print("=" * total_w)
    print("Period".ljust(period_w) + "Min (°C)".ljust(14) +
          "Max (°C)".ljust(14) + "Mean (°C)".ljust(16) + "Std (°C)")
    print("-" * total_w)
    for name in valid_periods:
        res = comparison_results[name]
        ts = res.get("temperature_summary")
        if ts:
            row = name[: period_w - 1].ljust(period_w) + f"{ts['min_c']}".ljust(
                14) + f"{ts['max_c']}".ljust(14) + f"{ts['mean_c']}".ljust(16) + f"{ts['std_c']:.1f}"
            print(row)
        else:
            print(name[: period_w - 1].ljust(period_w) + "N/A")
    print("=" * total_w)
                                                
    for name in valid_periods:
        res = comparison_results[name]
        if isinstance(res, dict) and res.get("delay_note"):
            print("\nDelay note:")
            print(res["delay_note"])
            break


def _append_summary_csv(
    path: str,
    results: dict,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    part_of_day: Optional[str] = None,
) -> None:
    """Append one row to CSV with date range, part_of_day and key correlations."""
    import csv
    start_str = start_date.strftime("%Y-%m-%d") if start_date else ""
    end_str = end_date.strftime("%Y-%m-%d") if end_date else ""
    part = part_of_day or ""
    corr = results.get("correlations", {})
    row = {
        "start_date": start_str,
        "end_date": end_str,
        "part_of_day": part,
        "temperature_vs_bysykkel": corr.get("temperature_vs_bysykkel"),
        "temperature_vs_public_transport": corr.get("temperature_vs_public_transport"),
        "temperature_vs_flight_activity": corr.get("temperature_vs_flight_activity"),
        "temperature_vs_multimodal": corr.get("temperature_vs_multimodal"),
        "temperature_vs_delay": corr.get("temperature_vs_delay"),
        "temperature_vs_travel_time": corr.get("temperature_vs_travel_time"),
        "temperature_vs_traffic_incidents": corr.get("temperature_vs_traffic_incidents"),
        "sample_size_minutes": results.get("sample_size_minutes"),
    }
    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            w.writeheader()
        w.writerow({k: ("" if v is None else v) for k, v in row.items()})


def _parse_date(s: str) -> datetime:
    """Parse ISO date or datetime string to UTC datetime."""
    s = s.strip()
    if not s:
        raise ValueError("Empty date string")
    if len(s) <= 10:              
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Weather impact analysis (single period or comparative)")
    parser.add_argument("--mode", choices=["single", "compare"], default="compare",
                        help="single = one period, compare = multiple periods")
    parser.add_argument("--periods", choices=["three_phases", "two_weeks", "dec_jan", "four_windows",
                        "weekday_weekend", "jan30_feb16", "wa_wb_wc", "wa1_wa2_wb_wc"], default="three_phases",
                        help="Predefined periods: wa_wb_wc = W-A/W-B/W-C; wa1_wa2_wb_wc = W-A split + W-B + W-C")
    parser.add_argument("--period-days", type=int, default=7,
                        help="For single mode: length of period in days (from end of data)")
    parser.add_argument("--start-date", type=str, default=None, metavar="YYYY-MM-DD",
                        help="Start of analysis window (single mode or filter)")
    parser.add_argument("--end-date", type=str, default=None, metavar="YYYY-MM-DD",
                        help="End of analysis window (single mode or filter)")
    parser.add_argument("--plots", action="store_true",
                        help="Save scatter, time series and correlation heatmap to analysis/output/ (single mode)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Directory for plot output (default: analysis/output)")
    parser.add_argument("--part-of-day", type=str, default=None, metavar="H-H",
                        help="Restrict to part of day (UTC), e.g. 0-4, 4-8, 8-12, 12-16, 16-20, 20-24")
    parser.add_argument("--summary-csv", type=str, default=None, metavar="FILE",
                        help="Append one row with date range, part_of_day and key correlations (single mode)")
    args = parser.parse_args()

    start_date = None
    end_date = None
    if args.start_date:
        start_date = _parse_date(args.start_date)
        if len(args.start_date.strip()) <= 10:
            start_date = start_date.replace(
                hour=0, minute=0, second=0, microsecond=0)
    if args.end_date:
        end_date = _parse_date(args.end_date)
        if len(args.end_date.strip()) <= 10:
            end_date = end_date.replace(
                hour=23, minute=59, second=59, microsecond=999_999)

    try:
        if args.mode == "single":
            kwargs = {}
            if start_date is not None:
                kwargs["start_date"] = start_date
            if end_date is not None:
                kwargs["end_date"] = end_date
            if not kwargs and args.period_days is not None:
                kwargs["period_days"] = args.period_days
            hour_start = hour_end = None
            if getattr(args, "part_of_day", None):
                part = args.part_of_day.strip()
                if "-" in part:
                    a, b = part.split("-", 1)
                    try:
                        hour_start = int(a.strip())
                        hour_end = int(b.strip())
                        kwargs["hour_start"] = hour_start
                        kwargs["hour_end"] = hour_end
                    except ValueError:
                        pass
            results = analyze_weather_impact(**kwargs)
            print("\n" + "=" * 60)
            print("WEATHER IMPACT ANALYSIS RESULTS")
            print("=" * 60)
            if results.get("date_range"):
                print("\nDate range analyzed:")
                print(f"  Start: {results['date_range']['start']}")
                print(f"  End: {results['date_range']['end']}")
            ts = results.get("temperature_summary")
            if ts:
                print("\nRoad surface temperature (°C):")
                print(
                    f"  Min: {ts['min_c']} °C, Max: {ts['max_c']} °C, Mean: {ts['mean_c']} °C, Std: {ts['std_c']:.1f} °C (n={ts['n_readings']})")
            print(f"\nSample size: {results['sample_size_minutes']} minutes")
            print("\nData summary:")
            for key, value in results["data_summary"].items():
                print(f"  {key}: {value}")
            print("\nCorrelations:")
            _skip_prefixes = ("precipitation_vs_", "wind_speed_vs_", "relative_humidity_vs_")
            for key, value in results["correlations"].items():
                if value is None and any(key.startswith(p) for p in _skip_prefixes):
                    continue
                print(
                    f"  {key}: {value:.3f}" if value is not None else f"  {key}: N/A")
            if results.get("correlations_by_zone"):
                print("\nCorrelations by zone (temp in zone vs global bikes/delay):")
                for zone, cz in results["correlations_by_zone"].items():
                    print(f"  {zone}: {cz}")
            if results.get("regression_delay"):
                reg = results["regression_delay"]
                preds = reg.get("predictors", ["road_temperature"])
                print("\nRegression (mean_delay_sec ~ " + " + ".join(preds) + "):")
                print(f"  R²: {reg.get('r_squared')}")
                print(f"  Coefficients: {reg.get('coefficients')}")
            print("\nInterpretation:")
            for key, value in results["interpretation"].items():
                print(f"  {key}: {value}")
            if results.get("delay_note"):
                print("\nDelay note:")
                print("  " + results["delay_note"])
            if getattr(args, "plots", False):
                save_weather_impact_plots(results, output_dir=getattr(args, "output_dir", None))
            summary_csv = getattr(args, "summary_csv", None)
            if summary_csv and results:
                _append_summary_csv(
                    summary_csv,
                    results,
                    start_date=start_date,
                    end_date=end_date,
                    part_of_day=getattr(args, "part_of_day", None),
                )
        else:
            period_sets = {
                "three_phases": PERIODS_THREE_PHASES,
                "two_weeks": PERIODS_TWO_WEEKS,
                "dec_jan": PERIODS_DEC_VS_JAN,
                "four_windows": PERIODS_FOUR_WINDOWS,
                "weekday_weekend": PERIODS_WEEKDAY_VS_WEEKEND,
                "jan30_feb16": PERIODS_JAN30_FEB16,
                "wa_wb_wc": PERIODS_WA_WB_WC,
                "wa1_wa2_wb_wc": PERIODS_WA1_WA2_WB_WC,
            }
            periods = period_sets[args.periods]
            print(f"Comparing {len(periods)} periods: {args.periods}")
            comparison_results = compare_periods(periods, read_from_s3=True)
            print_comparison_table(comparison_results)
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
