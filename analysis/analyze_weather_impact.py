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

# Add project root to Python path for utils import
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _pearson_corr(df: pl.DataFrame, col_x: str, col_y: str) -> float | None:
    """Pearson r when there are at least two rows and the result is finite."""
    if df.height < 2:
        return None
    c = df.select(pl.corr(col_x, col_y))[0, 0]
    return float(c) if c is not None and c == c else None


def _zonal_multimodal_timeseries(
    weather_by_zone: pl.DataFrame,
    bysykkel_by_zone: pl.DataFrame,
    combined: pl.DataFrame,
) -> tuple[pl.DataFrame | None, dict[str, dict[str, float | None]]]:
    """
    Po zoni: road temp (Vegvesen u zoni) + suma bajkova u zoni + PT i avio isti kao gradski (min–max skala).
    Indeks = _sb + _spt + _sf (bikes skalirano po zoni kroz vreme; PT/avio globalno kao u combined multimodal).
    """
    if weather_by_zone is None or weather_by_zone.height == 0:
        return None, {}
    if bysykkel_by_zone is None or bysykkel_by_zone.height == 0:
        return None, {}
    wz = weather_by_zone.join(
        bysykkel_by_zone, on=["minute", "zone"], how="left"
    )
    join_cols = ["minute"]
    for c in ("public_transport_activity", "flight_activity"):
        if c in combined.columns:
            join_cols.append(c)
    if len(join_cols) > 1:
        wz = wz.join(
            combined.select(join_cols).unique(subset=["minute"], keep="last"),
            on="minute",
            how="left",
        )

    def _bounds(col: str) -> tuple[float, float] | None:
        if col not in combined.columns:
            return None
        lo = combined.select(pl.col(col).fill_null(0).min())[0, 0]
        hi = combined.select(pl.col(col).fill_null(0).max())[0, 0]
        if lo is None or hi is None or lo != lo or hi != hi:
            return None
        return float(lo), float(hi)

    pt_b = _bounds("public_transport_activity")
    fl_b = _bounds("flight_activity")

    wz = wz.with_columns(pl.col("bikes_in_zone").fill_null(0).alias("_b"))
    wz = wz.with_columns(
        [
            pl.col("_b").min().over("zone").alias("_bmin"),
            pl.col("_b").max().over("zone").alias("_bmax"),
        ]
    )
    wz = wz.with_columns(
        pl.when(pl.col("_bmax") > pl.col("_bmin"))
        .then((pl.col("_b") - pl.col("_bmin")) / (pl.col("_bmax") - pl.col("_bmin")))
        .otherwise(pl.lit(0.0))
        .alias("_sb")
    )

    if pt_b and pt_b[1] > pt_b[0]:
        lo, hi = pt_b
        wz = wz.with_columns(
            (
                (pl.col("public_transport_activity").fill_null(0) - lo)
                / (hi - lo)
            ).alias("_spt")
        )
    else:
        wz = wz.with_columns(pl.lit(0.0).alias("_spt"))

    if fl_b and fl_b[1] > fl_b[0]:
        lo, hi = fl_b
        wz = wz.with_columns(
            (
                (pl.col("flight_activity").fill_null(0) - lo)
                / (hi - lo)
            ).alias("_sf")
        )
    else:
        wz = wz.with_columns(pl.lit(0.0).alias("_sf"))

    wz = wz.with_columns(
        (pl.col("_sb") + pl.col("_spt") + pl.col("_sf")).alias(
            "multimodal_activity_zonal"
        )
    )
    drop_tmp = ["_b", "_bmin", "_bmax", "_sb", "_spt", "_sf"]
    wz = wz.drop([c for c in drop_tmp if c in wz.columns])

    cz_mm: dict[str, dict[str, float | None]] = {}
    for z in wz["zone"].drop_nulls().unique().to_list():
        sub = wz.filter(pl.col("zone") == z)
        r = _pearson_corr(sub, "road_temperature", "multimodal_activity_zonal")
        cz_mm[str(z)] = {"temp_vs_multimodal_zonal": r}
    return wz, cz_mm


# Peak commuting: weekday (Mon–Fri) UTC hours 07:00–09:59 and 15:00–17:59; all other minutes = off-peak.
# `minute` is UTC; Polars `dt.hour()` on UTC timestamps matches these intervals.
PEAK_OFFPEAK_DEFINITION = (
    "UTC; Mon–Fri (weekday); peak = 07:00–09:59 and 15:00–17:59; "
    "off-peak = all other minutes in the sample."
)


def _commute_peak_mask_expr() -> pl.Expr:
    m = pl.col("minute")
    wd = m.dt.weekday()
    h = m.dt.hour()
    morning = (h >= 7) & (h < 10)
    afternoon = (h >= 15) & (h < 18)
    return ((wd <= 5) & (morning | afternoon)).alias("_is_commute_peak")


def correlations_by_commute_peak(combined: pl.DataFrame) -> dict | None:
    """
    Pearson r(road_temperature, indicator) separately for commute-peak vs off-peak minutes.
    Peak: weekday UTC 07:00–09:59 and 15:00–17:59. Indicators: bikes, PT activity, mean_delay_sec.
    """
    if combined.height == 0 or "minute" not in combined.columns:
        return None
    if "road_temperature" not in combined.columns:
        return None
    try:
        tagged = combined.with_columns(_commute_peak_mask_expr())
    except Exception:
        return None

    bikes_col = "bikes_in_use" if "bikes_in_use" in tagged.columns else (
        "bikes_available" if "bikes_available" in tagged.columns else None
    )

    def _segment(tag: pl.DataFrame, peak: bool) -> dict:
        seg = tag.filter(pl.col("_is_commute_peak") == peak).drop("_is_commute_peak")
        out: dict = {"n_minutes": seg.height}
        if bikes_col:
            d = seg.filter(
                pl.col("road_temperature").is_not_null() & pl.col(bikes_col).is_not_null()
            )
            out["n_temp_bikes"] = d.height
            out["pearson_temp_vs_bikes"] = _pearson_corr(d, "road_temperature", bikes_col)
        else:
            out["n_temp_bikes"] = 0
            out["pearson_temp_vs_bikes"] = None
        if "public_transport_activity" in seg.columns:
            d = seg.filter(
                pl.col("road_temperature").is_not_null()
                & pl.col("public_transport_activity").is_not_null()
            )
            out["n_temp_pt"] = d.height
            out["pearson_temp_vs_public_transport"] = _pearson_corr(
                d, "road_temperature", "public_transport_activity"
            )
        else:
            out["n_temp_pt"] = 0
            out["pearson_temp_vs_public_transport"] = None
        if "mean_delay_sec" in seg.columns:
            d = seg.filter(
                pl.col("road_temperature").is_not_null() & pl.col("mean_delay_sec").is_not_null()
            )
            out["n_temp_delay"] = d.height
            out["pearson_temp_vs_delay"] = _pearson_corr(d, "road_temperature", "mean_delay_sec")
        else:
            out["n_temp_delay"] = 0
            out["pearson_temp_vs_delay"] = None
        return out

    return {
        "definition": PEAK_OFFPEAK_DEFINITION,
        "peak": _segment(tagged, True),
        "offpeak": _segment(tagged, False),
    }


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
        # Load normalized files from S3
        file_keys = list_normalized_parquet_files_from_s3()
        if not file_keys:
            raise RuntimeError(
                "No normalized parquet files found on S3.\n"
                "Please run 'python normalize_data.py' first to create normalized files on S3."
            )
        # Učitaj samo fajlove čiji servisni datum u putanji (normalized/YYYY-MM-DD/) upada u
        # [start_date, end_date] kada su oba zadata — drastično smanjuje I/O i memoriju.
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
                # Determine source from S3 key (path contains source name, e.g. .../avinor_normalized_...)
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
        # Load from local directory
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

    # Combine dataframes per source (unify schemas to avoid Int64/Float64 mismatch)
    combined_by_source = {}
    for source, dfs in source_data.items():
        if dfs:
            if len(dfs) > 1:
                # Collect unified column types across all DataFrames
                all_cols: dict[str, pl.DataType] = {}
                for df in dfs:
                    for col, dt in df.schema.items():
                        if col not in all_cols or str(all_cols[col]) == "Null":
                            all_cols[col] = dt
                        elif str(dt) != "Null" and all_cols[col] != dt:
                            # Resolve conflicts: promote Int->Float, Null->anything
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

    # Apply time period filter if specified
    if start_date is not None or end_date is not None or period_days is not None:
        # Calculate date range
        if end_date is None:
            end_date = datetime.now()

        if period_days is not None:
            # Calculate start_date from end_date backwards
            start_date = end_date - timedelta(days=period_days)

        if start_date is None:
            # If only end_date is provided, use all data up to end_date
            start_date = datetime(1970, 1, 1)  # Very early date

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

        # Filter each source by timestamp (and optionally by weekday/weekend)
        for source in combined_by_source.keys():
            df = combined_by_source[source]
            original_count = df.height
            filter_ts = None
            if "timestamp" in df.columns:
                # entur_siri_et: timestamp (Unix s); for period filter use departure_timestamp when present (departure time)
                if source == "entur_siri_et":
                    # timestamp može biti Datetime, Unix s (Int), ili retko ms/µs kao Float
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
                        # For period use departure time when present, else timestamp (when update was created)
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
                # Filter by day of week: Polars dt.weekday() 1=Mon .. 7=Sun
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

    # Process each source separately since they have different schemas

    # Load location -> zone lookup for weather-by-part-of-city (if available)
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

    station_to_zone_df = None
    try:
        from analysis.bysykkel_zones import load_station_to_zone

        station_to_zone_df = load_station_to_zone(read_from_s3=read_from_s3)
        if station_to_zone_df is not None:
            print(
                f"Bysykkel station→zone: {station_to_zone_df.height} stations inside a city zone"
            )
    except Exception as e:
        print(f"Warning: Bysykkel station→zone not loaded: {e}")
        station_to_zone_df = None

    # Weather data from vegvesen: temperature + precipitation, wind, humidity (weather + location)
    # Prefer road_temperature; fallback to metric_value. Apply range -50..50 °C for temp.
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

        # Optional weather columns for analysis (all as mean per minute / per zone)
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
                # Global aggregation: one row per minute with mean temp + optional columns
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

                # By-zone aggregation when zone is available
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

    # Oslo Bysykkel: total bikes in use across all stations
    # metric_value now represents bikes_in_use (total_capacity - bikes_available)
    # Higher temperature -> more bike usage -> more bikes in use (positive correlation expected)
    bysykkel = None
    bysykkel_by_zone = None
    if "oslobysykkel" in combined_by_source:
        oslobysykkel_df = combined_by_source["oslobysykkel"]
        print(f"Oslo Bysykkel raw data: {oslobysykkel_df.height} records")

        bysykkel = (
            oslobysykkel_df
            .with_columns([
                # Ensure UTC timezone for consistent join
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

        if station_to_zone_df is not None and "location" in oslobysykkel_df.columns:
            ozj = oslobysykkel_df.join(station_to_zone_df, on="location", how="inner")
            if ozj.filter(pl.col("zone").is_not_null()).height > 0:
                bysykkel_by_zone = (
                    ozj.with_columns([
                        pl.col("timestamp")
                        .dt.replace_time_zone("UTC")
                        .dt.truncate("1m")
                        .alias("minute"),
                        pl.col("metric_value")
                        .cast(pl.Float64, strict=False)
                        .alias("bikes_in_use"),
                    ])
                    .filter(pl.col("bikes_in_use").is_not_null())
                    .group_by(["minute", "zone"])
                    .agg(pl.col("bikes_in_use").sum().alias("bikes_in_zone"))
                )
                print(
                    f"  Bysykkel by zone: {bysykkel_by_zone.height} (minute, zone) rows; "
                    f"zones {bysykkel_by_zone['zone'].unique().to_list()}"
                )

    # Avinor: flight activity
    avinor = None
    if "avinor" in combined_by_source:
        avinor_df = combined_by_source["avinor"]
        avinor = (
            avinor_df
            .with_columns([
                # Ensure UTC timezone for consistent join
                pl.col("timestamp")
                .dt.replace_time_zone("UTC")
                .dt.truncate("1m")
                .alias("minute")
            ])
            .group_by("minute")
            .agg(pl.len().alias("flight_activity"))
            .select(["minute", "flight_activity"])
        )

    # Join all data sources - start with weather as base
    if weather is None or weather.height == 0:
        raise RuntimeError("No weather data available for analysis")

    combined = weather

    if bysykkel is not None and bysykkel.height > 0:
        combined = combined.join(bysykkel, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("bikes_available")])

    # Public transport activity: broj vožnji (zapisa) po minuti iz Entur SIRI ET
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

    # Entur SIRI ET delay: aggregate by minute. Use departure_timestamp (departure time from
    # start_date+start_time) when present; else timestamp (when update was created).
    delay_minute = None
    if "entur_siri_et" in combined_by_source:
        delay_df = combined_by_source["entur_siri_et"]
        if delay_df.height > 0 and "timestamp" in delay_df.columns:
            # Jedinstveni Unix sekundi za agregaciju: departure_timestamp je Int (s);
            # timestamp može biti Datetime ili broj (s/ms) — ne smeju se mešati u istom tipu bez konverzije.
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
                delay_minute = delay_minute  # keep for reporting
            else:
                combined = combined.with_columns(
                    [pl.lit(None).alias("mean_delay_sec")])
        else:
            combined = combined.with_columns(
                [pl.lit(None).alias("mean_delay_sec")])
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("mean_delay_sec")])

    # Vegvesen travel_times: srednje vreme putovanja po minuti (period_start -> minute)
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

    # Vegvesen traffic_situations: broj incidenata po minuti
    if "vegvesen_traffic_situations" in combined_by_source:
        sit_df = combined_by_source["vegvesen_traffic_situations"]
        if sit_df.height > 0 and "minute" in sit_df.columns:
            sit_minute = (
                sit_df.group_by("minute")
                .agg(pl.col("event_count").sum().alias("traffic_incident_count"))
            )
            combined = combined.join(sit_minute, on="minute", how="left")

    # Multimodal activity index: min-max scale each mode (0–1) then sum (bikes + PT + flights)
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

    # Filter by part of day (UTC hours), e.g. 0-4 -> 00:00-03:59
    if hour_start is not None and hour_end is not None:
        combined = combined.filter(
            (pl.col("minute").dt.hour() >= hour_start)
            & (pl.col("minute").dt.hour() < hour_end)
        )
        if combined.height > 0:
            print(f"  Part of day {hour_start:02d}-{hour_end:02d} (UTC): {combined.height} minutes")
        keep_minutes = combined.select("minute").unique()
        if weather_by_zone is not None and keep_minutes.height > 0:
            weather_by_zone = weather_by_zone.join(
                keep_minutes, on="minute", how="inner"
            )
        if bysykkel_by_zone is not None and keep_minutes.height > 0:
            bysykkel_by_zone = bysykkel_by_zone.join(
                keep_minutes, on="minute", how="inner"
            )

    zonal_multimodal_df = None
    correlations_by_zone_zonal_mm: dict[str, dict[str, float | None]] = {}
    if weather_by_zone is not None and bysykkel_by_zone is not None:
        zonal_multimodal_df, correlations_by_zone_zonal_mm = _zonal_multimodal_timeseries(
            weather_by_zone, bysykkel_by_zone, combined
        )
        if zonal_multimodal_df is not None and zonal_multimodal_df.height > 0:
            print(
                "Zonal multimodal (bikes by station zone + global PT/flight scaling): "
                f"{zonal_multimodal_df.height} rows; r(temp, MM_zonal) by zone = "
                f"{correlations_by_zone_zonal_mm}"
            )

    correlations_peak_offpeak = correlations_by_commute_peak(combined)
    if correlations_peak_offpeak:
        npk = correlations_peak_offpeak["peak"]["n_minutes"]
        nop = correlations_peak_offpeak["offpeak"]["n_minutes"]
        print(
            f"  Peak×weather segments (weekday UTC 07:00–09:59, 15:00–17:59): "
            f"peak {npk:,} min, off-peak {nop:,} min"
        )

    # Calculate correlations - only use rows where both values exist
    # Bysykkel join adds column "bikes_in_use" (not "bikes_available")
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
            # Only use rows where both temperature and transport data exist
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
                # Check for NaN (NaN != NaN is True, so we check if equal to itself)
                correlations["temperature_vs_public_transport"] = (
                    corr_transport if corr_transport is not None and (
                        corr_transport == corr_transport) else None
                )
            else:
                correlations["temperature_vs_public_transport"] = None
        else:
            correlations["temperature_vs_public_transport"] = None

        if "flight_activity" in combined.columns:
            # Only use rows where both temperature and flight data exist
            flight_data = combined.filter(
                (pl.col("road_temperature").is_not_null())
                & (pl.col("flight_activity").is_not_null())
            )
            if flight_data.height > 1:
                corr_flights = flight_data.select(
                    pl.corr("road_temperature", "flight_activity")
                )[0, 0]
                # Check for NaN (NaN != NaN is True, so we check if equal to itself)
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

        # Delay (Entur trip-updates): correlation temperature vs mean_delay_sec
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
                # Robustness: heavy-tail delay minutes; same rows as raw r
                p1 = delay_data.select(
                    pl.col("mean_delay_sec").quantile(0.01)
                ).item()
                p99 = delay_data.select(
                    pl.col("mean_delay_sec").quantile(0.99)
                ).item()
                if (
                    p1 is not None
                    and p99 is not None
                    and p1 == p1
                    and p99 == p99
                    and float(p1) <= float(p99)
                ):
                    delay_w = delay_data.with_columns(
                        pl.col("mean_delay_sec")
                        .clip(float(p1), float(p99))
                        .alias("_mean_delay_winsor")
                    )
                    correlations["temperature_vs_delay_winsor_1_99"] = _pearson_corr(
                        delay_w, "road_temperature", "_mean_delay_winsor"
                    )
                else:
                    correlations["temperature_vs_delay_winsor_1_99"] = None
                delay_log = delay_data.with_columns(
                    pl.col("mean_delay_sec").log1p().alias("_log_delay")
                )
                correlations["temperature_vs_delay_log1p"] = _pearson_corr(
                    delay_log, "road_temperature", "_log_delay"
                )
            else:
                correlations["temperature_vs_delay"] = None
                correlations["temperature_vs_delay_winsor_1_99"] = None
                correlations["temperature_vs_delay_log1p"] = None
        else:
            correlations["temperature_vs_delay"] = None
            correlations["temperature_vs_delay_winsor_1_99"] = None
            correlations["temperature_vs_delay_log1p"] = None

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

        # Extra weather predictors: precipitation, wind, humidity (same targets as temperature)
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
            "temperature_vs_delay_winsor_1_99": None,
            "temperature_vs_delay_log1p": None,
            "temperature_vs_travel_time": None,
            "temperature_vs_traffic_incidents": None,
        }
        for prefix in ["precipitation", "wind_speed", "relative_humidity"]:
            for k in ["bysykkel", "public_transport", "flight_activity", "multimodal", "delay"]:
                correlations[f"{prefix}_vs_{k}"] = None

    # Interpretation
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

    # Korelacije po zoni (deo grada): temp u zoni vs global bikes/delay
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
            if "public_transport_activity" in merged.columns:
                d = merged.filter(
                    pl.col("temp_zone").is_not_null()
                    & pl.col("public_transport_activity").is_not_null()
                )
                if d.height > 1:
                    c = d.select(pl.corr("temp_zone", "public_transport_activity"))[0, 0]
                    cz["temp_vs_public_transport"] = (
                        float(c) if c is not None and c == c else None
                    )
            if "multimodal_activity" in merged.columns:
                d = merged.filter(
                    pl.col("temp_zone").is_not_null()
                    & pl.col("multimodal_activity").is_not_null()
                )
                if d.height > 1:
                    c = d.select(pl.corr("temp_zone", "multimodal_activity"))[0, 0]
                    cz["temp_vs_multimodal_global"] = (
                        float(c) if c is not None and c == c else None
                    )
            mmz = correlations_by_zone_zonal_mm.get(str(z))
            if mmz and mmz.get("temp_vs_multimodal_zonal") is not None:
                cz["temp_vs_multimodal_zonal"] = mmz["temp_vs_multimodal_zonal"]
            if cz:
                correlations_by_zone[str(z)] = cz

    # Regresija: mean_delay_sec ~ road_temperature + precipitation_intensity + wind_speed + relative_humidity
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

    # Delay data note: Entur SIRI ET provides arrival_delay/departure_delay and departure time.
    delay_note = (
        "Delay data: Entur SIRI ET (Estimated Timetable) provides arrival_delay/departure_delay. "
        "Analysis uses normalized entur_siri_et, aggregated by minute, with temperature_vs_delay correlation. "
        "Robustness: same-minute sample also reports r after winsorizing mean_delay_sec at the 1st–99th "
        "percentile and r using log(1+mean_delay_sec) to limit leverage of extreme delay spikes."
    )

    # Calculate actual date range from filtered data
    date_range = None
    if weather is not None and weather.height > 0:
        date_range = {
            "start": weather.select(pl.col("minute").min())[0, 0],
            "end": weather.select(pl.col("minute").max())[0, 0],
        }

    # Temperature summary (road surface temperature, °C) for the report
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
        "correlations_peak_offpeak": correlations_peak_offpeak,
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
        "correlations_by_zone_zonal_mm": correlations_by_zone_zonal_mm,
        "zonal_multimodal_df": zonal_multimodal_df,
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


# ---------------------------------------------------------------------------
# Predefined intervals for comparative analysis (MDPI-style study)
# Data availability: 2025-12-20 to 2026-01-11 (~23 days)
# ---------------------------------------------------------------------------

# Option A: Three periods (holiday / turn of year / post-holiday)
# Good for discussing seasonal and holiday effects on transport–weather correlations.
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

# Option B: Two full weeks (robustness / consistency check)
# Compare two non-overlapping 7-day windows to test stability of correlations.
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

# Option C: December vs January (calendar boundary)
# Simple two-period comparison for “month” effect (holiday density, etc.).
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

# Option D: Four shorter windows (sensitivity analysis)
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

# Option E: Weekdays vs weekend (same date range, split by day of week)
# Full window 2025-12-20 to 2026-01-11; compare Mon–Fri vs Sat–Sun.
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

# Option F: Data window 2026-01-30 to 2026-02-16 (partial first/last day)
# Three phases for comparative analysis on this dataset.
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

# Tri glavna prozora punog multimodalnog overlap-a (Oslo + ostali izvori)
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

# Zima podeljena na dva segmenta od po 8 dana (W-A1: do Nove godine; W-A2: silvestrovo / prvi januarski dani),
# zatim W-B i W-C — bolja usporedba dužine uzorka sa W-B/W-C.
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

    # Scatter: temperatura vs bikes_in_use
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

    # Scatter: temperatura vs mean_delay_sec
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

    # Time series: minute vs temp i delay (uzorak ako previše redova)
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

    # Heatmap korelacija
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

    if "multimodal_activity" in df.columns and "road_temperature" in df.columns:
        sub = df[["road_temperature", "multimodal_activity"]].dropna()
        if len(sub) > 10:
            fig, ax = plt.subplots()
            ax.scatter(
                sub["road_temperature"], sub["multimodal_activity"], alpha=0.3, s=5
            )
            ax.set_xlabel("Road temperature (°C)")
            ax.set_ylabel("Multimodal activity index (global)")
            ax.set_title("Temperature vs multimodal index (citywide)")
            fig.savefig(
                os.path.join(out, "scatter_temp_vs_multimodal_global.png"),
                dpi=100,
                bbox_inches="tight",
            )
            plt.close(fig)

    corr_by_z = results.get("correlations_by_zone") or {}
    if corr_by_z:
        import numpy as np

        zones = sorted(corr_by_z.keys())
        metric_keys = [
            "temp_vs_bysykkel",
            "temp_vs_delay",
            "temp_vs_public_transport",
            "temp_vs_multimodal_global",
            "temp_vs_multimodal_zonal",
        ]
        labels = ["Bikes", "Delay", "PT", "MM global", "MM zonal"]
        active = [
            (k, lab)
            for k, lab in zip(metric_keys, labels)
            if any(corr_by_z[z].get(k) is not None for z in zones)
        ]
        if active:
            x = np.arange(len(zones))
            w = 0.8 / max(len(active), 1)
            fig, ax = plt.subplots(figsize=(max(6, 1.2 * len(zones)), 5))
            for i, (key, lab) in enumerate(active):
                vals = [corr_by_z[z].get(key) for z in zones]
                heights = [float(v) if v is not None and v == v else 0.0 for v in vals]
                ax.bar(
                    x + (i - (len(active) - 1) / 2) * w,
                    heights,
                    width=w * 0.95,
                    label=lab,
                )
            ax.axhline(0, color="k", linewidth=0.5)
            ax.set_xticks(x)
            ax.set_xticklabels(zones)
            ax.set_ylabel("Pearson r")
            ax.set_title("By zone: temp in zone vs mobility (see legend)")
            ax.legend(loc="best", fontsize=8)
            fig.savefig(
                os.path.join(out, "bar_pearson_by_zone_incl_multimodal.png"),
                dpi=120,
                bbox_inches="tight",
            )
            plt.close(fig)

    combined_pl = results.get("combined_df")
    wby = results.get("weather_by_zone")
    if (
        wby is not None
        and wby.height > 0
        and combined_pl is not None
        and "multimodal_activity" in combined_pl.columns
    ):
        scat = wby.join(
            combined_pl.select(["minute", "multimodal_activity"]),
            on="minute",
            how="inner",
        )
        if scat.height > 10:
            sp = scat.to_pandas()
            zones_u = sorted(sp["zone"].dropna().unique().tolist())
            if zones_u:
                n = len(zones_u)
                fig, axes = plt.subplots(1, n, figsize=(4 * n, 4), squeeze=False)
                for i, z in enumerate(zones_u):
                    ax = axes[0, i]
                    part = sp[sp["zone"] == z][
                        ["road_temperature", "multimodal_activity"]
                    ].dropna()
                    if len(part) > 5:
                        ax.scatter(
                            part["road_temperature"],
                            part["multimodal_activity"],
                            alpha=0.35,
                            s=6,
                        )
                    ax.set_title(f"Zone {z}")
                    ax.set_xlabel("Temp in zone (°C)")
                    ax.set_ylabel("MM index (global)")
                fig.suptitle("Zonal road temp vs citywide multimodal index")
                fig.tight_layout()
                fig.savefig(
                    os.path.join(out, "scatter_temp_zone_vs_multimodal_global_facets.png"),
                    dpi=120,
                    bbox_inches="tight",
                )
                plt.close(fig)

    zmdf = results.get("zonal_multimodal_df")
    if zmdf is not None and zmdf.height > 10:
        zp = zmdf.to_pandas()
        if "multimodal_activity_zonal" in zp.columns:
            zones_u = sorted(zp["zone"].dropna().unique().tolist())
            if zones_u:
                n = len(zones_u)
                fig, axes = plt.subplots(1, n, figsize=(4 * n, 4), squeeze=False)
                for i, z in enumerate(zones_u):
                    ax = axes[0, i]
                    part = zp[zp["zone"] == z][
                        ["road_temperature", "multimodal_activity_zonal"]
                    ].dropna()
                    if len(part) > 5:
                        ax.scatter(
                            part["road_temperature"],
                            part["multimodal_activity_zonal"],
                            alpha=0.35,
                            s=6,
                        )
                    ax.set_title(f"Zone {z}")
                    ax.set_xlabel("Temp in zone (°C)")
                    ax.set_ylabel("MM index (zonal)")
                fig.suptitle(
                    "Zonal temp vs zonal multimodal (bikes in zone + global PT/flight)"
                )
                fig.tight_layout()
                fig.savefig(
                    os.path.join(out, "scatter_temp_vs_multimodal_zonal_facets.png"),
                    dpi=120,
                    bbox_inches="tight",
                )
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
        "temperature_vs_delay_winsor_1_99",
        "temperature_vs_delay_log1p",
    ]
    short_labels = [
        "Bysykkel",
        "PT",
        "Flight",
        "Multimodal",
        "Delay",
        "Delay W1–99",
        "Delay log1p",
    ]
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

    # Temperature summary (road surface °C) per period
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
    # Delay note (once; from first valid result)
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
        "temperature_vs_delay_winsor_1_99": corr.get("temperature_vs_delay_winsor_1_99"),
        "temperature_vs_delay_log1p": corr.get("temperature_vs_delay_log1p"),
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
    if len(s) <= 10:  # YYYY-MM-DD
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
            cpo = results.get("correlations_peak_offpeak")
            if cpo:
                print("\nPearson r by commute peak vs off-peak (temp vs bikes / PT / delay), UTC weekday windows:")
                print(f"  Definition: {cpo.get('definition', '')}")
                for seg in ("peak", "offpeak"):
                    s = cpo[seg]
                    print(
                        f"  {seg}: bikes r={s.get('pearson_temp_vs_bikes')}, "
                        f"PT r={s.get('pearson_temp_vs_public_transport')}, "
                        f"delay r={s.get('pearson_temp_vs_delay')} "
                        f"(n min={s.get('n_minutes')}, n temp+bikes={s.get('n_temp_bikes')}, "
                        f"n temp+PT={s.get('n_temp_pt')})"
                    )
            print("\nCorrelations:")
            _skip_prefixes = ("precipitation_vs_", "wind_speed_vs_", "relative_humidity_vs_")
            for key, value in results["correlations"].items():
                if value is None and any(key.startswith(p) for p in _skip_prefixes):
                    continue
                print(
                    f"  {key}: {value:.3f}" if value is not None else f"  {key}: N/A")
            if results.get("correlations_by_zone"):
                print(
                    "\nCorrelations by zone (temp in zone vs global bikes/PT/delay/MM; "
                    "MM zonal = bikes in zone + global PT/flight scaling):"
                )
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
