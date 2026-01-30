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

from utils.s3_upload import list_normalized_parquet_files_from_s3, read_parquet_from_s3


def analyze_weather_impact(
    normalized_dir: str = "normalized_data",
    city_bbox: dict | None = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    period_days: Optional[int] = None,
    weekday_only: bool = False,
    weekend_only: bool = False,
    read_from_s3: bool = True,
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
        print(f"Loading {len(file_keys)} normalized files from S3...")
        for s3_key in file_keys:
            try:
                df = read_parquet_from_s3(s3_key)
                if df is None:
                    print(f"Warning: Could not read {os.path.basename(s3_key)} from S3")
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
                print(f"Warning: Could not load {os.path.basename(s3_key)}: {e}")
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

    # Combine dataframes per source
    combined_by_source = {}
    for source, dfs in source_data.items():
        if dfs:
            combined_by_source[source] = pl.concat(dfs, how='diagonal')

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
        
        print(f"\nFiltering data by time period:")
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
            
            if "timestamp" in df.columns:
                # entur_siri_et: timestamp (Unix s); for period filter use departure_timestamp when present (departure time)
                if source == "entur_siri_et":
                    df = df.with_columns([
                        pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.replace_time_zone("UTC").alias("timestamp")
                    ])
                    if "departure_timestamp" in df.columns:
                        df = df.with_columns([
                            pl.from_epoch(pl.col("departure_timestamp"), time_unit="s").dt.replace_time_zone("UTC").alias("_dep_ts")
                        ])
                        # For period use departure time when present, else timestamp (when update was created)
                        filter_ts = pl.coalesce(pl.col("_dep_ts"), pl.col("timestamp"))
                    else:
                        filter_ts = pl.col("timestamp")
                else:
                    df = df.with_columns([
                        pl.col("timestamp").dt.replace_time_zone("UTC").alias("timestamp")
                    ])
                    filter_ts = pl.col("timestamp")

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
                print(f"  Warning: {source} has no 'timestamp' column, skipping time filter")

    # Process each source separately since they have different schemas

    # Weather data: temperature from vegvesen
    weather = None
    if "vegvesen" in combined_by_source:
        vegvesen_df = combined_by_source["vegvesen"]
        print(f"Vegvesen data loaded: {vegvesen_df.height} records")
        print(f"Vegvesen columns: {vegvesen_df.columns}")

        # Check if we have metric_value column
        if "metric_value" in vegvesen_df.columns:
            # Try to extract temperature - handle string values
            weather = (
                vegvesen_df
                .with_columns([
                    # Convert to UTC and truncate to ensure consistent timezone
                    pl.col("timestamp")
                    .dt.replace_time_zone("UTC")
                    .dt.truncate("1m")
                    .alias("minute"),
                    # Try to parse metric_value as float (it might be string)
                    pl.col("metric_value")
                    .cast(pl.Float64, strict=False)
                    .alias("temperature")
                ])
                .filter(
                    (pl.col("temperature").is_not_null())
                    & (pl.col("temperature").is_finite())
                    & (pl.col("temperature") != 0)  # Filter out zeros
                )
            )

            if weather.height > 0:
                weather = weather.group_by("minute").agg(
                    pl.col("temperature").mean().alias("road_temperature")
                )
                print(
                    f"Weather data aggregated: {weather.height} time buckets")
                if weather.height > 0:
                    print(
                        f"Weather minute range: {weather.select([pl.col('minute').min().alias('min_minute'), pl.col('minute').max().alias('max_minute')])}")
            else:
                print("Warning: No valid temperature values after filtering")
                print(
                    f"Sample metric_value values: {vegvesen_df.select('metric_value').head(5)}")
                weather = None
        else:
            print(
                f"Warning: metric_value column not found. Available columns: {vegvesen_df.columns}")
            weather = None

    # Oslo Bysykkel: total bikes in use across all stations
    # metric_value now represents bikes_in_use (total_capacity - bikes_available)
    # Higher temperature -> more bike usage -> more bikes in use (positive correlation expected)
    bysykkel = None
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

    combined = combined.with_columns([pl.lit(None).alias("public_transport_activity")])

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
            # Prefer departure_timestamp (actual departure); fallback to timestamp
            if "departure_timestamp" in delay_df.columns:
                agg_ts = pl.when(pl.col("departure_timestamp").is_not_null() & (pl.col("departure_timestamp") > 0))
                agg_ts = agg_ts.then(pl.col("departure_timestamp")).otherwise(pl.col("timestamp"))
            else:
                agg_ts = pl.col("timestamp")
            delay_df = delay_df.with_columns(agg_ts.alias("_agg_ts")).filter(pl.col("_agg_ts") > 0)
            if delay_df.height > 0:
                delay_minute = (
                    delay_df
                    .with_columns([
                        pl.from_epoch(pl.col("_agg_ts"), time_unit="s").alias("ts"),
                    ])
                    .with_columns([
                        pl.col("ts").dt.replace_time_zone("UTC").dt.truncate("1m").alias("minute"),
                    ])
                    .group_by("minute")
                    .agg(
                        pl.col("max_departure_delay_sec").mean().alias("mean_delay_sec"),
                        pl.col("max_arrival_delay_sec").mean().alias("mean_arrival_delay_sec"),
                    )
                )
                combined = combined.join(delay_minute.select(["minute", "mean_delay_sec"]), on="minute", how="left")
                delay_minute = delay_minute  # keep for reporting
            else:
                combined = combined.with_columns([pl.lit(None).alias("mean_delay_sec")])
        else:
            combined = combined.with_columns([pl.lit(None).alias("mean_delay_sec")])
    else:
        combined = combined.with_columns([pl.lit(None).alias("mean_delay_sec")])

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
                scales.append(((pl.col(f"_fill_{c}") - lo) / (hi - lo)).alias(f"_s_{c}"))
            else:
                scales.append(pl.lit(0.0).alias(f"_s_{c}"))
        combined = combined.with_columns(scales)
        combined = combined.with_columns([
            pl.sum_horizontal([f"_s_{c}" for c in activity_cols]).alias("multimodal_activity")
        ])
        combined = combined.drop([f"_fill_{c}" for c in activity_cols] + [f"_s_{c}" for c in activity_cols])

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
                    corr_bikes if corr_bikes is not None and (corr_bikes == corr_bikes) else None
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
                (pl.col("road_temperature").is_not_null()) & (pl.col("multimodal_activity").is_not_null())
            )
            if mm_data.height > 1:
                corr_mm = mm_data.select(pl.corr("road_temperature", "multimodal_activity"))[0, 0]
                correlations["temperature_vs_multimodal"] = (
                    corr_mm if corr_mm is not None and (corr_mm == corr_mm) else None
                )
            else:
                correlations["temperature_vs_multimodal"] = None
        else:
            correlations["temperature_vs_multimodal"] = None

        # Delay (Entur trip-updates): correlation temperature vs mean_delay_sec
        if "mean_delay_sec" in combined.columns:
            delay_data = combined.filter(
                (pl.col("road_temperature").is_not_null()) & (pl.col("mean_delay_sec").is_not_null())
            )
            if delay_data.height > 1:
                corr_delay = delay_data.select(pl.corr("road_temperature", "mean_delay_sec"))[0, 0]
                correlations["temperature_vs_delay"] = (
                    corr_delay if corr_delay is not None and (corr_delay == corr_delay) else None
                )
            else:
                correlations["temperature_vs_delay"] = None
        else:
            correlations["temperature_vs_delay"] = None
    else:
        correlations = {
            "temperature_vs_bysykkel": None,
            "temperature_vs_public_transport": None,
            "temperature_vs_flight_activity": None,
            "temperature_vs_multimodal": None,
            "temperature_vs_delay": None,
        }

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

    # Delay data note: Entur SIRI ET provides arrival_delay/departure_delay and departure time.
    delay_note = (
        "Delay data: Entur SIRI ET (Estimated Timetable) provides arrival_delay/departure_delay. "
        "Analysis uses normalized entur_siri_et, aggregated by minute, with temperature_vs_delay correlation."
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
        temp_valid = combined.filter(pl.col("road_temperature").is_not_null() & pl.col("road_temperature").is_finite())
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
    {"name": "Window 1 (Dec 20–25)", "start_date": datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc), "end_date": datetime(2025, 12, 25, 23, 59, 59, tzinfo=timezone.utc)},
    {"name": "Window 2 (Dec 26–31)", "start_date": datetime(2025, 12, 26, 0, 0, 0, tzinfo=timezone.utc), "end_date": datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)},
    {"name": "Window 3 (Jan 1–5)", "start_date": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "end_date": datetime(2026, 1, 5, 23, 59, 59, tzinfo=timezone.utc)},
    {"name": "Window 4 (Jan 6–11)", "start_date": datetime(2026, 1, 6, 0, 0, 0, tzinfo=timezone.utc), "end_date": datetime(2026, 1, 11, 23, 59, 59, tzinfo=timezone.utc)},
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
    valid_periods = [p for p in period_names if isinstance(comparison_results[p], dict) and "correlations" in comparison_results[p]]
    if not valid_periods:
        print("No valid period results to display.")
        return
    total_w = period_w + len(metrics) * col_w
    print("\n" + "=" * total_w)
    print("COMPARATIVE CORRELATIONS (for manuscript table)")
    print("=" * total_w)
    header = "Period".ljust(period_w) + "".join(s.ljust(col_w) for s in short_labels)
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
    print("Period".ljust(period_w) + "Min (°C)".ljust(14) + "Max (°C)".ljust(14) + "Mean (°C)".ljust(16) + "Std (°C)")
    print("-" * total_w)
    for name in valid_periods:
        res = comparison_results[name]
        ts = res.get("temperature_summary")
        if ts:
            row = name[: period_w - 1].ljust(period_w) + f"{ts['min_c']}".ljust(14) + f"{ts['max_c']}".ljust(14) + f"{ts['mean_c']}".ljust(16) + f"{ts['std_c']:.1f}"
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


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Weather impact analysis (single period or comparative)")
    parser.add_argument("--mode", choices=["single", "compare"], default="compare", help="single = one period, compare = multiple periods")
    parser.add_argument("--periods", choices=["three_phases", "two_weeks", "dec_jan", "four_windows", "weekday_weekend"], default="three_phases", help="Which predefined periods to compare")
    parser.add_argument("--period-days", type=int, default=7, help="For single mode: length of period in days (from end of data)")
    args = parser.parse_args()

    try:
        if args.mode == "single":
            results = analyze_weather_impact(period_days=args.period_days)
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
                print(f"  Min: {ts['min_c']} °C, Max: {ts['max_c']} °C, Mean: {ts['mean_c']} °C, Std: {ts['std_c']:.1f} °C (n={ts['n_readings']})")
            print(f"\nSample size: {results['sample_size_minutes']} minutes")
            print("\nData summary:")
            for key, value in results["data_summary"].items():
                print(f"  {key}: {value}")
            print("\nCorrelations:")
            for key, value in results["correlations"].items():
                print(f"  {key}: {value:.3f}" if value is not None else f"  {key}: N/A")
            print("\nInterpretation:")
            for key, value in results["interpretation"].items():
                print(f"  {key}: {value}")
            if results.get("delay_note"):
                print("\nDelay note:")
                print("  " + results["delay_note"])
        else:
            period_sets = {
                "three_phases": PERIODS_THREE_PHASES,
                "two_weeks": PERIODS_TWO_WEEKS,
                "dec_jan": PERIODS_DEC_VS_JAN,
                "four_windows": PERIODS_FOUR_WINDOWS,
                "weekday_weekend": PERIODS_WEEKDAY_VS_WEEKEND,
            }
            periods = period_sets[args.periods]
            print(f"Comparing {len(periods)} periods: {args.periods}")
            comparison_results = compare_periods(periods, read_from_s3=True)
            print_comparison_table(comparison_results)
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
