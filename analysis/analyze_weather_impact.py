import polars as pl
from pathlib import Path
import os


def analyze_weather_impact(
    normalized_dir: str = "normalized_data",
    city_bbox: dict | None = None,
):
    """
    Analyze correlation between weather conditions and:
    - Oslo Bysykkel availability
    - Public transport activity (Entur)
    - Flight delays (Avinor)

    Args:
        normalized_dir: Directory containing normalized parquet files
        city_bbox: Bounding box for filtering data (default: Oslo area)

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

    # normalized_data is always in the project root, not in analysis folder
    if not os.path.isabs(normalized_dir):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level from analysis/
        project_root = os.path.dirname(script_dir)
        normalized_dir = os.path.join(project_root, normalized_dir)

    # Ensure directory exists
    Path(normalized_dir).mkdir(parents=True, exist_ok=True)

    files = list(Path(normalized_dir).glob("*_normalized_*.parquet"))
    if not files:
        # Provide helpful error message
        abs_path = os.path.abspath(normalized_dir)
        raise RuntimeError(
            f"No normalized parquet files found in {abs_path}\n"
            f"Please run 'python normalize_data.py' first to create normalized files."
        )

    print(f"Loading {len(files)} normalized files...")
    # Load each source separately since they have different schemas
    source_data = {}
    for f in files:
        try:
            df = pl.read_parquet(f)
            # Determine source from filename or from dataframe
            source = None
            if "avinor" in f.name:
                source = "avinor"
            elif "entur" in f.name:
                source = "entur"
            elif "oslobysykkel" in f.name:
                source = "oslobysykkel"
            elif "vegvesen" in f.name:
                source = "vegvesen"
            else:
                # Try to get source from dataframe
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
                        f"Weather minute range: {weather.select([pl.col('minute').min(), pl.col('minute').max()])}")
            else:
                print("Warning: No valid temperature values after filtering")
                print(
                    f"Sample metric_value values: {vegvesen_df.select('metric_value').head(5)}")
                weather = None
        else:
            print(
                f"Warning: metric_value column not found. Available columns: {vegvesen_df.columns}")
            weather = None

    # Oslo Bysykkel: average bikes available
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
                .alias("bikes")
            ])
            .filter(pl.col("bikes").is_not_null())
            .group_by("minute")
            .agg(pl.col("bikes").mean().alias("bikes_available"))
        )
        print(f"Oslo Bysykkel aggregated: {bysykkel.height} time buckets")
        if bysykkel.height > 0:
            print(
                f"Bysykkel minute range: {bysykkel.select([pl.col('minute').min(), pl.col('minute').max()])}")

    # Entur: public transport activity (count of vehicles in Oslo area)
    entur_minute = None
    if "entur" in combined_by_source:
        entur_df = combined_by_source["entur"]
        print(f"Entur raw data: {entur_df.height} records")
        print(f"Entur columns: {entur_df.columns}")

        # Filter by city bounding box
        entur_df = entur_df.filter(
            (pl.col("lat") >= city_bbox["lat_min"])
            & (pl.col("lat") <= city_bbox["lat_max"])
            & (pl.col("lon") >= city_bbox["lon_min"])
            & (pl.col("lon") <= city_bbox["lon_max"])
        )
        print(f"Entur after bbox filter: {entur_df.height} records")

        if entur_df.height > 0:
            # Check timestamp range
            timestamp_range = entur_df.select([
                pl.col("timestamp").min().alias("min_ts"),
                pl.col("timestamp").max().alias("max_ts")
            ])
            print(f"Entur timestamp range: {timestamp_range}")

            entur_minute = (
                entur_df
                .with_columns([
                    # Ensure UTC timezone for consistent join
                    pl.col("timestamp")
                    .dt.replace_time_zone("UTC")
                    .dt.truncate("1m")
                    .alias("minute")
                ])
                .group_by("minute")
                .agg(pl.len().alias("public_transport_activity"))
            )
            print(f"Entur aggregated: {entur_minute.height} time buckets")
            if entur_minute.height > 0:
                print(
                    f"Entur minute range: {entur_minute.select([pl.col('minute').min(), pl.col('minute').max()])}")

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

    if entur_minute is not None and entur_minute.height > 0:
        combined = combined.join(entur_minute, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("public_transport_activity")])

    if avinor is not None and avinor.height > 0:
        combined = combined.join(avinor, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("flight_activity")])

    # Calculate correlations - only use rows where both values exist
    correlations = {}
    if combined.height > 1:
        if "bikes_available" in combined.columns:
            # Only use rows where both temperature and bikes data exist
            bikes_data = combined.filter(
                (pl.col("road_temperature").is_not_null())
                & (pl.col("bikes_available").is_not_null())
            )
            print(
                f"Bikes data overlap: {bikes_data.height} records with both temperature and bikes")
            if bikes_data.height > 1:
                corr_bikes = bikes_data.select(
                    pl.corr("road_temperature", "bikes_available")
                )[0, 0]
                # Check for NaN (NaN != NaN is True, so we check if equal to itself)
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
    else:
        correlations = {
            "temperature_vs_bysykkel": None,
            "temperature_vs_public_transport": None,
            "temperature_vs_flight_activity": None,
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

    return {
        "correlations": correlations,
        "interpretation": interpretation,
        "sample_size_minutes": combined.height,
        "data_summary": {
            "weather_records": weather.height if weather is not None else 0,
            "bysykkel_records": bysykkel.height if (bysykkel is not None and bysykkel.height > 0) else 0,
            "entur_records": entur_minute.height if (entur_minute is not None and entur_minute.height > 0) else 0,
            "avinor_records": avinor.height if (avinor is not None and avinor.height > 0) else 0,
        },
    }


if __name__ == "__main__":
    try:
        results = analyze_weather_impact()
        print("\n" + "=" * 60)
        print("WEATHER IMPACT ANALYSIS RESULTS")
        print("=" * 60)
        print(f"\nSample size: {results['sample_size_minutes']} minutes")
        print(f"\nData summary:")
        for key, value in results["data_summary"].items():
            print(f"  {key}: {value}")

        print(f"\nCorrelations:")
        for key, value in results["correlations"].items():
            if value is not None:
                print(f"  {key}: {value:.3f}")
            else:
                print(f"  {key}: N/A")

        print(f"\nInterpretation:")
        for key, value in results["interpretation"].items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
