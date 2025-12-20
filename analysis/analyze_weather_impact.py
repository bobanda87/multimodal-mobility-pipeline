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
    dfs = []
    for f in files:
        try:
            df = pl.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not load {f.name}: {e}")

    if not dfs:
        raise RuntimeError("No valid dataframes loaded")

    df = pl.concat(dfs)

    # Filter Entur data by city bounding box
    entur_df = df.filter(pl.col("source") == "entur")
    if entur_df.height > 0:
        entur_df = entur_df.filter(
            (pl.col("lat") >= city_bbox["lat_min"])
            & (pl.col("lat") <= city_bbox["lat_max"])
            & (pl.col("lon") >= city_bbox["lon_min"])
            & (pl.col("lon") <= city_bbox["lon_max"])
        )

    # Create time buckets (1 minute intervals)
    df = df.with_columns([
        pl.col("timestamp").dt.truncate("1m").alias("minute")
    ])

    # Weather data: temperature from vegvesen
    # Filter for temperature-related metrics
    weather = (
        df.filter(
            (pl.col("source") == "vegvesen")
            & (pl.col("metric_value").is_not_null())
        )
        .with_columns([
            # Extract temperature from metric_value
            # metric_value might be a string representation of temperature
            pl.col("metric_value")
            .cast(pl.Float64, strict=False)
            .alias("temperature")
        ])
        .filter(
            (pl.col("temperature").is_not_null())
            & (pl.col("temperature").is_finite())
        )
        .group_by("minute")
        .agg(pl.col("temperature").mean().alias("road_temperature"))
    )

    # Oslo Bysykkel: average bikes available
    bysykkel = (
        df.filter(
            (pl.col("source") == "oslobysykkel")
            & (pl.col("metric_value").is_not_null())
        )
        .with_columns([
            pl.col("metric_value").cast(
                pl.Float64, strict=False).alias("bikes")
        ])
        .filter(pl.col("bikes").is_not_null())
        .group_by("minute")
        .agg(pl.col("bikes").mean().alias("bikes_available"))
    )

    # Entur: public transport activity (count of vehicles in Oslo area)
    entur_minute = (
        entur_df.with_columns([
            pl.col("timestamp").dt.truncate("1m").alias("minute")
        ])
        .group_by("minute")
        .agg(pl.count().alias("public_transport_activity"))
    )

    # Avinor: flight delays (count delayed flights)
    avinor = (
        df.filter(pl.col("source") == "avinor")
        .group_by("minute")
        .agg(
            # Count flights where status indicates delay or metric_value is not null
            pl.count().alias("total_flights"),
            # For now, we'll use total flights as proxy
            # (would need to parse status to determine actual delays)
        )
        .with_columns([
            # Using total flights as proxy - in future could parse status
            pl.col("total_flights").alias("flight_activity")
        ])
        .select(["minute", "flight_activity"])
    )

    # Join all data sources
    combined = weather
    if bysykkel.height > 0:
        combined = combined.join(bysykkel, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("bikes_available")])

    if entur_minute.height > 0:
        combined = combined.join(entur_minute, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("public_transport_activity")])

    if avinor.height > 0:
        combined = combined.join(avinor, on="minute", how="left")
    else:
        combined = combined.with_columns(
            [pl.lit(None).alias("flight_activity")])

    # Fill nulls with 0 for correlation calculation
    combined = combined.fill_null(0)

    # Calculate correlations
    correlations = {}
    if combined.height > 1:
        if "bikes_available" in combined.columns:
            corr_bikes = combined.select(
                pl.corr("road_temperature", "bikes_available")
            )[0, 0]
            correlations["temperature_vs_bysykkel"] = (
                corr_bikes if corr_bikes is not None else 0.0
            )
        else:
            correlations["temperature_vs_bysykkel"] = None

        if "public_transport_activity" in combined.columns:
            corr_transport = combined.select(
                pl.corr("road_temperature", "public_transport_activity")
            )[0, 0]
            correlations["temperature_vs_public_transport"] = (
                corr_transport if corr_transport is not None else 0.0
            )
        else:
            correlations["temperature_vs_public_transport"] = None

        if "flight_activity" in combined.columns:
            corr_flights = combined.select(
                pl.corr("road_temperature", "flight_activity")
            )[0, 0]
            correlations["temperature_vs_flight_activity"] = (
                corr_flights if corr_flights is not None else 0.0
            )
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
            "weather_records": weather.height,
            "bysykkel_records": bysykkel.height if bysykkel.height > 0 else 0,
            "entur_records": entur_minute.height if entur_minute.height > 0 else 0,
            "avinor_records": avinor.height if avinor.height > 0 else 0,
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
