import os
from glob import glob
import polars as pl
from datetime import datetime

# Automatically determine base path: use mnt path on server, local path otherwise
if os.path.exists("/mnt/volume_ams3_01"):
    BASE_PATH = "/mnt/volume_ams3_01"
    OUTPUT_DIR = "normalized_data"
    DATA_SOURCES = {
        "avinor": f"{BASE_PATH}/avinor_data",
        "entur": f"{BASE_PATH}/entur_data",
        "oslobysykkel": f"{BASE_PATH}/oslobysykkel_data",
        "vegvesen": f"{BASE_PATH}/vegvesen_data",
    }
else:
    BASE_PATH = None
    OUTPUT_DIR = "normalized_data"
    DATA_SOURCES = {
        "avinor": "../avinor/avinor_data",
        "entur": "../entur/entur_data",
        "oslobysykkel": "../oslobysykkel/oslobysykkel_data",
        "vegvesen": "../vegvesen/vegvesen_data",
    }

os.makedirs(OUTPUT_DIR, exist_ok=True)


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
    """Find all Parquet files in a directory.

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


# --- AVINOR ------------------------------------------------------
def normalize_avinor(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Avinor flight data into a unified schema."""
    # Handle missing 'status' column by using None
    if "status" not in df.columns:
        df = df.with_columns([pl.lit(None).alias("status")])

    return (
        df
        .with_columns([
            pl.col("schedule_time")
            .str.replace("Z", "+00:00")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .alias("timestamp")
        ])
        .select([
            pl.col("airport").alias("location"),
            pl.col("status").alias("metric_value"),
            pl.col("timestamp"),
            pl.col("arr_dep").alias("metric_name"),
            pl.lit("avinor").alias("source"),
        ])
    )


# --- ENTUR -------------------------------------------------------
def normalize_entur(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Entur vehicle position data into a unified schema."""
    df = df.with_columns([
        pl.col("timestamp").cast(pl.Datetime),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
    ])
    return df.select([
        pl.col("trip_id").alias("metric_name"),
        pl.col("latitude").alias("lat"),
        pl.col("longitude").alias("lon"),
        pl.col("timestamp"),
        pl.lit("entur").alias("source"),
    ])


# --- OSLO BYSYKKEL -----------------------------------------------
def normalize_oslobysykkel(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Oslo Bysykkel station status data."""
    df = df.with_columns([
        pl.col("last_reported").cast(pl.Int64).alias("ts_int")
    ])
    df = df.with_columns([
        (pl.col("ts_int") * 1_000_000_000).cast(pl.Datetime).alias("timestamp")
    ])
    return df.select([
        pl.col("station_id").alias("location"),
        pl.col("num_bikes_available").alias("metric_value"),
        pl.col("num_docks_available"),
        pl.col("is_renting"),
        pl.col("is_returning"),
        pl.col("timestamp"),
        pl.lit("oslobysykkel").alias("source"),
    ])


# --- VEGVESEN ----------------------------------------------------
def normalize_vegvesen(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Vegvesen weather data."""
    df = df.with_columns([
        pl.col("ns10:measurementTimeDefault.ns10:timeValue")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
        .alias("timestamp")
    ])
    return df.select([
        pl.col("ns10:measurementSiteReference.@id").alias("location"),
        pl.col("ns10:physicalQuantity").alias("metric_name"),
        pl.col("ns10:physicalQuantity.ns10:physicalQuantity.ns10:basicData.ns10:roadSurfaceConditionMeasurements.roadSurfaceTemperature.temperature").alias("metric_value"),
        pl.col("timestamp"),
        pl.lit("vegvesen").alias("source"),
    ])


NORMALIZERS = {
    "avinor": normalize_avinor,
    "entur": normalize_entur,
    "oslobysykkel": normalize_oslobysykkel,
    "vegvesen": normalize_vegvesen,
}


def run_normalization():
    """Run normalization for all available data sources."""
    print("Starting normalization process...\n")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for source, data_dir in DATA_SOURCES.items():
        # For entur, specifically look for vehicle-positions files
        if source == "entur":
            parquet_files = get_all_parquet_files(
                data_dir, "*vehicle-positions*.parquet")
        else:
            parquet_files = get_all_parquet_files(data_dir)

        if not parquet_files:
            print(f"{source}: No parquet files found.")
            continue

        print(f"{source}: Found {len(parquet_files)} parquet files to normalize\n")

        all_normalized = []
        processed = 0
        errors = 0

        for parquet_file in parquet_files:
            try:
                df = pl.read_parquet(parquet_file)
                df_norm = NORMALIZERS[source](df)

                if df_norm.height > 0:
                    all_normalized.append(df_norm)
                    processed += 1
                    print(
                        f"  ✓ {os.path.basename(parquet_file)}: {df_norm.height} records")
                else:
                    print(
                        f"  - {os.path.basename(parquet_file)}: empty, skipping")
            except Exception as e:
                errors += 1
                print(
                    f"  ✗ {os.path.basename(parquet_file)}: Error -> {str(e)[:80]}")

        if all_normalized:
            # Combine all normalized dataframes
            combined_df = pl.concat(all_normalized)
            out_path = os.path.join(
                OUTPUT_DIR,
                f"{source}_normalized_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.parquet"
            )
            combined_df.write_parquet(out_path)
            print(
                f"\n{source}: Combined and saved -> {out_path} "
                f"({combined_df.height} total records from {processed} files)\n")
        else:
            print(
                f"\n{source}: No valid data to save (processed: {processed}, errors: {errors})\n")


if __name__ == "__main__":
    run_normalization()
