"""
Build location_id -> zone lookup from Vegvesen weather_site_table and Oslo zones.
Run once after: python vegvesen/vegvesen_collect.py site-table

Usage (from project root):
  .venv/bin/python analysis/build_vegvesen_location_to_zone.py
  .venv/bin/python analysis/build_vegvesen_location_to_zone.py --local path/to/file.parquet
"""
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
REFERENCE_DIR = SCRIPT_DIR / "reference"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl

from analysis.vegvesen_zones import assign_zone, load_zone_definitions  # noqa: E402, I001


def _find_location_column(df: pl.DataFrame) -> str | None:
    """Find column that holds measurement site id (prefer @id or id)."""
    for name in df.columns:
        n = name.lower()
        if n.endswith(".@id") or n == "id" or (".@id" in n and "reference" in n):
            return name
    for name in df.columns:
        if "id" in name.lower() and "identification" not in name.lower():
            return name
    return None


def _find_lat_lon_columns(df: pl.DataFrame) -> tuple[str | None, str | None]:
    """Find columns for latitude and longitude."""
    lat_col = lon_col = None
    for name in df.columns:
        n = name.lower()
        if "latitude" in n or (("lat" in n or "y" == n) and "lon" not in n):
            lat_col = name
        if "longitude" in n or ("lon" in n or "lng" in n or ("x" == n and "lon" in n)):
            lon_col = name
    if lat_col and lon_col:
        return lat_col, lon_col
    for name in df.columns:
        if "point" in name.lower() or "coordinate" in name.lower():
            base = name.rsplit(".", 1)[0] if "." in name else name
            for c in df.columns:
                if base in c and "lat" in c.lower():
                    lat_col = c
                if base in c and "lon" in c.lower():
                    lon_col = c
    return lat_col, lon_col


def build_location_to_zone(
    site_table_df: pl.DataFrame,
    zones: list[dict] | None = None,
) -> pl.DataFrame:
    """Build (location, zone) from site table with location/id and lat/lon columns."""
    loc_col = _find_location_column(site_table_df)
    lat_col, lon_col = _find_lat_lon_columns(site_table_df)
    if not loc_col:
        raise ValueError(
            "Could not find location/id column. Columns: " + str(site_table_df.columns)
        )
    if not lat_col or not lon_col:
        raise ValueError(
            "Could not find lat/lon columns. Columns: " + str(site_table_df.columns)
        )

    zones = zones or load_zone_definitions()
    df = site_table_df.select(
        pl.col(loc_col).cast(pl.Utf8).alias("location"),
        pl.col(lat_col).cast(pl.Float64, strict=False).alias("_lat"),
        pl.col(lon_col).cast(pl.Float64, strict=False).alias("_lon"),
    ).filter(
        pl.col("_lat").is_not_null() & pl.col("_lon").is_not_null()
    )
    zones_list = [
        assign_zone(row["_lat"], row["_lon"], zones)
        for row in df.select("_lat", "_lon").iter_rows(named=True)
    ]
    df = df.with_columns(pl.Series("zone", zones_list))
    df = df.filter(pl.col("zone").is_not_null()).select("location", "zone").unique("location")
    return df


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Build location_id -> zone from weather_site_table and Oslo zones."
    )
    parser.add_argument(
        "--local",
        type=str,
        default=None,
        help="Local path to weather_site_table.parquet (default: load from S3).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path (default: analysis/reference/vegvesen_location_to_zone.parquet).",
    )
    args = parser.parse_args()

    if args.local:
        path = Path(args.local)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        site_df = pl.read_parquet(path)
    else:
        from utils.s3_upload import S3_PARQUET_PATH, read_parquet_from_s3
        base = S3_PARQUET_PATH.rstrip("/")
        s3_key = f"{base}/vegvesen/reference/weather_site_table.parquet"
        site_df = read_parquet_from_s3(s3_key)
        if site_df is None:
            print("Could not load weather_site_table from S3. Run:")
            print("  python vegvesen/vegvesen_collect.py site-table")
            sys.exit(1)

    print("Site table:", site_df.height, "rows, columns:", site_df.columns)
    try:
        out_df = build_location_to_zone(site_df)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    print(
        "Location -> zone:", out_df.height, "stations,",
        "zones:", out_df["zone"].unique().to_list(),
    )

    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = args.output or REFERENCE_DIR / "vegvesen_location_to_zone.parquet"
    if not Path(out_path).is_absolute():
        out_path = REFERENCE_DIR / out_path
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.write_parquet(out_path)
    print("Saved:", out_path)

    # Snimi na S3 kada se fajl kreira (isti key koristi analyze_weather_impact)
    try:
        from utils.s3_upload import save_parquet_to_s3, S3_PARQUET_PATH
        s3_key = "vegvesen/reference/vegvesen_location_to_zone.parquet"
        full_key = f"{S3_PARQUET_PATH.rstrip('/')}/{s3_key}"
        df_pandas = out_df.to_pandas()
        if save_parquet_to_s3(df_pandas, s3_key):
            print("Saved to S3:", full_key)
        else:
            print("S3 upload failed (check AWS credentials in .env).")
            print("  analyze_weather_impact will use local file or skip 'correlations by zone'.")
    except Exception as e:
        print("S3 upload error:", e)
    print("Done.")


if __name__ == "__main__":
    main()
