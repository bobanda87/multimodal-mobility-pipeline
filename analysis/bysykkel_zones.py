"""
Stanice Oslo Bysykkel (GBFS station_information) → gruba zona grada (isti bbox kao Vegvesen).
Koristi se za sumu bikes_in_use po (minute, zone).
"""
from __future__ import annotations

import os
from pathlib import Path

import polars as pl

from analysis.vegvesen_zones import assign_zone, load_zone_definitions

SCRIPT_DIR = Path(__file__).resolve().parent
REFERENCE_DIR = SCRIPT_DIR / "reference"


def _find_lat_lon_columns(df: pl.DataFrame) -> tuple[str | None, str | None]:
    lat_c = lon_c = None
    for name in df.columns:
        nl = name.lower()
        if nl in ("lat", "latitude") and "lon" not in nl:
            lat_c = name
        if nl in ("lon", "lng", "longitude") and "lat" not in nl:
            lon_c = name
    if lat_c and lon_c:
        return lat_c, lon_c
    for name in df.columns:
        nl = name.lower()
        if "latitude" in nl:
            lat_c = name
        if "longitude" in nl or nl == "lng":
            lon_c = name
    return lat_c, lon_c


def station_information_to_location_zone(info_df: pl.DataFrame) -> pl.DataFrame:
    """Iz station_information Parquet-a napravi (location, zone). location = station_id."""
    if info_df.height == 0:
        return pl.DataFrame(schema={"location": pl.String, "zone": pl.String})
    sid = None
    for cand in ("station_id", "id", "short_name"):
        if cand in info_df.columns:
            sid = cand
            break
    if sid is None:
        return pl.DataFrame(schema={"location": pl.String, "zone": pl.String})
    lat_c, lon_c = _find_lat_lon_columns(info_df)
    if not lat_c or not lon_c:
        return pl.DataFrame(schema={"location": pl.String, "zone": pl.String})
    zones = load_zone_definitions()
    base = (
        info_df.select(
            [
                pl.col(sid).cast(pl.Utf8).alias("location"),
                pl.col(lat_c).cast(pl.Float64, strict=False).alias("_lat"),
                pl.col(lon_c).cast(pl.Float64, strict=False).alias("_lon"),
            ]
        )
        .filter(
            pl.col("location").is_not_null()
            & pl.col("_lat").is_not_null()
            & pl.col("_lon").is_not_null()
        )
        .unique("location", keep="last")
    )
    zids = [
        assign_zone(row["_lat"], row["_lon"], zones)
        for row in base.select("_lat", "_lon").iter_rows(named=True)
    ]
    out = base.with_columns(pl.Series("zone", zids)).filter(pl.col("zone").is_not_null())
    return out.select("location", "zone")


def load_station_to_zone(
    local_path: str | Path | None = None,
    read_from_s3: bool = True,
) -> pl.DataFrame | None:
    """
    Učitaj (location, zone). Redosled: eksplicitni local_path, zatim
    analysis/reference/oslobysykkel_station_to_zone.parquet, zatim (ako read_from_s3)
    poslednji station_information sa S3.
    """
    if local_path:
        path = Path(local_path).expanduser()
        if not path.is_absolute():
            path = (SCRIPT_DIR.parent / path).resolve()
    else:
        path = REFERENCE_DIR / "oslobysykkel_station_to_zone.parquet"

    if path.exists():
        df = pl.read_parquet(path)
        if "station_id" in df.columns and "location" not in df.columns:
            df = df.rename({"station_id": "location"})
        if "location" in df.columns and "zone" in df.columns:
            return df.select(["location", "zone"])

    if read_from_s3:
        try:
            from utils.s3_upload import list_parquet_files_from_s3, read_parquet_from_s3

            keys = list_parquet_files_from_s3(
                "oslobysykkel", "*station_information*.parquet", with_metadata=False
            )
            if keys:
                info_df = read_parquet_from_s3(keys[-1])
                if info_df is not None and info_df.height > 0:
                    lut = station_information_to_location_zone(info_df)
                    if lut.height > 0:
                        print(
                            "Bysykkel station→zone built from S3 "
                            f"({lut.height} stations): {os.path.basename(keys[-1])}"
                        )
                        return lut
        except Exception as e:
            print(f"Warning: could not load Bysykkel station_information from S3: {e}")
    return None
