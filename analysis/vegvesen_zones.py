"""
Zone definitions and mapping for Vegvesen weather stations (Oslo area).
Used to tag 'deo grada' (part of city) for weather-by-location analysis.
"""
import json
from pathlib import Path

import polars as pl

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_DIR = SCRIPT_DIR / "config"
REFERENCE_DIR = SCRIPT_DIR / "reference"
DEFAULT_ZONES_PATH = CONFIG_DIR / "oslo_zones.json"


def load_zone_definitions(path: str | Path | None = None) -> list[dict]:
    """Load zone definitions (bboxes) from JSON. Returns list of zone dicts."""
    path = path or DEFAULT_ZONES_PATH
    path = Path(path)
    if not path.is_absolute():
        path = SCRIPT_DIR / path
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("zones", data) if isinstance(data, dict) else data


def assign_zone(lat: float, lon: float, zones: list[dict] | None = None) -> str | None:
    """Assign zone id to (lat, lon). First matching bbox wins. None if no match."""
    if zones is None:
        zones = load_zone_definitions()
    for z in zones:
        if (
            z["lat_min"] <= lat <= z["lat_max"]
            and z["lon_min"] <= lon <= z["lon_max"]
        ):
            return z.get("id", z.get("name"))
    return None


def load_location_to_zone(
    from_s3: bool = False,
    local_path: str | Path | None = None,
    s3_key: str | None = None,
) -> pl.DataFrame | None:
    """
    Load lookup table location_id -> zone (columns: location, zone).
    Source: local Parquet/CSV or S3.
    """
    if local_path is not None:
        path = Path(local_path)
        if not path.is_absolute():
            path = REFERENCE_DIR / path
        if path.suffix == ".csv":
            df = pl.read_csv(path)
        else:
            df = pl.read_parquet(path)
        # Normalize column name to 'location' for join with vegvesen
        if "location_id" in df.columns and "location" not in df.columns:
            df = df.rename({"location_id": "location"})
        return df.select(["location", "zone"])

    if from_s3 and s3_key:
        try:
            from utils.s3_upload import read_parquet_from_s3

            df = read_parquet_from_s3(s3_key)
            if df is None:
                return None
            if "location_id" in df.columns and "location" not in df.columns:
                df = df.rename({"location_id": "location"})
            return df.select(["location", "zone"])
        except Exception:
            return None

    # Default: try local reference file
    default_local = REFERENCE_DIR / "vegvesen_location_to_zone.parquet"
    if default_local.exists():
        return load_location_to_zone(local_path=default_local)
    return None
