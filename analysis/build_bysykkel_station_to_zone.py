"""
Jednokratno: station_information (S3 ili lokalni Parquet) → analysis/reference/oslobysykkel_station_to_zone.parquet

  .venv/bin/python analysis/build_bysykkel_station_to_zone.py
  .venv/bin/python analysis/build_bysykkel_station_to_zone.py --local path/to/station_information_....parquet
"""
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl  # noqa: E402

from analysis.bysykkel_zones import station_information_to_location_zone  # noqa: E402


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build Bysykkel station_id → Oslo zone lookup.")
    parser.add_argument(
        "--local",
        type=str,
        default=None,
        help="Lokalni Parquet station_information (inače poslednji fajl sa S3).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Izlaz (default: analysis/reference/oslobysykkel_station_to_zone.parquet).",
    )
    args = parser.parse_args()

    out = Path(args.output) if args.output else SCRIPT_DIR / "reference" / "oslobysykkel_station_to_zone.parquet"
    if not out.is_absolute():
        out = SCRIPT_DIR / "reference" / out.name

    if args.local:
        p = Path(args.local)
        if not p.is_absolute():
            p = PROJECT_ROOT / p
        info_df = pl.read_parquet(p)
    else:
        from utils.s3_upload import list_parquet_files_from_s3, read_parquet_from_s3

        keys = list_parquet_files_from_s3(
            "oslobysykkel", "*station_information*.parquet", with_metadata=False
        )
        if not keys:
            print("No station_information parquet on S3. Use --local.")
            sys.exit(1)
        key = keys[-1]
        print(f"Reading {key}")
        info_df = read_parquet_from_s3(key)
        if info_df is None:
            print("Failed to read parquet.")
            sys.exit(1)

    lut = station_information_to_location_zone(info_df)
    if lut.height == 0:
        print("No stations with lat/lon inside configured zones.")
        sys.exit(1)
    out.parent.mkdir(parents=True, exist_ok=True)
    lut.write_parquet(out)
    print(f"Wrote {lut.height} rows to {out}")
    print(lut.group_by("zone").len().sort("zone"))


if __name__ == "__main__":
    main()
