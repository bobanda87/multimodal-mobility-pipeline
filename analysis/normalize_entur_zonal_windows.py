"""
Druga normalizacija Entur: jedan red po stajalištu u SIRI ET / trip-updates snimku, sa stop_id,
kašnjenjem, zonama (join na GTFS lookup) i study_window (WA1, WA2, WB, WC).

Upis na S3 (jedan Parquet po raw fajlu — izbegava OOM od dnevnog concat miliona redova):
  {S3_PARQUET_PATH}/normalized/{YYYY-MM-DD}/entur_zones__{raw_basename_bez_parquet}.parquet

Pretpostavka: analysis/reference/entur_stop_zone_lookup.parquet (pokreni build_entur_stop_zone_lookup.py).

  PYTHONPATH=. .venv/bin/python analysis/normalize_entur_zonal_windows.py --dry-run
  PYTHONPATH=. .venv/bin/python analysis/normalize_entur_zonal_windows.py
  PYTHONPATH=. .venv/bin/python analysis/normalize_entur_zonal_windows.py --skip-existing
  PYTHONPATH=. .venv/bin/python analysis/normalize_entur_zonal_windows.py --max-files 3
"""
from __future__ import annotations

import gc
import os
import re
import sys
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import polars as pl  # noqa: E402

from analysis.entur_stop_zonal_lib import (  # noqa: E402
    add_study_window_column,
    all_study_dates,
    enrich_with_trip_timestamps,
    explode_entur_to_stop_rows,
    join_stops_with_zone,
    load_stop_zone_lookup,
    raw_entur_keys_for_date,
)


def _safe_parquet_stem(s3_key: str) -> str:
    base = os.path.splitext(os.path.basename(s3_key))[0]
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", base)[:200]


def _s3_entur_zones_output_exists(rel_key: str) -> bool:
    from botocore.exceptions import ClientError

    from utils.s3_upload import S3_BUCKET_NAME, S3_PARQUET_PATH, s3_client

    if not s3_client:
        return False
    full = f"{S3_PARQUET_PATH.rstrip('/')}/{rel_key}".replace("//", "/")
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=full)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        return False
    except Exception:
        return False


def main():
    import argparse

    p = argparse.ArgumentParser(
        description="Normalize Entur to stop-level + zone for study windows WA1/WA2/WB/WC."
    )
    p.add_argument(
        "--lookup",
        type=str,
        default=None,
        help="Path to entur_stop_zone_lookup.parquet",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Process only first available raw file and print schema + row count.",
    )
    p.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Limit raw parquet files per calendar day (debug).",
    )
    p.add_argument(
        "--no-upload",
        action="store_true",
        help="Only aggregate in memory / print; do not upload to S3.",
    )
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help="Preskoči raw fajl ako već postoji odgovarajući entur_zones__*.parquet na S3 (nastavak posle kill/OOM).",
    )
    p.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Opcionalno: početni datum (YYYY-MM-DD) za sužavanje na pod-opseg study windows.",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Opcionalno: krajnji datum (YYYY-MM-DD) za sužavanje na pod-opseg study windows.",
    )
    args = p.parse_args()

    lookup = load_stop_zone_lookup(args.lookup)
    if lookup is None or lookup.height == 0:
        print(
            "Nema lookup tabele. Pokreni:\n"
            "  PYTHONPATH=. .venv/bin/python analysis/build_entur_stop_zone_lookup.py"
        )
        sys.exit(1)
    print(f"Loaded stop→zone lookup: {lookup.height} join keys")

    from utils.s3_upload import read_parquet_from_s3, save_parquet_to_s3

    dates = all_study_dates()
    if args.start_date:
        dates = [d for d in dates if d >= args.start_date]
    if args.end_date:
        dates = [d for d in dates if d <= args.end_date]
    if not dates:
        print("Nema datuma za obradu nakon --start-date/--end-date filtera.")
        return
    print(f"Calendar dates to process: {len(dates)} ({dates[0]} … {dates[-1]})")

    for date_str in dates:
        keys = raw_entur_keys_for_date(date_str)
        if args.max_files is not None:
            keys = keys[: args.max_files]
        if not keys:
            print(f"  {date_str}: no raw entur files")
            continue
        cal_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        day_written = 0
        day_skipped = 0
        for s3_key in keys:
            stem = _safe_parquet_stem(s3_key)
            rel_key = f"normalized/{date_str}/entur_zones__{stem}.parquet"
            if args.skip_existing and _s3_entur_zones_output_exists(rel_key):
                day_skipped += 1
                continue

            raw = read_parquet_from_s3(s3_key)
            if raw is None or raw.height == 0:
                continue
            stops = explode_entur_to_stop_rows(raw)
            if stops.height == 0:
                del raw
                gc.collect()
                continue
            stops = join_stops_with_zone(stops, lookup)
            stops = enrich_with_trip_timestamps(stops, raw, date_str, s3_key)
            del raw
            gc.collect()
            stops = add_study_window_column(stops, cal_date)
            stops = stops.with_columns(
                [
                    pl.lit(s3_key).alias("source_s3_key"),
                    pl.lit(date_str).alias("raw_folder_date"),
                ]
            )
            if args.dry_run:
                nn_zone = stops.filter(pl.col("zone").is_not_null()).height
                nn_geo = stops.filter(pl.col("stop_lat").is_not_null()).height
                print(f"Dry-run sample from {os.path.basename(s3_key)}:")
                print(stops.schema)
                print(
                    f"Rows: {stops.height}; GTFS match (stop_lat): {nn_geo}; "
                    f"zone non-null (unutar oslo_zones.json): {nn_zone}"
                )
                if nn_geo > 0 and nn_zone == 0:
                    print(
                        "  (Nula zona obično znači operator/linije van Oslo bbox-a u configu; "
                        "probaj fajl RUT_* za isti datum.)"
                    )
                return

            if args.no_upload:
                day_written += 1
                del stops
                gc.collect()
                continue

            print(f"  {date_str}: {stops.height:,} rows -> {rel_key}")
            ok = save_parquet_to_s3(stops.to_pandas(), rel_key)
            del stops
            gc.collect()
            if not ok:
                print("    S3 upload failed (credentials?)")
                sys.exit(1)
            day_written += 1

        if day_written == 0 and day_skipped == 0:
            print(f"  {date_str}: no stop-level rows")
        else:
            msg = f"  {date_str}: wrote {day_written} parquet file(s)"
            if day_skipped:
                msg += f", skipped {day_skipped} existing (--skip-existing)"
            print(msg)

    print("Done.")


if __name__ == "__main__":
    main()
