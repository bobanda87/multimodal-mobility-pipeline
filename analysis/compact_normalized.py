"""
Compact the normalized/ files of closed days: one Parquet per source and date folder.

Hourly normalization writes a new small file per run into the day's folder. This merges the files of
a (source, date) into one, applies the same duplicate rules the normalizer uses within a run (a
flight once per uniqueID with its latest status; a trip once per route and departure, latest
observation), writes the merged file, checks its row count, and only then deletes the inputs.

By default only days at least --min-age-days old are compacted, so a folder that the hourly run is
still writing to (late raw files around midnight) is never touched. A folder with a single file is
left alone, which makes the job idempotent.

  PYTHONPATH=. .venv/bin/python analysis/compact_normalized.py --dry-run
  PYTHONPATH=. .venv/bin/python analysis/compact_normalized.py --start-date 2026-03-01 --end-date 2026-03-31
"""
import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import polars as pl  # noqa: E402

from utils import s3_upload  # noqa: E402

# Sources compacted here; entur_zones* are a separate, per-raw-file layout and are left alone.
SOURCES = [
    "avinor",
    "oslobysykkel",
    "vegvesen",
    "vegvesen_travel_times",
    "vegvesen_traffic_situations",
    "entur_siri_et",
]
_FILE = re.compile(r"normalized/(\d{4}-\d{2}-\d{2})/(.+?)_normalized_(\d{4}-\d{2}-\d{2}_\d{6})\.parquet$")


def dedupe(source: str, df: pl.DataFrame) -> pl.DataFrame:
    """Duplicate rules of the normalizer, applied across the files of a day."""
    if source == "avinor" and {"uniqueID", "status_time"} <= set(df.columns):
        return df.sort("status_time", nulls_last=False).unique(subset=["uniqueID"], keep="last", maintain_order=True)
    if source == "entur_siri_et" and {"trip_id", "route_id", "departure_timestamp", "timestamp"} <= set(df.columns):
        return df.sort("timestamp").unique(
            subset=["trip_id", "route_id", "departure_timestamp"], keep="last", maintain_order=True)
    # No key rule for this source: drop exact duplicate rows (what re-running a day writes again)
    try:
        return df.unique(maintain_order=True)
    except pl.exceptions.PolarsError:  # columns with nested types cannot be compared
        return df


def align(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Concatenate frames cast to the schema of the first one; missing columns become null."""
    schema = frames[0].schema
    aligned = [
        f.select([
            pl.col(c).cast(schema[c]) if c in f.columns else pl.lit(None).cast(schema[c]).alias(c)
            for c in schema
        ])
        for f in frames
    ]
    return pl.concat(aligned)


def list_groups(start: str, end: str) -> dict[tuple[str, str], list[str]]:
    """{(source, date): [keys oldest first]} for the date folders in [start, end]."""
    groups = defaultdict(list)
    first = datetime.strptime(start, "%Y-%m-%d").date()
    last = datetime.strptime(end, "%Y-%m-%d").date()
    paginator = s3_upload.s3_client.get_paginator("list_objects_v2")
    base = s3_upload.S3_PARQUET_PATH.rstrip("/")
    for i in range((last - first).days + 1):
        day = (first + timedelta(days=i)).isoformat()
        for page in paginator.paginate(Bucket=s3_upload.S3_BUCKET_NAME, Prefix=f"{base}/normalized/{day}/"):
            for obj in page.get("Contents", []):
                m = _FILE.search(obj["Key"])
                if m and m.group(2) in SOURCES:
                    groups[(m.group(2), m.group(1))].append((m.group(3), obj["Key"]))
    return {k: [key for _, key in sorted(v)] for k, v in groups.items()}


def compact(source: str, day: str, keys: list[str], dry_run: bool) -> str:
    frames = [s3_upload.read_parquet_from_s3(k) for k in keys]
    if any(f is None for f in frames):
        return "skipped: a file could not be read"
    rows_in = sum(f.height for f in frames)
    merged = dedupe(source, align(frames))
    out_key = f"normalized/{day}/{source}_normalized_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.parquet"
    if dry_run:
        return f"[dry-run] {len(keys)} files, {rows_in} -> {merged.height} rows"

    s3_upload.save_parquet_to_s3(merged.to_pandas(), out_key)
    full_key = f"{s3_upload.S3_PARQUET_PATH.rstrip('/')}/{out_key}".replace("//", "/")
    written = s3_upload.read_parquet_from_s3(full_key)
    if written is None or written.height != merged.height:
        return f"kept inputs: verification of {out_key} failed"
    s3_upload.s3_client.delete_objects(
        Bucket=s3_upload.S3_BUCKET_NAME,
        Delete={"Objects": [{"Key": k} for k in keys]},
    )
    return f"{len(keys)} files, {rows_in} -> {merged.height} rows"


def run(start_date: str | None = None, end_date: str | None = None, min_age_days: int = 2,
        dry_run: bool = False, lookback_days: int = 14) -> None:
    today = datetime.now().date()
    end = end_date or (today - timedelta(days=min_age_days)).isoformat()
    start = start_date or (datetime.strptime(end, "%Y-%m-%d").date() - timedelta(days=lookback_days)).isoformat()
    print(f"Compacting normalized/ {start}..{end}{' (dry run)' if dry_run else ''}")
    for (source, day), keys in sorted(list_groups(start, end).items(), key=lambda x: (x[0][1], x[0][0])):
        if len(keys) < 2:
            continue
        print(f"{day} {source:30} {compact(source, day, keys, dry_run)}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", metavar="YYYY-MM-DD")
    parser.add_argument("--end-date", metavar="YYYY-MM-DD")
    parser.add_argument("--min-age-days", type=int, default=2,
                        help="Without --end-date, compact folders up to today minus this many days")
    parser.add_argument("--lookback-days", type=int, default=14,
                        help="Without --start-date, go back this far from the end date")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.start_date, args.end_date, args.min_age_days, args.dry_run, args.lookback_days)
