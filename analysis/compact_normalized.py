"""
Compact the normalized/ files: many small per-run files into few larger ones.

The normalizer writes a new small file per run (every minute) into the day's folder. Two modes merge
them, dropping exact duplicate rows only (successive states of a flight or trip are kept), writing the
merged file, checking its row count, and only then deleting the inputs:

* daily (default): one file per source and date folder, for days at least --min-age-days old, so a
  folder the normalizer is still writing to is never touched.
* --hourly: one file per source, folder and closed UTC hour (by the time the file was written to S3),
  named <source>_normalized_<YYYY-MM-DD>_<HH>0000c.parquet. Hours that ended less than --grace-minutes
  ago are left alone. The name is derived from the hour, so repeating a pass is harmless, and these
  files are not merged again by later hourly passes (the daily pass folds them into the day's file).

A group with a single file is left alone, which makes both modes idempotent.

  PYTHONPATH=. .venv/bin/python analysis/compact_normalized.py --hourly --dry-run
  PYTHONPATH=. .venv/bin/python analysis/compact_normalized.py --start-date 2026-03-01 --end-date 2026-03-31
"""
import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

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
# group 4 is "c" for a file written by the hourly compaction
_FILE = re.compile(r"normalized/(\d{4}-\d{2}-\d{2})/(.+?)_normalized_(\d{4}-\d{2}-\d{2}_\d{6})(c?)\.parquet$")


def dedupe(source: str, df: pl.DataFrame) -> pl.DataFrame:
    """
    Drop exact duplicate rows, nothing else.

    Re-running a day, or polling a source faster than it refreshes, writes the same row again. Rows that
    differ are kept even when they describe the same flight or trip at different times: the successive
    states (a delay 30 and 10 minutes before arrival) are what a near real-time model learns from, and
    collapsing them to the latest state belongs to the query, not to the stored data.
    """
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


def _exists(full_key: str) -> bool:
    try:
        s3_upload.s3_client.head_object(Bucket=s3_upload.S3_BUCKET_NAME, Key=full_key)
        return True
    except Exception:
        return False


def compact(source: str, day: str, keys: list[str], dry_run: bool, out_stamp: str | None = None,
            out_key: str | None = None) -> str:
    out_stamp = out_stamp or datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_key = out_key or f"normalized/{day}/{source}_normalized_{out_stamp}.parquet"
    full_key = f"{s3_upload.S3_PARQUET_PATH.rstrip('/')}/{out_key}".replace("//", "/")
    # An output left by an interrupted pass of the same hour is merged in again, and overwritten
    read_keys = keys + [full_key] if out_key.endswith("c.parquet") and _exists(full_key) else keys
    frames = [s3_upload.read_parquet_from_s3(k) for k in read_keys]
    if any(f is None for f in frames):
        return "skipped: a file could not be read"
    rows_in = sum(f.height for f in frames)
    merged = dedupe(source, align(frames))
    if dry_run:
        return f"[dry-run] {len(keys)} files, {rows_in} -> {merged.height} rows"

    s3_upload.save_parquet_to_s3(merged.to_pandas(), out_key)
    written = s3_upload.read_parquet_from_s3(full_key)
    if written is None or written.height != merged.height:
        return f"kept inputs: verification of {out_key} failed"
    s3_upload.s3_client.delete_objects(
        Bucket=s3_upload.S3_BUCKET_NAME,
        Delete={"Objects": [{"Key": k} for k in keys if k != full_key]},
    )
    return f"{len(keys)} files, {rows_in} -> {merged.height} rows"


def list_hourly_groups(now: datetime, lookback_hours: int, grace_minutes: int):
    """
    {(source, folder date, UTC hour): [keys oldest first]} for the closed hours of the last
    lookback_hours. Only per-run files count (not earlier hourly outputs), grouped by the time they
    were written to S3. Folder dates are local dates while hours are UTC, so a few extra days are listed.
    """
    now = now.astimezone(timezone.utc)
    open_from = now - timedelta(minutes=grace_minutes)
    earliest = (now - timedelta(hours=lookback_hours)).replace(minute=0, second=0, microsecond=0)
    groups = defaultdict(list)
    paginator = s3_upload.s3_client.get_paginator("list_objects_v2")
    base = s3_upload.S3_PARQUET_PATH.rstrip("/")
    first = earliest.date() - timedelta(days=1)
    for i in range((now.date() - first).days + 2):
        day = (first + timedelta(days=i)).isoformat()
        for page in paginator.paginate(Bucket=s3_upload.S3_BUCKET_NAME, Prefix=f"{base}/normalized/{day}/"):
            for obj in page.get("Contents", []):
                m = _FILE.search(obj["Key"])
                if not m or m.group(2) not in SOURCES or m.group(4):
                    continue
                hour = obj["LastModified"].astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
                if hour < earliest or hour + timedelta(hours=1) > open_from:
                    continue
                groups[(m.group(2), m.group(1), hour)].append((m.group(3), obj["Key"]))
    return {k: [key for _, key in sorted(v)] for k, v in groups.items()}


def run_hourly(dry_run: bool = False, lookback_hours: int = 26, grace_minutes: int = 10,
               now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    groups = list_hourly_groups(now, lookback_hours, grace_minutes)
    print(f"Compacting closed hours of the last {lookback_hours} h{' (dry run)' if dry_run else ''}")
    for (source, day, hour), keys in sorted(groups.items(), key=lambda x: (x[0][2], x[0][0], x[0][1])):
        if len(keys) < 2:
            continue
        print(f"{hour:%Y-%m-%d %H}:00Z {source:30} {compact(source, day, keys, dry_run, f'{hour:%Y-%m-%d_%H}0000c')}", flush=True)


# entur_siri_national: raw folder with one `journeys_` and one `calls_` file per collector pass
_SIRI_FILE = re.compile(r"entur_siri_national/(\d{4}-\d{2}-\d{2})/(journeys|calls)_(\d{6})(c?)\.parquet$")


def list_siri_hourly_groups(now: datetime, lookback_hours: int, grace_minutes: int):
    """{(kind, folder date, UTC hour): [keys oldest first]} for the closed hours of the last lookback_hours."""
    now = now.astimezone(timezone.utc)
    open_from = now - timedelta(minutes=grace_minutes)
    earliest = (now - timedelta(hours=lookback_hours)).replace(minute=0, second=0, microsecond=0)
    groups = defaultdict(list)
    paginator = s3_upload.s3_client.get_paginator("list_objects_v2")
    base = s3_upload.S3_PARQUET_PATH.rstrip("/")
    first = earliest.date() - timedelta(days=1)
    for i in range((now.date() - first).days + 2):
        day = (first + timedelta(days=i)).isoformat()
        for page in paginator.paginate(Bucket=s3_upload.S3_BUCKET_NAME, Prefix=f"{base}/entur_siri_national/{day}/"):
            for obj in page.get("Contents", []):
                m = _SIRI_FILE.search(obj["Key"])
                if not m or m.group(4):
                    continue
                hour = obj["LastModified"].astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
                if hour < earliest or hour + timedelta(hours=1) > open_from:
                    continue
                groups[(m.group(2), m.group(1), hour)].append((m.group(3), obj["Key"]))
    return {k: [key for _, key in sorted(v)] for k, v in groups.items()}


def run_siri_hourly(dry_run: bool = False, lookback_hours: int = 26, grace_minutes: int = 10,
                    now: datetime | None = None) -> None:
    """Merge the per-pass files of each closed UTC hour: <kind>_<HH>0000c.parquet (HH = the UTC hour)."""
    now = now or datetime.now(timezone.utc)
    groups = list_siri_hourly_groups(now, lookback_hours, grace_minutes)
    print(f"Compacting entur_siri_national, closed hours of the last {lookback_hours} h{' (dry run)' if dry_run else ''}")
    for (kind, day, hour), keys in sorted(groups.items(), key=lambda x: (x[0][2], x[0][0], x[0][1])):
        if len(keys) < 2:
            continue
        out_key = f"entur_siri_national/{day}/{kind}_{hour:%H}0000c.parquet"
        print(f"{hour:%Y-%m-%d %H}:00Z siri_{kind:9} {compact(f'siri_{kind}', day, keys, dry_run, out_key=out_key)}", flush=True)


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
    parser.add_argument("--hourly", action="store_true", help="Merge the per-run files of closed hours instead of closed days")
    parser.add_argument("--lookback-hours", type=int, default=26, help="With --hourly, how far back to look")
    parser.add_argument("--grace-minutes", type=int, default=10, help="With --hourly, leave hours that ended more recently alone")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.hourly:
        run_hourly(args.dry_run, args.lookback_hours, args.grace_minutes)
        run_siri_hourly(args.dry_run, args.lookback_hours, args.grace_minutes)
    else:
        run(args.start_date, args.end_date, args.min_age_days, args.dry_run, args.lookback_days)
