"""
Gaps in the collected data, from the time every file was written to S3. For the data-quality section of a
paper: how complete is each source over a period, and where are the holes.

  PYTHONPATH=. .venv/bin/python -m monitor.gap_report --start 2026-11-14 --end 2026-11-30
  PYTHONPATH=. .venv/bin/python -m monitor.gap_report --days 7 --list

A gap is a spacing between two consecutive files larger than --factor times the collector's cadence
(default 2.5, plus 30 s). LastModified is when the file reached S3, a few seconds after it was fetched.
The first and last file bound the observed period, so a collector that never started is not reported.
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3

from . import checks

# Only sources that write one file per cadence; the normalized layer is derived and is checked live instead
RAW = [s for s in checks.SOURCES if not s.normalized]


def file_times(s3, bucket: str, parquet_path: str, source: checks.Source, first_day, last_day) -> list[datetime]:
    times = []
    day = first_day
    while day <= last_day:
        prefix = f"{parquet_path}/{source.folder}/{day.isoformat()}/{source.name_prefix}"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            times += [o["LastModified"].astimezone(timezone.utc) for o in page.get("Contents", [])
                      if checks.is_per_run_file(source, o["Key"])]
        day += timedelta(days=1)
    return sorted(times)


def report(times: list[datetime], source: checks.Source, factor: float, start=None, end=None) -> dict:
    if start:
        times = [t for t in times if t >= start]
    if end:
        times = [t for t in times if t < end]
    if len(times) < 2:
        return {"source": source.name, "files": len(times), "span_s": 0, "expected": 0, "coverage": None, "gaps": []}
    threshold = factor * source.cadence_s + 30
    found = checks.gaps(times, threshold)
    span = (times[-1] - times[0]).total_seconds()
    missing = sum(g[2] - source.cadence_s for g in found)
    return {"source": source.name, "files": len(times), "span_s": span, "expected": round(span / source.cadence_s) + 1,
            "coverage": 1 - missing / span if span else None, "gaps": found, "gap_total_s": sum(g[2] for g in found),
            "longest_s": max((g[2] for g in found), default=0)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", help="first local date folder, YYYY-MM-DD")
    ap.add_argument("--end", help="last local date folder, YYYY-MM-DD (default today)")
    ap.add_argument("--days", type=int, help="instead of --start: the last N days")
    ap.add_argument("--since", help="ignore files written before this UTC time, e.g. 2026-11-14T19:00 (the start of the measurement window)")
    ap.add_argument("--factor", type=float, default=2.5)
    ap.add_argument("--list", action="store_true", help="list every gap")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    today = datetime.now(checks.OSLO).date()
    last = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else today
    first = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else last - timedelta(days=(args.days or 7) - 1)
    bucket = os.environ["S3_BUCKET_NAME"]; path = os.getenv("S3_PARQUET_PATH", "data/parquet").strip("/")
    s3 = boto3.client("s3")
    print(f"Data completeness, folders {first} .. {last}, gap = more than {args.factor} x cadence + 30 s between files\n")
    print(f"{'source':26} {'files':>7} {'expected':>8} {'coverage':>9} {'gaps':>5} {'gap total':>10} {'longest':>9}")
    detail = []
    for source in RAW:
        r = report(file_times(s3, bucket, path, source, first, last), source, args.factor,
                   start=datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc) if args.since else None)
        cov = f"{100 * r['coverage']:.2f}%" if r["coverage"] is not None else "-"
        print(f"{r['source']:26} {r['files']:7d} {r['expected']:8d} {cov:>9} {len(r['gaps']):5d} "
              f"{r.get('gap_total_s', 0) / 60:8.1f}m {r.get('longest_s', 0) / 60:7.1f}m")
        detail += [(r["source"], *g) for g in r["gaps"]]
    if args.list:
        print("\nGaps (UTC):")
        for name, a, b, s in sorted(detail, key=lambda x: x[1]):
            print(f"  {name:26} {a:%Y-%m-%d %H:%M:%S} -> {b:%Y-%m-%d %H:%M:%S}  {s / 60:7.1f} min")


if __name__ == "__main__":
    main()
