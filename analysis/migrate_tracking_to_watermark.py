"""
One-off migration from normalization_tracking.json (list of every normalized raw key) to one
watermark per source (see utils/normalization_state.py).

For each source the watermark becomes the LastModified of the newest raw file the old tracking file
lists as normalized. Raw files older than that which the tracking file does NOT list ("holes", e.g.
files that failed to read) would be skipped from now on, so they are reported; normalize them with a
--start-date/--end-date backfill first if they matter.

Read-only unless --write is given (then it only creates normalization_watermark.json).

  PYTHONPATH=. .venv/bin/python analysis/migrate_tracking_to_watermark.py
  PYTHONPATH=. .venv/bin/python analysis/migrate_tracking_to_watermark.py --write
"""
import argparse
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from analysis.normalize_data import RAW_SPECS, list_raw_files  # noqa: E402
from utils.normalization_state import WATERMARK_KEY, Watermarks  # noqa: E402
from utils.s3_upload import get_normalization_tracking_key, read_json_from_s3  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="Write the watermark file to S3")
    args = parser.parse_args()

    tracking = read_json_from_s3(get_normalization_tracking_key())
    if not isinstance(tracking, dict):
        sys.exit("No normalization tracking file found")

    watermarks = Watermarks()
    print(f"{'source':30} {'tracked':>8} {'raw':>8} {'holes':>6} {'new':>6}  watermark")
    for source in RAW_SPECS:
        tracked = set(tracking.get(source, []))
        raw = list_raw_files(source)
        done = [f for f in raw if f["key"] in tracked]
        if not done:
            print(f"{source:30} {len(tracked):8d} {len(raw):8d} {'-':>6} {len(raw):6d}  (none, everything is new)")
            continue
        newest = max(f["last_modified"] for f in done)
        boundary = [f["key"] for f in done if f["last_modified"] == newest]
        holes = sum(1 for f in raw if f["key"] not in tracked and f["last_modified"] < newest)
        new = sum(1 for f in raw if f["last_modified"] > newest)
        print(f"{source:30} {len(tracked):8d} {len(raw):8d} {holes:6d} {new:6d}  {newest.isoformat()}")
        watermarks.set(source, newest, boundary)

    if args.write:
        print(f"\nWriting s3://.../{WATERMARK_KEY}" if watermarks.save() else "\nWrite failed")
    else:
        print("\nDry run, nothing written. Use --write to create the watermark file.")


if __name__ == "__main__":
    main()
