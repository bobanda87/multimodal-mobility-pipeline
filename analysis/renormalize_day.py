"""
Rebuild the normalized files of one source and one raw (local) date from the raw files, e.g. after the normalization
gained a column. The rebuilt data replaces the old files in the same layout the hourly compaction produces:

  * closed hours   one hourly file `<source>_normalized_<UTC date>_<HH>0000c.parquet`
  * the open hour  one file per raw file, left for the hourly compaction to merge

Only raw files up to the source's watermark are rebuilt (later ones are normalized by the normal run, with the new
code). The normalize lock is held meanwhile, so the scheduled normalization skips instead of writing duplicates.
Old files are deleted only after the new ones are written and read back with at least the old rows.

    PYTHONPATH=. python -m analysis.renormalize_day --source vegvesen_travel_times --date 2026-11-15          # dry run
    PYTHONPATH=. python -m analysis.renormalize_day --source vegvesen_travel_times --date 2026-11-15 --apply

Run it away from minute 8-16 of an hour, when the hourly compaction works on the same files.
"""
import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from analysis import normalize_data as nd
from analysis.compact_normalized import align
from normalizer.handler import acquire_lock, release_lock
from utils import s3_upload as u
from utils.normalization_state import Watermarks

_STAMP = re.compile(r"_(\d{6})\.parquet$")


def _full(key: str) -> str:
    return f"{u.S3_PARQUET_PATH.rstrip('/')}/{key}".replace("//", "/")


def _old_keys(source: str, day: str) -> list[str]:
    prefix = _full(f"normalized/{day}/{source}_normalized_")
    keys = []
    for page in u.s3_client.get_paginator("list_objects_v2").paginate(Bucket=u.S3_BUCKET_NAME, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def run(source: str, day: str, apply: bool, now: datetime | None = None) -> None:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if apply and not acquire_lock("normalize", 900):
        raise SystemExit("the normalization is running right now; try again in a minute")
    try:
        watermark = Watermarks().last_modified(source)
        files = [f for f in nd.list_raw_files(source, day) if watermark is None or f["last_modified"] <= watermark]
        print(f"{source} {day}: {len(files)} raw files up to the watermark {watermark}")
        hours = defaultdict(list)
        for f in files:
            hours[f["last_modified"].astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)].append(f)
        open_from = now - timedelta(minutes=10)
        new_keys, rows_new = [], 0
        for hour, group in sorted(hours.items()):
            frames = []
            for f in group:
                df = u.read_parquet_from_s3(f["key"])
                if df is None:
                    raise SystemExit(f"cannot read {f['key']}")
                n = nd.NORMALIZERS[source](df)
                if n.height:
                    frames.append((f, n))
            if not frames:
                continue
            if hour + timedelta(hours=1) <= open_from:
                merged = align([n for _, n in frames]).unique(maintain_order=True)
                outputs = [(f"normalized/{day}/{source}_normalized_{hour:%Y-%m-%d_%H}0000c.parquet", merged)]
            else:
                outputs = [(f"normalized/{day}/{source}_normalized_{day}_{_STAMP.search(f['key']).group(1)}.parquet", n)
                           for f, n in frames]
            for key, df in outputs:
                rows_new += df.height
                new_keys.append(_full(key))
                print(f"  {hour:%Y-%m-%d %H}Z -> {key.split('/')[-1]} ({df.height} rows)")
                if apply and not u.save_parquet_to_s3(df.to_pandas(), key):
                    raise SystemExit(f"upload failed: {key}; nothing was deleted")
        if not apply:
            print(f"[dry run] would write {len(new_keys)} files, {rows_new} rows")
            return
        for k in new_keys:
            if u.read_parquet_from_s3(k) is None:
                raise SystemExit(f"cannot read back {k}; nothing was deleted")
        stale = [k for k in _old_keys(source, day) if k not in set(new_keys)]
        for i in range(0, len(stale), 1000):
            u.s3_client.delete_objects(Bucket=u.S3_BUCKET_NAME,
                                       Delete={"Objects": [{"Key": k} for k in stale[i:i + 1000]]})
        print(f"wrote {len(new_keys)} files ({rows_new} rows), deleted {len(stale)} old files")
    finally:
        if apply:
            release_lock("normalize")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True, choices=sorted(nd.RAW_SPECS))
    ap.add_argument("--date", required=True, help="raw (local) date folder, YYYY-MM-DD")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    run(args.source, args.date, args.apply)


if __name__ == "__main__":
    main()
