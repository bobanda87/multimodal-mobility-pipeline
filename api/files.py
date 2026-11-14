"""Locating Parquet files on S3."""
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3

from . import config

# A normalized file can start up to a few days before the date folder it is stored in
# (e.g. folder 2026-03-10 holds Avinor data from 2026-03-09), so look back this many days.
FOLDER_LOOKBACK_DAYS = 3

_s3 = boto3.client("s3", region_name=config.REGION)

# normalized/<day>/<source>_normalized_<YYYY-MM-DD>_<HHMMSS>[c].parquet; "c" marks an hourly-compacted file
_NORMALIZED_FILE = re.compile(r"_normalized_(\d{4}-\d{2}-\d{2})_(\d{6})(c?)\.parquet$")
# entur_siri_national/<day>/<journeys|calls>_<HHMMSS>[c].parquet
_SIRI_FILE = re.compile(r"entur_siri_national/(\d{4}-\d{2}-\d{2})/(journeys|calls)_(\d{6})(c?)\.parquet$")


def _list(prefix: str) -> list[tuple[str, datetime]]:
    """(key, LastModified UTC) of the Parquet objects under a prefix."""
    found = []
    for page in _s3.get_paginator("list_objects_v2").paginate(Bucket=config.BUCKET, Prefix=prefix):
        found += [(o["Key"], o["LastModified"].astimezone(timezone.utc))
                  for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    return found


def _hour(moment: datetime) -> datetime:
    return moment.replace(minute=0, second=0, microsecond=0)


def _without_covered(items, is_compacted, hour_of_compacted, hour_of_item):
    """
    Drop the per-run files whose hour already has a compacted file.

    The compaction writes the merged file first and deletes its inputs afterwards, so for a moment both
    exist; reading both would count every row twice. The compacted file wins.
    """
    covered = {hour_of_compacted(i) for i in items if is_compacted(i)}
    return [i for i in items if is_compacted(i) or hour_of_item(i) not in covered]


def normalized_files(source: str, start: datetime, end: datetime) -> list[str]:
    """
    S3 URIs of the normalized files of one source that can hold data in [start, end).

    Lists only the date folders in range (not the whole normalized/ prefix, which also holds tens
    of thousands of per-zone Entur files) and skips days without files, since collection has gaps.
    """
    first = (start - timedelta(days=FOLDER_LOOKBACK_DAYS)).date()
    days = [first + timedelta(days=i) for i in range((end.date() - first).days + 1)]

    def one_folder(day):
        items = _list(f"{config.PARQUET_PATH}/normalized/{day}/{source}_normalized_")
        parsed = []
        for key, modified in items:
            m = _NORMALIZED_FILE.search(key)
            if m:
                compacted_hour = datetime.strptime(f"{m.group(1)} {m.group(2)[:2]}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
                parsed.append((key, modified, bool(m.group(3)), compacted_hour))
        kept = _without_covered(parsed, lambda i: i[2], lambda i: i[3], lambda i: _hour(i[1]))
        return [i[0] for i in kept]

    with ThreadPoolExecutor(max_workers=8) as pool:
        listings = list(pool.map(one_folder, days))
    return [f"s3://{config.BUCKET}/{key}" for keys in listings for key in keys]


# ---- entur_siri_national: raw folder, one pass per file, named after the fetch time (Europe/Oslo) ----

_OSLO = ZoneInfo("Europe/Oslo")
_SLACK = timedelta(minutes=2)  # a file is written a few seconds after the pass it holds was fetched


def _siri_pass_time(day: str, hhmmss: str) -> datetime:
    return datetime.strptime(f"{day} {hhmmss}", "%Y-%m-%d %H%M%S").replace(tzinfo=_OSLO).astimezone(timezone.utc)


def _siri_hour_start(day: str, hhmmss: str) -> datetime:
    """UTC hour of an hourly-compacted file (name <kind>_<HH>0000c): the one that falls on the folder's local date."""
    hour = int(hhmmss[:2])
    folder = datetime.strptime(day, "%Y-%m-%d").date()
    for date in (folder - timedelta(days=1), folder):
        candidate = datetime(date.year, date.month, date.day, hour, tzinfo=timezone.utc)
        if candidate.astimezone(_OSLO).strftime("%Y-%m-%d") == day:
            return candidate
    return datetime.strptime(day, "%Y-%m-%d").replace(hour=hour, tzinfo=timezone.utc)


def siri_national_files(kind: str, start: datetime, end: datetime) -> list[str]:
    """
    S3 URIs of the entur_siri_national `journeys` or `calls` files holding passes fetched in [start, end).

    Per-pass files are chosen by the fetch time in their name, hourly-compacted ones by the hour in theirs,
    so a query over the last hour opens about 60 files, not a day's worth. A per-pass file whose hour has a
    compacted file is skipped (see _without_covered).
    """
    first = start.astimezone(_OSLO).date() - timedelta(days=1)
    last = end.astimezone(_OSLO).date() + timedelta(days=1)
    days = [first + timedelta(days=i) for i in range((last - first).days + 1)]

    def one_folder(day):
        parsed = []
        for key, modified in _list(f"{config.PARQUET_PATH}/entur_siri_national/{day}/{kind}_"):
            m = _SIRI_FILE.search(key)
            if m:
                folder, _, stamp, compacted = m.groups()
                parsed.append((key, modified, bool(compacted), _siri_hour_start(folder, stamp) if compacted else _siri_pass_time(folder, stamp)))
        # the compactor groups per-pass files by the time they were written (LastModified), so cover by that hour
        kept = _without_covered(parsed, lambda i: i[2], lambda i: i[3], lambda i: _hour(i[1]))
        return [i for i in kept]

    with ThreadPoolExecutor(max_workers=8) as pool:
        listings = list(pool.map(one_folder, days))
    chosen = []
    for items in listings:
        for key, _, compacted, moment in items:
            if compacted:
                keep = moment < end and moment + timedelta(hours=1) > start
            else:
                keep = start - _SLACK <= moment < end + _SLACK
            if keep:
                chosen.append(f"s3://{config.BUCKET}/{key}")
    return chosen
