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


def _list_folder(source: str, day) -> list[str]:
    prefix = f"{config.PARQUET_PATH}/normalized/{day}/{source}_normalized_"
    keys = []
    for page in _s3.get_paginator("list_objects_v2").paginate(Bucket=config.BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    return keys


def normalized_files(source: str, start: datetime, end: datetime) -> list[str]:
    """
    S3 URIs of the normalized files of one source that can hold data in [start, end).

    Lists only the date folders in range (not the whole normalized/ prefix, which also holds tens
    of thousands of per-zone Entur files) and skips days without files, since collection has gaps.
    """
    first = (start - timedelta(days=FOLDER_LOOKBACK_DAYS)).date()
    days = [first + timedelta(days=i) for i in range((end.date() - first).days + 1)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        listings = pool.map(lambda d: _list_folder(source, d), days)
    return [f"s3://{config.BUCKET}/{key}" for keys in listings for key in keys]


# ---- entur_siri_national: raw folder, one pass per file, named after the fetch time (Europe/Oslo) ----

_OSLO = ZoneInfo("Europe/Oslo")
_SIRI_FILE = re.compile(r"entur_siri_national/(\d{4}-\d{2}-\d{2})/(journeys|calls)_(\d{6})(c?)\.parquet$")
_SLACK = timedelta(minutes=2)  # a file is written a few seconds after the pass it holds was fetched


def _siri_pass_time(day: str, hhmmss: str) -> datetime:
    return datetime.strptime(f"{day} {hhmmss}", "%Y-%m-%d %H%M%S").replace(tzinfo=_OSLO).astimezone(timezone.utc)


def _siri_hour_start(day: str, hhmmss: str) -> datetime:
    """UTC hour of an hourly-compacted file (name <kind>_<HH>0000c): the one that falls on the folder's local date."""
    hour = int(hhmmss[:2])
    for date in (datetime.strptime(day, "%Y-%m-%d").date() - timedelta(days=1), datetime.strptime(day, "%Y-%m-%d").date()):
        candidate = datetime(date.year, date.month, date.day, hour, tzinfo=timezone.utc)
        if candidate.astimezone(_OSLO).strftime("%Y-%m-%d") == day:
            return candidate
    return datetime.strptime(day, "%Y-%m-%d").replace(hour=hour, tzinfo=timezone.utc)


def _list_siri_folder(kind: str, day) -> list[str]:
    prefix = f"{config.PARQUET_PATH}/entur_siri_national/{day}/{kind}_"
    keys = []
    for page in _s3.get_paginator("list_objects_v2").paginate(Bucket=config.BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    return keys


def siri_national_files(kind: str, start: datetime, end: datetime) -> list[str]:
    """
    S3 URIs of the entur_siri_national `journeys` or `calls` files holding passes fetched in [start, end).

    Per-pass files are chosen by the fetch time in their name, hourly-compacted ones by the hour in theirs,
    so a query over the last hour opens about 60 files, not a day's worth.
    """
    first = start.astimezone(_OSLO).date() - timedelta(days=1)
    last = end.astimezone(_OSLO).date() + timedelta(days=1)
    days = [first + timedelta(days=i) for i in range((last - first).days + 1)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        listings = list(pool.map(lambda d: _list_siri_folder(kind, d), days))
    chosen = []
    for keys in listings:
        for key in keys:
            m = _SIRI_FILE.search(key)
            if not m:
                continue
            day, _, stamp, hourly = m.groups()
            if hourly:
                hour = _siri_hour_start(day, stamp)
                keep = hour < end and hour + timedelta(hours=1) > start
            else:
                taken = _siri_pass_time(day, stamp)
                keep = start - _SLACK <= taken < end + _SLACK
            if keep:
                chosen.append(f"s3://{config.BUCKET}/{key}")
    return chosen
