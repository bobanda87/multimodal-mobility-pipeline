"""Locating normalized Parquet files on S3."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

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
