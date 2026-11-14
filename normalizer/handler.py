"""Lambda entry points: incremental normalization and daily compaction of the normalized files."""
import json
import time

from botocore.exceptions import ClientError

from analysis import compact_normalized, normalize_data
from utils.s3_upload import S3_BUCKET_NAME, S3_JSON_PATH, s3_client

_LOCK_HELD = ("PreconditionFailed", "ConditionalRequestConflict")


def _lock_key(name: str) -> str:
    return f"{S3_JSON_PATH.rstrip('/')}/{name}.lock"


def acquire_lock(name: str, ttl_seconds: float) -> bool:
    """
    Take an S3 lock object, False if another run holds it. Two overlapping runs would normalize the
    same raw files twice, and Lambda reserved concurrency is not available on this account.

    The object is created with a conditional write, so only one run can win. A lock past its expiry
    belongs to a run that timed out or crashed and is taken over.
    """
    key, now = _lock_key(name), time.time()
    body = json.dumps({"expires": now + ttl_seconds})
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=body, IfNoneMatch="*")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] not in _LOCK_HELD:
            raise
    try:
        expires = json.loads(s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)["Body"].read())["expires"]
    except Exception:
        expires = 0  # unreadable lock: treat as stale
    if expires > now:
        return False
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=body)
    return True


def release_lock(name: str) -> None:
    s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=_lock_key(name))


def _run_locked(name: str, context, task) -> dict:
    ttl = context.get_remaining_time_in_millis() / 1000 + 30 if context else 960
    if not acquire_lock(name, ttl):
        print(f"{name}: another run is in progress, skipping")
        return {"status": "skipped"}
    try:
        task()
    finally:
        release_lock(name)
    return {"status": "ok"}


def normalize_handler(event, context):
    """Normalize the raw files that arrived since each source's watermark."""
    return _run_locked("normalize", context, lambda: normalize_data.run_normalization(only_today=True))


def compact_handler(event, context):
    """Merge the small per-run files of closed days into one file per source and day."""
    return _run_locked("compact", context, compact_normalized.run)


def compact_hourly_handler(event, context):
    """Merge the per-run files of closed hours into one file per source, folder and hour."""
    return _run_locked("compact_hourly", context, compact_normalized.run_hourly)
