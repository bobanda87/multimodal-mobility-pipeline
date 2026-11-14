"""
Data monitor: runs every minute, tells you at once when a source stops delivering.

Per run: for every source in checks.SOURCES find the newest file (a list of the last few minutes of keys, one
request per source), decide OK or STALE, and on a change
  * log an ALERT line and send it to the webhook (ALERT_WEBHOOK_URL, ALERT_KIND),
  * on recovery append the gap to data_gaps/<month>.json (the record of holes for the paper),
and write the freshness of every source as CloudWatch metrics (embedded metric format in the log, so it
needs no extra permission). State between runs is data/json/monitor_state.json.

Test it without side effects: invoke with {"threshold_scale": 0.0001, "dry_run": true}.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

from . import checks

BUCKET = os.environ.get("S3_BUCKET_NAME", "")
PARQUET = os.getenv("S3_PARQUET_PATH", "data/parquet").strip("/")
JSON_PATH = os.getenv("S3_JSON_PATH", "data/json").strip("/")
STATE_KEY = f"{JSON_PATH}/monitor_state.json"
ALERT_URL = os.getenv("ALERT_WEBHOOK_URL", "")
ALERT_KIND = os.getenv("ALERT_KIND", "generic")
MUTED = {m for m in os.getenv("MONITOR_MUTE", "").split(",") if m}
BACKLOG = checks.Source("compaction_backlog_siri", "entur_siri_national", "journeys_", 3600, checks.COMPACTION_BACKLOG_AFTER_S)

_s3 = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _list(prefix: str, start_after: str | None = None):
    kw = {"Bucket": BUCKET, "Prefix": prefix}
    if start_after:
        kw["StartAfter"] = start_after
    while True:
        page = s3().list_objects_v2(**kw)
        yield from page.get("Contents", [])
        if not page.get("IsTruncated"):
            return
        kw["ContinuationToken"] = page["NextContinuationToken"]


def newest_file(source: checks.Source, now: datetime) -> datetime | None:
    """LastModified of the newest per-run file of a source, looking only at the recent keys."""
    lookback = source.stale_after_s + 600
    newest = None
    for day in checks.day_folders(now, lookback):
        base = f"{PARQUET}/{source.folder}/{day}/"
        for obj in _list(base + source.name_prefix, base + checks.start_after(source, day, now, lookback)):
            if checks.is_per_run_file(source, obj["Key"]):
                modified = obj["LastModified"].astimezone(timezone.utc)
                newest = modified if newest is None or modified > newest else newest
    return newest


def siri_backlog(now: datetime) -> dict:
    """Per-pass SIRI files older than the backlog limit: the compaction should have merged them."""
    limit = now - timedelta(seconds=checks.COMPACTION_BACKLOG_AFTER_S)
    old = []
    for day in checks.day_folders(now, checks.COMPACTION_BACKLOG_AFTER_S + 86400):
        for obj in _list(f"{PARQUET}/entur_siri_national/{day}/journeys_"):
            if checks.is_per_run_file(BACKLOG, obj["Key"]) and obj["LastModified"].astimezone(timezone.utc) < limit:
                old.append(obj["LastModified"].astimezone(timezone.utc))
    oldest_age = round((now - min(old)).total_seconds()) if old else 0
    return {"source": BACKLOG.name, "status": "STALE" if old else "OK", "age_s": oldest_age, "newest": None,
            "limit_s": checks.COMPACTION_BACKLOG_AFTER_S, "count": len(old)}


def _read_json(key):
    try:
        return json.loads(s3().get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except s3().exceptions.NoSuchKey:
        return None


def _write_json(key, data):
    s3().put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data, indent=2).encode(), ContentType="application/json")


def send(text: str, kind: str = "generic", urgent: bool = True) -> bool:
    """Post an alert to the webhook; never raises (a broken webhook must not stop the monitor)."""
    print(f"ALERT: {text}")
    if not ALERT_URL:
        return False
    try:
        if kind == "ntfy":
            body, headers = text.encode(), {"Title": "Data monitor", "Priority": "high" if urgent else "default",
                                            "Tags": "rotating_light" if urgent else "white_check_mark"}
        else:
            field = "content" if kind == "discord" else "text"
            payload = {"text": text, "content": text} if kind == "generic" else {field: text}
            body, headers = json.dumps(payload).encode(), {"Content-Type": "application/json"}
        request = urllib.request.Request(ALERT_URL, data=body, headers=headers, method="POST")
        urllib.request.urlopen(request, timeout=10).read()
        return True
    except Exception as e:
        print(f"ALERT_SEND_FAILED: {type(e).__name__}: {e}")
        return False


def emit_metrics(results: list[dict], now: datetime) -> None:
    """One embedded-metric-format line per source: CloudWatch turns it into DataFreshness/AgeSeconds."""
    for r in results:
        if r["source"] == BACKLOG.name:
            continue
        print(json.dumps({
            "_aws": {"Timestamp": int(now.timestamp() * 1000),
                     "CloudWatchMetrics": [{"Namespace": "DataFreshness", "Dimensions": [["Source"]],
                                            "Metrics": [{"Name": "AgeSeconds", "Unit": "Seconds"}]}]},
            "Source": r["source"], "AgeSeconds": r["age_s"] if r["age_s"] is not None else 86400}))


def run(now: datetime, scale: float = 1.0, dry_run: bool = False) -> dict:
    by_name = {s.name: s for s in checks.SOURCES}
    results = []
    for source in checks.SOURCES:
        if source.name in MUTED:
            continue
        try:
            result = checks.evaluate(source, newest_file(source, now), now, scale)
        except Exception as e:
            result = {"source": source.name, "status": "STALE", "age_s": None, "newest": None,
                      "limit_s": source.stale_after_s, "error": f"{type(e).__name__}: {e}"}
        results.append(result)
    if now.minute % 10 == 0 or dry_run:  # a full listing of two days: not every minute
        try:
            results.append(siri_backlog(now))
        except Exception as e:
            print(f"backlog check failed: {e}")

    stored = _read_json(STATE_KEY) or {"checks": {}}
    new_state, events = dict(stored["checks"]), []
    for r in results:
        state, found = checks.transition(stored["checks"].get(r["source"]), r, now)
        new_state[r["source"]] = state
        events += [{**e, "error": r.get("error"), "count": r.get("count")} for e in found]

    for e in events:
        if e["source"] == BACKLOG.name:
            text = (f"COMPACTION BACKLOG: {e.get('count')} per-pass SIRI files older than 3 h (oldest {e.get('age_s', 0) // 3600} h), the hourly compaction is not keeping up"
                    if e["type"] != "UP" else "RECOVERED: the SIRI compaction backlog is cleared")
        else:
            text = checks.describe(e, by_name.get(e["source"]))
            if e.get("error"):
                text += f" [check failed: {e['error']}]"
        if dry_run:
            print(f"ALERT (dry run, not sent): {text}")
        else:
            send(text, ALERT_KIND, urgent=e["type"] != "UP")
            if e["type"] == "UP" and e["source"] != BACKLOG.name:
                month = e["gap_end"][:7]
                key = f"{JSON_PATH}/data_gaps/{month}.json"
                _write_json(key, (_read_json(key) or []) + [{k: e[k] for k in ("source", "gap_start", "gap_end", "gap_s")}])
    if not dry_run and new_state != stored["checks"]:
        _write_json(STATE_KEY, {"checks": new_state, "updated": now.isoformat()})
    emit_metrics(results, now)
    stale = [r["source"] for r in results if r["status"] == "STALE"]
    print(f"monitor: {len(results)} checks, {len(stale)} stale{': ' + ', '.join(stale) if stale else ''}")
    return {"checks": len(results), "stale": stale, "events": [e["type"] + ":" + e["source"] for e in events]}


def lambda_handler(event, context):
    event = event or {}
    return run(datetime.now(timezone.utc), float(event.get("threshold_scale", 1.0)), bool(event.get("dry_run", False)))
