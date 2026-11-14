"""
What the data monitor checks and how it decides. No AWS calls here, so every rule is unit-testable.

A source is STALE when its newest file is older than `stale_after_s`. The thresholds are a few times the
collector's cadence (and above the longest gap seen in normal operation), so a single slow call does not
alert but a stopped collector does within minutes.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

OSLO = ZoneInfo("Europe/Oslo")  # the collectors run with TZ=Europe/Oslo: date folders and file names are local


@dataclass(frozen=True)
class Source:
    name: str                 # label in alerts and metrics
    folder: str               # S3 folder under S3_PARQUET_PATH holding <day>/ folders
    name_prefix: str          # file name prefix inside the day folder, e.g. "station_status_"
    cadence_s: int            # how often the collector writes a file
    stale_after_s: int        # alert when the newest file is older than this
    normalized: bool = False  # normalized files: <source>_normalized_<YYYY-MM-DD>_<HHMMSS>.parquet


# Raw sources. A file is written only when the feed returned something, so a quiet feed (e.g. alerts) is not listed.
SOURCES = [
    Source("bysykkel_station_status", "oslobysykkel", "station_status_", 15, 180),
    Source("entur_vehicle_positions", "entur_national", "vehicle-positions_", 30, 180),
    Source("entur_siri_journeys", "entur_siri_national", "journeys_", 60, 240),
    Source("avinor_osl", "avinor", "OSL_", 180, 600),                  # OSL always has flights; other airports skip quiet cycles
    Source("avinor_bgo", "avinor", "BGO_", 180, 600),
    Source("vegvesen_weather", "vegvesen", "weather_data_", 180, 600),
    Source("vegvesen_travel_times", "vegvesen", "travel_times_", 180, 600),
    Source("vegvesen_situations", "vegvesen", "traffic_situations_", 180, 600),
    # Normalized layer: written every minute as long as raw files arrive
    Source("normalized_bysykkel", "normalized", "oslobysykkel_normalized_", 60, 900, normalized=True),
    Source("normalized_vegvesen", "normalized", "vegvesen_normalized_", 60, 900, normalized=True),
    Source("normalized_avinor", "normalized", "avinor_normalized_", 60, 900, normalized=True),
]

# Per-pass SIRI files should be merged into an hourly file shortly after the hour closes; older ones mean the
# compaction is failing (it ran out of memory once), which would slowly make every query slower.
COMPACTION_BACKLOG_AFTER_S = 3 * 3600


def day_folders(now: datetime, lookback_s: int) -> list[str]:
    """Local date folders that can hold files written in the last lookback_s seconds."""
    local_now = now.astimezone(OSLO)
    days = {local_now.date(), (local_now - timedelta(seconds=lookback_s)).date()}
    return [d.isoformat() for d in sorted(days)]


def start_after(source: Source, day: str, now: datetime, lookback_s: int) -> str:
    """
    Key to list from, so only recent files are listed: names carry zero-padded HHMMSS (and, for normalized
    files, the date), so they sort by time within a day folder.
    """
    since = (now - timedelta(seconds=lookback_s)).astimezone(OSLO)
    stamp = since.strftime("%H%M%S") if since.strftime("%Y-%m-%d") == day else "000000"
    if source.normalized:
        date = since.strftime("%Y-%m-%d") if since.strftime("%Y-%m-%d") == day else day
        return f"{source.name_prefix}{date}_{stamp}"
    return f"{source.name_prefix}{stamp}"


def is_per_run_file(source: Source, key: str) -> bool:
    """Hourly-compacted files end in 'c.parquet' and are written long after their data, so they say nothing about freshness."""
    return key.endswith(".parquet") and not key.endswith("c.parquet")


def evaluate(source: Source, newest: datetime | None, now: datetime, scale: float = 1.0) -> dict:
    """The state of one source from the LastModified of its newest per-run file (None if there is none)."""
    limit = source.stale_after_s * scale
    if newest is None:
        return {"source": source.name, "status": "STALE", "age_s": None, "newest": None, "limit_s": limit}
    age = (now - newest).total_seconds()
    return {"source": source.name, "status": "STALE" if age > limit else "OK", "age_s": round(age),
            "newest": newest.astimezone(timezone.utc).isoformat(), "limit_s": limit}


REMINDER_EVERY_S = 3600


def transition(previous: dict | None, result: dict, now: datetime) -> tuple[dict, list[dict]]:
    """
    New state of a check and the events to announce.

    previous = {"status", "since", "last_ok", "last_alert"} or None. Events: DOWN when a check turns STALE,
    STILL_DOWN as an hourly reminder, UP when it recovers (carrying the gap).
    """
    previous = previous or {"status": "OK", "since": now.isoformat(), "last_ok": now.isoformat(), "last_alert": None}
    status, events = result["status"], []
    state = dict(previous)
    if status == "OK":
        if previous["status"] == "STALE":
            started = datetime.fromisoformat(previous["last_ok"])
            ended = datetime.fromisoformat(result["newest"]) if result.get("newest") else now
            events.append({"type": "UP", "source": result["source"], "gap_start": started.isoformat(),
                           "gap_end": ended.isoformat(), "gap_s": round((ended - started).total_seconds())})
            state.update(status="OK", since=now.isoformat(), last_ok=result["newest"] or now.isoformat(), last_alert=None)
    else:
        if previous["status"] == "OK":
            # the gap began at the newest file we saw while healthy
            state.update(status="STALE", since=now.isoformat(), last_ok=result["newest"] or previous["last_ok"], last_alert=now.isoformat())
            events.append({"type": "DOWN", "source": result["source"], "age_s": result["age_s"], "newest": result["newest"]})
        else:
            last = datetime.fromisoformat(previous["last_alert"]) if previous.get("last_alert") else None
            if last is None or (now - last).total_seconds() >= REMINDER_EVERY_S:
                state["last_alert"] = now.isoformat()
                events.append({"type": "STILL_DOWN", "source": result["source"], "age_s": result["age_s"],
                               "since": previous["since"]})
    return state, events


def gaps(timestamps: list[datetime], threshold_s: float) -> list[tuple[datetime, datetime, float]]:
    """(start, end, seconds) for every consecutive pair of file times further apart than threshold_s."""
    ts = sorted(timestamps)
    return [(a, b, (b - a).total_seconds()) for a, b in zip(ts, ts[1:]) if (b - a).total_seconds() > threshold_s]


def describe(event: dict, source: Source | None = None) -> str:
    """One human-readable alert line."""
    def mins(s):
        return f"{s / 60:.0f} min" if s >= 120 else f"{s:.0f} s"
    name = event["source"]
    expected = f" (a file is expected every {source.cadence_s} s)" if source else ""
    if event["type"] == "DOWN":
        found = "no recent file found" if event["age_s"] is None else f"newest file is {mins(event['age_s'])} old"
        return f"NO DATA: {name}: {found}{expected}"
    if event["type"] == "STILL_DOWN":
        found = "still no recent file" if event["age_s"] is None else f"newest file {mins(event['age_s'])} old"
        return f"STILL NO DATA: {name}: down since {event['since']}, {found}"
    return f"RECOVERED: {name}: data is back after a gap of {mins(event['gap_s'])} ({event['gap_start']} to {event['gap_end']})"
