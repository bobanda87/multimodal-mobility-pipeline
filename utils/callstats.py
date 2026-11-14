"""
How many calls to the outside sources succeeded and how many failed, kept in the log of every invocation.

Each collector makes calls (one per airport, one per feed, one per page). `CallStats.record` counts them by
outcome and `emit` prints one JSON line per feed at the end of the invocation, in CloudWatch's embedded
metric format. That one line is
  * a log event, kept 90 days and queryable with CloudWatch Logs Insights (monitor/call_report.py sums it
    per source and day, which is the table of successful and failed calls for a paper), and
  * three CloudWatch metrics (Collectors/CallsOk, CallsFailed, Retries per Source) for graphs and alarms,
    with no permission beyond writing logs. Set EMIT_METRICS=false to keep the log line but skip the metrics.

Outcomes: ok (answer used), empty (the source answered but had nothing, e.g. an airport without flights),
failed (no usable answer after the retries), skipped (not asked, e.g. out of time).
"""
import collections
import json
import os
import threading
import time

OUTCOMES = ("ok", "empty", "failed", "skipped")


def _percentile(values: list[float], q: float):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


class CallStats:
    """Thread-safe counters per feed for one invocation."""

    def __init__(self):
        self._lock = threading.Lock()
        self._feeds = {}
        self.started = time.time()

    def _feed(self, name):
        if name not in self._feeds:
            self._feeds[name] = {"counts": collections.Counter(), "retries": 0,
                                 "errors": collections.Counter(), "latency": []}
        return self._feeds[name]

    def record(self, feed: str, outcome: str, latency_s: float | None = None, error: str | None = None,
               retries: int = 0, count: int = 1) -> None:
        """Count `count` items of one outcome (one call by default; the SIRI parser counts journeys in bulk)."""
        assert outcome in OUTCOMES, outcome
        with self._lock:
            f = self._feed(feed)
            f["counts"][outcome] += count
            f["retries"] += retries
            if error:
                f["errors"][error] += count
            if latency_s is not None:
                f["latency"].append(latency_s)

    def summary(self) -> dict:
        """{feed: {...}} for tests and for emit()."""
        out = {}
        for name, f in self._feeds.items():
            c = f["counts"]
            out[name] = {
                "CallsOk": c["ok"], "CallsEmpty": c["empty"], "CallsFailed": c["failed"], "CallsSkipped": c["skipped"],
                "Retries": f["retries"],
                "LatencyMsP50": round(1000 * _percentile(f["latency"], 0.5)) if f["latency"] else None,
                "LatencyMsP95": round(1000 * _percentile(f["latency"], 0.95)) if f["latency"] else None,
                "Errors": dict(f["errors"]),
            }
        return out

    def emit(self) -> None:
        now_ms = int(time.time() * 1000)
        duration = round(time.time() - self.started, 1)
        metrics = os.getenv("EMIT_METRICS", "true").lower() != "false"
        for name, values in self.summary().items():
            line = {"Source": name, **values, "DurationS": duration}
            if metrics:
                line["_aws"] = {"Timestamp": now_ms, "CloudWatchMetrics": [{
                    "Namespace": "Collectors", "Dimensions": [["Source"]],
                    "Metrics": [{"Name": "CallsOk", "Unit": "Count"}, {"Name": "CallsFailed", "Unit": "Count"},
                                {"Name": "Retries", "Unit": "Count"}]}]}
            print(json.dumps(line))
