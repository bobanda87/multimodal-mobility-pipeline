"""The call counters: outcomes, retries, errors, latency percentiles, and the EMF line."""
import sys, io, json, pathlib, contextlib, os
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from utils.callstats import CallStats
c = CallStats()
for i in range(8): c.record("avinor", "ok", 0.2 + i * 0.01, retries=1 if i == 0 else 0)
c.record("avinor", "empty", 0.3); c.record("avinor", "failed", 1.0, "ConnectionError", retries=2); c.record("avinor", "failed", 1.5, "HTTP 503", retries=2); c.record("avinor", "skipped")
c.record("entur_siri_journeys", "ok", count=4000); c.record("entur_siri_journeys", "failed", error="ValueError", count=3)
s = c.summary()
print(s["avinor"]); print(s["entur_siri_journeys"])
a = s["avinor"]
assert (a["CallsOk"], a["CallsEmpty"], a["CallsFailed"], a["CallsSkipped"], a["Retries"]) == (8, 1, 2, 1, 5)
assert a["Errors"] == {"ConnectionError": 1, "HTTP 503": 1} and a["LatencyMsP50"] and a["LatencyMsP95"] >= a["LatencyMsP50"]
assert s["entur_siri_journeys"]["CallsOk"] == 4000 and s["entur_siri_journeys"]["CallsFailed"] == 3 and s["entur_siri_journeys"]["LatencyMsP50"] is None
buf = io.StringIO()
with contextlib.redirect_stdout(buf): c.emit()
lines = [json.loads(l) for l in buf.getvalue().strip().splitlines()]
assert {l["Source"] for l in lines} == {"avinor", "entur_siri_journeys"} and all("_aws" in l for l in lines)
m = lines[0]["_aws"]["CloudWatchMetrics"][0]; assert m["Namespace"] == "Collectors" and [x["Name"] for x in m["Metrics"]] == ["CallsOk", "CallsFailed", "Retries"]
os.environ["EMIT_METRICS"] = "false"; buf = io.StringIO()
with contextlib.redirect_stdout(buf): c.emit()
assert all("_aws" not in json.loads(l) for l in buf.getvalue().strip().splitlines()), "metrics can be switched off, the log line stays"
# thread safety
import threading
d = CallStats(); ts = [threading.Thread(target=lambda: [d.record("x", "ok", 0.1) for _ in range(500)]) for _ in range(8)]
[t.start() for t in ts]; [t.join() for t in ts]; assert d.summary()["x"]["CallsOk"] == 4000
print("callstats tests passed")
