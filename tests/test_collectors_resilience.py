"""Vegvesen: one broken feed must not skip the others; Bysykkel: a malformed body must not abort the invocation."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from vegvesen import vegvesen_collect as v
from oslobysykkel import oslobysykkel_collect as b
from utils import s3_upload as u

# Vegvesen: the first feed raises (truncated XML), the other two must still be fetched, and the invocation fails
seen = []
def fake(feed, url):
    seen.append(feed)
    if feed == "traffic_situations": raise ValueError("not well-formed")
v.fetch_datex_data = fake
try: v.run_once(); raise SystemExit("must raise")
except RuntimeError as e: print("vegvesen:", seen, "->", e)
assert seen == ["traffic_situations", "travel_times", "weather_data"]

# Bysykkel: a 200 with a body that is not JSON
class R:
    status_code = 200
    def json(self): raise ValueError("Expecting value")
b.requests.get = lambda *a, **k: R()
saved = []; b.save_parquet_to_s3 = lambda df, key: saved.append(key) or True
b.fetch_gbfs_data("station_status", "http://x")      # must not raise
print("bysykkel: malformed body handled, nothing saved:", saved == [])

# a failed upload in a collector invocation becomes an error
u.reset_failed_saves(); u.FAILED_SAVES.append("k")
try: u.raise_if_failed_saves(); raise SystemExit("must raise")
except RuntimeError: pass
u.reset_failed_saves()
print("collector resilience tests passed")
