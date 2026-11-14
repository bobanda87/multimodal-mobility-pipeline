import sys, json, os
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
os.environ["S3_BUCKET_NAME"] = "b"
from datetime import datetime, timedelta, timezone
from monitor import checks, handler
U = lambda *a: datetime(*a, tzinfo=timezone.utc)
S = {s.name: s for s in checks.SOURCES}
bys = S["bysykkel_station_status"]

# ---- evaluate ----
now = U(2026, 11, 14, 22, 0, 0)
assert checks.evaluate(bys, now - timedelta(seconds=40), now)["status"] == "OK"
assert checks.evaluate(bys, now - timedelta(seconds=181), now)["status"] == "STALE"
assert checks.evaluate(bys, None, now)["status"] == "STALE"
assert checks.evaluate(bys, now - timedelta(seconds=40), now, scale=0.01)["status"] == "STALE"
# ---- transitions: OK -> DOWN -> (quiet) -> reminder after 1 h -> UP with the gap ----
st, ev = checks.transition(None, checks.evaluate(bys, now - timedelta(seconds=20), now), now); assert ev == [] and st["status"] == "OK"
unchanged, ev = checks.transition(st, checks.evaluate(bys, now + timedelta(seconds=40) - timedelta(seconds=20), now + timedelta(seconds=40)), now + timedelta(seconds=40))
assert unchanged == st and ev == [], "a steady OK changes nothing (no S3 write)"
last_good = now - timedelta(seconds=20)
t = now + timedelta(minutes=4)                                         # newest file still the same -> 260 s old > 180 s
st2, ev = checks.transition(st, checks.evaluate(bys, last_good, t), t)
assert [e["type"] for e in ev] == ["DOWN"] and st2["status"] == "STALE" and st2["last_ok"] == last_good.isoformat(), (ev, st2)
print(checks.describe(ev[0], bys))
st3, ev = checks.transition(st2, checks.evaluate(bys, last_good, t + timedelta(minutes=5)), t + timedelta(minutes=5)); assert ev == [] and st3 == st2, "no repeat while it stays down"
t2 = t + timedelta(minutes=61)
st4, ev = checks.transition(st3, checks.evaluate(bys, last_good, t2), t2); assert [e["type"] for e in ev] == ["STILL_DOWN"]; print(checks.describe(ev[0], bys))
back = t2 + timedelta(minutes=2)
st5, ev = checks.transition(st4, checks.evaluate(bys, back - timedelta(seconds=5), back), back)
assert [e["type"] for e in ev] == ["UP"] and st5["status"] == "OK", ev
assert ev[0]["gap_start"] == last_good.isoformat() and abs(ev[0]["gap_s"] - ((back - timedelta(seconds=5)) - last_good).total_seconds()) < 1
print(checks.describe(ev[0], bys))
# ---- start_after / day folders across local midnight (Oslo = UTC+1 in November) ----
n = U(2026, 11, 14, 23, 5, 0)                                          # 00:05 local on the 15th
assert checks.day_folders(n, 600) == ["2026-11-14", "2026-11-15"], checks.day_folders(n, 600)
assert checks.start_after(bys, "2026-11-15", n, 600) == "station_status_000000"          # the window starts yesterday -> from the start of today
assert checks.start_after(bys, "2026-11-14", n, 600) == "station_status_235500"          # yesterday's folder: from 23:55 local
nz = S["normalized_bysykkel"]; assert checks.start_after(nz, "2026-11-14", n, 600) == "oslobysykkel_normalized_2026-11-14_235500"
# compacted files say nothing about freshness
assert checks.is_per_run_file(bys, "x/journeys_220500.parquet") and not checks.is_per_run_file(bys, "x/journeys_220000c.parquet")
# ---- gaps ----
g = checks.gaps([now, now + timedelta(seconds=15), now + timedelta(seconds=30), now + timedelta(seconds=300), now + timedelta(seconds=315)], 60)
assert len(g) == 1 and g[0][2] == 270
print("check logic tests passed")

# ---- handler with a fake S3 ----
files = {}     # key -> LastModified
store = {}
class Fake:
    class exceptions: NoSuchKey = type("NoSuchKey", (Exception,), {})
    def list_objects_v2(self, Bucket, Prefix, StartAfter=None, ContinuationToken=None):
        keys = sorted(k for k in files if k.startswith(Prefix) and (not StartAfter or k > StartAfter))
        return {"Contents": [{"Key": k, "LastModified": files[k]} for k in keys], "IsTruncated": False}
    def get_object(self, Bucket, Key):
        import io
        if Key not in store: raise self.exceptions.NoSuchKey()
        return {"Body": io.BytesIO(store[Key])}
    def put_object(self, Bucket, Key, Body, **k): store[Key] = Body
handler._s3 = Fake(); P = handler.PARQUET
def put(folder, day, name, when): files[f"{P}/{folder}/{day}/{name}"] = when
def fresh_everything(now, except_=()):
    day = now.astimezone(checks.OSLO).strftime("%Y-%m-%d"); stamp = now.astimezone(checks.OSLO).strftime("%H%M%S")
    for s in checks.SOURCES:
        if s.name in except_: continue
        name = f"{s.name_prefix}{day}_{stamp}.parquet" if s.normalized else f"{s.name_prefix}{stamp}.parquet"
        put(s.folder, day, name, now - timedelta(seconds=5))
sent = []
handler.ALERT_URL = "https://hook"; handler.send = lambda text, kind="generic", urgent=True: sent.append((urgent, text)) or print("   SENT:", text)
t0 = U(2026, 11, 14, 12, 3, 0)
fresh_everything(t0); r = handler.run(t0); assert r["stale"] == [] and sent == [] and "checks" in json.loads(store[handler.STATE_KEY])
# vegvesen weather stops
t1 = t0 + timedelta(minutes=1); fresh_everything(t1, except_=("vegvesen_weather",)); r = handler.run(t1); assert r["stale"] == [] and sent == []   # 65 s old, limit 600 s
t2 = t0 + timedelta(minutes=12); fresh_everything(t2, except_=("vegvesen_weather",)); r = handler.run(t2)
assert r["stale"] == ["vegvesen_weather"] and len(sent) == 1 and sent[0][0] is True and "vegvesen_weather" in sent[0][1], sent
t3 = t0 + timedelta(minutes=13); fresh_everything(t3, except_=("vegvesen_weather",)); handler.run(t3); assert len(sent) == 1, "no repeat"
# it comes back
t4 = t0 + timedelta(minutes=20); fresh_everything(t4); r = handler.run(t4)
assert r["stale"] == [] and len(sent) == 2 and sent[1][0] is False and "RECOVERED" in sent[1][1], sent
gaps_key = f"{handler.JSON_PATH}/data_gaps/2026-11.json"; ledger = json.loads(store[gaps_key]); print("   gap ledger:", ledger)
assert len(ledger) == 1 and ledger[0]["source"] == "vegvesen_weather" and ledger[0]["gap_s"] > 600
# a check that raises is reported, not hidden
orig = handler.newest_file
handler.newest_file = lambda s, n: (_ for _ in ()).throw(PermissionError("AccessDenied")) if s.name == "avinor_osl" else orig(s, n)
t5 = t0 + timedelta(minutes=21); fresh_everything(t5); r = handler.run(t5)
assert "avinor_osl" in r["stale"] and "check failed: PermissionError" in sent[-1][1], sent[-1]
handler.newest_file = orig
# dry run: alerts are printed but nothing is sent or written
sent.clear(); before = dict(store); r = handler.run(t5 + timedelta(minutes=1), scale=0.0001, dry_run=True)
assert sent == [] and store == before and len(r["stale"]) >= 10
# metrics line is valid EMF
import io, contextlib
buf = io.StringIO()
with contextlib.redirect_stdout(buf): handler.emit_metrics([{"source": "x", "age_s": 12}], t0)
line = json.loads(buf.getvalue().strip().splitlines()[0]); assert line["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Name"] == "AgeSeconds" and line["AgeSeconds"] == 12
print("handler tests passed")
