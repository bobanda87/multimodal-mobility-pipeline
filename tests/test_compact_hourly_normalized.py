import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from datetime import datetime, timezone, timedelta
import polars as pl
from analysis import compact_normalized as c

BASE = c.s3_upload.S3_PARQUET_PATH.rstrip("/")
NOW = datetime(2026, 11, 14, 21, 20, tzinfo=timezone.utc)   # hours before 20:00Z are closed (grace 10 min)

class Fake:
    def __init__(self): self.objs = {}      # key -> (LastModified, polars frame)
    def add(self, name, lm, df, day="2026-11-14"):
        self.objs[f"{BASE}/normalized/{day}/{name}"] = (lm, df)
    def get_paginator(self, _):
        fake = self
        class P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k, "LastModified": v[0], "Size": 1} for k, v in sorted(fake.objs.items()) if k.startswith(Prefix)]}
        return P()
    def head_object(self, Bucket, Key):
        if Key not in self.objs: raise KeyError(Key)
    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]: self.objs.pop(o["Key"])

f = Fake(); c.s3_upload.s3_client = f
c.s3_upload.read_parquet_from_s3 = lambda k: f.objs[k][1]
def save(pdf, key):
    f.objs[f"{BASE}/{key}"] = (NOW, pl.from_pandas(pdf)); return True
c.s3_upload.save_parquet_to_s3 = save
T = lambda h, m: datetime(2026, 11, 14, h, m, tzinfo=timezone.utc)
bk = lambda *r: pl.DataFrame({"location": [x[0] for x in r], "n": [x[1] for x in r]})
av = lambda *r: pl.DataFrame({"uniqueID": [x[0] for x in r], "status_time": [x[1] for x in r], "delay": [x[2] for x in r]})

# hour 18Z: three bysykkel files, one exact duplicate row; hour 19Z: two files; hour 20Z (open: ends 21:00 > 21:10? closed at 21:10 -> 20Z closed only after 21:10)
f.add("oslobysykkel_normalized_2026-11-14_190001.parquet", T(18, 1), bk(("a", 1), ("b", 2)))
f.add("oslobysykkel_normalized_2026-11-14_190101.parquet", T(18, 31), bk(("b", 2), ("c", 3)))     # (b,2) repeats
f.add("oslobysykkel_normalized_2026-11-14_195001.parquet", T(18, 55), bk(("d", 4)))
f.add("oslobysykkel_normalized_2026-11-14_200101.parquet", T(19, 5), bk(("e", 5)))               # only file of hour 19Z: left alone
f.add("oslobysykkel_normalized_2026-11-14_210501.parquet", T(20, 5), bk(("f", 6)))
f.add("oslobysykkel_normalized_2026-11-14_211001.parquet", T(20, 40), bk(("g", 7)))              # hour 20Z: closed (21:00 + 10 min grace <= 21:20)
f.add("oslobysykkel_normalized_2026-11-14_221001.parquet", T(21, 10), bk(("h", 8)))              # hour 21Z: open
f.add("oslobysykkel_normalized_2026-11-14_221101.parquet", T(21, 15), bk(("i", 9)))
f.add("oslobysykkel_normalized_2026-11-14_180000c.parquet", T(18, 50), bk(("x", 0)))              # earlier hourly output: never regrouped
f.add("avinor_normalized_2026-11-14_190001.parquet", T(18, 2), av(("F1", 1, 5.0), ("F2", 1, 0.0)))
f.add("avinor_normalized_2026-11-14_190301.parquet", T(18, 33), av(("F1", 2, 9.0), ("F2", 1, 0.0)))  # F1 new state kept; F2 exact repeat dropped
f.add("entur_siri_et_normalized_2026-11-14_190001.parquet", T(18, 3), pl.DataFrame({"trip_id": ["t"], "route_id": ["r"], "departure_timestamp": [10], "timestamp": [5], "d": [1]}))
f.add("entur_siri_et_normalized_2026-11-14_190201.parquet", T(18, 34), pl.DataFrame({"trip_id": ["t"], "route_id": ["r"], "departure_timestamp": [10], "timestamp": [9], "d": [7]}))
f.add("oslobysykkel_normalized_2026-11-14_130001.parquet", T(2, 0), bk(("old", 1)))             # beyond the 26 h lookback (before 19:00Z the day before? earliest = 2026-11-13 19:00Z)

groups = c.list_hourly_groups(NOW, 26, 10)
print("groups:", {(s, h.strftime('%H'), len(v)) for (s, d, h), v in groups.items()})
assert not any(k[0] == "oslobysykkel" and k[2].hour == 21 for k in groups), "open hour must not be grouped"
assert all(not key.endswith("c.parquet") for v in groups.values() for key in v), "hourly outputs must not be regrouped"

c.run_hourly(now=NOW)
names = sorted(k.split("/")[-1] for k in f.objs)
print("\nafter pass 1:"); [print("  ", n) for n in names]
out18 = f.objs[f"{BASE}/normalized/2026-11-14/oslobysykkel_normalized_2026-11-14_180000c.parquet"][1]
# the pre-existing 18Z 'c' file is x; merging the 18Z run files must not delete or reuse it... it is merged in (interrupted-output rule)
rows18 = sorted(out18["location"].to_list()); print("18Z bysykkel rows:", rows18)
assert rows18 == ["a", "b", "c", "d", "x"], rows18       # (b,2) deduped; existing output merged in, not lost
assert "oslobysykkel_normalized_2026-11-14_200101.parquet" in names
assert "oslobysykkel_normalized_2026-11-14_200000c.parquet" in names, "closed 20Z hour written"
assert "oslobysykkel_normalized_2026-11-14_210501.parquet" not in names and "oslobysykkel_normalized_2026-11-14_211001.parquet" not in names, "closed 20Z merged"
assert "oslobysykkel_normalized_2026-11-14_221001.parquet" in names and "oslobysykkel_normalized_2026-11-14_221101.parquet" in names, "open 21Z untouched"
a = f.objs[f"{BASE}/normalized/2026-11-14/avinor_normalized_2026-11-14_180000c.parquet"][1]; print("avinor:", a.to_dicts())
assert sorted((r["uniqueID"], r["status_time"], r["delay"]) for r in a.to_dicts()) == [("F1", 1, 5.0), ("F1", 2, 9.0), ("F2", 1, 0.0)], "successive states of a flight are all kept"
e = f.objs[f"{BASE}/normalized/2026-11-14/entur_siri_et_normalized_2026-11-14_180000c.parquet"][1]; print("entur:", e.to_dicts())
assert e.height == 2 and sorted(e["timestamp"].to_list()) == [5, 9], "successive observations of a trip are all kept"
assert "oslobysykkel_normalized_2026-11-14_130001.parquet" in names, "outside the lookback: untouched"

before = sorted(f.objs); c.run_hourly(now=NOW)
assert sorted(f.objs) == before, "second pass must change nothing"
print("\nhourly compaction tests passed (second pass idempotent)")
