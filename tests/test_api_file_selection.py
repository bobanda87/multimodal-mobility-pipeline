import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from datetime import datetime, timezone, timedelta
import duckdb
from api import files as f, config
config.BUCKET = "b"; f.config.BUCKET = "b"
U = lambda *a: datetime(*a, tzinfo=timezone.utc)
objs = {}
class P:
    def paginate(self, Bucket, Prefix): yield {"Contents": [{"Key": k, "LastModified": v} for k, v in sorted(objs.items()) if k.startswith(Prefix)]}
class S3:
    def get_paginator(self, _): return P()
f._s3 = S3()
B = config.PARQUET_PATH
# ---- normalized: hour 21Z has two per-run files and its compacted file (compaction caught mid-way) ----
objs[f"{B}/normalized/2026-11-14/oslobysykkel_normalized_2026-11-14_220501.parquet"] = U(2026, 11, 14, 21, 5)
objs[f"{B}/normalized/2026-11-14/oslobysykkel_normalized_2026-11-14_220601.parquet"] = U(2026, 11, 14, 21, 6)
objs[f"{B}/normalized/2026-11-14/oslobysykkel_normalized_2026-11-14_210000c.parquet"] = U(2026, 11, 14, 22, 10)
objs[f"{B}/normalized/2026-11-14/oslobysykkel_normalized_2026-11-14_230501.parquet"] = U(2026, 11, 14, 22, 5)   # hour 22Z, not compacted yet
got = f.normalized_files("oslobysykkel", U(2026, 11, 14, 20), U(2026, 11, 15, 1))
print("normalized ->", [k.split("_normalized_")[1] for k in got])
assert sorted(k.split("_normalized_")[1] for k in got) == ["2026-11-14_210000c.parquet", "2026-11-14_230501.parquet"], got
# ---- siri: same rule ----
objs.clear()
for h, m in ((21, 5), (21, 6), (22, 5)):
    objs[f"{B}/entur_siri_national/2026-11-14/calls_{h+1:02d}{m:02d}00.parquet"] = U(2026, 11, 14, h, m)
objs[f"{B}/entur_siri_national/2026-11-14/calls_210000c.parquet"] = U(2026, 11, 14, 22, 10)
got = f.siri_national_files("calls", U(2026, 11, 14, 21), U(2026, 11, 14, 23))
print("siri calls ->", [k.split("/")[-1] for k in got])
assert sorted(k.split("/")[-1] for k in got) == ["calls_210000c.parquet", "calls_230500.parquet"], got
# a compacted file outside the window is not read
got = f.siri_national_files("calls", U(2026, 11, 14, 22, 30), U(2026, 11, 14, 23))
assert all("c.parquet" not in k for k in got), got
print("selection tests passed")
# ---- retry ----
from api.deps import retry_once_on_missing_file
n = []
@retry_once_on_missing_file
def endpoint(x=1):
    n.append(1)
    if len(n) == 1: raise duckdb.IOException("HTTP 404 file gone")
    return "ok"
assert endpoint() == "ok" and len(n) == 2
m = []
@retry_once_on_missing_file
def always():
    m.append(1); raise duckdb.IOException("still gone")
try: always(); raise SystemExit("must raise")
except duckdb.IOException: assert len(m) == 2
@retry_once_on_missing_file
def other(): raise ValueError("a real bug must not be retried")
try: other()
except ValueError: pass
import inspect; assert list(inspect.signature(endpoint).parameters) == ["x"], "signature kept for FastAPI"
print("retry tests passed (one retry, real errors untouched, signature kept)")
