import sys, io
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from datetime import datetime, timezone
import pyarrow as pa, pyarrow.parquet as pq
from analysis import compact_normalized as c
BASE = c.s3_upload.S3_PARQUET_PATH.rstrip("/"); NOW = datetime(2026, 11, 14, 23, 25, tzinfo=timezone.utc)
def blob(table):
    b = io.BytesIO(); pq.write_table(table, b); return b.getvalue()
class Fake:
    def __init__(self): self.objs = {}
    def add(self, name, lm, table, day="2026-11-14"): self.objs[f"{BASE}/entur_siri_national/{day}/{name}"] = (lm, blob(table))
    def get_paginator(self, _):
        fake = self
        class P:
            def paginate(self, Bucket, Prefix): yield {"Contents": [{"Key": k, "LastModified": v[0], "Size": len(v[1])} for k, v in sorted(fake.objs.items()) if k.startswith(Prefix)]}
        return P()
    def head_object(self, Bucket, Key):
        if Key not in self.objs: raise KeyError(Key)
    def get_object(self, Bucket, Key): return {"Body": io.BytesIO(self.objs[Key][1])}
    def put_object(self, Bucket, Key, Body, **k): self.objs[Key] = (NOW, bytes(Body))
    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]: self.objs.pop(o["Key"])
f = Fake(); c.s3_upload.s3_client = f
T = lambda h, m: datetime(2026, 11, 14, h, m, tzinfo=timezone.utc)
# the schema drifts between passes: a column added later, an int column that gets nulls, a column that is all null in one pass
f.add("calls_220500.parquet", T(21, 5), pa.table({"trip": ["a", "b"], "order": pa.array([1, 2], pa.int64()), "delay": pa.array([None, None], pa.null()), "flag": [True, False]}))
f.add("calls_220600.parquet", T(21, 6), pa.table({"trip": ["c"], "order": pa.array([3.5], pa.float64()), "delay": pa.array([12.0], pa.float64()), "flag": pa.array([None], pa.bool_()), "pass_skipped": pa.array([0], pa.int64())}))
f.add("calls_220700.parquet", T(21, 7), pa.table({"trip": ["d", "e"], "order": pa.array([4, 5], pa.int64()), "delay": pa.array([1.0, None], pa.float64()), "flag": [True, True], "pass_skipped": pa.array([1, 0], pa.int64())}))
f.add("calls_230100.parquet", T(22, 1), pa.table({"trip": ["x"], "order": pa.array([1], pa.int64())}))   # hour 22Z with a single file: left alone
f.add("calls_000100.parquet", T(23, 1), pa.table({"trip": ["open"], "order": pa.array([9], pa.int64())}))  # open hour
c.run_siri_hourly(now=NOW)
names = sorted(k.split("/")[-1] for k in f.objs); print("files:", names)
assert names == ["calls_000100.parquet", "calls_210000c.parquet", "calls_230100.parquet"], names
out = pq.read_table(io.BytesIO(f.objs[f"{BASE}/entur_siri_national/2026-11-14/calls_210000c.parquet"][1]))
print("merged schema:", {n: str(t) for n, t in zip(out.schema.names, out.schema.types)}); print("rows:", out.to_pylist())
assert out.num_rows == 5 and out.schema.field("order").type == pa.float64() and out.schema.field("delay").type == pa.float64() and "pass_skipped" in out.schema.names
assert [r["trip"] for r in out.to_pylist()] == ["a", "b", "c", "d", "e"], "input order kept"
assert out.to_pylist()[0]["pass_skipped"] is None and out.to_pylist()[2]["pass_skipped"] == 0 and out.to_pylist()[1]["delay"] is None
before = sorted(f.objs); c.run_siri_hourly(now=NOW); assert sorted(f.objs) == before, "second pass changes nothing"
# an interrupted earlier pass left the output behind: it is merged in again and the inputs still go
f.add("calls_220800.parquet", T(21, 8), pa.table({"trip": ["f"], "order": pa.array([6], pa.int64()), "delay": pa.array([2.0], pa.float64())}))
f.add("calls_220900.parquet", T(21, 9), pa.table({"trip": ["g"], "order": pa.array([7], pa.int64()), "delay": pa.array([3.0], pa.float64())}))
c.run_siri_hourly(now=NOW)
out = pq.read_table(io.BytesIO(f.objs[f"{BASE}/entur_siri_national/2026-11-14/calls_210000c.parquet"][1]))
assert out.num_rows == 7 and sorted(r["trip"] for r in out.to_pylist()) == list("abcdefg"), out.num_rows
# verification failure keeps the inputs
f.add("calls_220950.parquet", T(21, 10), pa.table({"trip": ["h"], "order": pa.array([8], pa.int64())}))
f.add("calls_220955.parquet", T(21, 11), pa.table({"trip": ["i"], "order": pa.array([9], pa.int64())}))
real_put = f.put_object; f.put_object = lambda Bucket, Key, Body, **k: real_put(Bucket, Key, Body[:-8] if False else Body)
orig = c._parquet_rows; calls = {"n": 0}
def liar(b):   # the stored copy reports a different row count
    calls["n"] += 1; n = orig(b); return n if calls["n"] % 3 else n + 1
c._parquet_rows = liar
c.s3_upload.FAILED_SAVES.clear(); c.run_siri_hourly(now=NOW); c._parquet_rows = orig
assert "calls_220950.parquet" in {k.split("/")[-1] for k in f.objs}, "inputs must survive a failed verification"
print("siri streaming compaction tests passed")
