import sys; import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from datetime import datetime, timedelta, timezone
from utils import normalization_state as ns
saved = []
keys = []
ns.save_json_to_s3 = lambda data, key: (keys.append(key), saved.append(data))[1] or True
ns.read_json_from_s3 = lambda key: None
t = lambda s: datetime(2026, 3, 10, 12, 0, s, tzinfo=timezone.utc)
F = lambda k, s: {"key": k, "last_modified": t(s)}
w = ns.Watermarks()
files = [F("a", 1), F("b", 2), F("c", 3), F("d", 3)]
assert all(w.is_new("x", f) for f in files)
# 1) all ok -> watermark at second 3 with both boundary keys
assert w.advance("x", files, set()); e = saved[-1]["x"]
assert e["boundary_keys"] == ["c", "d"], e
assert [w.is_new("x", f) for f in files] == [False] * 4
assert w.is_new("x", F("e", 3)) and w.is_new("x", F("f", 4))          # same-second newcomer, later file
# 2) failed file: stop before it, it is retried
w2 = ns.Watermarks(); w2.advance("y", files, {"b"}); e = saved[-1]["y"]
assert e["last_modified"] == t(1).isoformat() and e["boundary_keys"] == ["a"], e
assert w2.is_new("y", F("b", 2)) and w2.is_new("y", F("c", 3)) and not w2.is_new("y", F("a", 1))
# 3) first file failed -> no change
w3 = ns.Watermarks(); assert not w3.advance("z", files, {"a"}); assert "z" not in w3._data
# 4) never moves backwards; equal second merges boundary
w.advance("x", [F("old", 1)], set()); assert w._data["x"]["last_modified"] == t(3).isoformat()
w.advance("x", [F("g", 3)], set()); assert w._data["x"]["boundary_keys"] == ["c", "d", "g"]
full = "data/json/" + keys[-1]
assert full == ns.WATERMARK_KEY, (full, ns.WATERMARK_KEY)
print("watermark unit tests passed; write key", keys[-1], "-> full", full, "== read key")
