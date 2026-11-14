import sys, json
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from datetime import datetime, timezone
import polars as pl
from utils import normalization_state as ns
saved_wm = []
ns.read_json_from_s3 = lambda key: None
ns.save_json_to_s3 = lambda data, key: saved_wm.append(json.loads(json.dumps(data))) or True
from analysis import normalize_data as nd
today = datetime.now().strftime("%Y-%m-%d")
T = lambda m: datetime(2026, 11, 15, 10, m, tzinfo=timezone.utc)
files = [{"key": f"data/parquet/oslobysykkel/{today}/station_status_10{m:02d}00.parquet", "last_modified": T(m)} for m in (1, 2)]
nd.list_raw_files = lambda source, date_prefix=None: files if source == "oslobysykkel" else []
nd.get_all_parquet_files_from_s3 = lambda *a, **k: []
nd.read_parquet_from_s3 = lambda key: pl.DataFrame({"x": [1, 2]})
nd.NORMALIZERS["oslobysykkel"] = lambda df, **k: df
writes = []
for ok in (False, True):
    saved_wm.clear(); writes.clear()
    nd.save_parquet_to_s3 = lambda df, key, ok=ok: writes.append(key) or ok
    nd.run_normalization(only_today=True)
    print(f"\n=== upload ok={ok}: parquet attempted {len(writes)}x, watermark writes: {len(saved_wm)}")
    if ok: assert len(saved_wm) >= 1 and "oslobysykkel" in saved_wm[-1], saved_wm
    else:  assert saved_wm == [], "a failed upload must not advance the watermark"
print("\nnormalization failed-upload test passed")
