"""Time split without leakage, error metrics, trip bootstrap, and a training run that learns a planted effect."""
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from prediction import metrics, train
from prediction.split import TRIP_KEY, split_by_time

rng = np.random.default_rng(1)
rows = []
for trip in range(600):
    start = 1_800_000_000 + trip * 600           # one trip every 10 minutes, ~16 stops each
    for stop in range(1, 6):
        aimed = start + stop * 120
        rain = rng.random() < 0.3
        delay = 60 + (150 if rain else 0) + rng.normal(0, 20)  # planted effect: rain adds 150 s
        rows.append({"datasource": "X", "trip_id": f"t{trip}", "data_frame_ref": "d", "ord": stop, "stop": f"S{stop}",
                     "aimed": float(aimed), "t_pred": float(aimed - 300), "horizon_s": 300, "target": delay,
                     "exp_delay": 60.0 + rng.normal(0, 10), "last_delay": 60.0, "delay_trend": 0.0,
                     "stops_ahead": stop, "since_last_s": 100.0, "line_ref": "L:1", "mode": "bus", "operator_ref": "O",
                     "direction_ref": "0", "hour": 8, "dow": 1, "local_date": "2027-01-15", "stop_lat": 59.9,
                     "stop_lon": 10.7, "wx_air_temp": 2.0, "wx_road_temp": 1.0, "wx_precip": 1.0 if rain else 0.0,
                     "wx_wind": 3.0, "wx_humidity": 80.0, "wx_dist_km": 1.0, "wx_age_s": 300.0,
                     "bk_bikes": 5.0, "bk_docks": 5.0, "bk_bikes_prev": 5.0, "bk_stations": 1})
df = pd.DataFrame(rows)

# split: no trip in two sets, sets are time ordered, the embargo leaves a gap before each boundary
tr, va, te, (b1, b2) = split_by_time(df, embargo_s=1800)
keys = lambda ix: set(map(tuple, df.iloc[ix][TRIP_KEY].values))
assert not (keys(tr) & keys(va)) and not (keys(va) & keys(te)) and not (keys(tr) & keys(te))
assert df.iloc[tr].aimed.max() < df.iloc[va].aimed.min() <= df.iloc[va].aimed.max() < df.iloc[te].aimed.min()
first = df.groupby(TRIP_KEY)["aimed"].transform("min")
assert (first.iloc[tr] < b1 - 1800).all() and (first.iloc[te] >= b2).all()
assert len(tr) > len(va) and len(te) > 0

# metrics
e = np.array([-60.0, 0.0, 30.0, 200.0])
s = metrics.summary(e)
assert s["mae"] == 72.5 and s["within_60s"] == 0.75 and s["bias"] == 42.5
t = df.iloc[te].reset_index(drop=True)
good, bad = t["target"].values.copy(), t["target"].values + 100
b = metrics.paired_bootstrap(t, good, bad, n_boot=200)
assert b["mae_diff"] < 0 and b["ci_high"] < 0 and b["trips"] == t.groupby(TRIP_KEY).ngroups
assert metrics.paired_bootstrap(t, good, good, n_boot=50)["mae_diff"] == 0

# training: the model has to find the rain effect that Entur's estimate (constant ~60 s) misses
out = Path(tempfile.mkdtemp())
res = train.run(df, out, params={**train.PARAMS, "n_estimators": 200, "n_jobs": 2}, shap_rows=200, embargo_s=1800)
overall = pd.read_csv(out / "metrics_overall.csv").set_index("model")
assert overall.loc["siri+weather", "mae"] < overall.loc["entur", "mae"] * 0.5, overall["mae"].to_dict()
assert overall.loc["siri+weather", "mae"] < overall.loc["siri", "mae"] * 0.7
bt = res["bootstrap"]["siri+weather_vs_siri"]
assert bt["ci_high"] < 0
shap = pd.read_csv(out / "shap_importance.csv")
assert "wx_precip" in shap.head(3).feature.tolist(), shap.head(5)
assert (out / "model_best.txt").exists() and (out / "metrics_by_horizon.csv").exists()
print("prediction split/metrics/train tests passed")
