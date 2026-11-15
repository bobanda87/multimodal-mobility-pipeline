"""Nightly job decisions: skip with too little data, first model is promoted, a worse challenger is not, a better one is."""
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from prediction import nightly, train


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey()
        import io
        return {"Body": io.BytesIO(self.objects[Key])}


def synthetic(days, rain_effect):
    rng = np.random.default_rng(3)
    rows = []
    t0 = 1_800_000_000
    for d in range(days):
        for trip in range(150):
            start = t0 + d * 86400 + trip * 300
            for stop in range(1, 5):
                aimed = start + stop * 120
                rain = rng.random() < 0.3
                rows.append({"datasource": "X", "trip_id": f"d{d}t{trip}", "data_frame_ref": "d", "ord": stop,
                             "stop": f"S{stop}", "aimed": float(aimed), "t_pred": float(aimed - 300), "horizon_s": 300,
                             "target": 60 + (rain_effect if rain else 0) + rng.normal(0, 15),
                             "exp_delay": 60.0 + rng.normal(0, 5), "last_delay": 60.0, "delay_trend": 0.0,
                             "stops_ahead": stop, "since_last_s": 100.0, "line_ref": "L:1", "mode": "bus",
                             "operator_ref": "O", "direction_ref": "0", "hour": 8, "dow": 1,
                             "local_date": f"2027-01-{d + 1:02d}", "stop_lat": 59.9, "stop_lon": 10.7,
                             "wx_air_temp": 2.0, "wx_road_temp": 1.0, "wx_precip": 1.0 if rain else 0.0, "wx_wind": 3.0,
                             "wx_humidity": 80.0, "wx_dist_km": 1.0, "bk_bikes": 5.0, "bk_docks": 5.0,
                             "bk_bikes_prev": 5.0, "bk_stations": 1, "tt_ratio": 1.1, "tt_trend": 0, "tt_seconds": 110.0, "tt_dist_m": 40.0})
    return pd.DataFrame(rows)


fake = FakeS3()
nightly.s3 = fake
nightly.CONFIG = "siri+weather"
nightly.MIN_TRAIN_DAYS, nightly.HOLDOUT_DAYS = 6, 2
state = {"days": []}
nightly.build_missing_days = lambda work, now: []
nightly.download_window = lambda work, now: state["days"]
frame = {}
train.load = lambda path: frame["df"]
train.PARAMS.update(n_estimators=150, n_jobs=2)
now = datetime(2027, 1, 20, 3, tzinfo=timezone.utc)

# too few days: nothing trained, nothing written
state["days"] = [f"2027-01-{d:02d}" for d in range(1, 5)]
frame["df"] = synthetic(4, 150)
r = nightly.run(now)
assert r["status"].startswith("skipped") and not fake.objects

# first run: no champion, the challenger becomes champion
state["days"] = [f"2027-01-{d:02d}" for d in range(1, 9)]
frame["df"] = synthetic(8, 150)
r = nightly.run(now)
assert r["promoted"] and "challenger_vs_entur" in r
champ = json.loads(fake.objects["prediction/champion.json"])
assert champ["model_key"] in fake.objects and champ["holdout_mae"] < champ["entur_mae"]
first_key = champ["model_key"]

# same world, new run: the challenger is no better than the champion, so it is not promoted
r = nightly.run(datetime(2027, 1, 21, 3, tzinfo=timezone.utc))
assert "challenger_vs_champion" in r and not r["promoted"], r
assert json.loads(fake.objects["prediction/champion.json"])["model_key"] == first_key
# every run leaves a report
assert sum(k.endswith("report.json") for k in fake.objects) == 2

# the delay pattern changes (rain now costs 400 s): the champion is wrong, the challenger learned it and replaces it
frame["df"] = synthetic(8, 400)
r = nightly.run(datetime(2027, 1, 22, 3, tzinfo=timezone.utc))
assert r["promoted"] and r["mae"]["challenger"] < r["mae"]["champion"] * 0.5, r["mae"]
assert json.loads(fake.objects["prediction/champion.json"])["model_key"] != first_key
print("prediction nightly tests passed")
