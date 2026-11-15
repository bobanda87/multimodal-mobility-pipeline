"""Feature table: every feature uses only what was known at prediction time, the target is the later recorded delay."""
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from prediction import build_dataset as bd

ref = Path(tempfile.mkdtemp())
pd.DataFrame({"stop_id": ["S1", "S2", "S3", "S4"], "name": list("abcd"), "lat": [59.9] * 4,
              "lon": [10.75, 10.7501, 10.7502, 10.7503]}).to_parquet(ref / "stops.parquet")
pd.DataFrame({"site_id": ["W1", "W2"], "name": ["near", "far"], "lat": [59.9, 60.9], "lon": [10.75, 10.75]}
             ).to_parquet(ref / "weather_sites.parquet")
pd.DataFrame({"station_id": ["B1"], "name": ["b"], "lat": [59.9], "lon": [10.75], "capacity": [20]}
             ).to_parquet(ref / "bike_stations.parquet")
pd.DataFrame({"route_id": ["L:1"], "route_type": ["700"]}).to_parquet(ref / "routes.parquet")
bd.REFERENCE = ref

T0 = 1_800_000_000.0  # aimed time of stop 1; stops are 600 s apart
con = duckdb.connect()
rows = []


def call(kind, ord_, fetched, exp_delay=None, act_delay=None):
    aimed = T0 + 600 * (ord_ - 1)
    rows.append(("X", "t1", "d", kind, ord_, f"S{ord_}", fetched, aimed,
                 None if exp_delay is None else aimed + exp_delay, None if act_delay is None else aimed + act_delay))


for o in (1, 2, 3, 4):
    call("estimated", o, T0 - 100, exp_delay=60)          # first state, before the trip starts
for o in (2, 3, 4):
    call("estimated", o, T0 + 700, exp_delay=130)         # state after stop 1 was passed
call("recorded", 1, T0 + 700, act_delay=120)
call("recorded", 1, T0 + 1300, act_delay=120)
call("recorded", 2, T0 + 1300, act_delay=140)
for o in (3, 4):
    call("estimated", o, T0 + 1300, exp_delay=999)        # newer than the t_pred of stop 3 at 300 s (T0+1500)? no: older
call("estimated", 3, T0 + 1700, exp_delay=777)            # fetched AFTER t_pred of horizon 300 for stop 3: must not be used
call("recorded", 3, T0 + 2000, act_delay=200)
call("recorded", 4, T0 + 2600, act_delay=210)
con.execute("CREATE TABLE c (datasource VARCHAR, trip_id VARCHAR, dfr VARCHAR, call_type VARCHAR, ord BIGINT, stop VARCHAR,"
            " fetched_at BIGINT, aimed DOUBLE, exp_t DOUBLE, act_t DOUBLE)")
con.executemany("INSERT INTO c VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
con.execute("CREATE TABLE jr AS SELECT 'X' datasource, 't1' trip_id, 'd' dfr, 'L:1' line_ref, NULL::VARCHAR mode,"
            " 'OP' operator_ref, '0' direction_ref")
con.execute("CREATE TABLE wx AS SELECT * FROM (VALUES ('W1', %f, 5.0, 1.0, 0.0, 2.0, 80.0), ('W1', %f, 6.0, 2.0, 0.5, 3.0, 85.0),"
            " ('W1', %f, 99.0, 99.0, 99.0, 99.0, 99.0), ('W2', %f, -50.0, 0, 0, 0, 0))"
            " t(site_id, ts, wx_air_temp, wx_road_temp, wx_precip, wx_wind, wx_humidity)"
            % (T0 - 600, T0 + 900, T0 + 1600, T0 + 900))
con.execute("CREATE TABLE bike_bin AS SELECT * FROM (VALUES ('B1', %d, 10.0, 5.0), ('B1', %d, 3.0, 12.0), ('B1', %d, 1.0, 14.0), ('B1', %d, 20.0, 1.0))"
            " t(station_id, bin, bikes, docks)" % (int(T0 // 300 * 300) + 600, int(T0 // 300 * 300) + 1200,
                                                    int(T0 // 300 * 300) + 1500, int(T0 // 300 * 300) - 300))

n = bd.make_features(con, T0, T0 + 3000, horizons=(300, 1800))
f = con.execute("SELECT * FROM features").df()
get = lambda ord_, h: f[(f.ord == ord_) & (f.horizon_s == h)].iloc[0]
assert n == len(f) and len(f) > 0

# aimed: stop 1 = T0, stop 2 = T0+600, stop 3 = T0+1200, stop 4 = T0+1800

# stop 3, horizon 300: t_pred = T0+900. Newest state of stop 3 fetched <= t_pred is the one of T0+700 (130 s); the states
# of T0+1300 (999) and T0+1700 (777) are in the future and must not be used.
r = get(3, 300)
assert r.target == 200 and r.exp_delay == 130, (r.target, r.exp_delay)
# only stop 1 had been recorded by then (stop 2 was reported at T0+1300)
assert r.last_delay == 120 and r.stops_ahead == 2 and pd.isna(r.delay_trend), (r.last_delay, r.stops_ahead)
assert abs(r.since_last_s - (T0 + 900 - (T0 + 120))) < 1e-6
assert r["mode"] == "bus"  # SIRI has no mode, derived from the GTFS route type 700

# stop 4, horizon 300: t_pred = T0+1500 -> state of T0+1300 (999), stop 2 recorded by then, trend 140 - 120
r = get(4, 300)
assert r.exp_delay == 999 and r.target == 210 and r.last_delay == 140 and r.stops_ahead == 2 and r.delay_trend == 20
assert abs(r.since_last_s - (T0 + 1500 - (T0 + 600 + 140))) < 1e-6

# stop 4, horizon 1800: t_pred = T0 -> first state (T0-100), the trip has not started: no last_delay
r = get(4, 1800)
assert r.exp_delay == 60 and pd.isna(r.last_delay) and pd.isna(r.stops_ahead)

# nothing was known before T0-100: no row for stop 3 at 1800 s or stop 1 at 300 s
assert f[(f.ord == 3) & (f.horizon_s == 1800)].empty and f[(f.ord == 1) & (f.horizon_s == 300)].empty
# never a stop the vehicle had already passed
assert (f.stops_ahead.dropna() > 0).all()

# weather: nearest site is W1 (W2 is 100 km away); newest observation at or before t_pred, never a later one
r = get(4, 300)  # t_pred = T0+1500: observations at T0-600 and T0+900; the one at T0+1600 is in the future
assert r.wx_site == "W1" and r.wx_air_temp == 6.0 and r.wx_age_s == 600 and r.wx_dist_km < 0.1
r = get(4, 1800)  # t_pred = T0 -> observation of T0-600
assert r.wx_air_temp == 5.0 and r.wx_age_s == 600

# bikes: last COMPLETED 5 minute bin before t_pred (the bin that contains t_pred is not used), trend vs 30 minutes earlier
r = get(4, 300)  # t_pred = T0+1500 -> bin T0+1200 (3 bikes); the bin T0+1500 holds 1 and is still running
assert r.bk_stations == 1 and r.bk_bikes == 3.0 and r.bk_docks == 12.0 and r.bk_bikes_prev == 20.0, (r.bk_bikes, r.bk_bikes_prev)
print("prediction feature tests passed")
