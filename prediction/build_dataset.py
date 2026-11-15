"""
Build the training table for the delay prediction, one Parquet file per local day, straight from S3 with DuckDB.

One row = (trip, upcoming stop, horizon): what was known `horizon` seconds before the aimed arrival at that stop,
and what the arrival delay then turned out to be.

    target        arrival delay at the stop in seconds, from the call that SIRI later reports as recorded
    exp_delay     what Entur itself expected for that stop at prediction time (the baseline to beat)
    last_delay    delay at the last stop the vehicle had already passed (persistence baseline)

Leakage guards:
  * every feature comes from a state that had been fetched at or before the prediction time (ASOF joins on fetched_at);
  * a stop at or before the last recorded stop of the trip is not a future stop, its row is dropped;
  * bike counts use the last completed 5 minute bin, weather its newest observation (at most 2 h old).

Multimodal features (each group is switched on/off by the ablation in train.py):
  wx_*  Vegvesen weather at the nearest station (distance kept as wx_dist_km)
  bk_*  Oslo Bysykkel bikes and docks within 300 m of the stop
  tt_*  Vegvesen measured travel time on the nearest monitored road segment within 150 m (ratio to free flow, trend)

    PYTHONPATH=. python -m prediction.build_dataset --start 2026-11-15 --end 2026-11-16 --until 2026-11-15T10:00
"""
import argparse
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api import files  # noqa: E402  (the same file selection as the API: compacted file wins over its inputs)
from prediction.s3db import connect  # noqa: E402

OSLO = ZoneInfo("Europe/Oslo")
REFERENCE = Path(__file__).parent / "reference"
HORIZONS_S = (300, 900, 1800)
DELAY_RANGE_S = (-1800, 10800)  # recorded delays outside this are treated as data errors
BIKE_RADIUS_M = 300
TRAFFIC_RADIUS_M = 150
TRAFFIC_MAX_AGE_S = 1800
LOOKAHEAD_H = 3  # follow-up data a target needs: the stop's arrival is recorded some time after it happens
WEATHER_MAX_AGE_S = 7200


def _lst(paths: list[str]) -> str:
    return "[" + ",".join("'" + p + "'" for p in paths) + "]"


def _haversine(lat1, lon1, lat2, lon2) -> str:
    return (f"2*6371000*asin(sqrt(sin(radians({lat2}-{lat1})/2)^2 + "
            f"cos(radians({lat1}))*cos(radians({lat2}))*sin(radians({lon2}-{lon1})/2)^2))")


def build(con, day_start: datetime, day_end: datetime, until: datetime | None = None, lookback_h: int = 3,
          lookahead_h: int = LOOKAHEAD_H, horizons=HORIZONS_S) -> int:
    """Create the temp table `features` for targets with aimed time in [day_start, day_end) (aware datetimes)."""
    lo, hi = day_start.astimezone(timezone.utc), day_end.astimezone(timezone.utc)
    if until is not None:
        hi = min(hi, until.astimezone(timezone.utc) - timedelta(hours=lookahead_h))
    if hi <= lo:
        return 0
    if not _load(con, lo - timedelta(hours=lookback_h), hi + timedelta(hours=lookahead_h)):
        return 0
    return make_features(con, lo.timestamp(), hi.timestamp(), horizons)


def _load(con, win_a: datetime, win_b: datetime) -> bool:
    """Temp tables from S3: c (SIRI calls), jr (journey attributes), wx (weather), bike_bin (bikes per 5 min)."""
    calls = files.siri_national_files("calls", win_a, win_b)
    journeys = files.siri_national_files("journeys", win_a, win_b)
    if not calls or not journeys:
        return False
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE c AS
        SELECT datasource, trip_id, coalesce(data_frame_ref, '') AS dfr, call_type, "order" AS ord, stop_point_ref AS stop,
               fetched_at,
               CASE WHEN aimed_arrival IS NOT NULL THEN aimed_arrival ELSE aimed_departure END AS aimed,
               CASE WHEN aimed_arrival IS NOT NULL THEN expected_arrival ELSE expected_departure END AS exp_t,
               CASE WHEN aimed_arrival IS NOT NULL THEN actual_arrival ELSE actual_departure END AS act_t
        FROM read_parquet({_lst(calls)}, union_by_name=true)
        WHERE "order" IS NOT NULL AND trip_id IS NOT NULL""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE jr AS
        SELECT datasource, trip_id, coalesce(data_frame_ref, '') AS dfr, arg_max(line_ref, fetched_at) AS line_ref,
               arg_max(vehicle_mode, fetched_at) AS mode, arg_max(operator_ref, fetched_at) AS operator_ref,
               arg_max(direction_ref, fetched_at) AS direction_ref
        FROM read_parquet({_lst(journeys)}, union_by_name=true)
        WHERE trip_id IS NOT NULL
        GROUP BY datasource, trip_id, dfr""")
    wx_files = files.normalized_files("vegvesen", win_a, win_b)
    if wx_files:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE wx AS
            SELECT location AS site_id, epoch(timestamp) AS ts, max(air_temperature) AS wx_air_temp,
                   max(road_temperature) AS wx_road_temp, max(precipitation_intensity) AS wx_precip,
                   max(wind_speed) AS wx_wind, max(relative_humidity) AS wx_humidity
            FROM read_parquet({_lst(wx_files)}, union_by_name=true)
            GROUP BY location, epoch(timestamp)""")
    else:
        _empty_weather(con)
    bike_files = files.normalized_files("oslobysykkel", win_a, win_b)
    if bike_files:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE bike_bin AS
            SELECT location AS station_id, (floor(epoch(timestamp) / 300) * 300)::BIGINT AS bin,
                   avg(num_bikes_available) AS bikes, avg(num_docks_available) AS docks
            FROM read_parquet({_lst(bike_files)}, union_by_name=true)
            GROUP BY location, bin""")
    else:
        _empty_bikes(con)
    tt_files = files.normalized_files("vegvesen_travel_times", win_a, win_b)
    if tt_files:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE tt AS
            SELECT location_id, epoch(period_end) AS ts, max(travel_time_sec) AS travel_time_sec,
                   arg_max(travel_time_trend, period_end) FILTER (WHERE travel_time_trend IS NOT NULL) AS trend,
                   max(free_flow_travel_time_sec) AS free_flow_sec
            FROM read_parquet({_lst(tt_files)}, union_by_name=true)
            GROUP BY location_id, epoch(period_end)""")
    else:
        _empty_tt(con)
    return True


def _empty_tt(con) -> None:
    con.execute("""CREATE OR REPLACE TEMP TABLE tt AS SELECT NULL::VARCHAR location_id, NULL::DOUBLE ts,
        NULL::DOUBLE travel_time_sec, NULL::VARCHAR trend, NULL::DOUBLE free_flow_sec LIMIT 0""")


def _empty_weather(con) -> None:
    con.execute("""CREATE OR REPLACE TEMP TABLE wx AS SELECT NULL::VARCHAR site_id, NULL::DOUBLE ts, NULL::DOUBLE wx_air_temp,
        NULL::DOUBLE wx_road_temp, NULL::DOUBLE wx_precip, NULL::DOUBLE wx_wind, NULL::DOUBLE wx_humidity LIMIT 0""")


def _empty_bikes(con) -> None:
    con.execute("""CREATE OR REPLACE TEMP TABLE bike_bin AS SELECT NULL::VARCHAR station_id, NULL::BIGINT bin,
        NULL::DOUBLE bikes, NULL::DOUBLE docks LIMIT 0""")


def make_features(con, lo: float, hi: float, horizons=HORIZONS_S) -> int:
    """Targets with aimed time in [lo, hi) (epoch seconds) from the temp tables c, jr, wx, bike_bin -> table `features`."""
    # what each stop's delay finally was, per (trip, stop)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tgt AS
        SELECT datasource, trip_id, dfr, ord, any_value(stop) AS stop, any_value(aimed) AS aimed,
               arg_max(act_t, fetched_at) - any_value(aimed) AS target
        FROM c
        WHERE call_type = 'recorded' AND act_t IS NOT NULL AND aimed IS NOT NULL
        GROUP BY datasource, trip_id, dfr, ord
        HAVING any_value(aimed) >= {lo} AND any_value(aimed) < {hi}
           AND target BETWEEN {DELAY_RANGE_S[0]} AND {DELAY_RANGE_S[1]}""")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE est AS
        SELECT datasource, trip_id, dfr, ord, fetched_at, exp_t - aimed AS exp_delay
        FROM c WHERE call_type = 'estimated' AND exp_t IS NOT NULL AND aimed IS NOT NULL""")

    # progress of each trip as it became known: the last recorded stop and its delay
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rec AS
        SELECT datasource, trip_id, dfr, avail, max(ord) AS last_ord,
               arg_max(act_t - aimed, ord) AS last_delay, arg_max(act_t, ord) AS last_act
        FROM (SELECT datasource, trip_id, dfr, ord, min(fetched_at) AS avail, any_value(act_t) AS act_t,
                     any_value(aimed) AS aimed
              FROM c WHERE call_type = 'recorded' AND act_t IS NOT NULL AND aimed IS NOT NULL
              GROUP BY datasource, trip_id, dfr, ord)
        GROUP BY datasource, trip_id, dfr, avail""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE rec2 AS
        SELECT *, lag(last_delay) OVER (PARTITION BY datasource, trip_id, dfr ORDER BY avail) AS prev_delay FROM rec""")

    con.execute(f"CREATE OR REPLACE TEMP TABLE routes AS SELECT route_id, try_cast(route_type AS INT) AS rt FROM read_parquet('{REFERENCE}/routes.parquet')")
    h_values = ",".join(f"({int(h)})" for h in horizons)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE s1 AS
        SELECT t.*, h.h AS horizon_s, t.aimed - h.h AS t_pred FROM tgt t CROSS JOIN (VALUES {h_values}) h(h)""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE s2 AS
        SELECT s1.*, e.exp_delay
        FROM s1 ASOF JOIN est e
          ON s1.datasource = e.datasource AND s1.trip_id = e.trip_id AND s1.dfr = e.dfr AND s1.ord = e.ord
         AND s1.t_pred >= e.fetched_at""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE s3 AS
        SELECT s2.*, r.last_ord, r.last_delay, r.last_act, r.prev_delay
        FROM s2 ASOF LEFT JOIN rec2 r
          ON s2.datasource = r.datasource AND s2.trip_id = r.trip_id AND s2.dfr = r.dfr AND s2.t_pred >= r.avail""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE base AS
        SELECT s3.datasource, s3.trip_id, s3.dfr AS data_frame_ref, s3.ord, s3.stop, s3.aimed, s3.t_pred, s3.horizon_s,
               s3.target, s3.exp_delay, s3.last_delay,
               s3.last_delay - s3.prev_delay AS delay_trend,
               s3.ord - s3.last_ord AS stops_ahead,
               s3.t_pred - s3.last_act AS since_last_s,
               j.line_ref, coalesce(j.mode, CASE WHEN rt.rt IN (3, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209) OR rt.rt BETWEEN 700 AND 716 THEN 'bus'
                   WHEN rt.rt = 2 OR rt.rt BETWEEN 100 AND 117 THEN 'rail' WHEN rt.rt = 0 OR rt.rt BETWEEN 900 AND 906 THEN 'tram'
                   WHEN rt.rt = 1 OR rt.rt BETWEEN 400 AND 405 THEN 'metro' WHEN rt.rt = 4 OR rt.rt BETWEEN 1000 AND 1021 THEN 'ferry'
                   WHEN rt.rt IS NOT NULL THEN 'other' END) AS mode, j.operator_ref, j.direction_ref,
               extract(hour FROM timezone('Europe/Oslo', to_timestamp(s3.aimed)))::INT AS hour,
               isodow(timezone('Europe/Oslo', to_timestamp(s3.aimed)))::INT AS dow,
               strftime(timezone('Europe/Oslo', to_timestamp(s3.aimed)), '%Y-%m-%d') AS local_date
        FROM s3 LEFT JOIN jr j ON s3.datasource = j.datasource AND s3.trip_id = j.trip_id AND s3.dfr = j.dfr
        LEFT JOIN routes rt ON rt.route_id = j.line_ref
        WHERE s3.last_ord IS NULL OR s3.ord > s3.last_ord""")

    _add_stops(con)
    _add_weather(con)
    _add_traffic(con)
    _add_bikes(con)
    return con.execute("SELECT count(*) FROM features").fetchone()[0]


def _add_stops(con) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE stops AS SELECT stop_id, lat, lon FROM read_parquet('{REFERENCE}/stops.parquet')")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE base2 AS
        SELECT b.*, s.lat AS stop_lat, s.lon AS stop_lon FROM base b LEFT JOIN stops s ON s.stop_id = b.stop""")


def _add_weather(con) -> None:
    """Weather of the nearest Vegvesen station, as of the prediction time."""
    nearest = f"""
        SELECT stop, site_id, dist_km FROM (
            SELECT d.stop, w.site_id, {_haversine('d.stop_lat', 'd.stop_lon', 'w.lat', 'w.lon')} / 1000.0 AS dist_km,
                   row_number() OVER (PARTITION BY d.stop ORDER BY {_haversine('d.stop_lat', 'd.stop_lon', 'w.lat', 'w.lon')}) AS rn
            FROM (SELECT DISTINCT stop, stop_lat, stop_lon FROM base2 WHERE stop_lat IS NOT NULL) d
            CROSS JOIN read_parquet('{REFERENCE}/weather_sites.parquet') w)
        WHERE rn = 1"""
    con.execute(f"CREATE OR REPLACE TEMP TABLE stop_site AS {nearest}")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base3 AS
        SELECT b.*, ss.dist_km AS wx_dist_km, ss.site_id AS wx_site
        FROM base2 b LEFT JOIN stop_site ss ON ss.stop = b.stop""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base4 AS
        SELECT b.*, w.wx_air_temp, w.wx_road_temp, w.wx_precip, w.wx_wind, w.wx_humidity, b.t_pred - w.ts AS wx_age_s
        FROM base3 b ASOF LEFT JOIN wx w ON b.wx_site = w.site_id AND b.t_pred >= w.ts""")
    # an observation older than the limit says nothing about now
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base5 AS
        SELECT * REPLACE (
            CASE WHEN wx_age_s > {WEATHER_MAX_AGE_S} THEN NULL ELSE wx_air_temp END AS wx_air_temp,
            CASE WHEN wx_age_s > {WEATHER_MAX_AGE_S} THEN NULL ELSE wx_road_temp END AS wx_road_temp,
            CASE WHEN wx_age_s > {WEATHER_MAX_AGE_S} THEN NULL ELSE wx_precip END AS wx_precip,
            CASE WHEN wx_age_s > {WEATHER_MAX_AGE_S} THEN NULL ELSE wx_wind END AS wx_wind,
            CASE WHEN wx_age_s > {WEATHER_MAX_AGE_S} THEN NULL ELSE wx_humidity END AS wx_humidity)
        FROM base4""")


def _add_traffic(con) -> None:
    """Measured travel time of the nearest monitored road segment within TRAFFIC_RADIUS_M, newest observation at prediction time."""
    cell_lat, cell_lon = 0.002, 0.004  # the grid keeps the distance computation to the neighbouring cells
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stop_road AS
        SELECT stop, location_id, dist_m FROM (
            SELECT d.stop, v.location_id,
                   {_haversine('d.stop_lat', 'd.stop_lon', 'v.lat', 'v.lon')} AS dist_m,
                   row_number() OVER (PARTITION BY d.stop ORDER BY {_haversine('d.stop_lat', 'd.stop_lon', 'v.lat', 'v.lon')}) AS rn
            FROM (SELECT DISTINCT stop, stop_lat, stop_lon, floor(stop_lat / {cell_lat})::BIGINT AS ci,
                         floor(stop_lon / {cell_lon})::BIGINT AS cj FROM base5 WHERE stop_lat IS NOT NULL) d
            CROSS JOIN (VALUES (-1), (0), (1)) a(di) CROSS JOIN (VALUES (-1), (0), (1)) b(dj)
            JOIN (SELECT location_id, lat, lon, floor(lat / {cell_lat})::BIGINT AS ci, floor(lon / {cell_lon})::BIGINT AS cj
                  FROM read_parquet('{REFERENCE}/traffic_vertices.parquet')) v
              ON v.ci = d.ci + a.di AND v.cj = d.cj + b.dj)
        WHERE rn = 1 AND dist_m <= {TRAFFIC_RADIUS_M}""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE base5r AS
        SELECT b.*, r.location_id AS tt_loc, r.dist_m AS tt_dist_m FROM base5 b LEFT JOIN stop_road r ON r.stop = b.stop""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE free_flow AS
        SELECT location_id, max(free_flow_sec) AS free_flow_sec FROM tt GROUP BY location_id""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base6 AS
        SELECT b.* EXCLUDE (tt_loc),
               CASE WHEN b.t_pred - t.ts <= {TRAFFIC_MAX_AGE_S} THEN t.travel_time_sec / nullif(f.free_flow_sec, 0) END AS tt_ratio,
               CASE WHEN b.t_pred - t.ts <= {TRAFFIC_MAX_AGE_S}
                    THEN CASE t.trend WHEN 'increasing' THEN 1 WHEN 'decreasing' THEN -1 WHEN 'stable' THEN 0 END END AS tt_trend,
               CASE WHEN b.t_pred - t.ts <= {TRAFFIC_MAX_AGE_S} THEN t.travel_time_sec END AS tt_seconds
        FROM base5r b
        ASOF LEFT JOIN (SELECT * FROM tt WHERE travel_time_sec IS NOT NULL) t ON b.tt_loc = t.location_id AND b.t_pred >= t.ts
        LEFT JOIN free_flow f ON f.location_id = b.tt_loc""")


def _add_bikes(con) -> None:
    """Bikes and docks at the Bysykkel stations within BIKE_RADIUS_M of the stop, last completed 5 minute bin."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stop_bike AS
        SELECT d.stop, b.station_id FROM (SELECT DISTINCT stop, stop_lat, stop_lon FROM base6 WHERE stop_lat IS NOT NULL) d
        JOIN read_parquet('{REFERENCE}/bike_stations.parquet') b
          ON {_haversine('d.stop_lat', 'd.stop_lon', 'b.lat', 'b.lon')} <= {BIKE_RADIUS_M}""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stop_bin AS
        SELECT sb.stop, bb.bin, sum(bb.bikes) AS bikes, sum(bb.docks) AS docks, count(*) AS n
        FROM stop_bike sb JOIN bike_bin bb ON bb.station_id = sb.station_id
        GROUP BY sb.stop, bb.bin""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE features AS
        SELECT b.*, k.bikes AS bk_bikes, k.docks AS bk_docks, kp.bikes AS bk_bikes_prev, k.n AS bk_stations
        FROM base6 b
        LEFT JOIN stop_bin k ON k.stop = b.stop AND k.bin = (floor(b.t_pred / 300) * 300)::BIGINT - 300
        LEFT JOIN stop_bin kp ON kp.stop = b.stop AND kp.bin = (floor(b.t_pred / 300) * 300)::BIGINT - 1800""")


def _local_midnight(day: str) -> datetime:
    return datetime.combine(datetime.strptime(day, "%Y-%m-%d").date(), time.min, tzinfo=OSLO)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True, help="first local day, YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="day after the last local day (exclusive)")
    ap.add_argument("--until", help="local time the data runs to (targets need a few hours of follow-up data), "
                                   "e.g. 2026-11-15T10:00; default: now")
    ap.add_argument("--out", default=str(Path(__file__).parent / "data"))
    ap.add_argument("--memory", default="9GB")
    args = ap.parse_args()
    until = datetime.fromisoformat(args.until).replace(tzinfo=OSLO) if args.until else datetime.now(OSLO)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    con = connect(memory_limit=args.memory)
    day = _local_midnight(args.start)
    while day < _local_midnight(args.end):
        nxt = _local_midnight((day + timedelta(days=1)).strftime("%Y-%m-%d"))
        target = out / f"features_{day:%Y-%m-%d}.parquet"
        rows = build(con, day, nxt, until=until)
        if rows:
            con.execute(f"COPY features TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"{day:%Y-%m-%d}: {rows} rows -> {target if rows else 'nothing (no complete data yet)'}", flush=True)
        day = nxt


if __name__ == "__main__":
    main()
