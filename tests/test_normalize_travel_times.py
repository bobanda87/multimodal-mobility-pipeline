"""travel_times normalization keeps the measured travel time, its trend and type next to the free flow reference."""
import polars as pl

from analysis.normalize_data import normalize_vegvesen_travel_times

COL = "ns2:messageContainer.ns2:payload.ns10:physicalQuantity"


def entry(loc, with_tt):
    bd = {"ns10:measurementOrCalculationTime": {"ns10:period": {"startOfPeriod": "2026-11-15T10:00:00.010+01:00",
                                                                "endOfPeriod": "2026-11-15T10:05:00.010+01:00"}},
          "ns10:freeFlowTravelTime": {"ns10:duration": "150.0"} if not with_tt else None,
          "ns10:freeFlowSpeed": {"speed": "50.0"} if not with_tt else None,
          "ns10:trafficStatus": {"ns10:trafficStatusValue": "freeFlow"} if not with_tt else None}
    if with_tt:
        bd.update({"ns10:travelTime": {"ns10:duration": "212.0"}, "ns10:travelTimeTrendType": "increasing",
                   "ns10:travelTimeType": "reconstituted"})
    return {"ns10:pertinentLocation": {"ns8:predefinedLocationReference": {"@id": loc}}, "ns10:basicData": bd}


raw = pl.DataFrame({COL: [[entry("100357", True), entry("100357", False)]]})
out = normalize_vegvesen_travel_times(raw)
assert out.columns == ["location_id", "period_start", "period_end", "free_flow_travel_time_sec", "free_flow_speed_kmh",
                       "traffic_status", "travel_time_sec", "travel_time_trend", "travel_time_type", "source"]
tt = out.filter(pl.col("travel_time_sec").is_not_null())
assert tt.height == 1 and tt["travel_time_sec"][0] == 212.0 and tt["travel_time_trend"][0] == "increasing"
ref = out.filter(pl.col("free_flow_travel_time_sec").is_not_null())
assert ref.height == 1 and ref["free_flow_travel_time_sec"][0] == 150.0 and ref["travel_time_sec"][0] is None

# a file without any measurement (the fields do not exist) still normalizes
no_tt = pl.DataFrame({COL: [[{**entry("1", False), "ns10:basicData": {k: v for k, v in entry("1", False)["ns10:basicData"].items()}}]]})
out2 = normalize_vegvesen_travel_times(no_tt)
assert out2.height == 1 and out2["travel_time_sec"][0] is None
print("travel time normalization tests passed")
