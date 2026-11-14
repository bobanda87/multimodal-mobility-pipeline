import sys, glob, os, time
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from entur import entur_siri_national_collect as n
# 1) the real pages still give the same numbers
# 2) malformed values must not lose the pass
good = b'''<Siri xmlns="http://www.siri.org.uk/siri"><ServiceDelivery><EstimatedTimetableDelivery><EstimatedJourneyVersionFrame>
<EstimatedVehicleJourney><RecordedAtTime>NOT-A-DATE</RecordedAtTime><LineRef>X:Line:1</LineRef><FramedVehicleJourneyRef><DataFrameRef>2026-11-15</DataFrameRef><DatedVehicleJourneyRef>X:SJ:1</DatedVehicleJourneyRef></FramedVehicleJourneyRef><DataSource>X</DataSource>
 <EstimatedCalls><EstimatedCall><StopPointRef>S1</StopPointRef><Order>abc</Order><AimedArrivalTime>garbage</AimedArrivalTime><ExpectedArrivalTime>2026-11-15T10:00:00+01:00</ExpectedArrivalTime><VisitNumber>x</VisitNumber></EstimatedCall></EstimatedCalls></EstimatedVehicleJourney>
<EstimatedVehicleJourney><RecordedAtTime>2026-11-15T10:00:00+01:00</RecordedAtTime><LineRef>X:Line:2</LineRef><DatedVehicleJourneyRef>X:SJ:2</DatedVehicleJourneyRef><DataSource>X</DataSource></EstimatedVehicleJourney>
<EstimatedVehicleJourney><LineRef>X:Line:3</LineRef></EstimatedVehicleJourney>
</EstimatedJourneyVersionFrame></EstimatedTimetableDelivery></ServiceDelivery></Siri>'''
j, errs = {}, []; r = n.parse_page(good, j, errs)
print("malformed page:", r, "rows ->", len(j), "journeys, errors:", errs)
assert r == 3 and len(j) == 2 and len(errs) == 1 and "without an id" in errs[0]  # the journey without an id is reported, the other two are kept
row, calls = j[("X", "X:SJ:1", "2026-11-15")]
assert row["recorded_at"] is None and calls[0]["order"] is None and calls[0]["visit_number"] is None and calls[0]["aimed_arrival"] is None
print("bad dates/ints become null:", {k: calls[0][k] for k in ("order", "aimed_arrival", "visit_number", "expected_arrival")})
# 3) the budget: stop asking for pages when out of time, keep what was fetched
calls_made = []
class R:  # a response that always says "more data"
    status_code = 200; content = good.replace(b"</EstimatedJourneyVersionFrame>", b"</EstimatedJourneyVersionFrame><MoreData>true</MoreData>")
n.requests.get = lambda *a, **k: (calls_made.append(1), time.sleep(0.2), R())[2]
n.PASS_BUDGET_S = 0.5
journeys, pages, rows, complete = n.fetch_pass("rid")
print(f"budget: stopped after {pages} pages, complete={complete}, journeys kept={len(journeys)}"); assert not complete and 1 <= pages < n.MAX_PAGES and journeys
# 4) empty calls: collect() must not write an empty calls file
saved = []
n.save_parquet_to_s3 = lambda df, key: saved.append(key) or True
n.fetch_pass = lambda errors=None: ({("X", "X:SJ:2", None): ({"datasource": "X", "trip_id": "X:SJ:2", "recorded_at": 1}, [])}, 1, 1, True)
out = n.collect(); print("collect with no calls ->", out, "files:", [k.split("/")[-1][:8] for k in saved]); assert len(saved) == 1 and saved[0].split("/")[-1].startswith("journeys_")
# 5) a failed upload makes the invocation fail
n.save_parquet_to_s3 = lambda df, key: (n.reset_failed_saves.__globals__["FAILED_SAVES"].append(key), False)[1]
try: n.collect(); raise SystemExit("must raise")
except RuntimeError as e: print("failed upload raises:", e)
print("siri robustness tests passed")
