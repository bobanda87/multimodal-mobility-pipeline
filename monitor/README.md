# Data monitor: know at once when a source stops delivering

`DataMonitorFunction` (in the collectors stack) runs every minute. For each source in
`monitor/checks.py` it looks at the newest file on S3 and compares its age with a threshold a few times the
collector's cadence:

| Check | A file is written | Alert when the newest is older than |
|---|---|---|
| `bysykkel_station_status` | every 15 s | 3 min |
| `entur_vehicle_positions` | every 30 s | 3 min |
| `entur_siri_journeys` | every 60 s | 4 min |
| `avinor_osl`, `avinor_bgo` | every 3 min | 10 min |
| `vegvesen_weather`, `_travel_times`, `_situations` | every 3 min | 10 min |
| `normalized_bysykkel`, `_vegvesen`, `_avinor` | every minute | 15 min |
| `compaction_backlog_siri` | | per-pass SIRI files older than 3 h still exist (checked every 10 min) |

On a change it
* logs `ALERT: ...` and posts it to the webhook (`AlertWebhookUrl`, `AlertKind`: generic, slack, teams, discord, ntfy),
  as `NO DATA` when a source goes quiet, an hourly `STILL NO DATA` while it stays down, `RECOVERED` with the length of the gap,
* appends every closed gap to `data/json/data_gaps/<YYYY-MM>.json`, the record of holes for a paper,
* keeps its state in `data/json/monitor_state.json` (an unchanged state is not rewritten).

Every run also writes the age of each source as the CloudWatch metric `DataFreshness/AgeSeconds`
(dimension `Source`) through the log, which needs no extra permission, so a dashboard or alarm can be added later.

## Setting the alert channel

Deploy with the webhook (the URL is a secret, it is not stored in the repository):

    sam deploy ... --parameter-overrides AlertWebhookUrl=<url> AlertKind=ntfy ScheduleState=DISABLED ...

Without a URL the alerts only reach the log. `MonitorMute` silences checks of collectors switched off on purpose.

## Test without side effects

    aws lambda invoke --function-name <DataMonitorFunction> --payload '{"threshold_scale": 0.0001, "dry_run": true}' out.json

treats every source as stale, prints what would be sent, and sends and writes nothing.

## Limits

* It watches files, not their content: a collector that writes empty or wrong rows still looks healthy.
* It cannot tell you that it is itself down. A second, independent check (for example a free uptime service
  pinging the API) would cover that.
* Detection takes the threshold plus up to a minute.

## Completeness report after the fact

    PYTHONPATH=. .venv/bin/python -m monitor.gap_report --since 2026-11-15T00:00 --start 2026-11-15 --list

lists, per source, expected and actual files, coverage, and every gap, from the time each file reached S3.
