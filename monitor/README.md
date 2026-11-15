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

## Reading logs and metrics in CloudWatch

Alerts of the data monitor, the per-call statistics and every error go to CloudWatch Logs; no webhook is needed.
Log retention is 90 days for the collectors, 30 for the monitor and 7 for the normalizer and the API.

Log group names (the collector groups carry a generated suffix, so look them up):

    aws logs describe-log-groups --query 'logGroups[].[logGroupName,retentionInDays]' --output text
    aws cloudformation describe-stack-resources --stack-name phd-collectors \
        --query "StackResources[?ResourceType=='AWS::Logs::LogGroup'].[LogicalResourceId,PhysicalResourceId]" --output text

Follow or search a group (names as above; the normalizer and API groups are `/aws/lambda/phd-<stack>-<Function>-<id>`):

    aws logs tail <log-group> --since 30m --follow
    aws logs filter-log-events --log-group-name <log-group> --start-time $(( ($(date +%s)-3600)*1000 )) \
        --filter-pattern '?ERROR ?Traceback ?"Task timed out" ?ALERT' --query 'events[].message' --output text

What to look for:

| Line | Meaning |
|---|---|
| `ALERT: ...` (monitor group) | a source is stale or a check failed; `RECOVERED: ...` when it is back |
| `{"Source": ..., "CallsOk": ..., "CallsFailed": ...}` | one statistics line per source and invocation (also a CloudWatch metric) |
| `Task timed out`, `Runtime exited`, `out of memory` | a function died; the REPORT line shows `Max Memory Used` |
| `upload(s) failed` | an S3 write failed; the invocation is marked as failed |

Summaries from the log, without writing queries:

    PYTHONPATH=. python -m monitor.call_report --days 1                  # ok / empty / failed per source
    PYTHONPATH=. python -m monitor.call_report --since 2026-11-14 --by-day
    PYTHONPATH=. python -m monitor.gap_report                            # recorded gaps in the data

Metrics (console: CloudWatch -> All metrics): namespace `Collectors` (CallsOk, CallsFailed, Retries per `Source`) and
`DataFreshness` (AgeSeconds per `Source`); the monitor's gaps are also in `data/json/data_gaps/<month>.json`.
Reading metrics with the CLI needs `cloudwatch:GetMetricData` and `ListMetrics`, which the deploy policy does not grant.
Logs Insights (console, or `aws logs start-query`) works on the same log groups, for example
`fields @timestamp, @message | filter @message like /CallsFailed": [1-9]/ | sort @timestamp desc`.
