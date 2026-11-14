# Normalization and compaction as Lambda functions

Two scheduled container functions (separate SAM stack, they need polars/pandas, which do not fit the
collectors' zip + layer packaging):

| Function | Schedule | What it does |
|---|---|---|
| `NormalizeFunction` | `NormalizeSchedule`, default every 15 min | normalizes raw files newer than each source's watermark and writes one Parquet per source and date folder to `normalized/<date>/` |
| `CompactFunction` | daily 03:30 Europe/Oslo | merges the files of closed days (older than 2 days) into one per source and day, removing duplicates, then deletes the inputs |

Progress is one watermark per source in `s3://<bucket>/data/json/normalization_watermark.json`
(`utils/normalization_state.py`), replacing the 30 MB `normalization_tracking.json`. A raw file that
fails to read holds the watermark back, so the next run retries it.

## Delay this adds

A record reaches the API about `collection interval + NormalizeSchedule + run time` after the source
published it: 0 to 15 min extra at the default, 0 to 60 min if you schedule hourly. A run of about one hour of data took
well under a minute to read and normalize in the measurements (see below), so 5 to 15 minutes is
affordable; the cost is more, smaller files, which the compactor merges daily.

## First deployment

1. Nothing to migrate when you only want newly collected data: with no watermark a source starts
   from today's raw folder, and the first successful run creates `normalization_watermark.json`.
   Old raw files stay un-normalized.
2. `sam build && sam deploy --guided` from this directory. Schedules start `DISABLED`; test with
   `sam remote invoke NormalizeFunction`, then deploy with `ScheduleState=ENABLED`.

Optional, only to continue from the old normalization state instead of starting fresh:
`PYTHONPATH=. .venv/bin/python analysis/migrate_tracking_to_watermark.py [--write]` sets each
watermark to the newest raw file the old `normalization_tracking.json` lists (read-only without
`--write`). The first run then normalizes everything after that point, which for old data is far too
much for one Lambda run (tens of thousands of small Avinor files); do that catch-up locally with
`python analysis/normalize_data.py --all`. The script's "holes" column counts older raw files the
old tracking file does not list (about half of all raw files here: days outside the study windows);
they are skipped from then on.

`ReservedConcurrentExecutions: 1` keeps two runs from overlapping; it needs an account concurrency
limit above the 10 AWS always keeps unreserved.
