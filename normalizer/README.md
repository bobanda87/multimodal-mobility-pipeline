# Normalization and compaction as Lambda functions

Two scheduled container functions (separate SAM stack, they need polars/pandas, which do not fit the
collectors' zip + layer packaging):

| Function | Schedule | What it does |
|---|---|---|
| `NormalizeFunction` | every `NormalizeEveryMinutes` minutes, default 5 | normalizes raw files newer than each source's watermark and writes one Parquet per source and date folder to `normalized/<date>/` |
| `CompactFunction` | daily 03:30 Europe/Oslo (`CompactScheduleState`, off by default) | merges the files of closed days (older than 2 days) into one per source and day, removing duplicates, then deletes the inputs |

Progress is one watermark per source in `s3://<bucket>/data/json/normalization_watermark.json`
(`utils/normalization_state.py`), replacing the 30 MB `normalization_tracking.json`. A raw file that
fails to read holds the watermark back, so the next run retries it.

## Delay this adds

A record reaches the API about `collection interval + NormalizeEveryMinutes + run time` after the source
published it: up to 5 min extra at the default of 5, up to 60 min if you schedule hourly. An incremental
run (a few minutes of new raw files) takes 1 to 3 s; a backlog of about 40 minutes of data took 56 s.

How to pick the value: each run writes one new Parquet per source into the day's folder, so the interval
sets the number of small files (288 per source and day at 5 min, 1440 at 1 min). The API opens the files of
the days it queries, and the compactor only merges days older than 2, so below about 3 minutes queries over
the last two days slow down for little gain. For "now" views that must be fresher than the normalization
interval, read the raw files instead (they are seconds old).

Change it with `sam deploy --parameter-overrides NormalizeEveryMinutes=<n> ScheduleState=ENABLED`.

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

Runs must not overlap (two overlapping runs would normalize the same raw files twice). Reserved
concurrency would enforce that, but this account's concurrency limit of 10 leaves none to reserve, so
`normalizer/handler.py` takes an S3 lock (`data/json/normalize.lock`, a conditional write) and a run that
finds it held skips. A lock past its expiry, left by a run that timed out, is taken over.
