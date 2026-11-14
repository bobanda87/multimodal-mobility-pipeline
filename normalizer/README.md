# Normalization and compaction as Lambda functions

Two scheduled container functions (separate SAM stack, they need polars/pandas, which do not fit the
collectors' zip + layer packaging):

| Function | Schedule | What it does |
|---|---|---|
| `NormalizeFunction` | every `NormalizeEveryMinutes` minutes, default 1 | normalizes raw files newer than each source's watermark and writes one Parquet per source and date folder to `normalized/<date>/` |
| `CompactHourlyFunction` | hourly at minute 10 (`HourlyCompactScheduleState`) | merges the per-run files of each closed UTC hour into one `<source>_normalized_<date>_<HH>0000c.parquet` per source and folder, removing exact duplicates, then deletes the inputs. Also merges the per-pass `journeys_*` and `calls_*` files of `entur_siri_national/` into `<kind>_<HH>0000c.parquet`, one input at a time with pyarrow (the first version used 2036 of 2048 MB for a quiet hour) |
| `CompactFunction` | daily 03:30 Europe/Oslo (`CompactScheduleState`, off by default) | folds the hourly files and any leftovers of days older than 2 days into one file per source and day, removing duplicates, then deletes the inputs |

Progress is one watermark per source in `s3://<bucket>/data/json/normalization_watermark.json`
(`utils/normalization_state.py`), replacing the 30 MB `normalization_tracking.json`. A raw file that
fails to read holds the watermark back, so the next run retries it.

## Delay this adds

A record reaches the API about `collection interval + NormalizeEveryMinutes + run time` after the source
published it: up to about a minute extra at the default of 1, up to 60 min if you schedule hourly. An
incremental run (a minute or a few of new raw files) takes about 3 s.

Running every minute writes one small Parquet per source and run into the day's folder (1440 per source
and day). The hourly compaction keeps that bounded: only the current hour keeps its per-run files
(about 60 per source), older hours are one file each, so the API opens roughly 60 + 24 files per source for
today. Raise `NormalizeEveryMinutes` if you would rather have fewer runs; change it with
`sam deploy --parameter-overrides NormalizeEveryMinutes=<n> ScheduleState=ENABLED HourlyCompactScheduleState=ENABLED`.

The collectors poll faster than some sources change (Vegvesen weather refreshes every 10 min and is polled
every 3), so consecutive raw files repeat rows; the compaction removes exact duplicates.

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
