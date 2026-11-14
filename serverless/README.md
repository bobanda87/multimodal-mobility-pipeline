# Collectors as Lambda functions (AWS SAM)

Seven scheduled functions (Avinor, Entur, Entur national, Entur SIRI national, Oslo Bysykkel, Vegvesen, and the data monitor) write Parquet only (no JSON) to
`s3://<bucket>/data/parquet/<source>/<date>/`. Each invocation does one collection pass.

| Function | Schedule | Notes |
|---|---|---|
| Entur | every 1 min | one request per invocation, operator/feed picked from the clock (same rotation as the old loop) |
| Entur national | every 1 min | national GTFS-RT feeds in one request each: `vehicle-positions` twice per invocation (every 30 s, only vehicles reported in the last 10 min), `alerts` every 5 min. Writes to `entur_national/<date>/`, a separate source folder, so the per-operator normalization never reads it |
| Entur SIRI national | every 1 min | SIRI ET for every operator in one pass (`rest/et` without `datasetId`, all `MoreData` pages, fixed `requestorId`): the first request after a pause returns everything in 3 pages, the following ones only what changed in 1. Keeps estimated and recorded calls (actual times). Writes `journeys_*` and `calls_*` to `entur_siri_national/<date>/`. Replaces the per-operator Entur function, which is disabled |
| Avinor | every 3 min | one request per airport (the feed has no call for several or all), only the ≈ 140 airports that had flights before (`avinor/active_airports.json`), 6 threads, 3 attempts each. Asking for all ≈ 2000 airports with 32 threads made the server drop 99% of the connections |
| Oslo Bysykkel | every 1 min | `station_status` 4 times per invocation (every 15 s, the feed's ttl); `station_information` and `system_information` once every 10 min |
| Vegvesen | every 3 min | needs `VegvesenUser` / `VegvesenPass` |
| Data monitor | every 1 min | checks the age of the newest file of every source and alerts when one goes quiet; see `monitor/README.md` |

## Deploy

Needs AWS credentials that may create Lambda, IAM roles, CloudWatch log groups and EventBridge Scheduler
(the `s3-uploader` key from `.env` cannot). Run from this directory:

```
sam build
sam deploy --guided --parameter-overrides VegvesenUser=... VegvesenPass=...
```

Schedules are created **DISABLED**. Test one function, check the file in S3, then start collecting:

```
sam remote invoke OsloBysykkelFunction --stack-name <stack>
sam deploy --parameter-overrides ScheduleState=ENABLED VegvesenUser=... VegvesenPass=...
```

Stop collecting: deploy with `ScheduleState=DISABLED`, or `sam delete`.
