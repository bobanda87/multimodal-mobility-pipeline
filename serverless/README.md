# Collectors as Lambda functions (AWS SAM)

Four scheduled functions (Avinor, Entur, Oslo Bysykkel, Vegvesen) write Parquet only (no JSON) to
`s3://<bucket>/data/parquet/<source>/<date>/`. Each invocation does one collection pass.

| Function | Schedule | Notes |
|---|---|---|
| Entur | every 1 min | one request per invocation, operator/feed picked from the clock (same rotation as the old loop) |
| Avinor | every 3 min | airports fetched in parallel (8 threads) |
| Oslo Bysykkel | every 1 min | `station_status` 4 times per invocation (every 15 s, the feed's ttl); `station_information` and `system_information` once every 10 min |
| Vegvesen | every 3 min | needs `VegvesenUser` / `VegvesenPass` |

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
