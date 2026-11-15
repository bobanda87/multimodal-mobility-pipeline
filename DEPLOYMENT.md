# Deploying the real-time pipeline

Three AWS SAM stacks in `eu-north-1`, all writing to one S3 bucket. Every schedule is created **DISABLED**; you
switch them on one at a time and check each (section 5).

| Stack | Directory | What | Package |
|---|---|---|---|
| `phd-collectors` | `serverless/` | Avinor, Oslo Bysykkel, Vegvesen, Entur national GTFS-RT, Entur national SIRI ET, the data monitor (and the obsolete per-operator Entur function, kept disabled) | zip (Python 3.12, public pandas layer) |
| `phd-normalizer` | `normalizer/` | `NormalizeFunction` (every minute), `CompactHourlyFunction` (hourly), `CompactFunction` (daily, off) | container image, arm64 |
| `phd-api` | `api/` | FastAPI behind a Lambda Function URL, Swagger UI at `/docs` | container image, arm64 |

Data flow: collectors write raw Parquet to `s3://<bucket>/data/parquet/<source>/<date>/`, the normalizer turns
the per-operator and Vegvesen/Bysykkel/Avinor files into `normalized/`, the compactions merge small files, the
API reads everything with DuckDB. Details per component: `serverless/README.md`, `normalizer/README.md`,
`api/README.md`, `monitor/README.md`.

## 1. Prerequisites

* An AWS account and an IAM user for deployments. `serverless/deploy-policy.json` is the policy it needs
  (CloudFormation, Lambda, IAM roles and ECR for the `phd-*` names, Scheduler, logs, the SAM artifact bucket).
  Its size is about 2.2 KB without whitespace, so attach it as a **customer managed** policy (inline policies stop at 2 KB).
  Creating the policy and the ECR repositories needs an administrator once.
* Two ECR repositories, once: `aws ecr create-repository --repository-name phd-api` and `... phd-normalizer`.
* An S3 bucket (`phdprojectdata` here) and, for the collectors, credentials for the Statens vegvesen DATEX API.
* Docker (Docker Desktop on a Mac). Build for **arm64**: on Apple Silicon the images then build natively; x86_64 images
  crash under Rosetta (DuckDB needs instructions it does not emulate).
* AWS SAM CLI **installed with pip, not Homebrew**: `pip install aws-sam-cli` (it is in `requirements.txt`). The
  Homebrew build (1.138) failed with a pyOpenSSL error on `sam build`.
* Python 3.12+ locally, and `make` (the collectors are built with a Makefile).

Account limits seen here: 10 concurrent Lambda executions for the whole account (so no reserved concurrency, and
the API shares them with the collectors) and 3008 MB maximum memory. Both can be raised in Service Quotas.

## 2. Settings

Keep secrets out of the repository; pass them on the command line.

| Stack | Parameter | Meaning |
|---|---|---|
| collectors | `VegvesenUser`, `VegvesenPass` | DATEX credentials (NoEcho) |
| collectors | `AlertWebhookUrl`, `AlertKind` | where the data monitor sends alerts (see `monitor/README.md`); empty = log only |
| collectors | `MonitorMute` | check names to skip |
| collectors, normalizer | `ScheduleState` | `ENABLED` or `DISABLED` (default) for the stack's schedules |
| normalizer | `NormalizeEveryMinutes` (1), `HourlyCompactScheduleState`, `CompactScheduleState` | |
| api | `ApiKey` | at least 16 characters; every request needs it in `X-API-Key` |

`BucketName` and `ParquetPath` default to `phdprojectdata` and `data/parquet`.

## 3. Deploy

Use the profile of the deployment user (`export AWS_PROFILE=...`).

**Collectors**

    cd serverless && sam build
    sam deploy --stack-name phd-collectors --region eu-north-1 --capabilities CAPABILITY_IAM --resolve-s3 \
        --parameter-overrides VegvesenUser=<user> VegvesenPass=<password> ScheduleState=DISABLED

**Normalizer**

    cd normalizer && sam build
    R=<account>.dkr.ecr.eu-north-1.amazonaws.com/phd-normalizer
    sam deploy --stack-name phd-normalizer --region eu-north-1 --capabilities CAPABILITY_IAM --resolve-s3 \
        --image-repositories NormalizeFunction=$R --image-repositories CompactFunction=$R --image-repositories CompactHourlyFunction=$R \
        --parameter-overrides ScheduleState=DISABLED

**API**

    cd api && sam build
    sam deploy --stack-name phd-api --region eu-north-1 --capabilities CAPABILITY_IAM --resolve-s3 \
        --image-repositories ApiFunction=<account>.dkr.ecr.eu-north-1.amazonaws.com/phd-api \
        --parameter-overrides ApiKey=$(openssl rand -hex 24)

The API key cannot be read back from CloudFormation (NoEcho): note it down when you generate it. The URL is in the stack output `ApiUrl`.
To rotate the key, deploy again with a new value.

Collectors and normalizer share state through S3, so deploy order does not matter. If `sam deploy` fails, CloudFormation
rolls back; a stack left in `ROLLBACK_COMPLETE` must be deleted (`aws cloudformation delete-stack`) before the next try.

## 4. Start the schedules, one at a time

    python serverless/schedule.py --list
    python serverless/schedule.py OsloBysykkelFunctionSchedule ENABLED

Do not deploy with `ScheduleState=ENABLED` for the collectors stack: that also starts the obsolete per-operator
`EnturFunction`. A later deploy with `DISABLED` leaves schedules you started by hand as they are.
Suggested order: Bysykkel, Entur national, Entur SIRI national, Avinor, Vegvesen, then `NormalizeFunctionSchedule`,
`CompactHourlyFunctionSchedule`, and last `DataMonitorFunctionSchedule`.

## 5. Check each one

1. **Files appear.** `aws s3 ls s3://<bucket>/data/parquet/<source>/<today>/ | tail`, spaced as the cadence says
   (Bysykkel 15 s, Entur national 30 s, SIRI 60 s, Avinor and Vegvesen 3 min).
2. **No errors in the log.** `aws logs tail /aws/lambda/<function> --since 10m`.
3. **Calls succeed.** `PYTHONPATH=. python -m monitor.call_report --days 1`: the `failed` column should be near zero.
4. **Normalizer.** After a few minutes `normalized/<today>/` has files and `data/json/normalization_watermark.json` moves.
5. **Monitor.** Invoke it with `{"threshold_scale": 0.0001, "dry_run": true}`: it prints an alert for every source and sends nothing.
6. **API.** `curl -H "X-API-Key: <key>" <ApiUrl>/sources`; open `<ApiUrl>/docs`, press Authorize, try an endpoint.

Where logs and metrics live, and how to pull them out: `monitor/README.md`, section "Reading logs and metrics in CloudWatch".

## 6. Update, roll back, remove

* Update: change the code, `sam build`, `sam deploy` with the same parameters. Images are pushed to ECR (a few minutes).
* Roll back: `git checkout <tag>` (see `git tag`) and deploy again.
* Remove: `aws cloudformation delete-stack --stack-name phd-api` (and the others). S3 data is not touched. Delete the ECR repositories by hand.

## 7. Cost (rough, from list prices, not verified for eu-north-1)

Lambda is the main cost: Bysykkel and the national Entur GTFS-RT function wait inside each invocation (15 s and 30 s
polls), the Avinor cycle takes about a minute. Expect tens of USD per month for all functions with the free tier
used up. CloudWatch custom metrics add a few USD (`EMIT_METRICS=false` removes them), S3 storage grows by about
2 to 3 GB a day. Check the AWS bill after a week.

## 8. Troubleshooting (all seen while building this)

| Symptom | Cause and fix |
|---|---|
| `sam build` crashes with `module 'lib' has no attribute 'GEN_EMAIL'` | Homebrew SAM CLI. Use `pip install aws-sam-cli` |
| `sam build` hangs on an x86_64 image on Apple Silicon | Rosetta cannot run DuckDB. Use `Architectures: [arm64]` |
| `make: pip: No such file` | Put the SAM virtualenv first on `PATH` so `pip` exists |
| `Lambda does not have permission to access the ECR image` | The deploy user needs `ecr:GetRepositoryPolicy` and `ecr:SetRepositoryPolicy` (in `deploy-policy.json`) |
| `MemorySize ... less than or equal to 3008` | Account limit, request an increase or stay at 3008 |
| `ReservedConcurrentExecutions ... below its minimum value of [10]` | Account limit of 10; do not reserve concurrency |
| `InvalidClientTokenId` | The CLI uses an old or wrong profile; check `aws sts get-caller-identity` |
| A function keeps timing out | `aws logs tail` the function; look at `Status: timeout` REPORT lines |
| Avinor: `Connection aborted` for most airports | Too many parallel requests; the function polls only active airports with 2 threads and a pause |
