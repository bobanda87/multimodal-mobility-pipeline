# Delay prediction (multimodal)

Predicts the arrival delay of public transport at upcoming stops from what SIRI ET knows `horizon` seconds before
the aimed arrival, with weather (Vegvesen) and Oslo Bysykkel as additional modalities. The plan and the scientific
requirements are in `../PREDICTION_PLAN.md`; all of this runs locally, on the data in S3.

    # one-off reference tables (stops/routes from the Entur GTFS export, weather stations from Vegvesen, bike stations)
    PYTHONPATH=. python -m prediction.make_reference stops <gtfs-norway.zip>
    PYTHONPATH=. python -m prediction.make_reference weather
    PYTHONPATH=. python -m prediction.make_reference bikes

    # 1. the table: one Parquet per local day in prediction/data/ (about 15 s per day, reads S3 directly with DuckDB)
    PYTHONPATH=. python -m prediction.build_dataset --start 2026-11-15 --end 2026-11-30

    # 2. baselines, LightGBM, ablation, bootstrap, SHAP -> prediction/results/
    PYTHONPATH=. python -m prediction.train

Memory: DuckDB is limited to 9 GB (`--memory`), so a 16 GB laptop works; nothing but the final table is stored locally
(`prediction/data/` and `prediction/results/` are not in git).

## What one row is
(trip, upcoming stop, horizon 5/15/30 min). `target` is the delay that SIRI later reports as recorded;
`exp_delay` is what Entur expected at prediction time (the baseline to beat); `last_delay` is the delay at the last stop
already passed. Every feature comes from a state fetched at or before the prediction time (ASOF joins on `fetched_at`),
stops already passed are dropped, bikes use the last completed 5 minute bin and weather its newest observation (max 2 h).
`tests/test_prediction_features.py` checks this against a hand-built trip.

## Model and evaluation
* The model predicts the correction `target - exp_delay`, so "learns nothing" equals the Entur baseline.
* Baselines: Entur's estimate, persistence of the last delay, zero, historical mean (line, hour, horizon).
* Split: by trip start time into 60/20/20 with a 3 h embargo before each boundary, never random, no trip in two sets.
* Ablation: `siri`, `+weather`, `+bikes`, `+weather+bikes`, all tested on the same rows; the bike columns exist only
  near Bysykkel stations, so `metrics_bike_area.csv` repeats the comparison there.
* Metrics: MAE, RMSE, median, p90, share within 60/180 s, bias; per horizon and mode; paired trip-level bootstrap
  (1000 resamples, 95 %) of the MAE difference against Entur and against the SIRI-only model; SHAP on a test sample.

## Not in this version
Road travel times (Vegvesen) and flights (Avinor) are collected but not joined: the travel time sites have no
coordinates in our data, and flights only matter near the airport. They are the next modalities.

## Do we retrain every day?
No. LightGBM has no "fine-tuning" in the neural network sense; a new model is trained from scratch in minutes.
* **For the paper** one fixed experiment: a frozen date range, one split, results reproducible from the tag.
* **For a running service** retrain on a schedule (weekly is a sensible start) on a rolling window of the last
  4 to 6 weeks, and compare it on the newest days against the model in use before replacing it. Daily retraining gains
  little unless the monitoring below shows drift.
* **Monitoring the model** (to add when it is served): MAE of the last days against the Entur baseline, per horizon;
  when the gain over Entur shrinks or the error rises, retrain.

## Nightly job on ECS Fargate (`nightly.py`, `Dockerfile`, `template.yaml`)
Every night at 04:00 (Europe/Oslo) a Fargate task builds the feature tables of the new complete days (to
`s3://<bucket>/prediction/features/`), trains a challenger on the last 35 days except the newest 3, tests it and the
model in use (the champion) on those 3 days, and promotes the challenger only if the paired trip-level bootstrap puts
it clearly below the champion's error. Reports go to `prediction/models/<stamp>/report.json`, the champion is named in
`prediction/champion.json`, metrics go to CloudWatch (namespace `Prediction`: Mae per model and horizon, Promoted,
GainVsEntur). With fewer than 10 complete days it only logs that it skipped. The task role can write only below `prediction/`.

Cost estimate (not measured): 4 vCPU and 16 GB for about 20 minutes a night is on the order of 0.1 USD a day.
EC2 is not needed; ECR only stores the image.

    # one-off: repository, then build and push (arm64), then the stack
    aws ecr create-repository --repository-name phd-prediction
    docker buildx build --platform linux/arm64 -f prediction/Dockerfile -t <acct>.dkr.ecr.eu-north-1.amazonaws.com/phd-prediction:<tag> --push .
    aws cloudformation deploy --stack-name phd-prediction --template-file prediction/template.yaml --capabilities CAPABILITY_IAM \
        --parameter-overrides ImageUri=<acct>.dkr.ecr.eu-north-1.amazonaws.com/phd-prediction:<tag> SubnetIds=<subnet-a>,<subnet-b>
    # first time only, with an administrator (not the deployer): the ECS service-linked role, else RunTask fails with
    # "Unable to assume the service linked role":  aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com
    # the schedule is created DISABLED; run once by hand (Outputs: RunNow), read /ecs/phd-prediction, then ScheduleState=ENABLED

### Reading the logs and errors of the job
The task writes to CloudWatch Logs, group `/ecs/phd-prediction` (90 days), stream prefix `nightly/`. A failure is an
`ERROR: nightly prediction job failed` line with the traceback above it, a `Failed=1` metric and exit code 1.

    aws logs tail /ecs/phd-prediction --since 3h                       # the last run
    aws logs filter-log-events --log-group-name /ecs/phd-prediction --start-time $(( ($(date +%s)-86400)*1000 )) \
        --filter-pattern '?ERROR ?Traceback ?skipped' --query 'events[].message' --output text
    aws ecs list-tasks --cluster <Cluster output> --desired-status STOPPED             # recent runs
    aws ecs describe-tasks --cluster <Cluster output> --tasks <task arn> --query 'tasks[].[stoppedReason,containers[0].exitCode]'

Reasons a task fails without any log line (image cannot be pulled, out of memory, no capacity) are only in
`stoppedReason` of `describe-tasks` (`OutOfMemoryError: Container killed due to memory usage` means a larger `Memory`).
Lines that are not errors: `skipped: N days, need M` (too little data yet) and the JSON report at the end of a run.
Metrics (console: CloudWatch -> All metrics -> `Prediction`): `Failed`, `Skipped`, `Promoted`, `TrainRows`, `GainVsEntur`,
and `Mae` per `Model` and `Horizon`. Where the other logs are read: `monitor/README.md`.
