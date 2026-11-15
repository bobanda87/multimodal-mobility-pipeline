"""
Nightly job (runs as an ECS Fargate task): tables for the new days, a challenger model, comparison with the model in
use (the champion), promotion only if the challenger is clearly better, and metrics for monitoring.

S3 layout (a new prefix, nothing the collectors or the API read):
  prediction/features/features_<local day>.parquet     the feature tables, one per complete day
  prediction/models/<UTC stamp>/model.txt + report.json every challenger's report, the model file only if promoted
  prediction/champion.json                              {"model_key", "trained_at", "config", "holdout_mae", ...}

Settings (environment): FIRST_DAY (earliest day used, default 2026-11-15: before it the data is not complete or lacks columns),
WINDOW_DAYS (default 35), HOLDOUT_DAYS (3), MIN_TRAIN_DAYS (7), CONFIG (siri+all).
A day is built only when it is complete: its last target needs three more hours of follow-up data.

    PYTHONPATH=. python -m prediction.nightly            # what the task runs
"""
import json
import os
import shutil
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import lightgbm as lgb
import numpy as np
import pandas as pd

from prediction import build_dataset, metrics, train
from prediction.s3db import BUCKET, REGION, connect
from prediction.split import split_by_time

OSLO = ZoneInfo("Europe/Oslo")
PREFIX = "prediction"
FIRST_DAY = os.getenv("FIRST_DAY", "2026-11-15")
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "35"))
HOLDOUT_DAYS = int(os.getenv("HOLDOUT_DAYS", "3"))
MIN_TRAIN_DAYS = int(os.getenv("MIN_TRAIN_DAYS", "7"))
CONFIG = os.getenv("CONFIG", "siri+all")
s3 = boto3.client("s3", region_name=REGION)


def emit(metrics_: dict[str, float], dimensions: dict[str, str] | None = None) -> None:
    """One CloudWatch EMF line (namespace Prediction); no extra permission needed beyond the log group."""
    dims = dimensions or {}
    print(json.dumps({"_aws": {"Timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                               "CloudWatchMetrics": [{"Namespace": "Prediction", "Dimensions": [list(dims)] if dims else [[]],
                                                      "Metrics": [{"Name": k, "Unit": "None"} for k in metrics_]}]},
                      **dims, **metrics_}), flush=True)


def _exists(key: str) -> bool:
    return bool(s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=1).get("KeyCount"))


def build_missing_days(workdir: Path, now: datetime) -> list[str]:
    """Build and upload the feature tables of the complete days in the window that have none yet."""
    con = connect(memory_limit=os.getenv("DUCK_MEMORY", "12GB"), threads=int(os.getenv("DUCK_THREADS", "4")))
    built = []
    today = now.astimezone(OSLO).date()
    for back in range(WINDOW_DAYS, 0, -1):
        day = today - timedelta(days=back)
        start = datetime(day.year, day.month, day.day, tzinfo=OSLO)
        end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=OSLO)
        key = f"{PREFIX}/features/features_{day:%Y-%m-%d}.parquet"
        if f"{day:%Y-%m-%d}" < FIRST_DAY or _exists(key) or now < end + timedelta(hours=build_dataset.LOOKAHEAD_H):
            continue
        rows = build_dataset.build(con, start, end, until=now)
        if not rows:
            print(f"{day}: no data")
            continue
        local = workdir / f"features_{day:%Y-%m-%d}.parquet"
        con.execute(f"COPY features TO '{local}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        s3.upload_file(str(local), BUCKET, key)
        local.unlink()
        built.append(f"{day:%Y-%m-%d}")
        print(f"{day}: {rows} rows -> s3://{BUCKET}/{key}", flush=True)
    con.close()
    return built


def download_window(workdir: Path, now: datetime) -> list[str]:
    days = []
    today = now.astimezone(OSLO).date()
    for back in range(WINDOW_DAYS, 0, -1):
        day = f"{today - timedelta(days=back):%Y-%m-%d}"
        key = f"{PREFIX}/features/features_{day}.parquet"
        if day >= FIRST_DAY and _exists(key):
            s3.download_file(BUCKET, key, str(workdir / f"features_{day}.parquet"))
            days.append(day)
    return days


def champion() -> tuple[dict | None, lgb.Booster | None]:
    try:
        meta = json.loads(s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}/champion.json")["Body"].read())
        body = s3.get_object(Bucket=BUCKET, Key=meta["model_key"])["Body"].read().decode()
        return meta, lgb.Booster(model_str=body)
    except s3.exceptions.NoSuchKey:
        return None, None


def predict_champion(booster: lgb.Booster, df: pd.DataFrame) -> np.ndarray:
    x = df[booster.feature_name()].copy()
    for c in train.CATEGORICAL:
        if c in x:  # the model file carries the categories it was trained with
            x[c] = x[c].astype("category")
    return df["exp_delay"].values + booster.predict(x)


def run(now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    work = Path(tempfile.mkdtemp(prefix="nightly_"))
    try:
        built = build_missing_days(work, now)
        days = download_window(work, now)
        report = {"run_at": now.isoformat(), "built_days": built, "days": days, "config": CONFIG}
        if len(days) < MIN_TRAIN_DAYS + HOLDOUT_DAYS:
            report["status"] = f"skipped: {len(days)} days, need {MIN_TRAIN_DAYS + HOLDOUT_DAYS}"
            print(report["status"])
            emit({"Skipped": 1})
            return report
        df = train.load(str(work))
        df = df[df["exp_delay"].notna()].reset_index(drop=True)
        df["bk_bikes_delta"] = df["bk_bikes"] - df["bk_bikes_prev"]
        holdout_days = days[-HOLDOUT_DAYS:]
        hold = df[df["local_date"].isin(holdout_days)].reset_index(drop=True)
        rest = df[~df["local_date"].isin(holdout_days)].reset_index(drop=True)
        tr, va, _, _ = split_by_time(rest, fractions=(0.85, 0.15, 0.0))
        model, pred, _ = train.fit_predict(rest.iloc[tr], rest.iloc[va], hold, train.CONFIGS[CONFIG])
        preds = {"entur": hold["exp_delay"].values.astype(float), "challenger": pred}
        meta, booster = champion()
        if booster is not None:
            preds["champion"] = predict_champion(booster, hold)
        overall = metrics.table(hold, preds).set_index("model")
        report.update({"train_rows": len(rest), "holdout_rows": len(hold), "holdout_days": holdout_days,
                       "mae": overall["mae"].to_dict()})
        by_h = metrics.table(hold, preds, ["horizon_s"])
        for _, r in by_h.iterrows():
            emit({"Mae": float(r["mae"])}, {"Model": r["model"], "Horizon": str(int(r["horizon_s"]))})
        reference = "champion" if "champion" in preds else "entur"
        boot = metrics.paired_bootstrap(hold, preds["challenger"], preds[reference])
        report["challenger_vs_" + reference] = boot
        promote = boot["ci_high"] < 0  # better than what is in use, with the trip-level interval below zero
        report["promoted"] = bool(promote)
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        if promote:
            key = f"{PREFIX}/models/{stamp}/model.txt"
            s3.put_object(Bucket=BUCKET, Key=key, Body=model.booster_.model_to_string().encode())
            s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/champion.json", Body=json.dumps({
                "model_key": key, "trained_at": now.isoformat(), "config": CONFIG, "train_days": days[:-HOLDOUT_DAYS],
                "holdout_mae": report["mae"]["challenger"], "entur_mae": report["mae"]["entur"]}).encode())
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/models/{stamp}/report.json", Body=json.dumps(report, indent=2).encode())
        emit({"Promoted": int(promote), "TrainRows": len(rest),
              "GainVsEntur": report["mae"]["entur"] - report["mae"][reference if reference == "champion" else "challenger"]})
        print(json.dumps(report, indent=2))
        return report
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> None:
    """Entry point of the task: a failure is a logged ERROR line with the traceback, a metric and a non-zero exit."""
    try:
        run()
    except Exception:
        traceback.print_exc()
        print("ERROR: nightly prediction job failed", flush=True)
        emit({"Failed": 1})
        sys.exit(1)
    emit({"Failed": 0})


if __name__ == "__main__":
    main()
