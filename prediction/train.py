"""
Train and evaluate the delay prediction: baselines, a LightGBM model and the ablation over data groups.

    PYTHONPATH=. python -m prediction.train --data prediction/data --out prediction/results

Models predict the CORRECTION to Entur's own expectation (target - exp_delay), so a model that learns nothing
equals the Entur baseline and every improvement is measurable against what travellers already get.

Feature groups (ablation: each configuration is trained separately and tested on the same rows):
  siri       what SIRI itself knows: Entur estimate, delay at the last passed stop, its trend, stops ahead, horizon
  calendar   hour, day of week
  network    mode, operator, line, stop position
  weather    Vegvesen weather at the nearest station (+ distance)
  bikes      Oslo Bysykkel bikes/docks within 300 m of the stop
  traffic    measured travel time (ratio to free flow, trend) of the nearest monitored road segment within 150 m
The multimodal comparison is also reported on the rows that have bike data (Oslo), where all groups exist.
"""
import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from prediction import metrics
from prediction.split import split_by_time

GROUPS = {
    "siri": ["horizon_s", "exp_delay", "last_delay", "delay_trend", "stops_ahead", "since_last_s"],
    "calendar": ["hour", "dow"],
    "network": ["mode", "operator_ref", "line_ref", "datasource", "stop_lat", "stop_lon"],
    "weather": ["wx_air_temp", "wx_road_temp", "wx_precip", "wx_wind", "wx_humidity", "wx_dist_km"],
    "bikes": ["bk_bikes", "bk_docks", "bk_bikes_delta", "bk_stations"],
    "traffic": ["tt_ratio", "tt_trend", "tt_seconds", "tt_dist_m"],
}
CATEGORICAL = ["mode", "operator_ref", "line_ref", "datasource"]
CONFIGS = {
    "siri": ["siri", "calendar", "network"],
    "siri+weather": ["siri", "calendar", "network", "weather"],
    "siri+bikes": ["siri", "calendar", "network", "bikes"],
    "siri+traffic": ["siri", "calendar", "network", "traffic"],
    "siri+weather+bikes": ["siri", "calendar", "network", "weather", "bikes"],
    "siri+all": ["siri", "calendar", "network", "weather", "bikes", "traffic"],
}
PARAMS = dict(objective="l1", learning_rate=0.05, n_estimators=2000, num_leaves=63, min_child_samples=100,
              subsample=0.8, subsample_freq=1, colsample_bytree=0.8, cat_smooth=50, verbose=-1, n_jobs=6,
              random_state=0)


def load(data_dir: str) -> pd.DataFrame:
    files = sorted(Path(data_dir).glob("features_*.parquet"))
    if not files:
        raise SystemExit(f"no features_*.parquet in {data_dir}; run prediction.build_dataset first")
    df = duckdb.sql("SELECT * FROM read_parquet([" + ",".join(f"'{f}'" for f in files) + "], union_by_name=true)").df()
    return df.sort_values("aimed").reset_index(drop=True)


def feature_columns(groups: list[str]) -> list[str]:
    return [c for g in groups for c in GROUPS[g]]


def to_matrix(df: pd.DataFrame, cols: list[str], categories: dict[str, pd.Index]) -> pd.DataFrame:
    x = df[cols].copy()
    for c in CATEGORICAL:
        if c in x:  # categories fixed on the training set; unseen values become missing
            x[c] = pd.Categorical(x[c], categories=categories[c])
    return x


def historical_mean(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Mean delay of the same line, hour of day and horizon in the training period (falls back to coarser keys)."""
    out = np.full(len(test), np.nan)
    for keys in (["line_ref", "hour", "horizon_s"], ["mode", "hour", "horizon_s"], ["horizon_s"]):
        means = train.groupby(keys)["target"].mean().rename("m").reset_index()
        m = test[keys].merge(means, on=keys, how="left")["m"].values
        out = np.where(np.isnan(out), m, out)
    return out


def fit_predict(train, val, test, groups, params=PARAMS):
    import lightgbm as lgb
    cols = feature_columns(groups)
    cats = {c: pd.Index(train[c].dropna().unique()) for c in CATEGORICAL if c in cols}
    y = lambda d: (d["target"] - d["exp_delay"]).values
    model = lgb.LGBMRegressor(**params)
    model.fit(to_matrix(train, cols, cats), y(train), eval_set=[(to_matrix(val, cols, cats), y(val))],
              callbacks=[lgb.early_stopping(50, verbose=False)])
    return model, test["exp_delay"].values + model.predict(to_matrix(test, cols, cats)), (cols, cats)


def run(df: pd.DataFrame, out: Path, params=PARAMS, shap_rows: int = 5000, embargo_s: int = 3 * 3600) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    df = df[df["exp_delay"].notna()].reset_index(drop=True)
    df["bk_bikes_delta"] = df["bk_bikes"] - df["bk_bikes_prev"]
    tr, va, te, bounds = split_by_time(df, embargo_s=embargo_s)
    train, val, test = df.iloc[tr], df.iloc[va], df.iloc[te].reset_index(drop=True)
    print(f"rows: train {len(train)}, val {len(val)}, test {len(test)}; "
          f"trips test {test.groupby(['datasource', 'trip_id', 'data_frame_ref']).ngroups}")

    preds = {
        "entur": test["exp_delay"].values.astype(float),
        "persistence": np.where(test["last_delay"].notna(), test["last_delay"], test["exp_delay"]).astype(float),
        "zero": np.zeros(len(test)),
        "historical_mean": historical_mean(train, test),
    }
    models = {}
    for name, groups in CONFIGS.items():
        m, p, meta = fit_predict(train, val, test, groups, params)
        preds[name], models[name] = p, (m, meta)
        print(f"{name}: best iteration {m.best_iteration_}")

    result = {"split": {"train": len(train), "val": len(val), "test": len(test), "bounds": [float(b) for b in bounds]}}
    overall = metrics.table(test, preds)
    overall.to_csv(out / "metrics_overall.csv", index=False)
    metrics.table(test, preds, ["horizon_s"]).to_csv(out / "metrics_by_horizon.csv", index=False)
    metrics.table(test, preds, ["mode"]).to_csv(out / "metrics_by_mode.csv", index=False)
    oslo = test["bk_stations"].notna().values
    if oslo.any():
        sub = test[oslo].reset_index(drop=True)
        metrics.table(sub, {k: v[oslo] for k, v in preds.items()}).to_csv(out / "metrics_bike_area.csv", index=False)
    boot = {}
    for name in CONFIGS:
        boot[f"{name}_vs_entur"] = metrics.paired_bootstrap(test, preds[name], preds["entur"])
    for name in ("siri+weather", "siri+bikes", "siri+traffic", "siri+weather+bikes", "siri+all"):
        boot[f"{name}_vs_siri"] = metrics.paired_bootstrap(test, preds[name], preds["siri"])
    result["bootstrap"] = boot
    best = min(CONFIGS, key=lambda n: overall.loc[overall.model == n, "mae"].iloc[0])
    result["best_config"] = best
    try:
        import shap
        m, (cols, cats) = models[best]
        sample = test.sample(min(shap_rows, len(test)), random_state=0)
        vals = shap.TreeExplainer(m).shap_values(to_matrix(sample, cols, cats))
        pd.DataFrame({"feature": cols, "mean_abs_shap": np.abs(vals).mean(axis=0)}).sort_values(
            "mean_abs_shap", ascending=False).to_csv(out / "shap_importance.csv", index=False)
    except Exception as e:  # SHAP is an add-on; the numbers above do not depend on it
        print("SHAP skipped:", type(e).__name__, e)
    models[best][0].booster_.save_model(str(out / "model_best.txt"))
    (out / "summary.json").write_text(json.dumps(result, indent=2))
    print(overall.to_string(index=False))
    print(json.dumps(boot, indent=1))
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="prediction/data")
    ap.add_argument("--out", default="prediction/results")
    ap.add_argument("--embargo-hours", type=float, default=3.0,
                    help="trips starting this long before a split boundary are dropped from the earlier set")
    args = ap.parse_args()
    run(load(args.data), Path(args.out), embargo_s=int(args.embargo_hours * 3600))


if __name__ == "__main__":
    main()
