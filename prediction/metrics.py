"""Error metrics, per group, and a trip-level bootstrap for differences between models."""
import numpy as np
import pandas as pd

TRIP_KEY = ["datasource", "trip_id", "data_frame_ref"]


def summary(err: np.ndarray) -> dict:
    """err = predicted - actual delay in seconds."""
    a = np.abs(err)
    return {
        "n": int(len(a)), "mae": float(a.mean()), "rmse": float(np.sqrt((err ** 2).mean())),
        "median_ae": float(np.median(a)), "p90_ae": float(np.quantile(a, 0.9)),
        "within_60s": float((a <= 60).mean()), "within_180s": float((a <= 180).mean()),
        "bias": float(err.mean()),
    }


def table(df: pd.DataFrame, preds: dict[str, np.ndarray], by: list[str] | None = None) -> pd.DataFrame:
    """One row per (group, model); `df` has the column `target`."""
    rows = []
    groups = [((), df.index)] if not by else [(k if isinstance(k, tuple) else (k,), g.index)
                                              for k, g in df.groupby(by)]
    pos = {i: n for n, i in enumerate(df.index)}
    for key, idx in groups:
        ix = np.array([pos[i] for i in idx])
        for name, p in preds.items():
            ok = ~np.isnan(p[ix])
            if ok.sum() == 0:
                continue
            row = dict(zip(by or [], key)) if by else {}
            rows.append({**row, "model": name, **summary(p[ix][ok] - df["target"].values[ix][ok])})
    return pd.DataFrame(rows)


def paired_bootstrap(df: pd.DataFrame, a: np.ndarray, b: np.ndarray, n_boot: int = 1000, seed: int = 0) -> dict:
    """
    Difference in MAE (a minus b, negative = a is better) with a 95% interval, resampling whole trips because
    the stops of one trip are strongly correlated.
    """
    d = np.abs(a - df["target"].values) - np.abs(b - df["target"].values)
    ok = ~np.isnan(d)
    codes = pd.factorize(pd.MultiIndex.from_frame(df[TRIP_KEY]))[0][ok]
    d = d[ok]
    n_trips = codes.max() + 1
    sums, counts = np.bincount(codes, weights=d, minlength=n_trips), np.bincount(codes, minlength=n_trips)
    rng = np.random.default_rng(seed)
    boots = np.empty(n_boot)
    for i in range(n_boot):
        pick = rng.integers(0, n_trips, n_trips)
        boots[i] = sums[pick].sum() / counts[pick].sum()
    lo, hi = np.quantile(boots, [0.025, 0.975])
    return {"mae_diff": float(d.mean()), "ci_low": float(lo), "ci_high": float(hi), "trips": int(n_trips)}
