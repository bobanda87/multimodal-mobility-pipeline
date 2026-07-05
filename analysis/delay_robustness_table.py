"""
Table 3 — Pearson correlation between road surface temperature and mean public
transport delay under alternative treatments of the delay series (raw / winsorized
1-99% / log1p), by analysis window (W-A1, W-A2, W-B, W-C).

Also reports sample size (n), p-value, and 95% CI (Fisher z-transform) for each
treatment, per MDPI reviewer request for significance testing / sample-size
reporting alongside correlation coefficients.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy import stats

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC, analyze_weather_impact


def winsorize(s: pl.Series, lower: float = 0.01, upper: float = 0.99) -> pl.Series:
    lo = s.quantile(lower, interpolation="linear")
    hi = s.quantile(upper, interpolation="linear")
    return s.clip(lower_bound=lo, upper_bound=hi)


def _pearson_stats(x: np.ndarray, y: np.ndarray) -> dict:
    n = len(x)
    r = float(np.corrcoef(x, y)[0, 1])
    t = r * np.sqrt((n - 2) / (1 - r**2))
    p = 2 * (1 - stats.t.cdf(abs(t), df=n - 2))
    z = np.arctanh(r)
    se = 1 / np.sqrt(n - 3)
    lo, hi = np.tanh(z - 1.96 * se), np.tanh(z + 1.96 * se)
    return {"n": n, "r": round(r, 3), "p": p, "ci95": (round(float(lo), 3), round(float(hi), 3))}


def compute_window(name: str, start, end, read_from_s3: bool = True) -> dict:
    res = analyze_weather_impact(start_date=start, end_date=end, read_from_s3=read_from_s3)
    combined = res["combined_df"]
    d = combined.filter(
        pl.col("road_temperature").is_not_null() & pl.col("mean_delay_sec").is_not_null()
    ).select("road_temperature", "mean_delay_sec")

    temp = d["road_temperature"].to_numpy()
    delay = d["mean_delay_sec"].to_numpy()
    delay_winsor = winsorize(d["mean_delay_sec"]).to_numpy()
    delay_log = np.log1p(np.clip(delay, a_min=0, a_max=None))

    return {
        "window": name,
        "raw": _pearson_stats(temp, delay),
        "winsor": _pearson_stats(temp, delay_winsor),
        "log1p": _pearson_stats(temp, delay_log),
    }


def build_table3(read_from_s3: bool = True) -> list[dict]:
    return [
        compute_window(p["name"], p["start_date"], p["end_date"], read_from_s3=read_from_s3)
        for p in PERIODS_WA1_WA2_WB_WC
    ]


def print_table3(rows: list[dict]) -> None:
    header = f"{'Window':10} {'Treatment':10} {'n':>6} {'r':>8} {'p':>10} {'95% CI':>18}"
    print(header)
    print("-" * len(header))
    for row in rows:
        for treatment in ("raw", "winsor", "log1p"):
            s = row[treatment]
            pstr = "<0.001" if s["p"] < 0.001 else f"{s['p']:.3f}"
            print(f"{row['window']:10} {treatment:10} {s['n']:6d} {s['r']:8.3f} {pstr:>10} "
                  f"[{s['ci95'][0]:.3f}, {s['ci95'][1]:.3f}]")


if __name__ == "__main__":
    table = build_table3(read_from_s3=True)
    print_table3(table)
