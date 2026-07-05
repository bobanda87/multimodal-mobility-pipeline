"""
Peak vs. off-peak comparison for Figures 3/4: mean activity level (bikes, public
transport, flights, multimodal index, delay) during weekday peak hours vs. all
other times, by analysis window (W-A1, W-A2, W-B, W-C).

Peak = weekday (Mon-Fri UTC) 07:00-09:59 and 15:00-17:59.

Reports n per group, group means, ratio (peak/off-peak) as effect size, Welch's
t-test p-value, and a 95% CI for the mean difference (Welch-Satterthwaite df) --
per MDPI reviewer request for significance testing / sample-size reporting
alongside peak/off-peak ratios.

Note: with large n (as in W-B/W-C), a ratio very close to 1.0 can still be
"significant" (p < 0.05) purely from sample size. `flag_trivial_effect` marks
these cases so the write-up can report effect size alongside p rather than
p alone.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy import stats

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC, analyze_weather_impact

PEAK_HOURS = {7, 8, 9, 15, 16, 17}

METRICS = [
    ("bysykkel", None),                      # resolved to bikes_in_use / bikes_available
    ("public_transport", "public_transport_activity"),
    ("flight", "flight_activity"),
    ("multimodal", "multimodal_activity"),
    ("delay", "mean_delay_sec"),
]

TRIVIAL_EFFECT_RATIO_BAND = 0.02  # |ratio - 1| below this = practically no effect


def _peak_flag(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        pl.col("minute").dt.weekday().alias("_wd"),   # ISO: 1=Mon..7=Sun
        pl.col("minute").dt.hour().alias("_hr"),
    ]).with_columns([
        ((pl.col("_wd") <= 5) & pl.col("_hr").is_in(list(PEAK_HOURS))).alias("is_peak")
    ])


def _welch_stats(peak: np.ndarray, offpeak: np.ndarray) -> dict:
    n_peak, n_off = len(peak), len(offpeak)
    m_peak, m_off = float(peak.mean()), float(offpeak.mean())
    v_peak, v_off = float(peak.var(ddof=1)), float(offpeak.var(ddof=1))
    ratio = m_peak / m_off if m_off != 0 else float("nan")

    t_stat, p_val = stats.ttest_ind(peak, offpeak, equal_var=False)

    se = np.sqrt(v_peak / n_peak + v_off / n_off)
    df = (v_peak / n_peak + v_off / n_off) ** 2 / (
        (v_peak / n_peak) ** 2 / (n_peak - 1) + (v_off / n_off) ** 2 / (n_off - 1)
    )
    diff = m_peak - m_off
    t_crit = stats.t.ppf(0.975, df)
    ci_diff = (diff - t_crit * se, diff + t_crit * se)

    trivial = abs(ratio - 1) < TRIVIAL_EFFECT_RATIO_BAND and p_val < 0.05

    return {
        "n_peak": n_peak, "n_offpeak": n_off,
        "mean_peak": round(m_peak, 3), "mean_offpeak": round(m_off, 3),
        "ratio": round(ratio, 3),
        "welch_t": round(float(t_stat), 3), "p": p_val,
        "ci95_diff": (round(float(ci_diff[0]), 3), round(float(ci_diff[1]), 3)),
        "flag_trivial_effect": trivial,
    }


def compute_window(name: str, start, end, read_from_s3: bool = True) -> dict:
    res = analyze_weather_impact(start_date=start, end_date=end, read_from_s3=read_from_s3)
    combined = _peak_flag(res["combined_df"])
    bikes_col = "bikes_in_use" if "bikes_in_use" in combined.columns else "bikes_available"

    row = {}
    for label, col in METRICS:
        col = bikes_col if col is None else col
        if col not in combined.columns:
            continue
        d = combined.filter(pl.col(col).is_not_null())
        peak_vals = d.filter(pl.col("is_peak"))[col].to_numpy()
        offpeak_vals = d.filter(~pl.col("is_peak"))[col].to_numpy()
        if len(peak_vals) < 2 or len(offpeak_vals) < 2:
            row[label] = {"n_peak": len(peak_vals), "n_offpeak": len(offpeak_vals), "note": "insufficient data"}
            continue
        row[label] = _welch_stats(peak_vals, offpeak_vals)
    return {"window": name, "metrics": row}


def build_table(read_from_s3: bool = True) -> list[dict]:
    return [
        compute_window(p["name"], p["start_date"], p["end_date"], read_from_s3=read_from_s3)
        for p in PERIODS_WA1_WA2_WB_WC
    ]


def print_table(rows: list[dict]) -> None:
    header = (f"{'Window':10} {'Metric':16} {'n_peak':>7} {'n_off':>7} "
              f"{'mean_peak':>12} {'mean_off':>12} {'ratio':>7} {'p':>10}  flag")
    print(header)
    print("-" * len(header))
    for row in rows:
        for label, r in row["metrics"].items():
            if "note" in r:
                print(f"{row['window']:10} {label:16} {r['n_peak']:7d} {r['n_offpeak']:7d}  {r['note']}")
                continue
            pstr = "<0.001" if r["p"] < 0.001 else f"{r['p']:.3f}"
            flag = "  <-- ratio~1, trivial effect despite p<0.05" if r["flag_trivial_effect"] else ""
            print(f"{row['window']:10} {label:16} {r['n_peak']:7d} {r['n_offpeak']:7d} "
                  f"{r['mean_peak']:12.3f} {r['mean_offpeak']:12.3f} {r['ratio']:7.3f} {pstr:>10}{flag}")


if __name__ == "__main__":
    table = build_table(read_from_s3=True)
    print_table(table)
