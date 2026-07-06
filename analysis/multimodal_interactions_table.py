"""
Pairwise Pearson correlations between transport modes (bike-share, public
transport, flights, mean PT delay) by analysis window (W-A1, W-A2, W-B, W-C).

Addresses R1's request for "a more advanced assessment of multimodal
interactions": whether modes move together (complementary) or in opposite
directions (substitution) under a given window's conditions, and whether that
relationship changes across windows -- consistent with this study's broader
finding that mobility relationships are context-dependent.

Note: multimodal_activity is deliberately excluded here since it is a
min-max-summed composite of bike/PT/flight (see analyze_weather_impact.py);
correlating it against its own components would be circular.
"""

from __future__ import annotations

from itertools import combinations

import numpy as np
import polars as pl
from scipy import stats

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC, analyze_weather_impact

MODE_COLS = {
    "bike": None,  # resolved to bikes_in_use / bikes_available
    "public_transport": "public_transport_activity",
    "flight": "flight_activity",
    "delay": "mean_delay_sec",
}


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
    bikes_col = "bikes_in_use" if "bikes_in_use" in combined.columns else "bikes_available"

    cols = {}
    for label, col in MODE_COLS.items():
        col = bikes_col if col is None else col
        if col in combined.columns:
            cols[label] = col

    pairs = {}
    for a, b in combinations(cols.keys(), 2):
        d = combined.filter(pl.col(cols[a]).is_not_null() & pl.col(cols[b]).is_not_null())
        if d.height > 3:
            x = d[cols[a]].to_numpy()
            y = d[cols[b]].to_numpy()
            pairs[f"{a}~{b}"] = _pearson_stats(x, y)
        else:
            pairs[f"{a}~{b}"] = {"n": d.height, "note": "insufficient data"}
    return {"window": name, "pairs": pairs}


def build_table(read_from_s3: bool = True) -> list[dict]:
    return [
        compute_window(p["name"], p["start_date"], p["end_date"], read_from_s3=read_from_s3)
        for p in PERIODS_WA1_WA2_WB_WC
    ]


def print_table(rows: list[dict]) -> None:
    header = f"{'Window':10} {'Pair':28} {'n':>6} {'r':>8} {'p':>10} {'95% CI':>18}"
    print(header)
    print("-" * len(header))
    for row in rows:
        for pair, s in row["pairs"].items():
            if "note" in s:
                print(f"{row['window']:10} {pair:28} {s['n']:6d}  {s['note']}")
                continue
            pstr = "<0.001" if s["p"] < 0.001 else f"{s['p']:.3f}"
            print(f"{row['window']:10} {pair:28} {s['n']:6d} {s['r']:8.3f} {pstr:>10} "
                  f"[{s['ci95'][0]:.3f}, {s['ci95'][1]:.3f}]")


if __name__ == "__main__":
    table = build_table(read_from_s3=True)
    print_table(table)
