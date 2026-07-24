"""
Two variants of the multimodal activity index, computed side by side:

- "bike_pt"        -- bikes + public transport only (new primary definition)
- "bike_pt_flight" -- bikes + public transport + flights (original definition,
                       now reported as a robustness check)

Both use the identical min-max-per-window-then-sum procedure already used for
`multimodal_activity` in analyze_weather_impact.py (missing values filled with
0 before scaling; each component independently scaled to [0, 1] over the
window's own range; scaled components summed, unweighted). Only the set of
component columns going into the sum differs between variants -- everything
else about the method is unchanged, so results are directly comparable.

For each of the four analysis windows (W-A1, W-A2, W-B, W-C) and each variant,
this script reports:
  1. Peak vs. off-peak comparison (Welch's t-test, same procedure as Table 4 /
     peak_offpeak_table.py)
  2. Correlation with road-surface temperature (Pearson r, n, p, 95% CI, same
     procedure as Table 2 / delay_robustness_table.py)
"""

from __future__ import annotations

import polars as pl

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC, analyze_weather_impact
from analysis.delay_robustness_table import _pearson_stats
from analysis.peak_offpeak_table import PEAK_HOURS, _peak_flag, _welch_stats

VARIANTS = {
    "bike_pt": ["bikes_in_use", "public_transport_activity"],
    "bike_pt_flight": ["bikes_in_use", "public_transport_activity", "flight_activity"],
}


def compute_multimodal_variant(combined: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Replicates the exact fill-null -> min-max-per-window -> sum logic used
    for `multimodal_activity` in analyze_weather_impact.py, restricted to `cols`."""
    cols = [c for c in cols if c in combined.columns]
    if not cols:
        return combined.with_columns(pl.lit(None).cast(pl.Float64).alias("_variant"))

    out = combined.with_columns([
        pl.col(c).fill_null(0).alias(f"_fill_{c}") for c in cols
    ])
    scales = []
    for c in cols:
        lo = out.select(pl.col(f"_fill_{c}").min())[0, 0]
        hi = out.select(pl.col(f"_fill_{c}").max())[0, 0]
        lo = lo if lo is not None else 0.0
        hi = hi if hi is not None else 0.0
        if hi > lo:
            scales.append(((pl.col(f"_fill_{c}") - lo) / (hi - lo)).alias(f"_s_{c}"))
        else:
            scales.append(pl.lit(0.0).alias(f"_s_{c}"))
    out = out.with_columns(scales)
    out = out.with_columns(
        pl.sum_horizontal([f"_s_{c}" for c in cols]).alias("_variant")
    )
    return out.drop([f"_fill_{c}" for c in cols] + [f"_s_{c}" for c in cols])


def analyze_window(name: str, start, end, read_from_s3: bool = True) -> dict:
    res = analyze_weather_impact(start_date=start, end_date=end, read_from_s3=read_from_s3)
    combined = res["combined_df"]
    bikes_col = "bikes_in_use" if "bikes_in_use" in combined.columns else "bikes_available"

    results = {}
    for variant_name, cols in VARIANTS.items():
        cols = [bikes_col if c == "bikes_in_use" else c for c in cols]
        variant_df = compute_multimodal_variant(combined, cols)
        flagged = _peak_flag(variant_df)

        # --- peak vs off-peak ---
        d = flagged.filter(pl.col("_variant").is_not_null())
        peak_vals = d.filter(pl.col("is_peak"))["_variant"].to_numpy()
        offpeak_vals = d.filter(~pl.col("is_peak"))["_variant"].to_numpy()
        peak_stats = (_welch_stats(peak_vals, offpeak_vals)
                     if len(peak_vals) > 1 and len(offpeak_vals) > 1 else None)

        # --- correlation with road temperature ---
        d2 = variant_df.filter(
            pl.col("_variant").is_not_null() & pl.col("road_temperature").is_not_null()
        )
        temp_stats = (_pearson_stats(d2["road_temperature"].to_numpy(), d2["_variant"].to_numpy())
                     if d2.height > 3 else None)

        results[variant_name] = {"peak_offpeak": peak_stats, "temp_corr": temp_stats}
    return {"window": name, "variants": results}


def build_report(read_from_s3: bool = True) -> list[dict]:
    return [
        analyze_window(p["name"], p["start_date"], p["end_date"], read_from_s3=read_from_s3)
        for p in PERIODS_WA1_WA2_WB_WC
    ]


def print_peak_offpeak_table(report: list[dict]) -> None:
    header = (f"{'Window':10} {'Variant':16} {'n_peak':>7} {'n_off':>7} "
             f"{'mean_peak':>10} {'mean_off':>10} {'ratio':>7} {'p':>10}")
    print(header)
    print("-" * len(header))
    for w in report:
        for variant, r in w["variants"].items():
            s = r["peak_offpeak"]
            if s is None:
                print(f"{w['window']:10} {variant:16} insufficient data")
                continue
            pstr = "<0.001" if s["p"] < 0.001 else f"{s['p']:.3f}"
            print(f"{w['window']:10} {variant:16} {s['n_peak']:7d} {s['n_offpeak']:7d} "
                 f"{s['mean_peak']:10.3f} {s['mean_offpeak']:10.3f} {s['ratio']:7.3f} {pstr:>10}")


def print_weather_corr_table(report: list[dict]) -> None:
    header = f"{'Window':10} {'Variant':16} {'n':>6} {'r':>8} {'p':>10} {'95% CI':>18}"
    print(header)
    print("-" * len(header))
    for w in report:
        for variant, r in w["variants"].items():
            s = r["temp_corr"]
            if s is None:
                print(f"{w['window']:10} {variant:16} insufficient data")
                continue
            pstr = "<0.001" if s["p"] < 0.001 else f"{s['p']:.3f}"
            print(f"{w['window']:10} {variant:16} {s['n']:6d} {s['r']:8.3f} {pstr:>10} "
                 f"[{s['ci95'][0]:.3f}, {s['ci95'][1]:.3f}]")


if __name__ == "__main__":
    report = build_report(read_from_s3=True)
    print("\n=== Peak vs. off-peak (Welch t-test) ===")
    print_peak_offpeak_table(report)
    print("\n=== Correlation with road-surface temperature (Pearson) ===")
    print_weather_corr_table(report)
