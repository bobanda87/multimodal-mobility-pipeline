"""
Figure 3 and Figure 4 for Section 4.1 (Spatio-Temporal Multimodal Mobility
Patterns) -- reconstructed from the paper text description, since the original
plotting code was not present in the repository (lost on another machine).

Figure 3: heatmap of peak/off-peak activity ratio by mode (rows) and period
(columns), matching the reference image style (YlOrRd colormap, annotated
cells).

Figure 4: "minute-weighted" pooled peak/off-peak ratio per mode, with a
period-range whisker (min-max ratio across the four windows). "Minute-weighted"
means peak (and off-peak) values are pooled across all four windows before
computing the ratio -- i.e. pooled_mean_peak = sum(n_i * mean_peak_i) / sum(n_i)
across windows, NOT a simple average of the four per-window ratios. This
formula was reverse-engineered from the reference figure's reported values
(PT: 1.22, Bikes: 1.06) and reproduces them almost exactly; see validation
notes in the analysis session.

The "Multimodal" mode reflects the current (bike+PT only) index definition,
per the reviewer-driven change; the bike+PT+flight variant is also computed
for reference/validation against the original figure (which reported 1.32
for the old, flight-inclusive definition).
"""

from __future__ import annotations

import numpy as np
import polars as pl

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC, analyze_weather_impact
from analysis.multimodal_index_variants import VARIANTS, compute_multimodal_variant
from analysis.peak_offpeak_table import PEAK_HOURS, _peak_flag

MODE_COLUMNS = {
    "Bikes": None,  # resolved to bikes_in_use / bikes_available
    "PT": "public_transport_activity",
    "Flight": "flight_activity",
}


def _peak_offpeak_raw(df: pl.DataFrame, col: str) -> dict | None:
    d = df.filter(pl.col(col).is_not_null())
    peak_vals = d.filter(pl.col("is_peak"))[col].to_numpy()
    offpeak_vals = d.filter(~pl.col("is_peak"))[col].to_numpy()
    if len(peak_vals) < 2 or len(offpeak_vals) < 2:
        return None
    return {
        "n_peak": len(peak_vals), "n_offpeak": len(offpeak_vals),
        "mean_peak": float(peak_vals.mean()), "mean_offpeak": float(offpeak_vals.mean()),
        "ratio": float(peak_vals.mean() / offpeak_vals.mean()),
    }


def collect_all_stats(read_from_s3: bool = True) -> dict:
    """
    Returns {mode_label: {window_name: {n_peak, n_offpeak, mean_peak, mean_offpeak, ratio}}}
    for Bikes, PT, Flight, Multimodal (bike+PT), Multimodal (bike+PT+flight).
    """
    stats = {label: {} for label in list(MODE_COLUMNS) + ["Multimodal", "Multimodal (bike+PT+flight)"]}

    for p in PERIODS_WA1_WA2_WB_WC:
        win = p["name"].split(" ")[0]
        res = analyze_weather_impact(start_date=p["start_date"], end_date=p["end_date"],
                                     read_from_s3=read_from_s3)
        combined = res["combined_df"]
        bikes_col = "bikes_in_use" if "bikes_in_use" in combined.columns else "bikes_available"
        flagged = _peak_flag(combined)

        for label, col in MODE_COLUMNS.items():
            col = bikes_col if col is None else col
            s = _peak_offpeak_raw(flagged, col) if col in combined.columns else None
            if s is not None:
                stats[label][win] = s

        for label, variant_key in [("Multimodal", "bike_pt"), ("Multimodal (bike+PT+flight)", "bike_pt_flight")]:
            cols = [bikes_col if c == "bikes_in_use" else c for c in VARIANTS[variant_key]]
            variant_df = compute_multimodal_variant(combined, cols)
            variant_flagged = _peak_flag(variant_df)
            s = _peak_offpeak_raw(variant_flagged, "_variant")
            if s is not None:
                stats[label][win] = s

    return stats


def pooled_weighted_ratio(mode_stats: dict) -> dict:
    """Minute-weighted pooled ratio across all windows for one mode."""
    total_peak_n = sum(w["n_peak"] for w in mode_stats.values())
    total_off_n = sum(w["n_offpeak"] for w in mode_stats.values())
    pooled_mean_peak = sum(w["n_peak"] * w["mean_peak"] for w in mode_stats.values()) / total_peak_n
    pooled_mean_off = sum(w["n_offpeak"] * w["mean_offpeak"] for w in mode_stats.values()) / total_off_n
    ratios = [w["ratio"] for w in mode_stats.values()]
    return {
        "weighted_ratio": pooled_mean_peak / pooled_mean_off,
        "range_min": min(ratios),
        "range_max": max(ratios),
    }


# ─────────────────────────────────────────────────────────────────────────
# Figure 3: heatmap
# ─────────────────────────────────────────────────────────────────────────

def fig3_heatmap(stats: dict, save_path: str | None = None, windows: list[str] | None = None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.family": "serif", "font.size": 10})

    modes = ["Bikes", "PT", "Flight", "Multimodal intra-urban (bike+PT)", "Multimodal with flight"]
    mode_key = {"Multimodal intra-urban (bike+PT)": "Multimodal", "Multimodal with flight": "Multimodal (bike+PT+flight)"}
    windows = windows or ["W-A1", "W-A2", "W-B", "W-C"]
    period_labels = {
        "W-A1": "W-A1 (2025-12-20 – 2025-12-27)", "W-A2": "W-A2 (2025-12-28 – 2026-01-04)",
        "W-B": "W-B (2026-01-31 – 2026-02-07)", "W-C": "W-C (2026-03-07 – 2026-03-14)",
    }

    grid = np.full((len(modes), len(windows)), np.nan)
    for i, m in enumerate(modes):
        key = mode_key.get(m, m)
        for j, w in enumerate(windows):
            if w in stats[key]:
                grid[i, j] = stats[key][w]["ratio"]

    fig, ax = plt.subplots(figsize=(10, 4.2))
    im = ax.imshow(grid, cmap="YlOrRd", aspect="auto", vmin=1.0)
    ax.set_xticks(range(len(windows)))
    ax.set_xticklabels([period_labels[w] for w in windows], rotation=20, ha="right", fontsize=8)
    ax.set_yticks(range(len(modes)))
    ax.set_yticklabels(modes)
    ax.set_title("Peak vs Off-peak Ratio by Mode and Period", fontsize=12)

    for i in range(len(modes)):
        for j in range(len(windows)):
            if not np.isnan(grid[i, j]):
                ax.text(j, i, f"{grid[i, j]:.2f}", ha="center", va="center", fontsize=9)

    cb = fig.colorbar(im, ax=ax, shrink=0.9)
    cb.set_label("Peak/Off-peak ratio")
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved -> {save_path}")
    return fig


# ─────────────────────────────────────────────────────────────────────────
# Figure 4: minute-weighted pooled ratio with period-range whiskers
# ─────────────────────────────────────────────────────────────────────────

def fig4_weighted_ratio(stats: dict, save_path: str | None = None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.family": "serif", "font.size": 11})

    modes_order = ["Multimodal intra-urban (bike+PT)", "Multimodal with flight", "Flight", "Bikes", "PT"]
    mode_key = {"Multimodal intra-urban (bike+PT)": "Multimodal", "Multimodal with flight": "Multimodal (bike+PT+flight)"}
    colors = {
        "Multimodal intra-urban (bike+PT)": "#4a3aa7", "Multimodal with flight": "#9b8fd4",
        "Flight": "#eb6834", "Bikes": "#1baf7a", "PT": "#2a78d6",
    }

    pooled = {m: pooled_weighted_ratio(stats[mode_key.get(m, m)]) for m in modes_order}

    fig, ax = plt.subplots(figsize=(9, 6))
    y_pos = np.arange(len(modes_order))

    for i, m in enumerate(modes_order):
        p = pooled[m]
        ax.barh(i, p["weighted_ratio"], color=colors[m], alpha=0.85, height=0.6, zorder=2)
        # The pooled (minute-weighted) ratio can fall outside the [min, max] range of the
        # four per-window ratios (it is not a simple average of the range endpoints) --
        # clip to 0 so the whisker doesn't extend past the point on that side.
        lower = max(0.0, p["weighted_ratio"] - p["range_min"])
        upper = max(0.0, p["range_max"] - p["weighted_ratio"])
        ax.errorbar(p["weighted_ratio"], i, xerr=[[lower], [upper]],
                   fmt="none", ecolor="black", elinewidth=1.3, capsize=5, zorder=3)
        ax.text(p["weighted_ratio"] + 0.03, i, f"{p['weighted_ratio']:.2f}",
               va="center", fontsize=10, zorder=4)
        # placed above the bar, in the row gap, so it never overlaps the bar fill or value label
        ax.text(0.92, i + 0.36, f"period range: {p['range_min']:.2f}-{p['range_max']:.2f}",
               fontsize=7.5, color="#52514e", ha="left", va="bottom")

    ax.axvline(1.0, color="gray", linestyle="--", linewidth=1, zorder=1)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(modes_order)
    ax.set_xlabel("Peak / Off-peak ratio")
    ax.set_title("Weighted Peak/Off-peak Ratios with Period Ranges", fontsize=12)
    ax.set_ylim(-0.5, len(modes_order) - 0.5 + 0.15)
    ax.set_xlim(0.9, max(p["range_max"] for p in pooled.values()) + 0.3)
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved -> {save_path}")
    return fig, pooled


if __name__ == "__main__":
    stats = collect_all_stats(read_from_s3=True)
    import os
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "peak_offpeak_output")
    os.makedirs(out_dir, exist_ok=True)
    fig3_heatmap(stats, save_path=os.path.join(out_dir, "fig3_heatmap.pdf"))
    fig, pooled = fig4_weighted_ratio(stats, save_path=os.path.join(out_dir, "fig4_weighted_ratio.pdf"))
    print("\nPooled weighted ratios:")
    for m, p in pooled.items():
        print(f"  {m:30} weighted={p['weighted_ratio']:.3f}  range=[{p['range_min']:.3f}, {p['range_max']:.3f}]")
    print("\nMultimodal (bike+PT+flight) [validation against original 1.32]:")
    p_old = pooled_weighted_ratio(stats["Multimodal (bike+PT+flight)"])
    print(f"  weighted={p_old['weighted_ratio']:.3f}  range=[{p_old['range_min']:.3f}, {p_old['range_max']:.3f}]")
