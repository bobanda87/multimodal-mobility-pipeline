"""
Generiše analysis/output/timeseries_temp_delay.png za ceo opseg W-A1, W-A2, W-B, W-C
(sa vertikalnim markerima granica prozora). Koristi satnu agregaciju radi čitljivosti.

Pokretanje (iz project root):
  POLARS_SKIP_CPU_CHECK=1 .venv/bin/python analysis/plot_timeseries_temp_delay_all_windows.py
"""
from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from datetime import datetime, timezone

from analysis.analyze_weather_impact import analyze_weather_impact


# Prozori (UTC) — mora da odgovara PERIODS_WA1_WA2_WB_WC
WINDOWS = [
    ("W-A1", datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2025, 12, 27, 23, 59, 59, tzinfo=timezone.utc)),
    ("W-A2", datetime(2025, 12, 28, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 1, 4, 23, 59, 59, tzinfo=timezone.utc)),
    ("W-B", datetime(2026, 1, 31, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 2, 7, 23, 59, 59, tzinfo=timezone.utc)),
    ("W-C", datetime(2026, 3, 7, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 3, 14, 23, 59, 59, tzinfo=timezone.utc)),
]


def main() -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import polars as pl

    start = min(w[1] for w in WINDOWS)
    end = max(w[2] for w in WINDOWS)

    print(f"Opseg: {start.date()} – {end.date()} (UTC)")
    results = analyze_weather_impact(
        start_date=start,
        end_date=end,
        read_from_s3=True,
    )
    combined = results.get("combined_df")
    if combined is None or combined.height == 0:
        print("Nema combined_df — prekid.")
        sys.exit(1)

    if "minute" not in combined.columns or "road_temperature" not in combined.columns:
        print("Nedostaju minute / road_temperature.")
        sys.exit(1)

    ts = combined.select(
        pl.col("minute").dt.truncate("1h").alias("hour"),
        pl.col("road_temperature").cast(pl.Float64, strict=False),
        pl.col("mean_delay_sec").cast(pl.Float64, strict=False)
        if "mean_delay_sec" in combined.columns
        else pl.lit(None).cast(pl.Float64).alias("mean_delay_sec"),
    ).group_by("hour").agg(
        pl.col("road_temperature").mean().alias("road_temperature"),
        pl.col("mean_delay_sec").mean().alias("mean_delay_sec"),
    ).sort("hour")

    pdf = ts.to_pandas()

    out_dir = os.path.join(PROJECT_ROOT, "analysis", "output")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "timeseries_temp_delay.png")

    from matplotlib.patches import Patch

    fig, ax1 = plt.subplots(figsize=(12, 4.5))

    # Pozadinski prozori (ispod linija)
    span_colors = ["#bae6fd", "#fde68a", "#bbf7d0", "#e9d5ff"]
    legend_patches = []
    for i, (name, ws, we) in enumerate(WINDOWS):
        c = span_colors[i % len(span_colors)]
        ax1.axvspan(ws, we, alpha=0.35, color=c, zorder=0)
        legend_patches.append(Patch(facecolor=c, edgecolor="none", alpha=0.5, label=name))

    ax1.plot(
        pdf["hour"],
        pdf["road_temperature"],
        color="C0",
        alpha=0.9,
        lw=1.0,
        zorder=2,
    )
    ax1.set_ylabel("Road temperature (°C)", color="C0")
    ax1.tick_params(axis="y", labelcolor="C0")

    ax2 = ax1.twinx()
    if pdf["mean_delay_sec"].notna().any():
        ax2.plot(
            pdf["hour"],
            pdf["mean_delay_sec"],
            color="C1",
            alpha=0.9,
            lw=1.0,
            zorder=2,
        )
        ax2.set_ylabel("Mean delay (s)", color="C1")
        ax2.tick_params(axis="y", labelcolor="C1")

    ax1.set_xlabel("Time (UTC)")
    ax1.set_title(
        "Time series: road temperature and PT delay (hourly means; analysis windows shaded)"
    )
    ax1.legend(handles=legend_patches, loc="upper left", fontsize=7, ncol=4, framealpha=0.9)

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Sačuvano: {out_path}")


if __name__ == "__main__":
    main()
