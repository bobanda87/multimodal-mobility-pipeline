"""
4-u-1 time series: temperatura + PT delay za W-A1, W-A2, W-B, W-C (poseban subplot po prozoru).
Jedno učitavanje S3 za ceo opseg, pa filter po intervalima.

Pokretanje (iz project root):
  POLARS_SKIP_CPU_CHECK=1 .venv/bin/python analysis/plot_timeseries_temp_delay_4panel.py
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

WINDOWS: list[tuple[str, datetime, datetime]] = [
    ("W-A1", datetime(2025, 12, 20, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2025, 12, 27, 23, 59, 59, tzinfo=timezone.utc)),
    ("W-A2", datetime(2025, 12, 28, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 1, 4, 23, 59, 59, tzinfo=timezone.utc)),
    ("W-B", datetime(2026, 1, 31, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 2, 7, 23, 59, 59, tzinfo=timezone.utc)),
    ("W-C", datetime(2026, 3, 7, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 3, 14, 23, 59, 59, tzinfo=timezone.utc)),
]


def _hourly_series(combined, start: datetime, end: datetime):
    import polars as pl

    sub = combined.filter(
        (pl.col("minute") >= pl.lit(start))
        & (pl.col("minute") <= pl.lit(end))
    )
    if sub.height == 0:
        return None
    delay_col = "mean_delay_sec" if "mean_delay_sec" in sub.columns else None
    agg_exprs = [
        pl.col("road_temperature").cast(pl.Float64, strict=False).mean().alias("road_temperature"),
    ]
    if delay_col:
        agg_exprs.append(
            pl.col(delay_col).cast(pl.Float64, strict=False).mean().alias("mean_delay_sec")
        )
    else:
        agg_exprs.append(pl.lit(None).cast(pl.Float64).alias("mean_delay_sec"))

    return (
        sub.with_columns(pl.col("minute").dt.truncate("1h").alias("hour"))
        .group_by("hour")
        .agg(agg_exprs)
        .sort("hour")
    )


def main() -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    start = min(w[1] for w in WINDOWS)
    end = max(w[2] for w in WINDOWS)

    print(f"Učitavanje opsega {start.date()} – {end.date()} (UTC) …")
    results = analyze_weather_impact(
        start_date=start,
        end_date=end,
        read_from_s3=True,
    )
    combined = results.get("combined_df")
    if combined is None or combined.height == 0:
        print("Nema combined_df.")
        sys.exit(1)
    if "minute" not in combined.columns or "road_temperature" not in combined.columns:
        print("Nedostaju kolone minute / road_temperature.")
        sys.exit(1)

    fig, axes = plt.subplots(2, 2, figsize=(12, 7), sharex=False)
    axes_flat = axes.flatten()

    for ax1, (title, ws, we) in zip(axes_flat, WINDOWS):
        ts = _hourly_series(combined, ws, we)
        if ts is None or ts.height == 0:
            ax1.set_title(f"{title} (nema podataka)")
            continue
        pdf = ts.to_pandas()

        ax1.plot(pdf["hour"], pdf["road_temperature"], color="C0", lw=1.0, alpha=0.9)
        ax1.set_ylabel("°C", color="C0", fontsize=9)
        ax1.tick_params(axis="y", labelcolor="C0", labelsize=8)
        ax1.set_title(title, fontsize=10)

        ax2 = ax1.twinx()
        if pdf["mean_delay_sec"].notna().any():
            ax2.plot(pdf["hour"], pdf["mean_delay_sec"], color="C1", lw=1.0, alpha=0.9)
            ax2.set_ylabel("s", color="C1", fontsize=9)
            ax2.tick_params(axis="y", labelcolor="C1", labelsize=8)

        ax1.tick_params(axis="x", labelsize=7)
        fig.autofmt_xdate()

    fig.suptitle(
        "Road surface temperature (blue) and mean PT delay (orange), hourly means by window",
        fontsize=11,
        y=1.02,
    )
    fig.tight_layout()
    out_dir = os.path.join(PROJECT_ROOT, "analysis", "output")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "timeseries_temp_delay_4panel.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Sačuvano: {out_path}")


if __name__ == "__main__":
    main()
