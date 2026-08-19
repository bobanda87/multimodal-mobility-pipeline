"""
Figure 5: "Road surface temperature (blue) and mean PT delay (orange), hourly
means by window" -- regenerated to match the original chart (screenshot
provided by the user; original generation code was not present in this repo).
One panel per window (W-A1, W-A2, W-B, W-C), dual-axis: road_temperature
(left, deg C) and mean_delay_sec (right, s), both resampled to HOURLY means
from the per-minute combined_df built by analyze_weather_impact.py.

Plain default-matplotlib style (no custom font/gridlines/legend), matching
the original: colors are named directly in the suptitle rather than a
separate legend, axis labels are just "°C" and "s", panel titles are just
the window code.

Y-axis tick labels use standard thousands separators for any value >= 1000
(e.g. 10000 -> "10,000", 1750 -> "1,750").
"""

from __future__ import annotations

import os

import polars as pl

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC, analyze_weather_impact

TEMP_COLOR = "C0"
DELAY_COLOR = "C1"


def _comma_1000(x, pos=None) -> str:
    v = int(round(x))
    sign = "-" if v < 0 else ""
    v = abs(v)
    return f"{sign}{v:,}" if v >= 1000 else f"{sign}{v}"


def _hourly_mean(combined: pl.DataFrame) -> pl.DataFrame:
    return (
        combined.filter(pl.col("minute").is_not_null())
        .with_columns(pl.col("minute").dt.truncate("1h").alias("hour"))
        .group_by("hour")
        .agg([
            pl.col("road_temperature").mean().alias("road_temperature"),
            pl.col("mean_delay_sec").mean().alias("mean_delay_sec")
            if "mean_delay_sec" in combined.columns else pl.lit(None).alias("mean_delay_sec"),
        ])
        .sort("hour")
    )


def _draw_panel(ax1, combined, window_code: str):
    from matplotlib.ticker import FuncFormatter

    hourly = _hourly_mean(combined)

    temp = hourly.filter(pl.col("road_temperature").is_not_null())
    ax1.plot(temp["hour"], temp["road_temperature"], color=TEMP_COLOR, linewidth=1.2)
    ax1.set_ylabel("°C", color=TEMP_COLOR)
    ax1.tick_params(axis="y", labelcolor=TEMP_COLOR)
    ax1.yaxis.set_major_formatter(FuncFormatter(_comma_1000))

    ax2 = ax1.twinx()
    delay = hourly.filter(pl.col("mean_delay_sec").is_not_null())
    if delay.height > 0:
        ax2.plot(delay["hour"], delay["mean_delay_sec"], color=DELAY_COLOR, linewidth=1.2)
    ax2.set_ylabel("s", color=DELAY_COLOR)
    ax2.tick_params(axis="y", labelcolor=DELAY_COLOR)
    ax2.yaxis.set_major_formatter(FuncFormatter(_comma_1000))

    ax1.set_title(window_code)


def fig5_timeseries(save_path: str | None = None, read_from_s3: bool = True):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    axes = axes.flatten()

    for i, p in enumerate(PERIODS_WA1_WA2_WB_WC):
        win = p["name"].split(" ")[0]
        print(f"  loading {win} ...")
        res = analyze_weather_impact(start_date=p["start_date"], end_date=p["end_date"],
                                     read_from_s3=read_from_s3)
        combined = res["combined_df"]
        _draw_panel(axes[i], combined, win)

    fig.suptitle("Road surface temperature (blue) and mean PT delay (orange), "
                "hourly means by window")
    fig.autofmt_xdate()
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved -> {save_path}")
    return fig


if __name__ == "__main__":
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fig5_output")
    os.makedirs(out_dir, exist_ok=True)
    fig5_timeseries(save_path=os.path.join(out_dir, "fig5_temp_delay_timeseries.pdf"))
