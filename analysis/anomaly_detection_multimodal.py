"""
AI-Based Anomaly Detection in Multimodal Urban Mobility Data Streams
Research Prototype — Academic Analytical Workflow

Periods analysed:
  W-B  2026-01-31 – 2026-02-07
  W-C  2026-03-07 – 2026-03-14

Transport modes: Public Transit · Bike-Share · Road Traffic · Aviation

Method: Isolation Forest (unsupervised, no neural networks)
"""

from __future__ import annotations

import os
import warnings
from datetime import datetime, timezone

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
matplotlib.use("Agg")  # non-interactive backend for saving; switch to TkAgg/Qt if needed

# ── Reproducibility ──────────────────────────────────────────────────────────
SEED = 42
np.random.seed(SEED)

# ── Study periods ─────────────────────────────────────────────────────────────
PERIODS = [
    {
        "name":  "W-B",
        "label": "W-B (2026-01-31 – 2026-02-07)",
        "start": datetime(2026, 1, 31,  0,  0,  0, tzinfo=timezone.utc),
        "end":   datetime(2026, 2,  7, 23, 59, 59, tzinfo=timezone.utc),
    },
    {
        "name":  "W-C",
        "label": "W-C (2026-03-07 – 2026-03-14)",
        "start": datetime(2026, 3,  7,  0,  0,  0, tzinfo=timezone.utc),
        "end":   datetime(2026, 3, 14, 23, 59, 59, tzinfo=timezone.utc),
    },
]

FREQ = "1h"  # hourly resolution

# ── Publication-friendly colour palette ───────────────────────────────────────
C = {
    "transit":   "#2166AC",
    "bikeshare": "#4DAC26",
    "traffic":   "#D6604D",
    "aviation":  "#762A83",
    "anomaly":   "#F46D43",
    "normal":    "#AAAAAA",
    "wb":        "#2166AC",
    "wc":        "#D6604D",
}

CONTAMINATION = "auto"  # Liu et al. (2008) original threshold; avoids forcing a fixed count
N_ANOMALY_INJ = 6      # synthetic anomalies injected per feature

MODE_LABELS = {
    "transit":   "Public Transit",
    "bikeshare": "Bike-Share",
    "traffic":   "Road Traffic",
    "aviation":  "Aviation",
}

FEATURE_COLS = {
    "transit":   ["pax_load", "delay_min", "headway_min"],
    "bikeshare": ["trips_started", "occupancy_rate", "bikes_available"],
    "traffic":   ["vehicle_count", "avg_speed_kmh", "congestion_idx"],
    "aviation":  ["pax_flow", "flight_delay_min", "gate_utilisation"],
}

PRIMARY_METRIC = {
    "transit":   ("delay_min",         "Delay (min)"),
    "bikeshare": ("trips_started",     "Trip starts h⁻¹"),
    "traffic":   ("vehicle_count",     "Vehicle count h⁻¹"),
    "aviation":  ("flight_delay_min",  "Flight delay (min)"),
}


# ═════════════════════════════════════════════════════════════════════════════
# 1.  SYNTHETIC DATASET GENERATION
# ═════════════════════════════════════════════════════════════════════════════

def _hourly_index(period: dict) -> pd.DatetimeIndex:
    return pd.date_range(period["start"], period["end"], freq=FREQ, tz=timezone.utc)


def _diurnal(hour: np.ndarray, peak1: int = 8, peak2: int = 17,
             base: float = 0.3, amplitude: float = 1.0) -> np.ndarray:
    """Double-Gaussian diurnal pattern (morning / evening commute peaks)."""
    g1 = np.exp(-0.5 * ((hour - peak1) / 2.0) ** 2)
    g2 = np.exp(-0.5 * ((hour - peak2) / 2.0) ** 2)
    return base + amplitude * (g1 + 0.80 * g2)


def _inject_anomalies(series: pd.Series, n: int, scale: float,
                      rng: np.random.Generator) -> tuple[pd.Series, np.ndarray]:
    """
    Point-anomaly injection: traffic spikes, delay surges, operational failures.
    Alternates direction (high / low) to model both overload and service collapse.
    Returns the perturbed series and a boolean ground-truth mask.
    """
    s = series.copy()
    idx = rng.choice(len(s), size=n, replace=False)
    direction = rng.choice([-1, 1], size=n)
    s.iloc[idx] += direction * scale * s.std()
    mask = np.zeros(len(s), dtype=bool)
    mask[idx] = True
    return s, mask


def generate_transit(idx: pd.DatetimeIndex, period_name: str) -> pd.DataFrame:
    """Public transport: passenger load, service delay, headway."""
    rng = np.random.default_rng(SEED + hash(period_name) % 97)
    hour = idx.hour.values

    base_load = 800 + 400 * _diurnal(hour)
    load = base_load + rng.normal(0, 40, len(idx))
    delay = (1.5 + 0.005 * (load - load.mean())
             + rng.exponential(0.8, len(idx)))
    headway = (12 - 6 * (_diurnal(hour) / _diurnal(hour).max())
               + rng.normal(0, 1, len(idx)))
    headway = np.clip(headway, 2, 25)

    df = pd.DataFrame({"pax_load": load, "delay_min": delay,
                       "headway_min": headway}, index=idx)
    df["pax_load"],  m1 = _inject_anomalies(df["pax_load"],  N_ANOMALY_INJ,     4.0, rng)
    df["delay_min"], m2 = _inject_anomalies(df["delay_min"], N_ANOMALY_INJ - 2, 5.0, rng)
    df["delay_min"]  = df["delay_min"].clip(lower=0)
    df["injected"]   = m1 | m2
    df["mode"]       = "transit"
    return df


def generate_bikeshare(idx: pd.DatetimeIndex, period_name: str) -> pd.DataFrame:
    """Bike-share: trip starts, station occupancy, available bikes."""
    rng = np.random.default_rng(SEED + hash(period_name) % 97 + 11)
    hour = idx.hour.values
    is_weekday = (idx.weekday < 5).astype(float)

    trip_base = (200 + 120 * _diurnal(hour, peak1=7, peak2=18)
                 * (0.60 + 0.40 * is_weekday))
    trips = (trip_base + rng.normal(0, 20, len(idx))).clip(0)

    occupancy = (0.50 + 0.20 * np.sin(2 * np.pi * hour / 24)
                 + rng.normal(0, 0.05, len(idx)))
    occupancy = np.clip(occupancy, 0.05, 0.98)

    bikes = (500 * (1 - occupancy) + rng.normal(0, 15, len(idx))).clip(0, 500)

    df = pd.DataFrame({"trips_started": trips, "occupancy_rate": occupancy,
                       "bikes_available": bikes}, index=idx)
    df["trips_started"],  m1 = _inject_anomalies(df["trips_started"],  N_ANOMALY_INJ,     4.5, rng)
    df["occupancy_rate"], m2 = _inject_anomalies(df["occupancy_rate"], N_ANOMALY_INJ - 3, 2.0, rng)
    df["occupancy_rate"] = df["occupancy_rate"].clip(0, 1)
    df["injected"] = m1 | m2
    df["mode"]     = "bikeshare"
    return df


def generate_traffic(idx: pd.DatetimeIndex, period_name: str) -> pd.DataFrame:
    """Road traffic: vehicle counts, average speed, congestion index."""
    rng = np.random.default_rng(SEED + hash(period_name) % 97 + 22)
    hour = idx.hour.values
    is_weekend = (idx.weekday >= 5).astype(float)

    count_base = (1200 + 600 * _diurnal(hour, peak1=8, peak2=17)
                  * (1 - 0.35 * is_weekend))
    count = (count_base + rng.normal(0, 80, len(idx))).clip(50)

    speed_max = 70.0
    speed = (speed_max * (1 - 0.0004 * (count - count.min()))
             + rng.normal(0, 3, len(idx)))
    speed = speed.clip(10, speed_max)

    congestion = (10 * (1 - speed / speed_max)
                  + rng.normal(0, 0.30, len(idx))).clip(0, 10)

    df = pd.DataFrame({"vehicle_count": count, "avg_speed_kmh": speed,
                       "congestion_idx": congestion}, index=idx)
    df["vehicle_count"], m1 = _inject_anomalies(df["vehicle_count"], N_ANOMALY_INJ,     4.0, rng)
    df["congestion_idx"], m2 = _inject_anomalies(df["congestion_idx"], N_ANOMALY_INJ - 1, 3.5, rng)
    df["congestion_idx"] = df["congestion_idx"].clip(0, 10)
    df["injected"] = m1 | m2
    df["mode"]     = "traffic"
    return df


def generate_aviation(idx: pd.DatetimeIndex, period_name: str) -> pd.DataFrame:
    """Airport: terminal passenger flow, flight delay, gate utilisation."""
    rng = np.random.default_rng(SEED + hash(period_name) % 97 + 33)
    hour = idx.hour.values

    active = ((hour >= 5) & (hour <= 22)).astype(float)
    pax = (1500 * active * _diurnal(hour, peak1=9, peak2=16, base=0.10)
           + rng.normal(0, 60, len(idx))).clip(0)

    delay = (8 + 0.003 * pax + rng.exponential(4, len(idx))).clip(0)

    gate_util = (0.40 * active
                 + 0.35 * _diurnal(hour, peak1=10, peak2=17, base=0.10) * active
                 + rng.normal(0, 0.04, len(idx))).clip(0, 1)

    df = pd.DataFrame({"pax_flow": pax, "flight_delay_min": delay,
                       "gate_utilisation": gate_util}, index=idx)
    df["pax_flow"],          m1 = _inject_anomalies(df["pax_flow"],          N_ANOMALY_INJ,     4.0, rng)
    df["flight_delay_min"],  m2 = _inject_anomalies(df["flight_delay_min"],  N_ANOMALY_INJ - 2, 5.0, rng)
    df["flight_delay_min"]  = df["flight_delay_min"].clip(0)
    df["injected"] = m1 | m2
    df["mode"]     = "aviation"
    return df


GENERATORS: dict = {
    "transit":   generate_transit,
    "bikeshare": generate_bikeshare,
    "traffic":   generate_traffic,
    "aviation":  generate_aviation,
}


# ═════════════════════════════════════════════════════════════════════════════
# 2.  PREPROCESSING PIPELINE
# ═════════════════════════════════════════════════════════════════════════════

def preprocess(df: pd.DataFrame,
               features: list[str]) -> tuple[np.ndarray, StandardScaler]:
    """Median-impute any gaps then z-score normalise."""
    X = df[features].copy()
    X = X.fillna(X.median())
    scaler = StandardScaler()
    return scaler.fit_transform(X), scaler


# ═════════════════════════════════════════════════════════════════════════════
# 3.  ANOMALY DETECTION — ISOLATION FOREST
# ═════════════════════════════════════════════════════════════════════════════

def detect(X_scaled: np.ndarray,
           contamination: float = CONTAMINATION) -> tuple[np.ndarray, np.ndarray]:
    """
    Fit IsolationForest.
      labels : +1 normal / -1 anomaly
      scores : decision_function (lower → more anomalous)
    """
    clf = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=SEED,
        n_jobs=-1,
    )
    clf.fit(X_scaled)
    return clf.predict(X_scaled), clf.decision_function(X_scaled)


# ═════════════════════════════════════════════════════════════════════════════
# 4.  ORCHESTRATION
# ═════════════════════════════════════════════════════════════════════════════

def run_pipeline() -> dict:
    """
    Execute the full workflow for all (period × mode) combinations.

    Returns
    -------
    results[period_name][mode] = {
        "df"      : DataFrame  (raw features + anomaly columns),
        "X_scaled": np.ndarray,
        "scaler"  : StandardScaler,
    }
    """
    results: dict = {}
    for period in PERIODS:
        pname = period["name"]
        idx   = _hourly_index(period)
        results[pname] = {}
        for mode, gen_fn in GENERATORS.items():
            raw = gen_fn(idx, pname)
            X_scaled, scaler = preprocess(raw, FEATURE_COLS[mode])
            labels, scores   = detect(X_scaled)
            raw["if_label"]  = labels          # -1 = anomaly
            raw["if_score"]  = scores          # decision-function value
            raw["anomaly"]   = labels == -1
            results[pname][mode] = {
                "df": raw, "X_scaled": X_scaled, "scaler": scaler,
            }
    return results


# ═════════════════════════════════════════════════════════════════════════════
# 5.  VISUALISATION
# ═════════════════════════════════════════════════════════════════════════════

plt.rcParams.update({
    "font.family":       "serif",
    "font.size":         9,
    "axes.titlesize":    10,
    "axes.labelsize":    9,
    "legend.fontsize":   8,
    "figure.dpi":        150,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.grid":         True,
    "grid.alpha":        0.3,
    "grid.linestyle":    "--",
})

_LEGEND_NORMAL = Line2D([0], [0], marker="o", color="w",
                         markerfacecolor=C["normal"], markersize=6,
                         label="Normal observation")
_LEGEND_ANOMALY = Line2D([0], [0], marker="D", color="w",
                          markerfacecolor=C["anomaly"], markersize=7,
                          label="Detected anomaly")


# ── Figure 1: Time-series with anomaly overlay ────────────────────────────────

def fig_timeseries(results: dict, save_path: str | None = None) -> plt.Figure:
    """
    4-row × 2-column grid: mode × period.
    Primary mobility metric plotted as a line; anomalous hours highlighted.
    """
    modes   = list(GENERATORS)
    periods = PERIODS

    fig, axes = plt.subplots(4, 2, figsize=(13, 11), sharey="row")
    fig.suptitle(
        "Anomaly Detection in Multimodal Urban Mobility Streams\n"
        "Isolation Forest · Hourly Resolution · Study Periods W-B and W-C",
        fontsize=11, fontweight="bold", y=1.005,
    )

    date_fmt = mdates.DateFormatter("%d %b\n%H:00")

    for row, mode in enumerate(modes):
        metric_col, metric_label = PRIMARY_METRIC[mode]
        for col, period in enumerate(periods):
            ax  = axes[row][col]
            df  = results[period["name"]][mode]["df"]
            nor = df[~df["anomaly"]]
            abn = df[df["anomaly"]]

            ax.plot(df.index, df[metric_col],
                    color=C[mode], alpha=0.45, linewidth=0.9, zorder=1)
            ax.scatter(nor.index, nor[metric_col],
                       s=6, color=C[mode], alpha=0.65, zorder=3)
            ax.scatter(abn.index, abn[metric_col],
                       s=35, color=C["anomaly"], zorder=5,
                       marker="D", edgecolors="white", linewidths=0.4)

            ax.set_title(f"{MODE_LABELS[mode]}  —  {period['label']}", pad=4)
            ax.set_ylabel(metric_label if col == 0 else "")
            ax.xaxis.set_major_formatter(date_fmt)
            ax.tick_params(axis="x", rotation=20, labelsize=7.5)

            n_anom = int(df["anomaly"].sum())
            ax.text(0.985, 0.95,
                    f"n = {n_anom} anomalies",
                    transform=ax.transAxes, ha="right", va="top",
                    fontsize=7.5, color=C["anomaly"],
                    bbox=dict(boxstyle="round,pad=0.25", fc="white",
                              ec=C["anomaly"], alpha=0.85, linewidth=0.6))

    fig.legend(handles=[_LEGEND_NORMAL, _LEGEND_ANOMALY],
               loc="lower center", ncol=2, frameon=True,
               bbox_to_anchor=(0.5, -0.015))
    fig.tight_layout()
    _save(fig, save_path)
    return fig


# ── Figure 2: Anomaly score distributions ─────────────────────────────────────

def fig_score_distributions(results: dict, save_path: str | None = None) -> plt.Figure:
    """
    Histogram of Isolation Forest decision-function values per mode.
    Negative threshold (anomaly boundary) shown as a dashed vertical line.
    """
    fig, axes = plt.subplots(2, 2, figsize=(10, 7))
    fig.suptitle(
        "Isolation Forest Anomaly Score Distributions by Transport Mode",
        fontsize=11, fontweight="bold",
    )

    for ax, mode in zip(axes.flat, GENERATORS):
        for period, ls in zip(PERIODS, ["-", "--"]):
            df     = results[period["name"]][mode]["df"]
            scores = df["if_score"]
            ax.hist(scores, bins=32, alpha=0.55, color=C[mode],
                    histtype="stepfilled", linestyle=ls,
                    edgecolor="white", linewidth=0.4,
                    label=period["label"])

        # anomaly boundary: score where label flips to -1
        all_scores = pd.concat(
            [results[p["name"]][mode]["df"]["if_score"] for p in PERIODS])
        all_labels = pd.concat(
            [results[p["name"]][mode]["df"]["if_label"] for p in PERIODS])
        threshold = all_scores[all_labels == -1].max()
        ax.axvline(threshold, color=C["anomaly"], linestyle=":",
                   linewidth=1.4, label=f"Anomaly boundary ({threshold:.3f})")

        ax.set_title(MODE_LABELS[mode])
        ax.set_xlabel("Anomaly score (decision function)")
        ax.set_ylabel("Frequency")
        ax.legend(fontsize=7.5)

    fig.tight_layout()
    _save(fig, save_path)
    return fig


# ── Figure 3: Heatmap — anomaly rate by hour × weekday ───────────────────────

def fig_heatmap(results: dict, save_path: str | None = None) -> plt.Figure:
    """
    Heatmap of mean anomaly rate across hour-of-day (x) and weekday (y),
    pooled over both study periods, per transport mode.
    """
    days  = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    modes = list(GENERATORS)

    fig, axes = plt.subplots(1, 4, figsize=(15, 4))
    fig.suptitle(
        "Anomaly Rate by Hour-of-Day and Day-of-Week  (W-B + W-C pooled)",
        fontsize=11, fontweight="bold",
    )

    for ax, mode in zip(axes, modes):
        df_all = pd.concat(
            [results[p["name"]][mode]["df"] for p in PERIODS])
        df_all["hour"]    = df_all.index.hour
        df_all["weekday"] = df_all.index.weekday

        pivot = (df_all.groupby(["weekday", "hour"])["anomaly"]
                       .mean()
                       .unstack(fill_value=0.0)
                       .reindex(index=range(7), columns=range(24),
                                fill_value=0.0))

        im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd",
                       vmin=0, vmax=0.30, origin="lower",
                       interpolation="nearest")

        ax.set_xticks(range(0, 24, 3))
        ax.set_xticklabels(range(0, 24, 3), fontsize=7.5)
        ax.set_yticks(range(7))
        ax.set_yticklabels(days, fontsize=7.5)
        ax.set_xlabel("Hour of day", fontsize=8)
        if ax is axes[0]:
            ax.set_ylabel("Weekday", fontsize=8)
        ax.set_title(MODE_LABELS[mode], fontsize=9)

        cb = plt.colorbar(im, ax=ax, shrink=0.82)
        cb.set_label("Anomaly rate", fontsize=7.5)
        cb.ax.tick_params(labelsize=7)

    fig.tight_layout()
    _save(fig, save_path)
    return fig


# ── Figure 4: Multimodal comparison bar chart ─────────────────────────────────

def fig_multimodal_comparison(results: dict, save_path: str | None = None) -> plt.Figure:
    """
    Grouped bar: total anomaly count per mode × period.
    Secondary axis: anomaly rate (%).
    """
    modes = list(GENERATORS)
    x     = np.arange(len(modes))
    width = 0.32

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax2 = ax1.twinx()
    ax2.spines["right"].set_visible(True)

    period_colors = [C["wb"], C["wc"]]

    for i, period in enumerate(PERIODS):
        pname  = period["name"]
        counts = [int(results[pname][m]["df"]["anomaly"].sum()) for m in modes]
        rates  = [100 * results[pname][m]["df"]["anomaly"].mean() for m in modes]
        offset = (i - 0.5) * width

        ax1.bar(x + offset, counts, width,
                label=period["label"],
                color=period_colors[i], alpha=0.80, edgecolor="white")
        ax2.plot(x + offset + width / 2, rates, "s--",
                 color=period_colors[i], markersize=6, alpha=0.90,
                 linewidth=1.2)

    ax1.set_xticks(x)
    ax1.set_xticklabels([MODE_LABELS[m] for m in modes], fontsize=9)
    ax1.set_ylabel("Total anomalies detected")
    ax1.set_ylim(bottom=0)
    ax1.legend(fontsize=8)
    ax1.set_title(
        "Anomaly Count and Rate per Transport Mode by Study Period",
        fontsize=11, fontweight="bold",
    )

    ax2.set_ylabel("Anomaly rate (%)", color=C["anomaly"])
    ax2.tick_params(axis="y", labelcolor=C["anomaly"])
    ax2.set_ylim(0, 20)

    fig.tight_layout()
    _save(fig, save_path)
    return fig


# ── Figure 5: Feature-level box-plots (normal vs. anomalous) ─────────────────

def fig_feature_boxplots(results: dict, save_path: str | None = None) -> plt.Figure:
    """
    Box-plots of normalised feature values split by anomaly flag,
    pooled across both study periods.
    """
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle(
        "Feature Distribution: Normal vs. Anomalous Observations  (W-B + W-C pooled)",
        fontsize=11, fontweight="bold",
    )

    for ax, mode in zip(axes.flat, GENERATORS):
        df_all = pd.concat(
            [results[p["name"]][mode]["df"] for p in PERIODS])
        feats  = FEATURE_COLS[mode]

        # min-max normalise each feature to [0, 1] for visual comparability
        norm = df_all[feats].copy()
        for col in feats:
            lo, hi = norm[col].min(), norm[col].max()
            norm[col] = (norm[col] - lo) / (hi - lo + 1e-9)

        pos_n = np.arange(len(feats)) * 2.2
        pos_a = pos_n + 0.75

        kw_normal = dict(
            patch_artist=True,
            boxprops=dict(facecolor=C[mode], alpha=0.55),
            medianprops=dict(color="black", linewidth=1.5),
            flierprops=dict(marker=".", markersize=3, alpha=0.4),
            whiskerprops=dict(linestyle="-", linewidth=0.8),
            capprops=dict(linewidth=0.8),
        )
        kw_anom = dict(
            patch_artist=True,
            boxprops=dict(facecolor=C["anomaly"], alpha=0.60),
            medianprops=dict(color="black", linewidth=1.5),
            flierprops=dict(marker="D", markersize=3, alpha=0.5,
                            markerfacecolor=C["anomaly"]),
            whiskerprops=dict(linestyle="--", linewidth=0.8),
            capprops=dict(linewidth=0.8),
        )

        ax.boxplot(
            [norm.loc[~df_all["anomaly"], c].values for c in feats],
            positions=pos_n, widths=0.6, **kw_normal,
        )
        ax.boxplot(
            [norm.loc[df_all["anomaly"], c].values for c in feats],
            positions=pos_a, widths=0.6, **kw_anom,
        )

        tick_pos   = (pos_n + pos_a) / 2
        tick_names = [c.replace("_", "\n") for c in feats]
        ax.set_xticks(tick_pos)
        ax.set_xticklabels(tick_names, fontsize=7.5)
        ax.set_ylabel("Normalised value [0 – 1]")
        ax.set_title(MODE_LABELS[mode])
        ax.set_xlim(-0.8, pos_a[-1] + 0.8)
        ax.legend(handles=[
            Patch(facecolor=C[mode],       alpha=0.55, label="Normal"),
            Patch(facecolor=C["anomaly"],  alpha=0.60, label="Anomaly"),
        ], fontsize=7.5)

    fig.tight_layout()
    _save(fig, save_path)
    return fig


# ── Figure 6: Rolling anomaly density ─────────────────────────────────────────

def fig_rolling_density(results: dict, save_path: str | None = None) -> plt.Figure:
    """
    24-hour rolling anomaly density (fraction of anomalies in a sliding window)
    for each mode, both periods overlaid.
    """
    modes = list(GENERATORS)
    fig, axes = plt.subplots(2, 2, figsize=(12, 7))
    fig.suptitle(
        "24-Hour Rolling Anomaly Density by Transport Mode",
        fontsize=11, fontweight="bold",
    )

    for ax, mode in zip(axes.flat, modes):
        for period, ls in zip(PERIODS, ["-", "--"]):
            df = results[period["name"]][mode]["df"]
            # rolling 24-step (= 24 h) mean of the binary anomaly flag
            density = df["anomaly"].astype(float).rolling(24, min_periods=1).mean()
            # re-index to hour-of-day 0..N for aligned plotting
            x = np.arange(len(density))
            ax.plot(x, density * 100, linestyle=ls, color=C[mode],
                    linewidth=1.2, alpha=0.85, label=period["label"])
            ax.fill_between(x, 0, density * 100, alpha=0.12, color=C[mode])

        ax.set_title(MODE_LABELS[mode])
        ax.set_xlabel("Hour offset from period start")
        ax.set_ylabel("Rolling anomaly density (%)")
        ax.set_ylim(0, 40)
        ax.legend(fontsize=7.5)

    fig.tight_layout()
    _save(fig, save_path)
    return fig


def _save(fig: plt.Figure, path: str | None) -> None:
    if path:
        fig.savefig(path, bbox_inches="tight", dpi=150)
        print(f"   Saved → {os.path.basename(path)}")


# ═════════════════════════════════════════════════════════════════════════════
# 6.  STATISTICAL SUMMARY TABLE
# ═════════════════════════════════════════════════════════════════════════════

def build_summary(results: dict) -> pd.DataFrame:
    rows = []
    for period in PERIODS:
        pname = period["name"]
        for mode in GENERATORS:
            df  = results[pname][mode]["df"]
            sc  = df["if_score"]
            n   = len(df)
            na  = int(df["anomaly"].sum())
            rows.append({
                "Period":             period["label"],
                "Mode":               MODE_LABELS[mode],
                "Hours":              n,
                "Anomalies":          na,
                "Anomaly rate (%)":   f"{100 * na / n:.2f}",
                "Score μ":            f"{sc.mean():.4f}",
                "Score σ":            f"{sc.std():.4f}",
                "Score min":          f"{sc.min():.4f}",
                "Precision* (inj.)":  _precision(df),
                "Features":           ", ".join(FEATURE_COLS[mode]),
            })
    return pd.DataFrame(rows)


def _precision(df: pd.DataFrame) -> str:
    """Fraction of detected anomalies that match injected positions."""
    if "injected" not in df.columns:
        return "n/a"
    tp = (df["anomaly"] & df["injected"]).sum()
    fp = (df["anomaly"] & ~df["injected"]).sum()
    if tp + fp == 0:
        return "0.00"
    return f"{tp / (tp + fp):.2f}"


def print_summary(table: pd.DataFrame) -> None:
    bar = "─" * 120
    print(f"\n{bar}")
    print("  ANOMALY DETECTION SUMMARY — MULTIMODAL URBAN MOBILITY  "
          "(W-B: 2026-01-31→02-07 / W-C: 2026-03-07→03-14)")
    print(bar)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(table.to_string(index=False))
    print(f"{bar}\n  * Precision relative to synthetically injected anomalies\n")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "anomaly_detection_output")
    os.makedirs(out_dir, exist_ok=True)

    def p(name: str) -> str:
        return os.path.join(out_dir, name)

    print("\n═" * 55)
    print(" AI-BASED ANOMALY DETECTION IN MULTIMODAL URBAN MOBILITY")
    print("═" * 55)

    print("\n[1/4]  Generating synthetic datasets + running Isolation Forest …")
    results = run_pipeline()

    print("[2/4]  Building statistical summary …")
    table = build_summary(results)
    print_summary(table)
    table.to_csv(p("anomaly_summary.csv"), index=False)
    print(f"   Saved → anomaly_summary.csv")

    print("[3/4]  Rendering figures (PDF, 150 dpi) …")
    fig_timeseries(results,           save_path=p("fig1_timeseries_anomalies.pdf"))
    fig_score_distributions(results,  save_path=p("fig2_score_distributions.pdf"))
    fig_heatmap(results,              save_path=p("fig3_heatmap_anomaly_rate.pdf"))
    fig_multimodal_comparison(results, save_path=p("fig4_multimodal_comparison.pdf"))
    fig_feature_boxplots(results,     save_path=p("fig5_feature_boxplots.pdf"))
    fig_rolling_density(results,      save_path=p("fig6_rolling_anomaly_density.pdf"))

    print(f"\n[4/4]  Done.  All outputs in:\n       {out_dir}/\n")


if __name__ == "__main__":
    main()
