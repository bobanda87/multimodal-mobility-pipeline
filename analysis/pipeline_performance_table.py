"""
Empirical pipeline performance metrics (latency, throughput, robustness) for the
four analysis windows (W-A1, W-A2, W-B, W-C), computed directly from historical
raw S3 collector output -- no additional instrumentation/logging required.

Addresses the reviewer concern that the paper claims "near real-time" analytics
without measured latency, throughput, or robustness to source variability.

Three metrics, each derived from data already on S3:

1. Robustness (source-arrival regularity): every raw file's S3 key embeds the
   wall-clock time it was collected (LastModified). Consecutive-file gaps per
   source, compared to the collector's nominal polling interval (see
   entur_siri_et_collect.py / avinor_collect.py / oslobysykkel_collect.py /
   vegvesen_collect.py), reveal outages, throttling, or HTTP failures.
   Computed over ALL days in each window (listing-only, no data download).

2. Throughput: records per file x nominal pull frequency, from a sample of
   files per window (records/file varies by source; avinor writes one file per
   airport per 180s cycle, so total throughput also scales with active
   airport count in the sample).

3. Latency: Entur SIRI-ET records carry `RecordedAtTime` -- the source
   (transit operator) system's own event timestamp -- inside the payload,
   preserved as the `timestamp` column. The gap between file collection time
   (S3 key) and each record's RecordedAtTime is a genuine, measured end-to-end
   freshness number, sampled from real historical pulls (first 2 days of each
   window, to bound the number of files read).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import polars as pl

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC
from utils.s3_upload import list_parquet_files_from_s3, read_parquet_from_s3

SOURCES = {
    "entur": 60,          # seconds, nominal poll interval (round-robin: siri_et/vehicle-pos/alerts)
    "avinor": 180,
    "oslobysykkel": 180,
    "vegvesen": 180,
}

THROUGHPUT_SAMPLE_FILES = 30     # per source, per window
LATENCY_SAMPLE_DAYS = 2          # first N days of each window, entur siri_et only


def _dates_in_window(start: datetime, end: datetime) -> list[str]:
    days = []
    d = start.date()
    while d <= end.date():
        days.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return days


# ─────────────────────────────────────────────────────────────────────────
# 1. Robustness: inter-arrival gap analysis (listing-only, full window)
# ─────────────────────────────────────────────────────────────────────────

def compute_robustness(source: str, nominal_interval: int, dates: list[str]) -> dict:
    all_files = []
    for d in dates:
        all_files.extend(list_parquet_files_from_s3(source, with_metadata=True, date_prefix=d))
    all_files.sort(key=lambda f: f["last_modified"])
    if len(all_files) < 2:
        return {"n_files": len(all_files), "note": "insufficient data"}

    times = [f["last_modified"] for f in all_files]
    gaps = np.array([(times[i + 1] - times[i]).total_seconds() for i in range(len(times) - 1)])

    on_time = float(np.mean(gaps <= 2 * nominal_interval))
    outage = gaps[gaps > 10 * nominal_interval]

    return {
        "n_files": len(all_files),
        "n_gaps": len(gaps),
        "median_gap_s": round(float(np.median(gaps)), 1),
        "mean_gap_s": round(float(np.mean(gaps)), 1),
        "p95_gap_s": round(float(np.percentile(gaps, 95)), 1),
        "max_gap_s": round(float(np.max(gaps)), 1),
        "pct_within_2x_nominal": round(100 * on_time, 1),
        "n_outage_gaps_gt10x": len(outage),
    }


# ─────────────────────────────────────────────────────────────────────────
# 2. Throughput: records/file from a bounded sample, per window
# ─────────────────────────────────────────────────────────────────────────

def compute_throughput(source: str, nominal_interval: int, dates: list[str],
                        sample_size: int = THROUGHPUT_SAMPLE_FILES) -> dict:
    files = []
    for d in dates:
        files.extend(list_parquet_files_from_s3(source, with_metadata=True, date_prefix=d))
        if len(files) >= sample_size:
            break
    files = files[:sample_size]
    if not files:
        return {"note": "no files found"}

    row_counts = []
    for f in files:
        try:
            df = read_parquet_from_s3(f["key"])
            if df is not None:
                row_counts.append(df.height if hasattr(df, "height") else len(df))
        except Exception:
            continue
    if not row_counts:
        return {"note": "no readable files"}

    mean_rows = float(np.mean(row_counts))
    result = {
        "n_files_sampled": len(row_counts),
        "mean_records_per_file": round(mean_rows, 1),
        "median_records_per_file": round(float(np.median(row_counts)), 1),
        "records_per_min_per_file_type": round(mean_rows * (60 / nominal_interval), 1),
    }

    if source == "avinor":
        files_per_cycle = _avinor_files_per_cycle(dates, nominal_interval)
        if files_per_cycle is not None:
            result["active_files_per_cycle"] = round(files_per_cycle, 1)
            agg_per_min = mean_rows * files_per_cycle * (60 / nominal_interval)
            result["aggregate_records_per_min"] = round(agg_per_min, 1)

    return result


def _avinor_files_per_cycle(dates: list[str], nominal_interval: int, n_cycles: int = 5) -> float | None:
    """
    Avinor writes one file per active airport per collection cycle (only airports
    with departures in the query window produce a file -- see avinor_collect.py).
    Estimate active-airports-per-cycle by counting files in a multi-cycle window
    anchored at the midpoint of a sample day (avoids collector-startup artifacts
    at the very first timestamp of the day).
    """
    files = list_parquet_files_from_s3("avinor", with_metadata=True, date_prefix=dates[0])
    times = sorted(f["last_modified"] for f in files)
    if len(times) < 20:
        return None
    mid = times[len(times) // 2]
    window = timedelta(seconds=n_cycles * nominal_interval)
    window_files = [t for t in times if mid <= t <= mid + window]
    return len(window_files) / n_cycles


# ─────────────────────────────────────────────────────────────────────────
# 3. Latency: Entur SIRI-ET RecordedAtTime vs. collection time
# ─────────────────────────────────────────────────────────────────────────

def compute_latency_entur(dates: list[str], n_days: int = LATENCY_SAMPLE_DAYS) -> dict:
    sample_dates = dates[:n_days]
    all_latencies = []
    for d in sample_dates:
        files = list_parquet_files_from_s3("entur", with_metadata=True, date_prefix=d,
                                            pattern="*siri_et*.parquet")
        for f in files:
            try:
                df = read_parquet_from_s3(f["key"])
            except Exception:
                continue
            if df is None or df.height == 0 or "timestamp" not in df.columns:
                continue
            fetch_epoch = f["last_modified"].timestamp()
            ts = df["timestamp"].cast(pl.Int64, strict=False).drop_nulls()
            ts = ts.filter(ts > 0)
            if len(ts) == 0:
                continue
            lat = fetch_epoch - ts.to_numpy()
            lat = lat[(lat >= 0) & (lat < 3600)]  # drop clock-skew negatives / bad outliers
            all_latencies.extend(lat.tolist())

    if not all_latencies:
        return {"note": "no valid latency samples"}
    arr = np.array(all_latencies)
    return {
        "n_records": len(arr),
        "n_days_sampled": len(sample_dates),
        "median_latency_s": round(float(np.median(arr)), 1),
        "mean_latency_s": round(float(np.mean(arr)), 1),
        "p95_latency_s": round(float(np.percentile(arr, 95)), 1),
        "max_latency_s": round(float(np.max(arr)), 1),
    }


# ─────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────

def build_report() -> dict:
    report = {}
    for period in PERIODS_WA1_WA2_WB_WC:
        name = period["name"]
        dates = _dates_in_window(period["start_date"], period["end_date"])
        print(f"\n=== {name} ({dates[0]}..{dates[-1]}) ===")

        window_report = {"robustness": {}, "throughput": {}}
        for source, interval in SOURCES.items():
            print(f"  robustness: {source} ...")
            window_report["robustness"][source] = compute_robustness(source, interval, dates)
            print(f"  throughput: {source} ...")
            window_report["throughput"][source] = compute_throughput(source, interval, dates)

        print("  latency: entur siri_et ...")
        window_report["latency_entur_siri_et"] = compute_latency_entur(dates)

        report[name] = window_report
        print_report({name: window_report})
    return report


def print_report(report: dict) -> None:
    for name, w in report.items():
        print(f"\n{'=' * 70}\n{name}\n{'=' * 70}")
        print("-- Robustness (inter-arrival gaps) --")
        for source, r in w["robustness"].items():
            if "note" in r:
                print(f"  {source:14} {r['note']}")
                continue
            print(f"  {source:14} n={r['n_files']:6d}  median_gap={r['median_gap_s']:7.1f}s "
                  f" p95={r['p95_gap_s']:8.1f}s  max={r['max_gap_s']:9.1f}s "
                  f" on-time(<=2x)={r['pct_within_2x_nominal']:5.1f}%  "
                  f"outages(>10x)={r['n_outage_gaps_gt10x']}")
        print("-- Throughput (sampled) --")
        for source, r in w["throughput"].items():
            if "note" in r:
                print(f"  {source:14} {r['note']}")
                continue
            print(f"  {source:14} n_sampled={r['n_files_sampled']:4d}  "
                  f"mean_records/file={r['mean_records_per_file']:8.1f}  "
                  f"~records/min={r['records_per_min_per_file_type']:8.1f}")
            if "aggregate_records_per_min" in r:
                print(f"  {'':14} active_files/cycle={r['active_files_per_cycle']:6.1f}  "
                      f"aggregate_records/min={r['aggregate_records_per_min']:8.1f}  "
                      f"(mean_records/file x active_files/cycle x 60/interval)")
        print("-- Latency (Entur SIRI-ET, RecordedAtTime -> collection) --")
        r = w["latency_entur_siri_et"]
        if "note" in r:
            print(f"  {r['note']}")
        else:
            print(f"  n={r['n_records']}  days_sampled={r['n_days_sampled']}  "
                  f"median={r['median_latency_s']}s  mean={r['mean_latency_s']}s  "
                  f"p95={r['p95_latency_s']}s  max={r['max_latency_s']}s")


if __name__ == "__main__":
    rep = build_report()
    print_report(rep)
