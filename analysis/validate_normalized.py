"""
Validacija normalizovanih parquet fajlova sa S3 za zadati interval datuma.

Korišćenje (iz project root, sa .venv):
  .venv/bin/python analysis/validate_normalized.py
  .venv/bin/python analysis/validate_normalized.py --source vegvesen
  ... --start-date 2026-01-30 --end-date 2026-02-20 --source vegvesen

Podrazumevani interval: 30. januar – 20. februar (2026).
"""
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.s3_upload import (  # noqa: E402
    list_normalized_parquet_files_from_s3,
    read_parquet_from_s3,
)

# Obavezne kolone po izvoru (za analizu / weather-impact)
REQUIRED_COLUMNS = {
    "avinor": ["timestamp", "location", "metric_value", "source"],
    "entur_siri_et": [
        "timestamp", "departure_timestamp",
        "max_departure_delay_sec", "max_arrival_delay_sec",
    ],
    "oslobysykkel": ["timestamp", "location", "metric_value", "source"],
    "vegvesen": ["timestamp", "location", "road_temperature", "source"],
    "vegvesen_travel_times": [
        "location_id", "period_start", "period_end",
        "free_flow_travel_time_sec", "traffic_status", "source",
    ],
}


def _source_from_key(key: str) -> str | None:
    """Iz S3 ključa izvući tip izvora."""
    name = os.path.basename(key).lower()
    if "avinor_normalized_" in name:
        return "avinor"
    if "entur_siri_et_normalized_" in name:
        return "entur_siri_et"
    if "oslobysykkel_normalized_" in name:
        return "oslobysykkel"
    if "vegvesen_travel_times_normalized_" in name:
        return "vegvesen_travel_times"
    if "vegvesen_normalized_" in name:
        return "vegvesen"
    return None


def _date_from_key(key: str) -> str | None:
    """Iz S3 ključa izvući datum YYYY-MM-DD (.../normalized/YYYY-MM-DD/...)."""
    parts = key.replace("\\", "/").split("/")
    for i, p in enumerate(parts):
        if p == "normalized" and i + 1 < len(parts):
            date_str = parts[i + 1]
            if len(date_str) == 10 and date_str.count("-") == 2:
                try:
                    datetime.strptime(date_str, "%Y-%m-%d")
                    return date_str
                except ValueError:
                    pass
    return None


def _filter_keys_by_interval(keys, start_date: str, end_date: str):
    """Zadržati ključeve čiji datum je u [start_date, end_date]."""
    out = []
    for key in keys:
        d = _date_from_key(key)
        if d and start_date <= d <= end_date:
            out.append(key)
    return out


def validate_file(s3_key: str, source: str, start_date: str, end_date: str) -> dict:
    """
    Učitaj jedan normalizovani fajl sa S3; proveri šemu i opseg timestamp-a.
    Vraća dict: ok, errors, rows, timestamp_min, timestamp_max, missing_cols.
    """
    result = {
        "ok": True,
        "errors": [],
        "rows": 0,
        "timestamp_min": None,
        "timestamp_max": None,
        "missing_cols": [],
    }
    df = read_parquet_from_s3(s3_key)
    if df is None:
        result["ok"] = False
        result["errors"].append("Nije moguće učitati fajl sa S3")
        return result

    result["rows"] = len(df)
    if result["rows"] == 0:
        result["errors"].append("Fajl nema redova")
        result["ok"] = False
        return result

    required = REQUIRED_COLUMNS.get(source)
    if required:
        missing = [c for c in required if c not in df.columns]
        if missing:
            result["missing_cols"] = missing
            result["errors"].append(f"Nedostaju kolone: {missing}")
            result["ok"] = False

    # Opseg timestamp-a (ako postoji kolona)
    ts_col = "timestamp" if "timestamp" in df.columns else None
    if not ts_col and source == "vegvesen_travel_times":
        ts_col = "period_start" if "period_start" in df.columns else None
    if ts_col:
        try:
            ts = df[ts_col]
            if hasattr(ts, "min") and ts.min() is not None:
                result["timestamp_min"] = str(ts.min())
            if hasattr(ts, "max") and ts.max() is not None:
                result["timestamp_max"] = str(ts.max())
            min_s, max_s = result["timestamp_min"], result["timestamp_max"]
            if min_s and max_s:
                try:
                    # entur_siri_et: timestamp je Unix epoch (sekunde) – konvertuj u datum
                    if source == "entur_siri_et" and ts_col == "timestamp":
                        try:
                            min_val = ts.min()
                            max_val = ts.max()
                            if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
                                if min_val > 1e9 and max_val > 1e9:
                                    utc = timezone.utc
                                    min_dt = datetime.fromtimestamp(int(min_val), tz=utc).strftime("%Y-%m-%d")
                                    max_dt = datetime.fromtimestamp(int(max_val), tz=utc).strftime("%Y-%m-%d")
                                else:
                                    # timestamp 0 ili nevažeći – preskoči proveru opsega (kvalitet podataka)
                                    min_dt, max_dt = start_date, end_date
                            else:
                                min_dt, max_dt = min_s[:10], max_s[:10]
                        except (ValueError, OSError):
                            min_dt, max_dt = min_s[:10], max_s[:10]
                    else:
                        min_dt = min_s[:10]
                        max_dt = max_s[:10]
                    # Vegvesen / Avinor: podaci mogu prelaziti u susedni dan (leta, merenja)
                    if source in ("vegvesen", "avinor"):
                        pass
                    # Entur: dozvoli do 1 dan pre/posle intervala (prelaz u ponoć)
                    elif source == "entur_siri_et":
                        start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
                        end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
                        try:
                            min_d = datetime.strptime(min_dt, "%Y-%m-%d").date()
                            max_d = datetime.strptime(max_dt, "%Y-%m-%d").date()
                            if (min_d - start_d).days < -1 or (max_d - end_d).days > 1:
                                result["errors"].append(
                                    f"Timestamp van intervala: {min_dt}..{max_dt} "
                                    f"(očekivano {start_date}..{end_date})"
                                )
                                result["ok"] = False
                        except ValueError:
                            pass
                    elif min_dt < start_date or max_dt > end_date:
                        result["errors"].append(
                            f"Timestamp van intervala: {min_dt}..{max_dt} "
                            f"(očekivano {start_date}..{end_date})"
                        )
                        result["ok"] = False
                except Exception:
                    pass
        except Exception as e:
            result["errors"].append(f"Greška pri čitanju timestamp: {e}")
            result["ok"] = False

    return result


def run_validation(
    start_date: str = "2026-01-30",
    end_date: str = "2026-02-20",
    sources_filter: list | None = None,
):
    """Lista normalizovanih fajlova sa S3, filtrira po intervalu, validira, štampa."""
    print("=" * 60)
    print("VALIDACIJA NORMALIZOVANIH PODATAKA")
    print(f"Interval: {start_date} — {end_date}")
    if sources_filter:
        print(f"Izvori: {', '.join(sources_filter)}")
    print("=" * 60)

    all_keys = list_normalized_parquet_files_from_s3()
    if not all_keys:
        print("Nema normalizovanih parquet fajlova na S3.")
        return

    keys = _filter_keys_by_interval(all_keys, start_date, end_date)
    # Filter by source if requested
    if sources_filter:
        keys = [k for k in keys if _source_from_key(k) in sources_filter]
    print(f"\nPronađeno {len(keys)} normalizovanih fajlova u intervalu.\n")

    # Grupiši po (datum, izvor)
    by_date_source = {}
    for key in keys:
        d = _date_from_key(key)
        src = _source_from_key(key)
        if d and src:
            by_date_source.setdefault(d, {}).setdefault(src, []).append(key)

    # Očekivani datumi u intervalu
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    expected_dates = []
    d = start
    while d <= end:
        expected_dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    all_sources = [
        "avinor", "oslobysykkel", "vegvesen",
        "vegvesen_travel_times", "entur_siri_et",
    ]
    sources_order = sources_filter if sources_filter else all_sources

    # 1) Tabela pokrivenosti: datum × izvor (broj redova)
    print("--- POKRIVENOST (datum × izvor) ---")
    labels = [s[:8] if len(s) > 8 else s for s in sources_order]
    print(f"{'Datum':<12} " + "".join(f"{lab:>10}" for lab in labels))
    print("-" * (12 + 10 * len(sources_order)))

    all_ok = True
    validation_details = []

    for date_str in sorted(expected_dates):
        row_parts = [f"{date_str:<12}"]
        for src in sources_order:
            files = by_date_source.get(date_str, {}).get(src, [])
            if not files:
                row_parts.append(f"{'—':>10}")
                continue
            # Jedan fajl po danu/izvoru; uzimamo poslednji po ključu
            key = sorted(files)[-1]
            vr = validate_file(key, src, start_date, end_date)
            validation_details.append((date_str, src, key, vr))
            if not vr["ok"]:
                all_ok = False
            total_rows = vr["rows"]
            mark = "✓" if vr["ok"] else "✗"
            row_parts.append(f"{total_rows:>8} {mark}")
        print(" ".join(row_parts))

    # 2) Greške validacije
    errors_list = [(d, s, k, v)
                   for (d, s, k, v) in validation_details if not v["ok"]]
    if errors_list:
        print("\n--- GREŠKE VALIDACIJE ---")
        for date_str, src, key, vr in errors_list:
            print(f"  {date_str} {src}: {os.path.basename(key)}")
            for e in vr["errors"]:
                print(f"    - {e}")
    else:
        print("\nNema grešaka validacije.")

    # 3) Nedostajući datumi/izvori
    print("\n--- REZIME ---")
    missing = []
    for date_str in expected_dates:
        for src in sources_order:
            if not by_date_source.get(date_str, {}).get(src):
                missing.append((date_str, src))
    if missing:
        by_d = defaultdict(list)
        for d, s in missing:
            by_d[d].append(s)
        print("Datumi/izvori bez normalizovanih podataka:")
        for d in sorted(by_d.keys()):
            print(f"  {d}: {', '.join(by_d[d])}")
    else:
        print("Svi datumi u intervalu imaju podatke za sve izvore.")

    print("\nValidacija završena.")
    return all_ok


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Validacija normalizovanih podataka za interval."
    )
    parser.add_argument("--start-date", type=str,
                        default="2026-01-30", metavar="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str,
                        default="2026-02-20", metavar="YYYY-MM-DD")
    parser.add_argument(
        "--source",
        type=str,
        action="append",
        dest="sources",
        help="Samo ovaj izvor (npr. --source vegvesen)",
    )
    args = parser.parse_args()
    run_validation(
        start_date=args.start_date,
        end_date=args.end_date,
        sources_filter=args.sources,
    )
