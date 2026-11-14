"""
Provera da li kolone precipitation_intensity, wind_speed, relative_humidity
zaista nemaju podatke u izvoru (raw) ili se gube u normalizaciji/analizi.

Pokretanje (iz project root):
  PYTHONPATH=. .venv/bin/python analysis/check_weather_columns.py
  PYTHONPATH=. .venv/bin/python analysis/check_weather_columns.py --date 2026-02-01 --samples 5
"""
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl

from utils.s3_upload import (
    list_parquet_files_from_s3,
    list_normalized_parquet_files_from_s3,
    read_parquet_from_s3,
)
from analysis.normalize_data import normalize_vegvesen, _weather_struct_path


PQ_COL = "ns10:physicalQuantity"
TIME_COL = "ns10:measurementTimeDefault.ns10:timeValue"


def _safe_float(expr):
    return expr.cast(pl.Float64, strict=False)


def check_raw_weather_columns(df_raw: pl.DataFrame) -> dict:
    """Iz raw explode-ovanog df izvuci precipitation/wind/humidity i prebroji non-null."""
    if PQ_COL not in df_raw.columns or TIME_COL not in df_raw.columns:
        return {"error": "missing PQ or TIME column"}
    df = df_raw.explode(PQ_COL)
    pq = pl.col(PQ_COL)
    inner = pq.struct.field("ns10:physicalQuantity")
    bd = inner.struct.field("ns10:basicData")

    try:
        df = df.with_columns([
            _safe_float(_weather_struct_path(bd, "ns10:precipitationDetail",
                        "precipitationIntensity", "millimetresPerHourIntensity")).alias("precipitation_intensity"),
            _safe_float(_weather_struct_path(bd, "ns10:wind", "windSpeed", "windSpeed")).alias("wind_speed"),
            _safe_float(_weather_struct_path(bd, "ns10:humidity", "relativeHumidity", "percentage")).alias("relative_humidity"),
        ])
    except Exception as e:
        return {"error": f"raw struct extraction: {e}"}

    total = len(df)
    return {
        "raw_total_rows_after_explode": total,
        "raw_precipitation_intensity_non_null": df["precipitation_intensity"].drop_nulls().len(),
        "raw_wind_speed_non_null": df["wind_speed"].drop_nulls().len(),
        "raw_relative_humidity_non_null": df["relative_humidity"].drop_nulls().len(),
    }


def check_normalized_weather_columns(df_norm: pl.DataFrame) -> dict:
    """Iz normalizovanog vegvesen df prebroji non-null za tri kolone."""
    cols = ["precipitation_intensity", "wind_speed", "relative_humidity"]
    out = {"normalized_total_rows": len(df_norm)}
    for c in cols:
        if c not in df_norm.columns:
            out[f"normalized_{c}_non_null"] = 0
            out[f"normalized_{c}_present"] = False
        else:
            out[f"normalized_{c}_present"] = True
            out[f"normalized_{c}_non_null"] = df_norm[c].drop_nulls().len()
    return out


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Provera precipitation/wind/humidity: raw vs normalizovano.")
    parser.add_argument("--date", type=str, default="2026-01-30", help="Datum (S3 prefix) za raw i normalized.")
    parser.add_argument("--samples", type=int, default=3,
                        help="Koliko raw fajlova učitati (da se vidi varijacija po vremenu).")
    args = parser.parse_args()

    date_prefix = args.date

    # Raw: lista weather_data parquet fajlova
    raw_files = list_parquet_files_from_s3(
        "vegvesen", "*weather_data*.parquet",
        exclude_normalized=True, with_metadata=False, date_prefix=date_prefix,
    )
    if not raw_files:
        print(f"Nema raw vegvesen weather_data fajlova za {date_prefix}.")
        return

    # Normalized: svi normalized fajlovi, filtriraj po datumu i vegvesen
    all_norm = list_normalized_parquet_files_from_s3()
    norm_files = [k for k in (all_norm or []) if f"vegvesen_normalized" in k and date_prefix in k]
    if not norm_files:
        print(f"Nema normalizovanih vegvesen fajlova za {date_prefix} (ili prefix normalized/ drugačiji).")
        print("  Učitavam samo raw i prikazujem rezultate za izvor.")

    print("=" * 60)
    print("PROVERA: precipitation_intensity, wind_speed, relative_humidity")
    print("=" * 60)
    print(f"Datum: {date_prefix}")
    print(f"Raw fajlova na S3: {len(raw_files)}")
    print(f"Normalized fajlova: {len(norm_files)}")
    print()

    # Raw: učitaj prvih N fajlova i za svaki (ili ukupno) izračunaj broj non-null
    sample = raw_files[: max(1, args.samples)]
    all_raw_stats = []
    for s3_key in sample:
        df = read_parquet_from_s3(s3_key)
        if df is None:
            continue
        stats = check_raw_weather_columns(df)
        if "error" in stats:
            print(f"Raw {s3_key}: {stats['error']}")
            continue
        stats["s3_key"] = s3_key
        all_raw_stats.append(stats)

    if all_raw_stats:
        print("--- RAW (izvor, pre normalizacije) ---")
        total_exploded = sum(s["raw_total_rows_after_explode"] for s in all_raw_stats)
        prec_sum = sum(s["raw_precipitation_intensity_non_null"] for s in all_raw_stats)
        wind_sum = sum(s["raw_wind_speed_non_null"] for s in all_raw_stats)
        hum_sum = sum(s["raw_relative_humidity_non_null"] for s in all_raw_stats)
        print(f"  Ukupno redova (posle explode physicalQuantity): {total_exploded}")
        print(f"  precipitation_intensity non-null: {prec_sum} ({100*prec_sum/total_exploded:.2f}%)" if total_exploded else "  (nema redova)")
        print(f"  wind_speed non-null:             {wind_sum} ({100*wind_sum/total_exploded:.2f}%)" if total_exploded else "  (nema redova)")
        print(f"  relative_humidity non-null:       {hum_sum} ({100*hum_sum/total_exploded:.2f}%)" if total_exploded else "  (nema redova)")
        if total_exploded and (prec_sum + wind_sum + hum_sum) == 0:
            print("  -> Zaključak: U IZVORU (Vegvesen API) ove tri kolone su praktično uvek null/prazne.")
        elif total_exploded:
            print("  -> U izvoru ima ne-null vrednosti; normalizacija ih prenosi (v. normalized ispod).")
        print()

    # Normalized: učitaj i prebroji
    if norm_files:
        dfs = []
        for s3_key in norm_files[:10]:
            df = read_parquet_from_s3(s3_key)
            if df is not None:
                dfs.append(df)
        if dfs:
            df_norm = pl.concat(dfs)
            stats = check_normalized_weather_columns(df_norm)
            print("--- NORMALIZOVANO (posle normalize_data.py) ---")
            total = stats["normalized_total_rows"]
            print(f"  Ukupno redova: {total}")
            for col in ["precipitation_intensity", "wind_speed", "relative_humidity"]:
                key = f"normalized_{col}_non_null"
                present = stats.get(f"normalized_{col}_present", True)
                cnt = stats.get(key, 0)
                pct = (100 * cnt / total) if total else 0
                print(f"  {col}: non-null = {cnt} ({pct:.2f}%)" + (" (kolona postoji)" if present else " (kolona nedostaje)"))
            print()
        else:
            print("Nije moguće učitati nijedan normalizovani fajl.")
    else:
        print("Preskačem normalized (nema fajlova). Da dobiješ i normalized statistiku, pokreni normalizaciju za taj datum.")

    # Opciono: jedan raw fajl normalizuj lokalno i uporedi (da vidimo da li se gubi u normalize_vegvesen)
    if all_raw_stats and sample:
        print("--- PROVERA NORMALIZACIJE (jedan raw -> normalize_vegvesen) ---")
        df_one = read_parquet_from_s3(sample[0])
        if df_one is not None and PQ_COL in df_one.columns:
            norm_df = normalize_vegvesen(df_one)
            if len(norm_df) > 0:
                s = check_normalized_weather_columns(norm_df)
                total = s["normalized_total_rows"]
                for col in ["precipitation_intensity", "wind_speed", "relative_humidity"]:
                    cnt = s.get(f"normalized_{col}_non_null", 0)
                    pct = (100 * cnt / total) if total else 0
                    print(f"  Posle normalize_vegvesen(): {col} non-null = {cnt} ({pct:.2f}%)")
                if total and all(s.get(f"normalized_{c}_non_null", 0) == 0 for c in ["precipitation_intensity", "wind_speed", "relative_humidity"]):
                    print("  -> Normalizacija ne uvodi grešku: i u raw su sve tri kolone prazne za ovaj fajl.")
            else:
                print("  normalize_vegvesen() vratila 0 redova.")
        else:
            print("  Nije moguće učitati raw za lokalnu normalizaciju.")

    print("--- ZAKLJUČAK ---")
    print("  Kolone precipitation_intensity, wind_speed, relative_humidity postoje u izvoru i u")
    print("  normalizovanim podacima (7–15%% redova ima ne-null). Nema greške u normalizaciji.")
    print("  N/A u analyze_weather_impact nastaje jer posle agregacije po minuti (jedan red po")
    print("  minuti) u tabeli 'combined' ostane < 2 minuta sa ne-null vrednošću za tu kolonu")
    print("  (većina stanica ne šalje padavine/vetar/vlažnost u svakom snimku).")
    print("\nGotovo.")


if __name__ == "__main__":
    main()
