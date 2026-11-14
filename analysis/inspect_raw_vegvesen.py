"""
Ispitivanje sadržaja raw vegvesen weather_data parquet fajlova sa S3.

Pokretanje (iz project root, .venv):
  .venv/bin/python analysis/inspect_raw_vegvesen.py
  .venv/bin/python analysis/inspect_raw_vegvesen.py --date 2026-01-30
"""
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import polars as pl

from utils.s3_upload import (  # noqa: E402
    list_parquet_files_from_s3,
    read_parquet_from_s3,
)

# Isti nazivi kolona kao u normalize_data.py
TIME_COL = "ns10:measurementTimeDefault.ns10:timeValue"
LOC_COL = "ns10:measurementSiteReference.@id"
PQ_COL = "ns10:physicalQuantity"


def _run_aggregate(files, date_prefix):
    """Učitaj sve fajlove za datum, spoji, prikaži globalni min/max i raspodelu po datumu."""
    print(f"Učitavam sve {len(files)} fajlova za {date_prefix}...")
    all_ts = []
    for s3_key in files:
        df = read_parquet_from_s3(s3_key)
        if df is None or TIME_COL not in df.columns:
            continue
        ts_str = df[TIME_COL]
        if ts_str.dtype != pl.Utf8 and str(ts_str.dtype) != "String":
            ts_str = ts_str.cast(pl.Utf8)
        ts = ts_str.str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False
        )
        all_ts.append(ts)
    if not all_ts:
        print("Nije moguće učitati timestamp kolonu iz nijednog fajla.")
        return
    combined = pl.concat(all_ts)
    valid = combined.drop_nulls()
    if valid.len() == 0:
        print("Nema validnih timestamp vrednosti.")
        return
    min_ts = valid.min()
    max_ts = valid.max()
    print(f"\n  GLOBALNI raspon (svi raw fajlovi za {date_prefix}):")
    print(f"    Timestamp min: {min_ts}")
    print(f"    Timestamp max: {max_ts}")
    date_series = valid.dt.strftime("%Y-%m-%d")
    by_date = date_series.value_counts()
    by_date = by_date.sort(by=by_date.columns[0])
    print(f"\n  Broj redova po datumu (ukupno {len(by_date)} različitih datuma):")
    date_col = by_date.columns[0]
    for row in by_date.iter_rows(named=True):
        print(f"    {row[date_col]}: {row['count']} redova")
    print("\nGotovo.")


def _run_deep(files, date_prefix):
    """
    Detaljno: šta je jedan red, odakle stari datumi, koje stanice, šta koristimo za analizu.
    """
    print(f"Učitavam prvi fajl za {date_prefix} (deep struktura)...\n")
    df = read_parquet_from_s3(files[0])
    if df is None:
        print("Greška pri čitanju.")
        return

    # --- 1) Pre explode: šta je jedan red u raw fajlu ---
    print("=" * 60)
    print("1) STRUKTURA RAW FAJLA (pre explode)")
    print("=" * 60)
    print(f"  Jedan red = jedan 'site measurement' iz API odgovora (Datex2 GetMeasuredWeatherData).")
    print(f"  Redova: {df.height}")
    print(f"  Kolone: {list(df.columns)}")
    if TIME_COL in df.columns:
        ts_str = df[TIME_COL].cast(pl.Utf8)
        ts = ts_str.str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False
        )
        dates = ts.dt.strftime("%Y-%m-%d")
        uniq_dates = dates.unique().len()
        print(f"  Jedinstvenih datuma u vremenskoj koloni: {uniq_dates}")
    if LOC_COL in df.columns:
        n_sites = df[LOC_COL].n_unique()
        print(f"  Jedinstvenih stanica (measurementSiteReference.@id): {n_sites}")
    if PQ_COL in df.columns:
        # Broj elemenata u listi po redu (otprilike)
        lens = df[PQ_COL].list.len()
        print(f"  physicalQuantity: lista dužine min={lens.min()}, max={lens.max()} po redu")
    print()

    # --- 2) Posle explode (kao u normalizeru) ---
    if PQ_COL not in df.columns or TIME_COL not in df.columns:
        print("Nedostaje physicalQuantity ili time kolona.")
        return
    exploded = df.explode(PQ_COL)
    print("2) POSLE EXPLODE (kao u normalize_vegvesen)")
    print("=" * 60)
    print(f"  Jedan red = jedno merenje (jedna stanica, jedan trenutak, jedan tip merenja).")
    print(f"  Redova: {exploded.height}")
    ts_str = exploded[TIME_COL].cast(pl.Utf8)
    ts = ts_str.str.to_datetime(
        format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False
    )
    date_series = ts.dt.strftime("%Y-%m-%d")
    by_date = date_series.value_counts()
    by_date = by_date.sort(by=by_date.columns[0])
    date_col = by_date.columns[0]
    print(f"  Raspodela po datumu:")
    for row in by_date.iter_rows(named=True):
        print(f"    {row[date_col]}: {row['count']} redova")
    print()

    # --- 3) Za stare datume: koje stanice? ---
    print("3) ŠTA SU 'STARI DATUMI'? (stanice po datumu)")
    print("=" * 60)
    exploded = exploded.with_columns(ts.dt.strftime("%Y-%m-%d").alias("_date"))
    if LOC_COL in exploded.columns:
        for row in by_date.iter_rows(named=True):
            d = row[date_col]
            cnt = row["count"]
            sub = exploded.filter(pl.col("_date") == d)
            sites = sub[LOC_COL].unique()
            n_sites = len(sites)
            sample = sites.head(5).to_list()
            print(f"  {d}: {cnt} redova, {n_sites} stanica. Primer stanica: {sample}")
    print()
    print("  Objašnjenje: API GetMeasuredWeatherData u jednom odgovoru vraća")
    print("  više 'site measurement' zapisa. Svaki zapis ima jedan measurementTimeDefault")
    print("  i listu physicalQuantity (tipovi merenja). Stari datumi su zapisi koji")
    print("  nose staru vremensku oznaku (npr. poslednje ažurirano merenje te stanice).")
    print("  Zato u fajlu za jedan dan ima i 2024-09-09: to su pojedinačna merenja")
    print("  sa tom oznakom, ne ceo dan. Za analizu po danu treba filtrirati na datum.")
    print()

    # --- 4) Šta sve imamo i šta možemo iskoristiti ---
    print("4) ŠTA IMAMO U PODACIMA I ZA ANALIZU")
    print("=" * 60)
    print("  U normalizovanom vegvesen (vremenski podaci) već koristimo:")
    print("    - location (ID merne stanice) -> za 'deo grada' treba mapirati na zonu/oblast")
    print("    - timestamp -> zavisnost od vremena, doba dana")
    print("    - road_temperature, air_temperature, precipitation_intensity, wind_*, humidity")
    print("    - metric_name, measurement_type")
    print()
    print("  Za tvoj fokus:")
    print("    - Zavisnost od vremena: timestamp + sve meteorološke kolone (već imamo).")
    print("    - Deo grada: location = measurement site ID; treba lookup tabela")
    print("      (stanica -> koordinate ili zona/borough) da grupišeš po delu grada.")
    print("    - Multimodal: vegvesen weather + vegvesen_travel_times (congestion),")
    print("      oslobysykkel (bike), entur (javni prevoz), avinor (avio). analyze_weather_impact.py")
    print("      već spaja vremenske uslove sa bajsom, PT i avionom.")
    print()
    print("  Dodatno u raw koje možemo iskoristiti:")
    print("    - measurement_type (@xsi:type) za finiju tipizaciju merenja")
    print("    - metric_name (@index) za grupisane statistike po tipu")
    print("    - Sve meteorološke kolone su već u normalizovanom (road/air temp, padavine, vetar).")
    print("  Za 'deo grada': ako Vegvesen ima spisak stanica sa koordinatama,")
    print("  možemo ih uvesti i mapirati location -> lat/lon ili zona (npr. Oslo nord/sør).")
    print("\nGotovo.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Ispitaj raw vegvesen weather_data fajl(ove).")
    parser.add_argument(
        "--date",
        type=str,
        default="2026-01-30",
        metavar="YYYY-MM-DD",
        help="Datum folder na S3 (npr. 2026-01-30)",
    )
    parser.add_argument(
        "--files",
        type=int,
        default=1,
        help="Koliko fajlova da učita (po redosledu); 0 = svi.",
    )
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="Učitaj sve fajlove za datum i prikaži globalni min/max i raspodelu po datumu.",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Detaljno: struktura reda, odakle stari datumi, stanice po datumu, šta sve možemo koristiti.",
    )
    args = parser.parse_args()

    date_prefix = args.date
    files = list_parquet_files_from_s3(
        "vegvesen",
        "*weather_data*.parquet",
        exclude_normalized=True,
        with_metadata=False,
        date_prefix=date_prefix,
    )
    if not files:
        print(f"Nema raw vegvesen weather_data fajlova za {date_prefix}.")
        return

    if args.aggregate:
        _run_aggregate(files, date_prefix)
        return

    if args.deep:
        _run_deep(files, date_prefix)
        return

    to_load = files[: args.files] if args.files else files
    print(f"Za {date_prefix} nađeno {len(files)} fajlova. Učitavam {len(to_load)}.\n")

    for i, s3_key in enumerate(to_load):
        print("=" * 60)
        print(f"FAJL {i + 1}: {os.path.basename(s3_key)}")
        print("=" * 60)
        df = read_parquet_from_s3(s3_key)
        if df is None:
            print("  Greška pri čitanju.\n")
            continue

        print(f"  Redova: {df.height}, Kolona: {len(df.columns)}")
        print(f"  Kolone: {df.columns}")

        if TIME_COL not in df.columns:
            # Možda je ugnježdeno (struct)
            for c in df.columns:
                if "time" in c.lower() or "Time" in c:
                    print(f"  Kolona sa 'time': {c}")
            print()
            continue

        # Parsiranje vremena
        ts_str = df[TIME_COL]
        if ts_str.dtype != pl.Utf8 and str(ts_str.dtype) != "String":
            ts_str = ts_str.cast(pl.Utf8)
        ts = ts_str.str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False
        )
        valid = ts.drop_nulls()
        if valid.len() == 0:
            print("  Nema validnih timestamp vrednosti.")
            print()
            continue

        min_ts = valid.min()
        max_ts = valid.max()
        print(f"  Timestamp min: {min_ts}")
        print(f"  Timestamp max: {max_ts}")

        # Datum (YYYY-MM-DD) za svaki red
        date_series = ts.dt.strftime("%Y-%m-%d")
        by_date = date_series.value_counts()
        dates_sorted = by_date.sort(by=by_date.columns[0])
        n = len(dates_sorted)
        date_col = dates_sorted.columns[0]
        print(f"\n  Broj redova po datumu (ukupno {n} različitih datuma):")
        if n <= 20:
            for row in dates_sorted.iter_rows(named=True):
                print(f"    {row[date_col]}: {row['count']} redova")
        else:
            head = dates_sorted.head(10)
            tail = dates_sorted.tail(10)
            for row in head.iter_rows(named=True):
                print(f"    {row[date_col]}: {row['count']} redova")
            print("    ...")
            for row in tail.iter_rows(named=True):
                print(f"    {row[date_col]}: {row['count']} redova")

        print()

    print("Gotovo.")


if __name__ == "__main__":
    main()
