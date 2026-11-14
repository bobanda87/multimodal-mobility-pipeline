"""
Provera kolone start_time u jednom raw Entur SIRI ET parquet fajlu.
Koristi se da vidimo da li je start_time null svuda ili samo u nekim redovima.

Pokretanje (iz project root, PYTHONPATH postavljen):
  PYTHONPATH=. .venv/bin/python analysis/inspect_entur_start_time.py
  PYTHONPATH=. .venv/bin/python analysis/inspect_entur_start_time.py --date 2026-02-17 --file OST_siri_et_000543
"""
from utils.s3_upload import list_parquet_files_from_s3, read_parquet_from_s3
import polars as pl
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def main():
    import argparse
    p = argparse.ArgumentParser(
        description="Inspect start_time in one Entur SIRI ET parquet.")
    p.add_argument("--date", default="2026-02-17",
                   help="Date folder (YYYY-MM-DD)")
    p.add_argument("--file", default="OST_siri_et_000543",
                   help="Substring of filename to find (e.g. OST_siri_et_000543)")
    args = p.parse_args()

    files = list_parquet_files_from_s3(
        "entur", "*_siri_et_*.parquet", with_metadata=False, date_prefix=args.date
    )
    if not files:
        print(f"Nema entur SIRI ET fajlova za {args.date}.")
        return

    # Nađi prvi fajl čije ime sadrži args.file
    match = [k for k in files if args.file in os.path.basename(k)]
    if not match:
        print(
            f"Nijedan fajl ne sadrži '{args.file}' u imenu. Uzimam prvi fajl za datum.")
        s3_key = files[0]
    else:
        s3_key = match[0]

    print(f"Učitavam: {s3_key}\n")
    df = read_parquet_from_s3(s3_key)
    if df is None:
        print("Greška pri čitanju.")
        return

    n = df.height
    print(f"Redova: {n}")
    print(f"Kolone: {df.columns}\n")

    if "start_time" not in df.columns:
        print("Kolone 'start_time' nema u fajlu.")
        return

    st = df["start_time"]
    print("--- start_time ---")
    print(f"  dtype: {st.dtype}")
    print(f"  null count: {st.null_count()}")
    print(f"  non-null count: {n - st.null_count()}")

    if st.null_count() == n:
        print("  -> Svi redovi imaju start_time = null (ceo fajl 'loš').")
    elif st.null_count() == 0:
        print("  -> Nema null vrednosti.")
    else:
        print("  -> Samo neki redovi imaju null (mešovito).")

    # Uzorke: nekoliko ne-null i nekoliko null
    non_null = df.filter(pl.col("start_time").is_not_null())
    if non_null.height > 0:
        sample = non_null.select(["start_time"]).head(5)
        print("\n  Uzorak ne-null start_time:")
        print(sample)
    null_rows = df.filter(pl.col("start_time").is_null())
    if null_rows.height > 0:
        print(f"\n  Redova sa null start_time: {null_rows.height}")
        # Da vidimo imaju li ti redovi start_date ili stop_time_updates
        if "start_date" in df.columns:
            with_date = null_rows.filter(
                pl.col("start_date").is_not_null()).height
            print(f"  Od njih sa start_date ne-null: {with_date}")


if __name__ == "__main__":
    main()
