"""The per-date filter of the Vegvesen sources compares the local date, not the UTC date."""
import polars as pl

LOCAL = lambda c: pl.col(c).dt.convert_time_zone("Europe/Oslo").dt.strftime("%Y-%m-%d")

df = pl.DataFrame({"timestamp": ["2026-11-14T23:30:00+00:00", "2026-11-14T22:30:00+00:00"]}).with_columns(
    pl.col("timestamp").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z"))
assert df.filter(LOCAL("timestamp") == "2026-11-15").height == 1  # 00:30 local belongs to the 15th
assert df.filter(LOCAL("timestamp") == "2026-11-14").height == 1
src = open("analysis/normalize_data.py").read()
assert 'pl.col("timestamp").dt.strftime("%Y-%m-%d") == date_str' not in src
assert 'pl.col("minute").dt.strftime("%Y-%m-%d") == date_str' not in src
print("ok")
