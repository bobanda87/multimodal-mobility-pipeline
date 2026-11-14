"""
Pomoćne funkcije za normalizaciju Entur SIRI ET / trip-updates na nivou stajališta + zona (Oslo bbox iz config).
Ne menja postojeći entur_siri_et pipeline — koristi se odvojeno (entur_zones_normalized).
"""
from __future__ import annotations

import os
import re
from datetime import date
from typing import Iterable

import polars as pl

from analysis.normalize_data import (
    entur_snapshot_epoch_from_s3_key,
    fill_entur_bad_timestamps_with_epoch,
    normalize_entur_trip_updates,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Isti prozori kao u analyze_weather_impact (W-A1 / W-A2 / W-B / W-C)
STUDY_WINDOWS = [
    {
        "name": "WA1",
        "label": "W-A1 (2025-12-20 – 2025-12-27)",
        "start": date(2025, 12, 20),
        "end": date(2025, 12, 27),
    },
    {
        "name": "WA2",
        "label": "W-A2 (2025-12-28 – 2026-01-04)",
        "start": date(2025, 12, 28),
        "end": date(2026, 1, 4),
    },
    {
        "name": "WB",
        "label": "W-B (2026-01-31 – 2026-02-07)",
        "start": date(2026, 1, 31),
        "end": date(2026, 2, 7),
    },
    {
        "name": "WC",
        "label": "W-C (2026-03-07 – 2026-03-14)",
        "start": date(2026, 3, 7),
        "end": date(2026, 3, 14),
    },
]


def calendar_date_in_study_windows(d: date) -> list[str]:
    """Koji study_window kodovi pokrivaju kalendarski datum (obično 0 ili 1)."""
    return [w["name"] for w in STUDY_WINDOWS if w["start"] <= d <= w["end"]]


def _struct_field_names(schema_stu: pl.DataType | None) -> list[str]:
    if schema_stu is None:
        return []
    if isinstance(schema_stu, pl.Struct):
        return [f.name for f in schema_stu.fields]
    if isinstance(schema_stu, pl.List):
        inner = getattr(schema_stu, "inner", schema_stu)
        if isinstance(inner, pl.Struct):
            return [f.name for f in inner.fields]
    return []


def explode_entur_to_stop_rows(raw: pl.DataFrame) -> pl.DataFrame:
    """
    Iz jednog raw DataFrame-a (SIRI ET ili trip-updates Parquet) napravi jedan red po stajalištu u listi.
    Očekuje kolonu stop_time_updates (list struct).
    """
    if raw.height == 0 or "stop_time_updates" not in raw.columns:
        return pl.DataFrame()

    trip_col = "trip_id" if "trip_id" in raw.columns else (
        "id" if "id" in raw.columns else None
    )
    if trip_col is None:
        return pl.DataFrame()

    base_cols = [trip_col, "timestamp", "stop_time_updates"]
    if "route_id" in raw.columns:
        base_cols.insert(1, "route_id")
    optional = [c for c in ("start_date", "start_time") if c in raw.columns]
    for c in ("operator_ref", "direction_ref", "monitored", "data_source"):
        if c in raw.columns:
            optional.append(c)
    select_cols = [c for c in base_cols + optional if c in raw.columns]
    df = raw.select(select_cols)
    if trip_col != "trip_id":
        df = df.rename({trip_col: "trip_id"})
    if "route_id" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("route_id"))

    exploded = df.explode("stop_time_updates")
    stu = pl.col("stop_time_updates")
    schema_stu = raw.schema.get("stop_time_updates")
    fields = _struct_field_names(schema_stu)

    if not fields:
        return pl.DataFrame()

    def _field(name: str, dtype: pl.DataType) -> pl.Expr:
        if name in fields:
            return stu.struct.field(name).alias(name)
        return pl.lit(None).cast(dtype).alias(name)

    exprs = [
        _field("stop_id", pl.Utf8),
        _field("arrival_delay", pl.Int64),
        _field("departure_delay", pl.Int64),
    ]
    if "stop_point_name" in fields:
        exprs.append(_field("stop_point_name", pl.Utf8))
    exploded = exploded.with_columns(exprs).drop("stop_time_updates")
    exploded = exploded.filter(pl.col("stop_id").is_not_null() & (pl.col("stop_id") != ""))
    exploded = exploded.with_columns(
        [
            pl.col("arrival_delay").fill_null(0).cast(pl.Int64).alias("arrival_delay_sec"),
            pl.col("departure_delay").fill_null(0).cast(pl.Int64).alias("departure_delay_sec"),
        ]
    )
    exploded = exploded.drop("arrival_delay", strict=False).drop(
        "departure_delay", strict=False
    )
    gkeys = [c for c in ("trip_id", "route_id", "timestamp") if c in exploded.columns]
    exploded = exploded.with_row_index("_rid").with_columns(
        (
            pl.col("_rid")
            - pl.col("_rid").min().over(gkeys if gkeys else "trip_id")
        )
        .cast(pl.Int32)
        .alias("stop_sequence")
    ).drop("_rid")
    return exploded


def load_stop_zone_lookup(path: str | os.PathLike | None = None) -> pl.DataFrame | None:
    """Učitaj Parquet sa kolonama join_key, stop_lat, stop_lon, zone (ili stop_id + lat/lon + zone)."""
    p = path or os.path.join(SCRIPT_DIR, "reference", "entur_stop_zone_lookup.parquet")
    p = os.path.abspath(p)
    if not os.path.isfile(p):
        return None
    df = pl.read_parquet(p)
    if "join_key" in df.columns:
        return df.select(
            [
                pl.col("join_key").cast(pl.Utf8),
                pl.col("stop_lat").cast(pl.Float64, strict=False),
                pl.col("stop_lon").cast(pl.Float64, strict=False),
                pl.col("zone").cast(pl.Utf8),
            ]
        )
    if "stop_id" in df.columns and "stop_lat" in df.columns:
        return df.select(
            [
                pl.col("stop_id").cast(pl.Utf8).alias("join_key"),
                pl.col("stop_lat").cast(pl.Float64, strict=False),
                pl.col("stop_lon").cast(pl.Float64, strict=False),
                pl.col("zone").cast(pl.Utf8),
            ]
        )
    return None


def join_stops_with_zone(stops: pl.DataFrame, lookup: pl.DataFrame) -> pl.DataFrame:
    suf = (
        pl.col("stop_id")
        .cast(pl.Utf8)
        .str.split(":")
        .list.get(-1)
        .alias("_stop_suffix")
    )
    x = stops.with_columns([suf])
    lu1 = lookup.rename(
        {
            "join_key": "_k1",
            "stop_lat": "lat1",
            "stop_lon": "lon1",
            "zone": "zone1",
        }
    )
    lu2 = lookup.rename(
        {
            "join_key": "_k2",
            "stop_lat": "lat2",
            "stop_lon": "lon2",
            "zone": "zone2",
        }
    )
    out = (
        x.join(lu1, left_on="stop_id", right_on="_k1", how="left")
        .join(lu2, left_on="_stop_suffix", right_on="_k2", how="left")
        .with_columns(
            [
                pl.coalesce(["lat1", "lat2"]).alias("stop_lat"),
                pl.coalesce(["lon1", "lon2"]).alias("stop_lon"),
                pl.coalesce(["zone1", "zone2"]).alias("zone"),
            ]
        )
        .drop(
            [
                "_stop_suffix",
                "_k1",
                "lat1",
                "lon1",
                "zone1",
                "_k2",
                "lat2",
                "lon2",
                "zone2",
            ],
            strict=False,
        )
    )
    return out


def enrich_with_trip_timestamps(
    stop_rows: pl.DataFrame, raw: pl.DataFrame, date_str: str, s3_key: str
) -> pl.DataFrame:
    """Dodaj departure_timestamp iz normalize_entur_trip_updates (jedan red po vožnji)."""
    if "departure_timestamp" in stop_rows.columns:
        stop_rows = stop_rows.drop("departure_timestamp")
    trip_one = normalize_entur_trip_updates(raw)
    snap = entur_snapshot_epoch_from_s3_key(date_str, s3_key)
    if trip_one.height > 0 and snap is not None:
        trip_one = fill_entur_bad_timestamps_with_epoch(trip_one, snap)

    # stop_rows nose raw timestamp (često 0 za trip-updates), zato timestamp ne koristimo za join.
    keys = [k for k in ("trip_id", "route_id") if k in stop_rows.columns]
    for c in ("start_date", "start_time"):
        if c in stop_rows.columns and c in trip_one.columns:
            keys.append(c)
    if not keys:
        keys = ["trip_id"] if "trip_id" in stop_rows.columns else []

    # Uskladi tipove join ključeva (npr. start_time: Null vs Utf8) pre join-a.
    key_casts: list[pl.Expr] = []
    for k in keys:
        if k in stop_rows.columns:
            if k == "timestamp":
                key_casts.append(pl.col(k).cast(pl.Int64, strict=False))
            else:
                key_casts.append(pl.col(k).cast(pl.Utf8, strict=False))
    if key_casts:
        stop_rows = stop_rows.with_columns(key_casts)
    if trip_one.height > 0:
        t_casts: list[pl.Expr] = []
        for k in keys:
            if k in trip_one.columns:
                if k == "timestamp":
                    t_casts.append(pl.col(k).cast(pl.Int64, strict=False))
                else:
                    t_casts.append(pl.col(k).cast(pl.Utf8, strict=False))
        if t_casts:
            trip_one = trip_one.with_columns(t_casts)

    if trip_one.height > 0 and keys:
        trip_small = (
            trip_one.select(
                [k for k in keys if k in trip_one.columns]
                + [c for c in ("departure_timestamp",) if c in trip_one.columns]
            )
            .unique(subset=[k for k in keys if k in trip_one.columns], keep="last")
        )
        out = stop_rows.join(
            trip_small, on=[k for k in keys if k in trip_small.columns], how="left"
        )
    else:
        out = stop_rows.with_columns(
            pl.lit(None).cast(pl.Int64).alias("departure_timestamp")
        )

    # Fallback: ako nema departure_timestamp, uzmi snapshot epoch iz imena fajla.
    if snap is not None:
        out = out.with_columns(
            pl.when(
                pl.col("departure_timestamp")
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                > 0
            )
            .then(pl.col("departure_timestamp").cast(pl.Int64, strict=False))
            .otherwise(pl.lit(int(snap)).cast(pl.Int64))
            .alias("departure_timestamp")
        )
    return out


def add_study_window_column(
    df: pl.DataFrame, calendar_date: date
) -> pl.DataFrame:
    names = calendar_date_in_study_windows(calendar_date)
    if not names:
        return df.with_columns([pl.lit(None).cast(pl.Utf8).alias("study_window")])
    # jedan datum = jedan prozor u našoj konfiguraciji
    return df.with_columns([pl.lit(names[0]).cast(pl.Utf8).alias("study_window")])


def raw_entur_keys_for_date(date_str: str) -> list[str]:
    """S3 ključevi za jedan kalendar folder (siri_et + trip-updates)."""
    from utils.s3_upload import list_parquet_files_from_s3

    keys: list[str] = []
    for pat in ("*_siri_et_*.parquet", "*trip-updates*.parquet"):
        keys.extend(
            list_parquet_files_from_s3(
                "entur", pat, exclude_normalized=True, date_prefix=date_str
            )
        )
    return sorted(set(keys))


def daterange_str(start: date, end: date) -> Iterable[str]:
    d = start
    while d <= end:
        yield d.isoformat()
        d = date.fromordinal(d.toordinal() + 1)


def all_study_dates() -> list[str]:
    """Unija kalendarskih datuma svih prozora."""
    out: set[str] = set()
    for w in STUDY_WINDOWS:
        out.update(daterange_str(w["start"], w["end"]))
    return sorted(out)
