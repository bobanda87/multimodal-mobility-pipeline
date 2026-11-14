"""
Gradi analysis/reference/entur_stop_zone_lookup.parquet: GTFS stop_id → lat/lon → zona (oslo_zones.json).

  PYTHONPATH=. .venv/bin/python analysis/build_entur_stop_zone_lookup.py
  PYTHONPATH=. .venv/bin/python analysis/build_entur_stop_zone_lookup.py --local-zip ~/Downloads/rb_norway-aggregated-gtfs.zip

Podrazumevani URL (norveški agregirani GTFS) može da se promeni ENV-om ENTUR_GTFS_ZIP_URL.
"""
from __future__ import annotations

import io
import os
import sys
import zipfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl  # noqa: E402

from analysis.vegvesen_zones import assign_zone, load_zone_definitions  # noqa: E402

DEFAULT_GTFS_ZIP_URL = os.environ.get(
    "ENTUR_GTFS_ZIP_URL",
    "https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_norway-aggregated-gtfs.zip",
)


def _read_stops_from_gtfs_zip(zip_path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as zf:
        if "stops.txt" not in zf.namelist():
            raise FileNotFoundError("stops.txt not in zip")
        with zf.open("stops.txt") as fh:
            buf = io.BytesIO(fh.read())
    return pl.read_csv(buf, separator=",")


def _oslo_area_mask(lat: pl.Expr, lon: pl.Expr) -> pl.Expr:
    """Grublji filter da smanjimo lookup (širi od oslo_zones)."""
    return (
        lat.is_between(59.75, 60.08, closed="both")
        & lon.is_between(10.40, 11.05, closed="both")
    )


def build_lookup(
    stops: pl.DataFrame,
    oslo_filter: bool,
) -> pl.DataFrame:
    need = {"stop_id", "stop_lat", "stop_lon"}
    cols = set(stops.columns)
    if not need <= cols:
        raise ValueError(f"stops.txt missing columns: {need - cols}")
    st = stops.select(
        [
            pl.col("stop_id").cast(pl.Utf8),
            pl.col("stop_lat").cast(pl.Float64, strict=False),
            pl.col("stop_lon").cast(pl.Float64, strict=False),
        ]
    ).filter(
        pl.col("stop_id").is_not_null()
        & pl.col("stop_lat").is_not_null()
        & pl.col("stop_lon").is_not_null()
    )
    if oslo_filter:
        st = st.filter(_oslo_area_mask(pl.col("stop_lat"), pl.col("stop_lon")))
    zones = load_zone_definitions()
    zlist = [
        assign_zone(row["stop_lat"], row["stop_lon"], zones)
        for row in st.select("stop_lat", "stop_lon").iter_rows(named=True)
    ]
    st = st.with_columns(pl.Series("zone", zlist))
    rows: list[dict] = []
    for row in st.iter_rows(named=True):
        sid = row["stop_id"]
        if not sid:
            continue
        lat, lon, zone = row["stop_lat"], row["stop_lon"], row["zone"]
        rows.append(
            {
                "join_key": sid,
                "stop_lat": lat,
                "stop_lon": lon,
                "zone": zone,
            }
        )
        if ":" in sid:
            rows.append(
                {
                    "join_key": sid.split(":")[-1],
                    "stop_lat": lat,
                    "stop_lon": lon,
                    "zone": zone,
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "join_key": pl.Utf8,
                "stop_lat": pl.Float64,
                "stop_lon": pl.Float64,
                "zone": pl.Utf8,
            }
        )
    long_df = pl.from_dicts(
        rows,
        schema={
            "join_key": pl.Utf8,
            "stop_lat": pl.Float64,
            "stop_lon": pl.Float64,
            "zone": pl.Utf8,
        },
    )
    return long_df.unique(subset=["join_key"], keep="first")


def main():
    import argparse

    p = argparse.ArgumentParser(description="Build Entur stop_id → zone lookup from GTFS.")
    p.add_argument(
        "--local-zip",
        type=str,
        default=None,
        help="Lokalni rb_norway-aggregated-gtfs.zip (inače download sa ENTUR_GTFS_ZIP_URL).",
    )
    p.add_argument(
        "--oslo-tight-only",
        action="store_true",
        help="Zadrži samo stajališta u uskom Oslo bbox-u (manji lookup; može dati 0 pogodataka za tripove van centra).",
    )
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help="Izlazni Parquet (default: analysis/reference/entur_stop_zone_lookup.parquet).",
    )
    args = p.parse_args()
    out = Path(args.output) if args.output else SCRIPT_DIR / "reference" / "entur_stop_zone_lookup.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    if args.local_zip:
        zp = Path(args.local_zip).expanduser()
        if not zp.is_absolute():
            zp = PROJECT_ROOT / zp
    else:
        try:
            import urllib.request

            print(f"Downloading {DEFAULT_GTFS_ZIP_URL} ...")
            zp = SCRIPT_DIR / "reference" / "_gtfs_norway_tmp.zip"
            zp.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(DEFAULT_GTFS_ZIP_URL, zp)
            print(f"Saved to {zp}")
        except Exception as e:
            print(f"Download failed: {e}")
            print("Use --local-zip path/to/rb_norway-aggregated-gtfs.zip")
            sys.exit(1)

    stops = _read_stops_from_gtfs_zip(zp)
    print(f"stops.txt rows: {stops.height}")
    lu = build_lookup(stops, oslo_filter=args.oslo_tight_only)
    print(f"lookup join_key rows (with suffix variants): {lu.height}")
    lu.write_parquet(out)
    print(f"Wrote {out}")
    if not args.local_zip and zp.name == "_gtfs_norway_tmp.zip":
        try:
            zp.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    main()
