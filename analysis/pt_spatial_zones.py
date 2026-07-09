"""
Genuine stop-level spatial analysis of RUT (Oslo) public transport activity and
delay, replacing the previous 2-zone (nord/vest) approximation that R3 flagged
as not substantively spatial (only 2 vegvesen road-weather stations fall inside
the Oslo bounding box -- a hard data ceiling, not a code limitation).

Pipeline:
1. Read RAW RUT SIRI-ET files (not the normalized ones -- normalization
   collapses to trip level and drops `stop_id`; see analyze_weather_impact.py's
   entur_siri_et loading path for the trip-level version).
2. Explode `stop_time_updates` to get one row per (stop_id, record) with
   arrival/departure delay.
3. Join stop_id -> (lat, lon) via a local static GTFS `stops.txt` export
   (NSR:Quay:* ids match directly; ~99.8% match rate observed on a sample day).
4. Restrict to stops inside the Oslo bounding box used elsewhere in this study
   (lat 59.85-60.00, lon 10.60-10.85) for consistency with the rest of the paper.
5. Cluster stop coordinates into K spatial zones via k-means (data-driven,
   no external administrative boundary file required). If an official Oslo
   bydel (borough) boundary GeoJSON is supplied via `bydel_geojson_path`,
   points are assigned to bydeler by point-in-polygon instead -- the more
   defensible option for a GIS journal, once that file is available.
6. Aggregate PT activity (stop-update count) and PT delay (mean arrival delay)
   per zone per minute, and correlate against road-surface temperature.
"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import polars as pl
from sklearn.cluster import KMeans

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC, analyze_weather_impact
from utils.s3_upload import list_parquet_files_from_s3, read_parquet_from_s3

STOPS_TXT_PATH = "/Users/bobandavidovic/Downloads/Current_latest-gtfs/stops.txt"

OSLO_BBOX = {"lat_min": 59.85, "lat_max": 60.00, "lon_min": 10.60, "lon_max": 10.85}

N_ZONES = 6
KMEANS_SEED = 42


def load_stop_coords(path: str = STOPS_TXT_PATH) -> pl.DataFrame:
    stops = pl.read_csv(path, schema_overrides={"stop_lat": pl.Float64, "stop_lon": pl.Float64})
    return stops.select(["stop_id", "stop_lat", "stop_lon"]).filter(
        pl.col("stop_lat").is_not_null() & pl.col("stop_lon").is_not_null()
    )


def load_stop_level_pt(dates: list[str]) -> pl.DataFrame:
    """Explode raw RUT SIRI-ET files for the given dates into per-stop records."""
    frames = []
    for d in dates:
        files = list_parquet_files_from_s3("entur", with_metadata=True, date_prefix=d,
                                            pattern="RUT_siri_et_*.parquet")
        for f in files:
            try:
                df = read_parquet_from_s3(f["key"])
            except Exception:
                continue
            if df is None or df.height == 0 or "stop_time_updates" not in df.columns:
                continue
            fetch_minute = f["last_modified"].replace(second=0, microsecond=0)
            exploded = (
                df.select("stop_time_updates")
                .explode("stop_time_updates")
                .unnest("stop_time_updates")
                .select(["stop_id", "arrival_delay", "departure_delay"])
                .with_columns(pl.lit(fetch_minute).alias("minute"))
            )
            frames.append(exploded)
    if not frames:
        return pl.DataFrame(schema={"stop_id": pl.Utf8, "arrival_delay": pl.Int64,
                                    "departure_delay": pl.Int64, "minute": pl.Datetime})
    return pl.concat(frames, how="vertical_relaxed")


def assign_zones_kmeans(stop_coords: pl.DataFrame, n_zones: int = N_ZONES) -> pl.DataFrame:
    """
    k-means cluster labels are arbitrary and NOT stable across runs (sklearn
    assigns cluster indices based on internal centroid-init order, which can
    permute even with a fixed random_state if the input row order or exact
    point set differs slightly between calls -- e.g. due to live S3 data
    changing between pulls). To keep zone names reproducible and interpretable,
    relabel clusters by centroid longitude (west -> east) after fitting, so
    "zone_0" always means "westernmost cluster" regardless of fit order.
    """
    X = stop_coords.select(["stop_lat", "stop_lon"]).to_numpy()
    km = KMeans(n_clusters=n_zones, random_state=KMEANS_SEED, n_init=10)
    raw_labels = km.fit_predict(X)

    centroid_lon = {i: km.cluster_centers_[i][1] for i in range(n_zones)}
    order = sorted(centroid_lon, key=centroid_lon.get)  # west -> east
    relabel = {old: f"zone_{new}" for new, old in enumerate(order)}

    zones = [relabel[i] for i in raw_labels]
    return stop_coords.with_columns(pl.Series("zone", zones))


def build_window_zonal_pt(name: str, start: datetime, end: datetime,
                          n_zones: int = N_ZONES) -> dict:
    dates = []
    d = start.date()
    from datetime import timedelta
    while d <= end.date():
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    stop_coords = load_stop_coords()
    pt = load_stop_level_pt(dates)
    if pt.height == 0:
        return {"window": name, "note": "no stop-level PT data found"}

    joined = pt.join(stop_coords, on="stop_id", how="inner")
    oslo = joined.filter(
        (pl.col("stop_lat") >= OSLO_BBOX["lat_min"]) & (pl.col("stop_lat") <= OSLO_BBOX["lat_max"]) &
        (pl.col("stop_lon") >= OSLO_BBOX["lon_min"]) & (pl.col("stop_lon") <= OSLO_BBOX["lon_max"])
    )
    if oslo.height == 0:
        return {"window": name, "note": "no stops within Oslo bbox"}

    distinct_stops = oslo.select(["stop_id", "stop_lat", "stop_lon"]).unique()
    zoned_stops = assign_zones_kmeans(distinct_stops, n_zones)
    oslo_zoned = oslo.join(zoned_stops.select(["stop_id", "zone"]), on="stop_id", how="left")

    zone_minute = (
        oslo_zoned.group_by(["zone", "minute"])
        .agg([
            pl.len().alias("pt_activity"),
            pl.col("arrival_delay").mean().alias("pt_delay_mean"),
        ])
    )

    weather = analyze_weather_impact(start_date=start, end_date=end, read_from_s3=True)
    temp_df = weather["combined_df"].select(["minute", "road_temperature"])

    combined = zone_minute.join(temp_df, on="minute", how="inner")

    results = {}
    for zone in sorted(combined["zone"].unique().to_list()):
        zdf = combined.filter(pl.col("zone") == zone)
        row = {}
        for metric in ["pt_activity", "pt_delay_mean"]:
            d2 = zdf.filter(pl.col(metric).is_not_null() & pl.col("road_temperature").is_not_null())
            if d2.height > 3:
                r = d2.select(pl.corr("road_temperature", metric))[0, 0]
                row[metric] = {"n": d2.height, "r": round(float(r), 3) if r is not None else None}
            else:
                row[metric] = {"n": d2.height, "r": None}
        results[zone] = row

    zone_centroids = (
        zoned_stops.group_by("zone")
        .agg([pl.col("stop_lat").mean().alias("centroid_lat"),
              pl.col("stop_lon").mean().alias("centroid_lon"),
              pl.len().alias("n_stops")])
    )

    return {
        "window": name,
        "n_stops_oslo": distinct_stops.height,
        "n_zones": n_zones,
        "zone_centroids": zone_centroids,
        "correlations": results,
    }


def build_report(n_zones: int = N_ZONES) -> list[dict]:
    return [
        build_window_zonal_pt(p["name"], p["start_date"], p["end_date"], n_zones)
        for p in PERIODS_WA1_WA2_WB_WC
    ]


def print_report(report: list[dict]) -> None:
    for w in report:
        print(f"\n{'=' * 70}\n{w['window']}\n{'=' * 70}")
        if "note" in w:
            print(f"  {w['note']}")
            continue
        print(f"  n_stops (Oslo bbox): {w['n_stops_oslo']}  n_zones: {w['n_zones']}")
        print(w["zone_centroids"])
        for zone, metrics in sorted(w["correlations"].items()):
            act = metrics["pt_activity"]
            dly = metrics["pt_delay_mean"]
            print(f"  {zone:10} activity: n={act['n']:5d} r={act['r']}   "
                  f"delay: n={dly['n']:5d} r={dly['r']}")


# ─────────────────────────────────────────────────────────────────────────
# Thematic map (Figure 6 replacement) -- validated categorical palette,
# see dataviz skill references/palette.md
# ─────────────────────────────────────────────────────────────────────────

ZONE_COLORS = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948",
               "#e87ba4", "#eb6834"]

OSM_USER_AGENT = "phdproject-mdpi-revision/1.0 (research use; single-figure basemap fetch)"


def _lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    import math
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def _tile_to_lonlat(x: int, y: int, zoom: int) -> tuple[float, float]:
    import math
    n = 2.0 ** zoom
    lon = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    return lon, lat


def fetch_osm_basemap(lat_min: float, lat_max: float, lon_min: float, lon_max: float,
                       zoom: int = 12):
    """Fetch and stitch OSM tiles covering the given bbox. Returns (image_array, extent)."""
    import io
    import time

    import numpy as np
    import requests
    from PIL import Image

    x_min, y_max = _lonlat_to_tile(lon_min, lat_min, zoom)
    x_max, y_min = _lonlat_to_tile(lon_max, lat_max, zoom)
    x_min, x_max = sorted([x_min, x_max])
    y_min, y_max = sorted([y_min, y_max])

    n_tiles = (x_max - x_min + 1) * (y_max - y_min + 1)
    if n_tiles > 64:
        raise ValueError(f"Too many tiles ({n_tiles}) — increase zoom step or shrink bbox")

    tile_size = 256
    mosaic = Image.new("RGB", ((x_max - x_min + 1) * tile_size, (y_max - y_min + 1) * tile_size))
    headers = {"User-Agent": OSM_USER_AGENT}

    for xi, x in enumerate(range(x_min, x_max + 1)):
        for yi, y in enumerate(range(y_min, y_max + 1)):
            url = f"https://tile.openstreetmap.org/{zoom}/{x}/{y}.png"
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            tile_img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            mosaic.paste(tile_img, (xi * tile_size, yi * tile_size))
            time.sleep(0.2)  # stay well under OSM's usage-policy rate limit

    lon_w, lat_n = _tile_to_lonlat(x_min, y_min, zoom)
    lon_e, lat_s = _tile_to_lonlat(x_max + 1, y_max + 1, zoom)
    extent = (lon_w, lon_e, lat_s, lat_n)
    return np.array(mosaic), extent


def _draw_zone_map(ax, zoned_stops: pl.DataFrame, window_name: str,
                   basemap: bool = True, basemap_zoom: int = 12, title_prefix: str = ""):
    """Core drawing logic for the categorical zone map; draws into a given ax."""
    zones = sorted(zoned_stops["zone"].unique().to_list())
    color_map = {z: ZONE_COLORS[i % len(ZONE_COLORS)] for i, z in enumerate(zones)}

    lat_min, lat_max = float(zoned_stops["stop_lat"].min()), float(zoned_stops["stop_lat"].max())
    lon_min, lon_max = float(zoned_stops["stop_lon"].min()), float(zoned_stops["stop_lon"].max())
    pad_lat, pad_lon = (lat_max - lat_min) * 0.06, (lon_max - lon_min) * 0.06

    if basemap:
        try:
            img, extent = fetch_osm_basemap(lat_min - pad_lat, lat_max + pad_lat,
                                            lon_min - pad_lon, lon_max + pad_lon,
                                            zoom=basemap_zoom)
            ax.imshow(img, extent=extent, zorder=0)
        except Exception as e:
            print(f"Warning: basemap fetch failed ({e}); plotting without basemap.")
            basemap = False

    marker_alpha = 0.55 if basemap else 0.75
    marker_edge = "white" if basemap else "none"
    for z in zones:
        sub = zoned_stops.filter(pl.col("zone") == z)
        ax.scatter(sub["stop_lon"], sub["stop_lat"], s=14 if basemap else 10,
                   alpha=marker_alpha, color=color_map[z],
                   edgecolors=marker_edge, linewidths=0.3, zorder=2,
                   label=f"{z} (n={sub.height})")

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"{title_prefix}RUT stop-level spatial zones (k-means, k={len(zones)}) — {window_name}",
                fontweight="bold", fontsize=10)
    ax.set_xlim(lon_min - pad_lon, lon_max + pad_lon)
    ax.set_ylim(lat_min - pad_lat, lat_max + pad_lat)
    mean_lat = (lat_min + lat_max) / 2
    ax.set_aspect(1.0 / np.cos(np.radians(mean_lat)))
    ax.legend(loc="upper left", fontsize=7, frameon=True, markerscale=1.3,
             facecolor="white", framealpha=0.9)


def _set_plot_style():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "font.family": "serif", "font.size": 9,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.alpha": 0.25, "grid.linestyle": "--",
    })
    return plt


def fig_zone_map(zoned_stops: pl.DataFrame, window_name: str, save_path: str | None = None,
                 basemap: bool = True, basemap_zoom: int = 12):
    plt = _set_plot_style()
    fig, ax = plt.subplots(figsize=(7, 7))
    _draw_zone_map(ax, zoned_stops, window_name, basemap=basemap, basemap_zoom=basemap_zoom)
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved map -> {save_path}")
    return fig


def fig_zone_map_combined(panels: list[tuple[str, pl.DataFrame]], save_path: str | None = None,
                          basemap: bool = True, basemap_zoom: int = 12):
    """
    Two-panel (or N-panel) version of fig_zone_map: panels is a list of
    (window_name, zoned_stops) tuples, e.g. [("W-B", zoned_wb), ("W-C", zoned_wc)].
    """
    plt = _set_plot_style()
    n = len(panels)
    fig, axes = plt.subplots(1, n, figsize=(7 * n, 7))
    if n == 1:
        axes = [axes]
    labels = "abcdefgh"
    for i, (window_name, zoned_stops) in enumerate(panels):
        _draw_zone_map(axes[i], zoned_stops, window_name, basemap=basemap,
                       basemap_zoom=basemap_zoom, title_prefix=f"({labels[i]}) ")
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved combined map -> {save_path}")
    return fig


def _draw_zone_correlation_map(ax, fig, zoned_stops: pl.DataFrame, correlations: dict,
                               metric: str, metric_label: str, window_name: str,
                               basemap: bool = True, basemap_zoom: int = 12,
                               r_bound: float | None = None, title_prefix: str = ""):
    """Core drawing logic for the choropleth correlation map; draws into a given ax."""
    from matplotlib.colors import LinearSegmentedColormap, Normalize
    from matplotlib.cm import ScalarMappable

    zones = sorted(zoned_stops["zone"].unique().to_list())
    zone_r = {z: correlations[z][metric]["r"] for z in zones if z in correlations}
    zone_n = {z: correlations[z][metric]["n"] for z in zones if z in correlations}

    if r_bound is None:
        valid_r = [abs(r) for r in zone_r.values() if r is not None]
        r_bound = max(valid_r) * 1.15 if valid_r else 0.5
        r_bound = max(r_bound, 0.05)  # avoid a degenerate near-zero scale

    # Diverging blue<->red, neutral gray midpoint (dataviz skill palette.md)
    cmap = LinearSegmentedColormap.from_list(
        "diverging_br", ["#e34948", "#f0efec", "#2a78d6"])
    norm = Normalize(vmin=-r_bound, vmax=r_bound, clip=True)

    lat_min, lat_max = float(zoned_stops["stop_lat"].min()), float(zoned_stops["stop_lat"].max())
    lon_min, lon_max = float(zoned_stops["stop_lon"].min()), float(zoned_stops["stop_lon"].max())
    pad_lat, pad_lon = (lat_max - lat_min) * 0.06, (lon_max - lon_min) * 0.06

    if basemap:
        try:
            img, extent = fetch_osm_basemap(lat_min - pad_lat, lat_max + pad_lat,
                                            lon_min - pad_lon, lon_max + pad_lon,
                                            zoom=basemap_zoom)
            ax.imshow(img, extent=extent, zorder=0)
        except Exception as e:
            print(f"Warning: basemap fetch failed ({e}); plotting without basemap.")
            basemap = False

    for z in zones:
        sub = zoned_stops.filter(pl.col("zone") == z)
        r = zone_r.get(z)
        color = cmap(norm(r)) if r is not None else "#c3c2b7"
        ax.scatter(sub["stop_lon"], sub["stop_lat"], s=16, alpha=0.65,
                   color=color, edgecolors="white", linewidths=0.3, zorder=2)

        cen_lat, cen_lon = float(sub["stop_lat"].mean()), float(sub["stop_lon"].mean())
        label = f"r = {r:.2f}\n(n={zone_n.get(z)})" if r is not None else "n/a"
        ax.annotate(label, xy=(cen_lon, cen_lat), ha="center", va="center",
                   fontsize=9, fontweight="bold", color="#0b0b0b", zorder=3,
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                             edgecolor="#0b0b0b", alpha=0.85, linewidth=0.6))

    sm = ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cb = fig.colorbar(sm, ax=ax, shrink=0.75, pad=0.02)
    cb.set_label(f"Pearson r (temperature vs. {metric_label})", fontsize=8)

    legend_lines = [
        f"{z}:  r = {zone_r.get(z):.2f}  (n={zone_n.get(z)})" if zone_r.get(z) is not None
        else f"{z}:  n/a"
        for z in zones
    ]
    ax.text(0.02, 0.98, "\n".join(legend_lines), transform=ax.transAxes,
           ha="left", va="top", fontsize=7.5, family="monospace",
           bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                     edgecolor="#0b0b0b", alpha=0.9, linewidth=0.6),
           zorder=4)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"{title_prefix}Spatial variation: temperature vs. {metric_label} — {window_name}",
                fontweight="bold", fontsize=10)
    ax.set_xlim(lon_min - pad_lon, lon_max + pad_lon)
    ax.set_ylim(lat_min - pad_lat, lat_max + pad_lat)
    mean_lat = (lat_min + lat_max) / 2
    ax.set_aspect(1.0 / np.cos(np.radians(mean_lat)))


def fig_zone_correlation_map(zoned_stops: pl.DataFrame, correlations: dict, metric: str,
                             metric_label: str, window_name: str, save_path: str | None = None,
                             basemap: bool = True, basemap_zoom: int = 12,
                             r_bound: float | None = None):
    """
    Choropleth-style map: each stop is colored by its zone's Pearson r for `metric`
    (temperature vs. pt_activity or pt_delay_mean), with the exact r (and n) printed
    at each zone's centroid directly on the basemap.
    """
    plt = _set_plot_style()
    plt.rcParams.update({"axes.grid": False})
    fig, ax = plt.subplots(figsize=(7, 7))
    _draw_zone_correlation_map(ax, fig, zoned_stops, correlations, metric, metric_label,
                               window_name, basemap=basemap, basemap_zoom=basemap_zoom,
                               r_bound=r_bound)
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved correlation map -> {save_path}")
    return fig


def fig_zone_correlation_map_combined(panels: list[tuple[str, pl.DataFrame, dict]], metric: str,
                                      metric_label: str, save_path: str | None = None,
                                      basemap: bool = True, basemap_zoom: int = 12,
                                      r_bound: float | None = None):
    """
    Two-panel (or N-panel) version of fig_zone_correlation_map: panels is a list of
    (window_name, zoned_stops, correlations) tuples.
    """
    plt = _set_plot_style()
    plt.rcParams.update({"axes.grid": False})
    n = len(panels)
    fig, axes = plt.subplots(1, n, figsize=(7.8 * n, 7))
    if n == 1:
        axes = [axes]
    labels = "abcdefgh"
    for i, (window_name, zoned_stops, correlations) in enumerate(panels):
        _draw_zone_correlation_map(axes[i], fig, zoned_stops, correlations, metric, metric_label,
                                   window_name, basemap=basemap, basemap_zoom=basemap_zoom,
                                   r_bound=r_bound, title_prefix=f"({labels[i]}) ")
    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved combined correlation map -> {save_path}")
    return fig


if __name__ == "__main__":
    rep = build_report()
    print_report(rep)

    import os
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pt_spatial_output")
    os.makedirs(out_dir, exist_ok=True)

    # Re-derive zoned stop coordinates for W-C (cleanest window) to plot the map.
    for p in PERIODS_WA1_WA2_WB_WC:
        if p["name"].startswith("W-C"):
            dates = []
            from datetime import timedelta
            d = p["start_date"].date()
            while d <= p["end_date"].date():
                dates.append(d.strftime("%Y-%m-%d"))
                d += timedelta(days=1)
            stop_coords = load_stop_coords()
            pt = load_stop_level_pt(dates)
            joined = pt.join(stop_coords, on="stop_id", how="inner")
            oslo = joined.filter(
                (pl.col("stop_lat") >= OSLO_BBOX["lat_min"]) & (pl.col("stop_lat") <= OSLO_BBOX["lat_max"]) &
                (pl.col("stop_lon") >= OSLO_BBOX["lon_min"]) & (pl.col("stop_lon") <= OSLO_BBOX["lon_max"])
            )
            distinct_stops = oslo.select(["stop_id", "stop_lat", "stop_lon"]).unique()
            zoned_stops = assign_zones_kmeans(distinct_stops, N_ZONES)
            fig_zone_map(zoned_stops, p["name"], save_path=os.path.join(out_dir, "fig6_pt_spatial_zones_map.pdf"))
            break
