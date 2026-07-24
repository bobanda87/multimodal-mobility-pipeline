"""
Multi-city spatial autocorrelation (Global + Local Moran's I / LISA) of public
transport activity and delay, replacing the Oslo-only k-means zonal-correlation
approach in pt_spatial_zones.py.

Motivation: a reviewer noted the paper's descriptive/correlation analyses read
as transport-engineering rather than geo-spatial, and suggested reframing
toward genuine spatial statistics (hotspots, Moran's I, LISA) across multiple
Norwegian cities. This module does that.

Design (see analysis-session validation before implementing -- summarized here
so the reasoning survives independent of the chat history):

- 6 cities, chosen for geographic spread across Norway and confirmed
  SIRI-ET stop-level density: Oslo (RUT), Bergen (SKY), Trondheim (ATB),
  Stavanger (KOL), Kristiansand (AKT), Tromso (TRO). The Entur SIRI-ET
  collector already polls all Norwegian regional operators (not just RUT),
  and the static GTFS stops.txt feed is Entur's *national* stop register
  (158k stops), so no new data collection was required.
- Zones are 1km x 1km grid cells (not k-means -- grid cells have
  well-defined queen-contiguity neighbors, which arbitrary k-means clusters
  do not), retaining cells with >= 15 GTFS stops. This yields ~240 zones
  total (23-85 per city) -- comfortably above the ~25-30 units generally
  needed for stable Moran's I, and enough per city for within-city LISA to
  be meaningful rather than a single national number dominated by Oslo's
  much larger stop count.
- Per-zone metrics (PT activity = total stop-update count, PT delay = mean
  arrival delay) are WHOLE-WINDOW aggregates, not per-minute -- cell-level
  noise from having only 15-90 stops per cell washes out over the
  hundreds of thousands of stop-time-updates observed per city per window.
- Weights: queen contiguity on the (cx, cy) grid index, built per city.
  Cities are hundreds of km apart, so cross-city weight is correctly zero
  by construction (grid indices are city-local) without needing a distance
  cutoff. A minority of cells end up isolated (no queen-adjacent cell also
  above the stop threshold); these fall back to their 2 nearest same-city
  zone centroids, so every zone has >=1 neighbor for row-standardized
  weights (standard "island" treatment in spatial statistics).
- Global Moran's I is computed PER CITY, not pooled nationally -- pooling
  across disconnected components would just be dominated by whichever city
  has the most zones (Oslo) and wouldn't say anything about between-city
  variation, so a per-city table is the only defensible unit of comparison.
- Local Moran's I (LISA) is computed per zone; local statistics don't have
  the pooling problem the global statistic does, so all cities' LISA
  results can be shown together on one combined map.
- Stop-level SIRI-ET data is unavailable for W-A1/W-A2 (see
  pt_spatial_zones.py docstring), so this analysis covers W-B and W-C only,
  same as the Oslo-only version it replaces.
"""

from __future__ import annotations

import math
from datetime import timedelta

import numpy as np
import polars as pl

from analysis.analyze_weather_impact import PERIODS_WA1_WA2_WB_WC
from analysis.pt_spatial_zones import (
    OSM_USER_AGENT,  # noqa: F401  (re-exported for callers that want the same UA)
    STOPS_TXT_PATH,
    _set_plot_style,
    fetch_osm_basemap,
    load_stop_coords,
)
from utils.s3_upload import list_parquet_files_from_s3, read_parquet_from_s3

CITIES = {
    "Oslo": {"operator": "RUT", "lat_c": 59.925, "lon_c": 10.725, "dlat": 0.075, "dlon": 0.125},
    "Bergen": {"operator": "SKY", "lat_c": 60.39, "lon_c": 5.32, "dlat": 0.075, "dlon": 0.125},
    "Trondheim": {"operator": "ATB", "lat_c": 63.43, "lon_c": 10.39, "dlat": 0.075, "dlon": 0.125},
    "Stavanger": {"operator": "KOL", "lat_c": 58.97, "lon_c": 5.73, "dlat": 0.075, "dlon": 0.125},
    "Kristiansand": {"operator": "AKT", "lat_c": 58.15, "lon_c": 7.99, "dlat": 0.06, "dlon": 0.10},
    "Tromso": {"operator": "TRO", "lat_c": 69.65, "lon_c": 18.96, "dlat": 0.06, "dlon": 0.10},
}

CELL_KM = 1.0
MIN_STOPS_PER_CELL = 15
N_PERMUTATIONS = 999
LISA_ALPHA = 0.05
WINDOWS = [p for p in PERIODS_WA1_WA2_WB_WC if p["name"].startswith(("W-B", "W-C"))]


def _dates_in_window(start, end) -> list[str]:
    dates = []
    d = start.date()
    while d <= end.date():
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


# ─────────────────────────────────────────────────────────────────────────
# Grid zoning
# ─────────────────────────────────────────────────────────────────────────

def assign_grid_zones(stop_coords: pl.DataFrame, city_name: str, lat_c: float, lon_c: float,
                      dlat: float, dlon: float, cell_km: float = CELL_KM,
                      min_stops: int = MIN_STOPS_PER_CELL) -> pl.DataFrame:
    """
    Filter stops to the city bbox, assign each to a 1km grid cell, and keep
    only cells with >= min_stops stops. Returns one row per RETAINED stop
    (stop_id, stop_lat, stop_lon, cx, cy, zone_id) -- stops in dropped cells
    are excluded entirely (they never appear in the returned frame).
    """
    sub = stop_coords.filter(
        (pl.col("stop_lat") - lat_c).abs().lt(dlat) & (pl.col("stop_lon") - lon_c).abs().lt(dlon)
    )
    lat_cell_deg = cell_km / 111.0
    lon_cell_deg = cell_km / (111.0 * math.cos(math.radians(lat_c)))
    cells = sub.with_columns([
        ((pl.col("stop_lat") - (lat_c - dlat)) / lat_cell_deg).floor().cast(pl.Int32).alias("cx"),
        ((pl.col("stop_lon") - (lon_c - dlon)) / lon_cell_deg).floor().cast(pl.Int32).alias("cy"),
    ])
    counts = cells.group_by(["cx", "cy"]).agg(pl.len().alias("n_stops_in_cell"))
    kept_cells = counts.filter(pl.col("n_stops_in_cell") >= min_stops)
    retained = cells.join(kept_cells.select(["cx", "cy"]), on=["cx", "cy"], how="inner")
    return retained.with_columns(
        (pl.lit(f"{city_name}_") + pl.col("cx").cast(pl.Utf8) + pl.lit("_") + pl.col("cy").cast(pl.Utf8))
        .alias("zone_id")
    )


def zone_summary(zoned_stops: pl.DataFrame) -> pl.DataFrame:
    """One row per zone: cx, cy, centroid lat/lon, n_stops."""
    return (
        zoned_stops.group_by(["zone_id", "cx", "cy"])
        .agg([
            pl.col("stop_lat").mean().alias("centroid_lat"),
            pl.col("stop_lon").mean().alias("centroid_lon"),
            pl.len().alias("n_stops"),
        ])
    )


# ─────────────────────────────────────────────────────────────────────────
# Stop-level PT loading (generalized from pt_spatial_zones.load_stop_level_pt
# to accept any Entur operator prefix, not just RUT)
# ─────────────────────────────────────────────────────────────────────────

def load_stop_level_pt_for_operator(operator: str, dates: list[str]) -> pl.DataFrame:
    frames = []
    for d in dates:
        files = list_parquet_files_from_s3("entur", with_metadata=True, date_prefix=d,
                                            pattern=f"{operator}_siri_et_*.parquet")
        for f in files:
            try:
                df = read_parquet_from_s3(f["key"])
            except Exception:
                continue
            if df is None or df.height == 0 or "stop_time_updates" not in df.columns:
                continue
            exploded = (
                df.select("stop_time_updates")
                .explode("stop_time_updates")
                .unnest("stop_time_updates")
                .select(["stop_id", "arrival_delay"])
            )
            frames.append(exploded)
    if not frames:
        return pl.DataFrame(schema={"stop_id": pl.Utf8, "arrival_delay": pl.Int64})
    return pl.concat(frames, how="vertical_relaxed")


# ─────────────────────────────────────────────────────────────────────────
# Per-city, per-window zone-level dataset
# ─────────────────────────────────────────────────────────────────────────

def build_city_window_zones(city_name: str, city_cfg: dict, start, end,
                            stop_coords: pl.DataFrame | None = None) -> dict:
    if stop_coords is None:
        stop_coords = load_stop_coords()

    zoned_stops = assign_grid_zones(stop_coords, city_name, city_cfg["lat_c"], city_cfg["lon_c"],
                                    city_cfg["dlat"], city_cfg["dlon"])
    if zoned_stops.height == 0:
        return {"city": city_name, "note": "no grid cells cleared the min-stops threshold"}

    zones = zone_summary(zoned_stops)
    stop_to_zone = zoned_stops.select(["stop_id", "zone_id"]).unique()

    dates = _dates_in_window(start, end)
    pt = load_stop_level_pt_for_operator(city_cfg["operator"], dates)
    if pt.height == 0:
        return {"city": city_name, "note": "no stop-level PT data found for this window"}

    joined = pt.join(stop_to_zone, on="stop_id", how="inner")
    if joined.height == 0:
        return {"city": city_name, "note": "no PT records matched a retained zone"}

    agg = (
        joined.group_by("zone_id")
        .agg([
            pl.len().alias("activity"),
            pl.col("arrival_delay").mean().alias("delay"),
            pl.col("arrival_delay").is_not_null().sum().alias("n_delay_obs"),
        ])
    )
    zone_df = zones.join(agg, on="zone_id", how="left").sort(["cx", "cy"])
    return {"city": city_name, "zones": zone_df}


# ─────────────────────────────────────────────────────────────────────────
# Spatial weights: queen contiguity on the grid, with nearest-neighbor
# fallback for isolated cells (standard island treatment)
# ─────────────────────────────────────────────────────────────────────────

def build_queen_weights(zone_df: pl.DataFrame, n_fallback_neighbors: int = 2) -> dict[str, list[str]]:
    ids = zone_df["zone_id"].to_list()
    cxy = dict(zip(ids, zip(zone_df["cx"].to_list(), zone_df["cy"].to_list())))
    cell_to_id = {v: k for k, v in cxy.items()}

    neighbors: dict[str, list[str]] = {}
    for zid in ids:
        cx, cy = cxy[zid]
        neigh = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                if dx == 0 and dy == 0:
                    continue
                other = cell_to_id.get((cx + dx, cy + dy))
                if other is not None:
                    neigh.append(other)
        neighbors[zid] = neigh

    centroids = dict(zip(ids, zip(zone_df["centroid_lat"].to_list(), zone_df["centroid_lon"].to_list())))
    for zid in ids:
        if neighbors[zid]:
            continue
        lat0, lon0 = centroids[zid]
        dists = sorted(
            ((other, (centroids[other][0] - lat0) ** 2 + (centroids[other][1] - lon0) ** 2)
             for other in ids if other != zid),
            key=lambda t: t[1],
        )
        fallback = [other for other, _ in dists[:n_fallback_neighbors]]
        neighbors[zid] = fallback
        for other in fallback:
            if zid not in neighbors[other]:
                neighbors[other].append(zid)

    return neighbors


# ─────────────────────────────────────────────────────────────────────────
# Global + Local Moran's I
# ─────────────────────────────────────────────────────────────────────────

def compute_morans(zone_df: pl.DataFrame, metric: str, seed: int = 42) -> dict | None:
    from esda.moran import Moran, Moran_Local
    from libpysal.weights import W

    d = zone_df.filter(pl.col(metric).is_not_null())
    if d.height < 8:
        return None

    neighbors = build_queen_weights(d)
    w = W(neighbors)
    w.transform = "r"

    ids = d["zone_id"].to_list()
    y = np.array([d.filter(pl.col("zone_id") == i)[metric][0] for i in ids], dtype=float)
    # reorder y to match w.id_order
    id_to_val = dict(zip(ids, y))
    y_ordered = np.array([id_to_val[i] for i in w.id_order], dtype=float)

    avg_neighbors = float(np.mean([len(v) for v in neighbors.values()]))

    np.random.seed(seed)
    mi = Moran(y_ordered, w, permutations=N_PERMUTATIONS)
    mi_local = Moran_Local(y_ordered, w, permutations=N_PERMUTATIONS, seed=seed)

    quadrant_label = {1: "HH", 2: "LH", 3: "LL", 4: "HL"}
    local_results = {}
    for zid, q, p_sim, Is in zip(w.id_order, mi_local.q, mi_local.p_sim, mi_local.Is):
        sig = p_sim < LISA_ALPHA
        local_results[zid] = {
            "cluster": quadrant_label[q] if sig else "ns",
            "p_sim": float(p_sim),
            "local_I": float(Is),
        }

    return {
        "n_zones": d.height,
        "avg_neighbors": round(avg_neighbors, 2),
        "I": round(float(mi.I), 4),
        "p_sim": float(mi.p_sim),
        "EI": round(float(mi.EI), 4),
        "local": local_results,
    }


# ─────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────

def build_report() -> dict:
    stop_coords = load_stop_coords()
    report = {}
    for p in WINDOWS:
        win = p["name"].split(" ")[0]
        print(f"\n=== {win} ===")
        report[win] = {}
        for city_name, city_cfg in CITIES.items():
            print(f"  {city_name} ...")
            res = build_city_window_zones(city_name, city_cfg, p["start_date"], p["end_date"],
                                          stop_coords=stop_coords)
            if "note" in res:
                report[win][city_name] = {"note": res["note"]}
                print(f"    {res['note']}")
                continue
            zone_df = res["zones"]
            city_result = {"zone_df": zone_df, "metrics": {}}
            for metric in ["activity", "delay"]:
                m = compute_morans(zone_df, metric)
                city_result["metrics"][metric] = m
                if m is not None:
                    print(f"    {metric:8} n={m['n_zones']:3d} avg_neighbors={m['avg_neighbors']:.1f} "
                         f"I={m['I']:+.3f} p={m['p_sim']:.3f}")
            report[win][city_name] = city_result
    return report


def print_global_table(report: dict) -> None:
    header = f"{'Window':7} {'City':13} {'Metric':9} {'n_zones':>8} {'avg_nb':>7} {'I':>8} {'p_sim':>8}"
    print(header)
    print("-" * len(header))
    for win, cities in report.items():
        for city, res in cities.items():
            if "note" in res:
                print(f"{win:7} {city:13} {res['note']}")
                continue
            for metric, m in res["metrics"].items():
                if m is None:
                    print(f"{win:7} {city:13} {metric:9} insufficient data")
                    continue
                pstr = "<0.001" if m["p_sim"] < 0.001 else f"{m['p_sim']:.3f}"
                print(f"{win:7} {city:13} {metric:9} {m['n_zones']:8d} {m['avg_neighbors']:7.2f} "
                     f"{m['I']:+8.3f} {pstr:>8}")


def print_lisa_summary(report: dict) -> None:
    header = f"{'Window':7} {'City':13} {'Metric':9} {'HH':>4} {'LL':>4} {'HL':>4} {'LH':>4} {'ns':>4}"
    print(header)
    print("-" * len(header))
    for win, cities in report.items():
        for city, res in cities.items():
            if "note" in res:
                continue
            for metric, m in res["metrics"].items():
                if m is None:
                    continue
                counts = {"HH": 0, "LL": 0, "HL": 0, "LH": 0, "ns": 0}
                for v in m["local"].values():
                    counts[v["cluster"]] += 1
                print(f"{win:7} {city:13} {metric:9} {counts['HH']:4d} {counts['LL']:4d} "
                     f"{counts['HL']:4d} {counts['LH']:4d} {counts['ns']:4d}")


# ─────────────────────────────────────────────────────────────────────────
# LISA cluster maps
# ─────────────────────────────────────────────────────────────────────────

LISA_COLORS = {"HH": "#d7301f", "LL": "#2166ac", "HL": "#fdbb84", "LH": "#92c5de", "ns": "#e0e0e0"}
LISA_LABELS = {"HH": "High-High (hot spot)", "LL": "Low-Low (cold spot)",
              "HL": "High-Low (outlier)", "LH": "Low-High (outlier)", "ns": "Not significant"}


def _cell_bbox(cx: int, cy: int, city_cfg: dict, cell_km: float = CELL_KM) -> tuple[float, float, float, float]:
    """Returns (lat_lo, lat_hi, lon_lo, lon_hi) for grid cell (cx, cy)."""
    lat_c, lon_c, dlat, dlon = city_cfg["lat_c"], city_cfg["lon_c"], city_cfg["dlat"], city_cfg["dlon"]
    lat_cell_deg = cell_km / 111.0
    lon_cell_deg = cell_km / (111.0 * math.cos(math.radians(lat_c)))
    lat_lo = (lat_c - dlat) + cx * lat_cell_deg
    lon_lo = (lon_c - dlon) + cy * lon_cell_deg
    return lat_lo, lat_lo + lat_cell_deg, lon_lo, lon_lo + lon_cell_deg


def _draw_lisa_city_panel(ax, zone_df: pl.DataFrame, local: dict, city_cfg: dict, city_name: str,
                          basemap: bool = True, basemap_zoom: int = 12):
    import matplotlib.patches as mpatches

    lat_c, lon_c, dlat, dlon = city_cfg["lat_c"], city_cfg["lon_c"], city_cfg["dlat"], city_cfg["dlon"]
    lat_min, lat_max = lat_c - dlat, lat_c + dlat
    lon_min, lon_max = lon_c - dlon, lon_c + dlon

    if basemap:
        try:
            img, extent = fetch_osm_basemap(lat_min, lat_max, lon_min, lon_max, zoom=basemap_zoom)
            ax.imshow(img, extent=extent, zorder=0)
        except Exception as e:
            print(f"Warning: basemap fetch failed for {city_name} ({e}); plotting without basemap.")
            basemap = False

    for row in zone_df.iter_rows(named=True):
        zid = row["zone_id"]
        cluster = local.get(zid, {}).get("cluster", "ns") if local else "ns"
        lat_lo, lat_hi, lon_lo, lon_hi = _cell_bbox(row["cx"], row["cy"], city_cfg)
        rect = mpatches.Rectangle((lon_lo, lat_lo), lon_hi - lon_lo, lat_hi - lat_lo,
                                  facecolor=LISA_COLORS[cluster], alpha=0.8 if basemap else 0.9,
                                  edgecolor="white", linewidth=0.3, zorder=2)
        ax.add_patch(rect)

    ax.set_xlim(lon_min, lon_max)
    ax.set_ylim(lat_min, lat_max)
    mean_lat = (lat_min + lat_max) / 2
    ax.set_aspect(1.0 / np.cos(np.radians(mean_lat)))
    ax.set_title(city_name, fontweight="bold", fontsize=10)
    ax.set_xlabel("Longitude", fontsize=7)
    ax.set_ylabel("Latitude", fontsize=7)
    ax.tick_params(labelsize=6)


def fig_lisa_multi_city(report: dict, window: str, metric: str, save_path: str | None = None,
                        basemap: bool = True, basemap_zoom: int = 12):
    """6-panel (2x3) LISA cluster map, one panel per city, for one window/metric."""
    import matplotlib.patches as mpatches

    plt = _set_plot_style()
    plt.rcParams.update({"axes.grid": False})
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()

    metric_label = "PT activity" if metric == "activity" else "PT delay"
    win_res = report[window]
    for i, city_name in enumerate(CITIES):
        ax = axes[i]
        res = win_res.get(city_name, {})
        if "note" in res or res.get("metrics", {}).get(metric) is None:
            ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(city_name, fontweight="bold", fontsize=10)
            continue
        zone_df = res["zone_df"]
        m = res["metrics"][metric]
        _draw_lisa_city_panel(ax, zone_df, m["local"], CITIES[city_name], city_name,
                              basemap=basemap, basemap_zoom=basemap_zoom)
        ax.text(0.02, 0.02, f"I={m['I']:+.2f}, p={m['p_sim']:.3f}", transform=ax.transAxes,
               fontsize=8, va="bottom", ha="left",
               bbox=dict(boxstyle="round,pad=0.25", facecolor="white", alpha=0.85, linewidth=0.4))

    legend_handles = [mpatches.Patch(facecolor=LISA_COLORS[k], edgecolor="white", label=LISA_LABELS[k])
                      for k in ["HH", "LL", "HL", "LH", "ns"]]
    fig.legend(handles=legend_handles, loc="lower center", ncol=5, fontsize=9, frameon=True,
              bbox_to_anchor=(0.5, -0.02))
    fig.suptitle(f"LISA cluster map: {metric_label} — {window}", fontweight="bold", fontsize=13)
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    if save_path:
        fig.savefig(save_path, bbox_inches="tight", dpi=200)
        print(f"Saved LISA map -> {save_path}")
    return fig


if __name__ == "__main__":
    import os
    import pickle

    rep = build_report()
    print("\n\n=== Global Moran's I summary ===")
    print_global_table(rep)
    print("\n=== LISA cluster-type counts (per zone classification) ===")
    print_lisa_summary(rep)

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "multi_city_output")
    os.makedirs(out_dir, exist_ok=True)
    cache_path = os.path.join(out_dir, "report_cache.pkl")
    with open(cache_path, "wb") as f:
        pickle.dump(rep, f)
    print(f"\nCached report -> {cache_path}")
