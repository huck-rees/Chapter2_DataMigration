"""Calculate fraction of distance downstream and drainage area for river reaches.

Uses SWORD v17b topology to trace the full mainstem river upstream and
downstream from each study reach, then computes normalised position
metrics.  Elevation is sampled at each SWORD reach midpoint via GEE
(NASADEM / ArcticDEM) to build a longitudinal profile.

Usage
-----
    python calc_fraction_ds.py <master_csv> <sword_cont_abb> [--ee-project ID]

Arguments
---------
    master_csv       Path to master CSV.  Required columns:
                         river_name, working_directory, SWORD_cont_abb
    sword_cont_abb   Continent to process this run (af, as, eu, na, oc, sa).
                     Only rivers whose SWORD_cont_abb matches are processed.

Outputs
-------
    {OUTPUT_DIR}/{river_name}/{river_name}_SWORD_reaches.gpkg
    {OUTPUT_DIR}/{cont}_fraction_ds_results.csv
    {OUTPUT_DIR}/{cont}_fraction_ds_report.pdf
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
import numpy as np
import pandas as pd

try:
    import contextily as ctx
    HAS_CTX = True
except ImportError:
    HAS_CTX = False

import ee

# ── Constants ──────────────────────────────────────────────────────
SWORD_DIR = r"E:\Dissertation\Data\SWORD"
OUTPUT_DIR = (
    r"E:\Dissertation\Data\SWORD\Extracted_Rivers\Chapter_2_dist_ds_analysis"
)
SWORD_VERSION = "v17b"
ARCTICDEM_MIN_LATITUDE = 60.0
VALID_CONTINENTS = {"af", "as", "eu", "na", "oc", "sa"}


# ── Elevation sampling via GEE ─────────────────────────────────────

def get_elevation(lat: float, lon: float, max_retries: int = 3) -> float | None:
    """Sample DEM elevation at (lat, lon) via Google Earth Engine."""
    for attempt in range(max_retries):
        try:
            point = ee.Geometry.Point([lon, lat])
            if lat >= ARCTICDEM_MIN_LATITUDE:
                dem = ee.Image("UMN/PGC/ArcticDEM/V3/2m_mosaic")
                scale = 32
            else:
                dem = ee.Image("NASA/NASADEM_HGT/001")
                scale = 30
            sample = dem.select("elevation").sample(point, scale).first()
            if sample is None:
                return None
            elev = sample.get("elevation").getInfo()
            if elev is None or not (-500 < elev < 9000):
                return None
            return float(elev)
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return None
    return None


def sample_river_elevations(
    river_reaches: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Sample elevation at the midpoint of each traced SWORD reach."""
    records: list[dict] = []
    n = len(river_reaches)
    for i, (_, row) in enumerate(river_reaches.iterrows()):
        midpoint = row.geometry.interpolate(0.5, normalized=True)
        elev = get_elevation(midpoint.y, midpoint.x)
        records.append({
            "reach_id": int(row["reach_id"]),
            "reach_order": int(row["reach_order"]),
            "dist_out": float(row["dist_out"]),
            "elevation_m": elev,
        })
        if (i + 1) % 25 == 0 or i == n - 1:
            ok = sum(1 for r in records if r["elevation_m"] is not None)
            print(f"      ... {i + 1}/{n} sampled ({ok} OK)")
    return pd.DataFrame(records)


# ── SWORD topology helpers ─────────────────────────────────────────

def build_lookups(sword_reaches: gpd.GeoDataFrame) -> tuple[dict, dict, dict]:
    """Return (dn_lookup, dist_lookup, upstream_lookup) from SWORD reaches."""
    dn_lookup = sword_reaches.set_index("reach_id")["rch_id_dn"].to_dict()
    dist_lookup = sword_reaches.set_index("reach_id")["dist_out"].to_dict()

    upstream_lookup: dict[int, list[int]] = {}
    for _, row in sword_reaches.iterrows():
        rid = int(row["reach_id"])
        dn_raw = str(row["rch_id_dn"])
        dn_ids = [int(x) for x in dn_raw.split() if int(x) != 0]
        for dn_id in dn_ids:
            upstream_lookup.setdefault(dn_id, []).append(rid)

    return dn_lookup, dist_lookup, upstream_lookup


def trace_upstream_mainstem(
    reach_id: int,
    upstream_lookup: dict,
    dist_lookup: dict,
    max_iter: int = 5000,
) -> list[int]:
    """Walk upstream along the mainstem (largest dist_out at confluences)."""
    path: list[int] = []
    current = reach_id
    for _ in range(max_iter):
        candidates = upstream_lookup.get(current, [])
        if not candidates:
            break
        best = max(candidates, key=lambda r: dist_lookup.get(r, 0))
        path.append(best)
        current = best
    return list(reversed(path))


def trace_downstream(
    reach_id: int,
    dn_lookup: dict,
    max_iter: int = 5000,
) -> list[int]:
    """Walk downstream via rch_id_dn (first non-zero entry at splits)."""
    path: list[int] = []
    current = reach_id
    for _ in range(max_iter):
        dn_raw = dn_lookup.get(current)
        if dn_raw is None:
            break
        dn_ids = [int(x) for x in str(dn_raw).split() if int(x) != 0]
        if not dn_ids:
            break
        current = dn_ids[0]
        path.append(current)
    return path


# ── PDF page ───────────────────────────────────────────────────────

def _add_basemap(ax, **kwargs):
    """Attempt to add an Esri satellite basemap; silently skip on failure."""
    if not HAS_CTX:
        return
    try:
        ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery, **kwargs)
    except Exception:
        pass


def draw_page(
    pdf: PdfPages,
    river_name: str,
    ds_order: int,
    river_3857: gpd.GeoDataFrame,
    ds_boundary_3857: gpd.GeoDataFrame,
    elev_df: pd.DataFrame,
    sword_reach_id: int | None,
    frac_dist_ds: float | None,
    frac_dr_ar: float | None,
) -> None:
    """Draw one three-panel page in the report PDF."""
    fig = plt.figure(figsize=(16, 13))
    gs = fig.add_gridspec(2, 2, height_ratios=[3, 1], width_ratios=[2, 1])
    ax_map = fig.add_subplot(gs[0, 0])
    ax_ctx = fig.add_subplot(gs[0, 1])
    ax_prof = fig.add_subplot(gs[1, :])

    # Highlight the SWORD reach for this ds_order
    selected_3857 = (
        river_3857[river_3857["reach_id"] == sword_reach_id]
        if sword_reach_id is not None
        else river_3857.iloc[:0]
    )

    # ── Top-left: mainstem over satellite ──────────────────────────
    river_3857.plot(
        ax=ax_map, column="reach_order", cmap="viridis", linewidth=2.5,
        legend=True,
        legend_kwds={"label": "Reach order (US \u2192 DS)", "shrink": 0.5},
    )
    if not selected_3857.empty:
        selected_3857.plot(ax=ax_map, color="red", linewidth=4)
    ds_boundary_3857.plot(
        ax=ax_map, facecolor="none", edgecolor="red",
        linewidth=2, linestyle="--",
    )
    _add_basemap(ax_map)

    legend_handles = [
        Line2D([0], [0], color="red", lw=2, ls="--",
               label=f"Reach {ds_order} boundary"),
    ]
    if sword_reach_id is not None:
        legend_handles.insert(
            0, Line2D([0], [0], color="red", lw=4,
                      label=f"SWORD {sword_reach_id}"),
        )
    ax_map.legend(handles=legend_handles, loc="best")

    if frac_dist_ds is not None and frac_dr_ar is not None:
        title = (
            f"{river_name} \u2014 Reach {ds_order}\n"
            f"frac_dist_ds = {frac_dist_ds:.4f}   "
            f"frac_dr_ar = {frac_dr_ar:.4f}"
        )
    else:
        title = f"{river_name} \u2014 Reach {ds_order}\n(metrics unavailable)"
    ax_map.set_title(title)
    ax_map.set_xlabel("Easting (m)")
    ax_map.set_ylabel("Northing (m)")

    # ── Top-right: zoomed-out context ──────────────────────────────
    xmin, xmax = ax_map.get_xlim()
    ymin, ymax = ax_map.get_ylim()
    xc, yc = (xmin + xmax) / 2, (ymin + ymax) / 2
    hw = (xmax - xmin) * 2
    hh = (ymax - ymin) * 2

    river_3857.plot(ax=ax_ctx, color="gold", linewidth=1.2)
    if not selected_3857.empty:
        selected_3857.plot(ax=ax_ctx, color="red", linewidth=2.5)

    ax_ctx.set_xlim(xc - hw, xc + hw)
    ax_ctx.set_ylim(yc - hh, yc + hh)
    ax_ctx.set_aspect("equal")
    _add_basemap(ax_ctx)

    ax_ctx.add_patch(Rectangle(
        (xmin, ymin), xmax - xmin, ymax - ymin,
        fill=False, edgecolor="yellow", linewidth=1.5,
    ))
    ax_ctx.set_title("Regional context")
    ax_ctx.set_xticks([])
    ax_ctx.set_yticks([])

    # ── Bottom: longitudinal profile ───────────────────────────────
    valid = elev_df.dropna(subset=["elevation_m"]).copy()
    if not valid.empty:
        max_do = valid["dist_out"].max()
        valid["dist_km"] = (max_do - valid["dist_out"]) / 1000

        ax_prof.plot(
            valid["dist_km"], valid["elevation_m"],
            "o-", color="steelblue", markersize=3, linewidth=1.2,
            label="SWORD reaches",
        )
        if sword_reach_id is not None:
            sel = valid[valid["reach_id"] == sword_reach_id]
            if not sel.empty:
                ax_prof.plot(
                    sel["dist_km"], sel["elevation_m"],
                    "ro", markersize=10, zorder=5,
                    label=f"Reach {ds_order}",
                )
        ax_prof.legend(loc="best")

    ax_prof.set_title("Longitudinal profile")
    ax_prof.set_xlabel("Distance from headwaters (km)")
    ax_prof.set_ylabel("Elevation (m)")
    ax_prof.grid(True, alpha=0.3)

    plt.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


# ── Per-river processing ──────────────────────────────────────────

def process_river(
    river_name: str,
    working_directory: str,
    sword_reaches: gpd.GeoDataFrame,
    dn_lookup: dict,
    dist_lookup: dict,
    upstream_lookup: dict,
) -> tuple[
    gpd.GeoDataFrame | None,
    gpd.GeoDataFrame | None,
    pd.DataFrame | None,
    list[dict] | None,
]:
    """Trace, sample, and compute metrics for one river.

    Returns (river_reaches, reach_gdf, elev_df, result_rows) or four Nones.
    """
    # ── Load reach shapefile ───────────────────────────────────────
    reach_path = os.path.join(
        working_directory, "RiverMapping", "Reaches",
        river_name, f"{river_name}.shp",
    )
    if not os.path.isfile(reach_path):
        print(f"    ERROR: reach shapefile not found: {reach_path}")
        return None, None, None, None

    reach_gdf = gpd.read_file(reach_path)
    if reach_gdf.crs != sword_reaches.crs:
        reach_gdf = reach_gdf.to_crs(sword_reaches.crs)

    # ── Find SWORD reaches inside the study area ───────────────────
    intersecting = gpd.sjoin(
        sword_reaches, reach_gdf, how="inner", predicate="intersects",
    ).drop(columns="index_right", errors="ignore")
    intersecting = intersecting.drop_duplicates(subset="reach_id")

    if intersecting.empty:
        print("    ERROR: no SWORD reaches intersect the reach boundary")
        return None, None, None, None

    # Auto-identify anchor: most downstream SWORD reach in the area
    anchor_idx = intersecting["dist_out"].idxmin()
    anchor_id = int(intersecting.loc[anchor_idx, "reach_id"])
    print(
        f"    Anchor SWORD reach: {anchor_id}  "
        f"(dist_out = {intersecting.loc[anchor_idx, 'dist_out'] / 1000:.0f} km)"
    )

    # ── Trace full mainstem ────────────────────────────────────────
    us_path = trace_upstream_mainstem(anchor_id, upstream_lookup, dist_lookup)
    ds_path = trace_downstream(anchor_id, dn_lookup)
    full_path = us_path + [anchor_id] + ds_path
    print(
        f"    Traced: {len(us_path)} US + 1 + {len(ds_path)} DS "
        f"= {len(full_path)} reaches"
    )

    river_reaches = sword_reaches[
        sword_reaches["reach_id"].isin(full_path)
    ].copy()
    order_map = {rid: i + 1 for i, rid in enumerate(full_path)}
    river_reaches["reach_order"] = river_reaches["reach_id"].map(order_map)
    river_reaches = river_reaches.sort_values("reach_order").reset_index(
        drop=True
    )

    # ── Save .gpkg ─────────────────────────────────────────────────
    river_dir = os.path.join(OUTPUT_DIR, river_name)
    os.makedirs(river_dir, exist_ok=True)
    gpkg_path = os.path.join(river_dir, f"{river_name}_SWORD_reaches.gpkg")
    river_reaches.to_file(gpkg_path, driver="GPKG")
    print(f"    Saved: {gpkg_path}")

    # ── Sample elevations ──────────────────────────────────────────
    print(f"    Sampling elevations ({len(river_reaches)} reaches) ...")
    elev_df = sample_river_elevations(river_reaches)
    ok = elev_df["elevation_m"].notna().sum()
    print(f"    Elevation complete: {ok}/{len(elev_df)} successful")

    # ── Compute per-reach metrics ──────────────────────────────────
    max_dist_out = river_reaches["dist_out"].max()
    min_dist_out = river_reaches["dist_out"].min()
    total_dist = max_dist_out - min_dist_out

    # facc at the most downstream traced reach
    has_facc = "facc" in river_reaches.columns
    if has_facc:
        ds_most = river_reaches.loc[river_reaches["dist_out"].idxmin()]
        max_facc = float(ds_most["facc"])
    else:
        max_facc = None

    # One spatial join: map traced SWORD reaches → ds_order polygons
    reach_polys = reach_gdf[["ds_order", "geometry"]].copy()
    reach_polys = reach_polys.rename(columns={"ds_order": "ds_order_poly"})

    sword_to_ds = gpd.sjoin(
        river_reaches, reach_polys, how="inner", predicate="intersects",
    ).drop(columns="index_right", errors="ignore")

    results: list[dict] = []
    for ds_order in sorted(reach_gdf["ds_order"].unique()):
        ds_order = int(ds_order)
        matches = sword_to_ds[sword_to_ds["ds_order_poly"] == ds_order]

        if matches.empty:
            print(f"    ds_order {ds_order}: no SWORD overlap")
            results.append({
                "river_name": river_name,
                "ds_order": ds_order,
                "frac_dist_ds": None,
                "frac_dr_ar": None,
                "sword_reach_id": None,
            })
            continue

        target = matches.loc[matches["dist_out"].idxmin()]
        target_dist = float(target["dist_out"])
        target_id = int(target["reach_id"])

        frac_dist_ds = (
            (max_dist_out - target_dist) / total_dist
            if total_dist > 0 else None
        )
        frac_dr_ar = (
            float(target["facc"]) / max_facc
            if has_facc and max_facc and max_facc > 0 else None
        )

        results.append({
            "river_name": river_name,
            "ds_order": ds_order,
            "frac_dist_ds": frac_dist_ds,
            "frac_dr_ar": frac_dr_ar,
            "sword_reach_id": target_id,
        })

        metric_str = (
            f"frac_dist_ds={frac_dist_ds:.4f}  frac_dr_ar={frac_dr_ar:.4f}"
            if frac_dist_ds is not None and frac_dr_ar is not None
            else "metrics unavailable"
        )
        print(f"    ds_order {ds_order}: SWORD {target_id}  {metric_str}")

    return river_reaches, reach_gdf, elev_df, results


# ── Main driver ────────────────────────────────────────────────────

def run(
    master_csv_path: str,
    sword_cont_abb: str,
    ee_project: str | None = None,
) -> None:
    """Run fraction-downstream analysis for all rivers on one continent."""
    sword_cont_abb = sword_cont_abb.lower()
    if sword_cont_abb not in VALID_CONTINENTS:
        raise ValueError(
            f"Invalid continent '{sword_cont_abb}'. "
            f"Must be one of {sorted(VALID_CONTINENTS)}."
        )

    # ── Load and filter master CSV ─────────────────────────────────
    master = pd.read_csv(master_csv_path)
    required = {"river_name", "working_directory", "SWORD_cont_abb"}
    missing = required - set(master.columns)
    if missing:
        raise ValueError(f"Master CSV missing columns: {missing}")

    master["SWORD_cont_abb"] = master["SWORD_cont_abb"].str.strip().str.lower()
    rivers = master[master["SWORD_cont_abb"] == sword_cont_abb].reset_index(
        drop=True
    )
    if rivers.empty:
        print(f"No rivers found for continent '{sword_cont_abb}'.")
        return

    print(f"Rivers to process ({sword_cont_abb.upper()}): {len(rivers)}")
    for _, r in rivers.iterrows():
        print(f"  - {r['river_name']}")
    print()

    # ── Load SWORD (once per continent) ────────────────────────────
    sword_path = os.path.join(
        SWORD_DIR,
        f"{sword_cont_abb}_sword_reaches_{SWORD_VERSION}.gpkg",
    )
    print(f"Loading {os.path.basename(sword_path)} ...")
    sword_reaches = gpd.read_file(sword_path, engine="pyogrio")
    print(f"Loaded {len(sword_reaches):,} SWORD reaches.")

    if "facc" not in sword_reaches.columns:
        print(
            "WARNING: 'facc' column not found in SWORD reaches. "
            "frac_dr_ar will be unavailable."
        )
    print()

    # ── Build topology lookups (once) ──────────────────────────────
    print("Building topology lookups ...")
    dn_lookup, dist_lookup, upstream_lookup = build_lookups(sword_reaches)
    print(f"Upstream lookup: {len(upstream_lookup):,} entries.\n")

    # ── Initialise GEE (once) ──────────────────────────────────────
    print("Initialising Google Earth Engine ...")
    if ee_project:
        ee.Initialize(project=ee_project)
    else:
        ee.Initialize()
    print("GEE ready.\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Process each river ─────────────────────────────────────────
    all_results: list[dict] = []
    pdf_queue: list[tuple] = []

    for i, (_, row) in enumerate(rivers.iterrows()):
        river_name = row["river_name"]
        working_dir = row["working_directory"]

        print("=" * 60)
        print(f"[{i + 1}/{len(rivers)}]  {river_name}")
        print("=" * 60)

        river_reaches, reach_gdf, elev_df, results = process_river(
            river_name, working_dir, sword_reaches,
            dn_lookup, dist_lookup, upstream_lookup,
        )

        if results is not None:
            all_results.extend(results)
            pdf_queue.append(
                (river_name, river_reaches, reach_gdf, elev_df, results)
            )
        print()

    # ── Write master results CSV ───────────────────────────────────
    if all_results:
        results_df = pd.DataFrame(all_results)
        csv_out = os.path.join(
            OUTPUT_DIR, f"{sword_cont_abb}_fraction_ds_results.csv"
        )
        results_df.to_csv(csv_out, index=False)
        print(f"Results CSV: {csv_out}")
        print(results_df.to_string(index=False))
        print()

    # ── Generate PDF report ────────────────────────────────────────
    if pdf_queue:
        pdf_out = os.path.join(
            OUTPUT_DIR, f"{sword_cont_abb}_fraction_ds_report.pdf"
        )
        print(f"Generating PDF: {pdf_out}")

        with PdfPages(pdf_out) as pdf:
            for river_name, river_reaches, reach_gdf, elev_df, results in pdf_queue:
                river_3857 = river_reaches.to_crs(epsg=3857)
                reach_gdf_3857 = reach_gdf.to_crs(epsg=3857)

                for res in results:
                    ds_order = res["ds_order"]
                    sword_rid = res.get("sword_reach_id")

                    ds_bnd = reach_gdf_3857[
                        reach_gdf_3857["ds_order"] == ds_order
                    ]
                    if ds_bnd.empty:
                        continue

                    draw_page(
                        pdf,
                        river_name,
                        ds_order,
                        river_3857,
                        ds_bnd,
                        elev_df,
                        sword_rid,
                        res.get("frac_dist_ds"),
                        res.get("frac_dr_ar"),
                    )
                    print(f"  Page: {river_name}  ds_order={ds_order}")

        print(f"\nPDF complete: {pdf_out}")

    print("\nDone.")


# ── CLI ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Calculate fraction of distance downstream (frac_dist_ds) "
            "and fraction of drainage area (frac_dr_ar) for each river "
            "reach using SWORD v17b topology."
        ),
    )
    parser.add_argument(
        "master_csv",
        help="Path to master CSV (river_name, working_directory, SWORD_cont_abb).",
    )
    parser.add_argument(
        "sword_cont_abb",
        help="SWORD continent abbreviation to process (af, as, eu, na, oc, sa).",
    )
    parser.add_argument(
        "--ee-project",
        default=None,
        help="Google Earth Engine project ID (required by newer earthengine-api).",
    )
    args = parser.parse_args()

    run(args.master_csv, args.sword_cont_abb, ee_project=args.ee_project)
