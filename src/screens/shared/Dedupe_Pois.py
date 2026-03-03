#!/usr/bin/env python3
"""
Dedupe_Pois.py — Deduplicate POI + transit point layers within a distance (default 30m),
keeping the "most important" feature in each cluster, and copying all other layers through.

Usage:
  python Dedupe_Pois.py input.gpkg output.gpkg [distance_m]

Behavior:
  - Dedupe targets:
      * poi_* layers
      * points_* layers   (transit stops / stations)
  - Non-target layers (e.g. lines_*, admin_regions) are copied unchanged.
  - Writes a complete output GeoPackage that preserves all layers.
"""

import os
import sys
from typing import Optional
import pyogrio
import geopandas as gpd
import pandas as pd
import networkx as nx


# -------------------------
# Scoring / importance
# -------------------------
def score_row(row) -> int:
    score = 0

    # "Importance" signals
    if pd.notna(row.get("wikidata")):
        score += 100
    if pd.notna(row.get("wikipedia")):
        score += 80
    if pd.notna(row.get("operator")):
        score += 30
    if pd.notna(row.get("brand")):
        score += 25
    if pd.notna(row.get("ref")):
        score += 20
    if pd.notna(row.get("name")) and str(row.get("name")).strip() != "":
        score += 10

    # Prefer area/relation/way over node when duplicated
    osm_type = row.get("osm_type")
    if osm_type == "area":
        score += 5
    elif osm_type == "relation":
        score += 4
    elif osm_type == "way":
        score += 3
    elif osm_type == "node":
        score += 1

    return score


def _detect_geom_col(df: pd.DataFrame) -> str:
    if "geometry" in df.columns:
        return "geometry"
    if "geom" in df.columns:
        return "geom"
    raise RuntimeError("No geometry column found (expected 'geometry' or 'geom').")


# -------------------------
# Pair building (distance graph)
# -------------------------
def _pairs_via_sjoin_nearest(gdf_m: gpd.GeoDataFrame, dist_m: float) -> pd.DataFrame:
    left = gdf_m.reset_index(drop=False).rename(columns={"index": "i"})
    right = left.copy().rename(columns={"i": "j"})

    pairs = gpd.sjoin_nearest(
        left,
        right,
        how="inner",
        max_distance=dist_m,
        distance_col="d",
    )

    pairs = pairs[pairs["i"] != pairs["j"]][["i", "j"]].drop_duplicates()
    return pairs


def _pairs_via_strtree(gdf_m: gpd.GeoDataFrame, dist_m: float) -> pd.DataFrame:
    try:
        from shapely.strtree import STRtree  # Shapely 2.x
    except Exception:
        from shapely.strtree import STRtree  # type: ignore (Shapely 1.8)

    geoms = list(gdf_m.geometry.values)
    tree = STRtree(geoms)

    geom_id_to_idx = {id(g): i for i, g in enumerate(geoms)}

    edges = []
    for i, geom in enumerate(geoms):
        buf = geom.buffer(dist_m)
        hits = tree.query(buf)

        if len(hits) == 0:
            continue

        if hasattr(hits, "dtype"):
            cand_idxs = list(hits)
        else:
            cand_idxs = [geom_id_to_idx.get(id(h)) for h in hits]
            cand_idxs = [j for j in cand_idxs if j is not None]

        for j in cand_idxs:
            if i == j:
                continue
            if geom.distance(geoms[j]) <= dist_m:
                edges.append((i, j))

    return pd.DataFrame(edges, columns=["i", "j"]).drop_duplicates()


# -------------------------
# Writing helper (controls mode)
# -------------------------
def _write_layer(gdf: gpd.GeoDataFrame, gpkg_out: str, layer: str, first_write: bool) -> bool:
    """
    Writes layer to gpkg_out. Uses mode='w' for the very first write, then mode='a'.
    Returns False once the file has been created (i.e., after first write).
    """
    mode = "w" if first_write else "a"
    gdf.to_file(gpkg_out, layer=layer, driver="GPKG", mode=mode)
    return False


# -------------------------
# Dedupe one layer
# -------------------------
def dedupe_layer_keep_best(
    gpkg_in: str,
    layer: str,
    dist_m: float = 30.0,
    crs_meters: str = "EPSG:27700",  # British National Grid: good for Wales/Scotland/UK
) -> Optional[gpd.GeoDataFrame]:
    df = pyogrio.read_dataframe(gpkg_in, layer=layer)
    if df.empty:
        return None

    geom_col = _detect_geom_col(df)

    # Assume your stored points are lon/lat
    gdf = gpd.GeoDataFrame(df, geometry=geom_col, crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notnull()].copy()
    gdf = gdf[gdf.geometry.type == "Point"].copy()
    if gdf.empty:
        return None

    # Project to meters for distance-based dedupe
    gdf_m = gdf.to_crs(crs_meters)

    # Build connectivity graph of points within dist_m
    try:
        pairs = _pairs_via_sjoin_nearest(gdf_m, dist_m)
        nodes = gdf.index.tolist()
    except Exception as e:
        print(f"[WARN] {layer}: sjoin_nearest failed ({type(e).__name__}: {e}). Falling back to STRtree.")
        gdf = gdf.reset_index(drop=True)
        gdf_m = gdf.to_crs(crs_meters)
        pairs = _pairs_via_strtree(gdf_m, dist_m)
        nodes = gdf.index.tolist()

    G = nx.Graph()
    G.add_nodes_from(nodes)

    if not pairs.empty:
        G.add_edges_from(pairs.itertuples(index=False, name=None))

    clusters = list(nx.connected_components(G))

    rows = []
    for comp in clusters:
        idxs = sorted(comp)
        chunk = gdf.loc[idxs].copy()
        chunk["importance_score"] = chunk.apply(score_row, axis=1)
        best = chunk.sort_values(by=["importance_score"], ascending=False).iloc[0].copy()
        best["merge_count"] = len(chunk)
        rows.append(best)

    if not rows:
        return None

    merged = gpd.GeoDataFrame(rows, geometry=geom_col)

    # Ensure CRS set once
    merged = merged.set_crs("EPSG:4326", allow_override=True)

    print(f"{layer}: {len(gdf)} -> {len(merged)} after dedupe")
    return merged


# -------------------------
# Copy-through a layer unchanged
# -------------------------
def copy_layer(gpkg_in: str, layer: str) -> Optional[gpd.GeoDataFrame]:
    try:
        gdf = gpd.read_file(gpkg_in, layer=layer)
    except Exception:
        # Some layers might not read cleanly via geopandas; fall back to pyogrio
        df = pyogrio.read_dataframe(gpkg_in, layer=layer)
        if df.empty:
            return None
        geom_col = _detect_geom_col(df)
        gdf = gpd.GeoDataFrame(df, geometry=geom_col)

    if gdf is None or len(gdf) == 0:
        return gdf

    # If CRS missing, assume lon/lat for points/lines/admin in your pipeline
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)

    return gdf


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: python Dedupe_Pois.py <input_gpkg> <output_gpkg> [distance_m]")
        return 2

    gpkg_in = sys.argv[1]
    gpkg_out = sys.argv[2]
    dist_m = float(sys.argv[3]) if len(sys.argv) >= 4 else 30.0

    if not os.path.exists(gpkg_in):
        print(f"Input not found: {gpkg_in}")
        return 2

    # Start fresh
    if os.path.exists(gpkg_out):
        os.remove(gpkg_out)

    layers = [name for name, _geomtype in pyogrio.list_layers(gpkg_in)]

    # Dedupe POIs + transit point layers
    dedupe_layers = [lyr for lyr in layers if lyr.startswith("poi_") or lyr.startswith("points_")]
    passthrough_layers = [lyr for lyr in layers if lyr not in dedupe_layers]

    if not dedupe_layers and not passthrough_layers:
        print("No layers found. Nothing to do.")
        return 0

    print(f"Deduping {len(dedupe_layers)} point layers (poi_* + points_*) at {dist_m:.0f}m...")
    print(f"Copying through {len(passthrough_layers)} other layers unchanged...")

    first_write = True

    # 1) Dedupe target layers
    for lyr in dedupe_layers:
        merged = dedupe_layer_keep_best(gpkg_in, lyr, dist_m=dist_m)
        if merged is None:
            # If empty or no point geometry, still copy it through so output stays complete
            gdf_copy = copy_layer(gpkg_in, lyr)
            if gdf_copy is None:
                print(f"{lyr}: empty (skipped)")
                continue
            first_write = _write_layer(gdf_copy, gpkg_out, lyr, first_write)
            print(f"{lyr}: copied through (no dedupe applied / empty)")
            continue

        first_write = _write_layer(merged, gpkg_out, lyr, first_write)

    # 2) Copy all remaining layers unchanged
    for lyr in passthrough_layers:
        gdf_copy = copy_layer(gpkg_in, lyr)
        if gdf_copy is None:
            print(f"{lyr}: empty (skipped)")
            continue
        first_write = _write_layer(gdf_copy, gpkg_out, lyr, first_write)
        print(f"{lyr}: copied through")

    print(f"Done. Wrote: {gpkg_out}")
    return 0


if __name__ == "__main__":
    # Avoid debug traceback spam; still returns correct exit code
    sys.exit(main())
