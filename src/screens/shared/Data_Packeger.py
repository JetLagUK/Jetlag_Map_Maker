#!/usr/bin/env python3
"""
Data_Packeger.py — 4-pass GeoPackage extractor (split into modules)

Run (GUI mode):
  python Data_Packeger.py

Run (CLI mode):
  python Data_Packeger.py scotland.osm.pbf output_folder/
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any, Dict, List
from typing import Optional
import pandas as pd
import geopandas as gpd
import pyogrio

from osm_extract_common import (
    ensure_dir, pick_file, pick_output_dir, run_dedupe,
    POINT_LAYER_NAMES, LINE_LAYER_NAMES, ADMIN_LAYER, FIELDS
)
from osm_extract_passes import PointsPass, LinesPass, AdminPass, POIAreasCentroidPass


class LayerWriter:
    def __init__(self, gpkg_path: str) -> None:
        self.gpkg_path = gpkg_path
        self.rows: Dict[str, List[Dict[str, Any]]] = {
            k: [] for k in (POINT_LAYER_NAMES + LINE_LAYER_NAMES + [ADMIN_LAYER])
        }

    def add(self, layer: str, geom, props: Dict[str, Any]) -> None:
        row = {k: props.get(k) for k in FIELDS}
        row["geom"] = geom
        self.rows[layer].append(row)

    def flush(self) -> None:
        if os.path.exists(self.gpkg_path):
            os.remove(self.gpkg_path)

        first = True

        for layer, rows in self.rows.items():
            if not rows:
                continue

            df = pd.DataFrame(rows)

            for k in FIELDS:
                if k not in df.columns:
                    df[k] = None
            if "geom" not in df.columns:
                raise RuntimeError(f"Internal error: layer '{layer}' missing geom column")

            df = df[FIELDS + ["geom"]]

            # Write with default geometry column name 'geometry' in the GPKG
            gdf = gpd.GeoDataFrame(df, geometry="geom", crs="EPSG:4326").rename(columns={"geom": "geometry"})
            gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")

            pyogrio.write_dataframe(
                gdf,
                self.gpkg_path,
                layer=layer,
                driver="GPKG",
                append=not first,
            )
            first = False


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract mapping layers from an OSM PBF into a GeoPackage (4 passes).")
    parser.add_argument("pbf", nargs="?", help="Input .osm.pbf file")
    parser.add_argument("out_dir", nargs="?", help="Output directory")
    args = parser.parse_args()

    pbf = args.pbf or pick_file()
    out_dir = args.out_dir or pick_output_dir()

    if not pbf or not out_dir:
        print("Cancelled (no input/output selected).")
        return 1

    ensure_dir(out_dir)
    gpkg_path = os.path.normpath(os.path.join(out_dir, "layers.gpkg"))

    print(f"Input PBF  : {pbf}")
    print(f"Output GPKG: {gpkg_path}\n")

    writer = LayerWriter(gpkg_path)

    print("=== PASS 1/4: POINTS (locations=False) ===")
    points = PointsPass(writer)
    t0 = time.time()
    points.apply_file(pbf, locations=False)
    t1 = time.time()
    print(f"[POINTS] Done in {t1 - t0:,.1f}s. Nodes seen: {points.n_nodes:,}. Features: {sum(points.layer_counts.values()):,}\n")

    print("=== PASS 2/4: LINES (locations=True) ===")
    lines = LinesPass(writer)
    t0 = time.time()
    lines.apply_file(pbf, locations=True)
    t1 = time.time()
    print(f"[LINES] Done in {t1 - t0:,.1f}s. Ways seen: {lines.n_ways:,}. Features: {sum(lines.layer_counts.values()):,}\n")

    print("=== PASS 3/4: ADMIN (locations=True) ===")
    admin = AdminPass(writer)
    t0 = time.time()
    admin.apply_file(pbf, locations=True)
    t1 = time.time()
    print(f"[ADMIN] Done in {t1 - t0:,.1f}s. Relations seen: {admin.n_rels:,}. Features: {sum(admin.layer_counts.values()):,}\n")

    print("=== PASS 4/4: POI AREAS -> CENTROID POINTS (locations=True) ===")
    poi_areas = POIAreasCentroidPass(writer)
    t0 = time.time()
    poi_areas.apply_file(pbf, locations=True, idx="flex_mem")
    t1 = time.time()
    print(f"[POI_AREAS] Done in {t1 - t0:,.1f}s. Areas seen: {poi_areas.n_areas:,}. Features: {sum(poi_areas.layer_counts.values()):,}\n")

    print("=== WRITING GPKG (pyogrio) ===")
    writer.flush()

    run_dedupe(out_dir)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())