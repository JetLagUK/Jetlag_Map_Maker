# shared/coverage_utils.py
from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Optional
import urllib.request

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon


def poly_url_from_pbf_url(pbf_url: str) -> str:
    # common Geofabrik pattern
    # https://download.geofabrik.de/europe/scotland-latest.osm.pbf -> .../scotland.poly
    url = pbf_url
    url = url.replace("-latest.osm.pbf", ".poly")
    url = url.replace(".osm.pbf", ".poly")
    return url


def download_text(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as r:
        data = r.read()
    out_path.write_bytes(data)


def parse_geofabrik_poly(poly_text: str) -> Polygon | MultiPolygon:
    """
    Minimal parser for Geofabrik .poly:
    - supports one or more rings
    - ignores holes for now unless you want to extend it
    """
    lines = [ln.strip() for ln in poly_text.splitlines() if ln.strip()]
    # First line is name; then rings begin with an id; ring ends with "END"; file ends with final "END"
    rings: List[List[Tuple[float, float]]] = []
    cur: List[Tuple[float, float]] = []
    in_ring = False

    for ln in lines[1:]:
        if ln.upper() == "END":
            if in_ring:
                if len(cur) >= 3:
                    # close ring
                    if cur[0] != cur[-1]:
                        cur.append(cur[0])
                    rings.append(cur)
                cur = []
                in_ring = False
            else:
                break
            continue

        # ring header (e.g. "1" or "!2")
        if not in_ring and (ln[0].isdigit() or ln[0] == "!"):
            in_ring = True
            continue

        if in_ring:
            parts = ln.replace(",", " ").split()
            if len(parts) >= 2:
                lon = float(parts[0])
                lat = float(parts[1])
                cur.append((lon, lat))

    if not rings:
        raise ValueError("No rings found in .poly")

    # If multiple rings, treat as MultiPolygon of separate outers (simple + safe)
    polys = [Polygon(r) for r in rings]
    if len(polys) == 1:
        return polys[0]
    return MultiPolygon(polys)


def save_coverage_geojson(out_dir: Path, geom, region_id: str, region_name: str) -> Path:
    cov_path = out_dir / "coverage.geojson"
    gdf = gpd.GeoDataFrame(
        [{"region_id": region_id, "region_name": region_name}],
        geometry=[geom],
        crs="EPSG:4326",
    )
    gdf.to_file(cov_path, driver="GeoJSON")
    return cov_path
