# poi/coverage_router.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Callable, List, Tuple

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, box
from shapely.ops import unary_union

import config


def _aoi_geom_from_config():
    # config.overpass_poly is "lat lon lat lon ..."
    poly = getattr(config, "overpass_poly", None)
    if poly:
        parts = [p for p in str(poly).replace(",", " ").split() if p]
        coords = []
        for i in range(0, len(parts), 2):
            lat = float(parts[i])
            lon = float(parts[i + 1])
            coords.append((lon, lat))
        if coords and coords[0] != coords[-1]:
            coords.append(coords[0])
        return Polygon(coords)

    bb = getattr(config, "bound_box", None)
    if bb:
        south, west, north, east = bb
        return box(west, south, east, north)

    return None


def _load_all_coverages() -> Optional[Polygon | MultiPolygon]:
    base = Path(getattr(config, "LOCAL_DATA_DIR", "local_data_outputs"))
    if not base.exists():
        return None

    cov_files = list(base.rglob("coverage.geojson"))
    if not cov_files:
        return None

    geoms = []
    for f in cov_files:
        try:
            gdf = gpd.read_file(f)
            if not gdf.empty:
                geoms.extend([g for g in gdf.geometry if g is not None])
        except Exception:
            continue

    if not geoms:
        return None

    return unary_union(geoms)


def compute_missing_area():
    """
    Returns:
      (aoi_geom, coverage_union, missing_geom)
    missing_geom may be empty or None.
    """
    aoi = _aoi_geom_from_config()
    if aoi is None:
        return None, None, None

    cov = _load_all_coverages()
    if cov is None:
        return aoi, None, aoi  # no local coverage at all

    missing = aoi.difference(cov)
    if missing.is_empty:
        return aoi, cov, None

    return aoi, cov, missing
