# poi/overpass_poly_utils.py
from __future__ import annotations

from typing import List
from shapely.geometry import Polygon, MultiPolygon


def polygon_to_overpass_poly(p: Polygon) -> str:
    # Overpass wants: (poly:"lat lon lat lon ...")
    coords = list(p.exterior.coords)
    parts = []
    for (x, y) in coords:  # x=lon, y=lat
        parts.append(f"{y} {x}")
    return f'(poly:"{" ".join(parts)}")'


def geom_to_area_clauses(geom) -> List[str]:
    """
    Returns a list of Overpass area clause strings.
    For MultiPolygon: one clause per polygon.
    """
    if geom is None:
        return []
    if isinstance(geom, Polygon):
        return [polygon_to_overpass_poly(geom)]
    if isinstance(geom, MultiPolygon):
        return [polygon_to_overpass_poly(p) for p in geom.geoms]
    # If it’s something else (GeometryCollection), just skip for now
    return []
