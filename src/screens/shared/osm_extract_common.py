from __future__ import annotations

import os
import subprocess
import sys
from typing import Any, Dict, Optional, Iterable, Tuple, List

import osmium
from shapely.geometry import Point

# ---- Optional GUI (file pickers) ----
try:
    import tkinter as tk
    from tkinter import filedialog
except Exception:
    tk = None
    filedialog = None


# =========================
# Dedupe runner
# =========================

def run_dedupe(out_dir: str) -> None:
    this_dir = os.path.dirname(os.path.abspath(__file__))
    dedupe_py = os.path.join(this_dir, "Dedupe_Pois.py")

    in_gpkg = os.path.join(out_dir, "layers.gpkg")
    out_gpkg = os.path.join(out_dir, "layers_clean.gpkg")

    if not os.path.exists(dedupe_py):
        print(f"[DEDUPE] Skipped (missing): {dedupe_py}")
        return
    if not os.path.exists(in_gpkg):
        print(f"[DEDUPE] Skipped (missing input): {in_gpkg}")
        return

    print("=== DEDUPE: POIs within 30m (keep most important) ===")
    cmd = [sys.executable, dedupe_py, in_gpkg, out_gpkg, "30"]

    res = subprocess.run(cmd, capture_output=True, text=True)

    print("----- DEDUPE STDOUT -----")
    print(res.stdout)

    print("----- DEDUPE STDERR -----")
    print(res.stderr)

    if res.returncode != 0:
        raise RuntimeError(f"Dedupe failed (exit code {res.returncode})")

    print(f"[DEDUPE] Output: {out_gpkg}\n")


# =========================
# Simple filesystem helpers
# =========================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# =========================
# Tag helpers
# =========================

def tget(tags, key: str) -> Optional[str]:
    try:
        return tags.get(key)
    except Exception:
        return None


def is_truthy(tags, key: str) -> bool:
    v = tget(tags, key)
    if v is None:
        return False
    return v.lower() in ("yes", "true", "1")


def is_named(tags) -> bool:
    n = tget(tags, "name")
    return bool(n and n.strip())


def is_subway_explicit(tags) -> bool:
    return (
        tget(tags, "railway") in ("subway_entrance", "subway")
        or tget(tags, "station") == "subway"
        or is_truthy(tags, "subway")
        or (tget(tags, "public_transport") in ("stop_position", "platform") and is_truthy(tags, "subway"))
    )


def is_train_station(tags) -> bool:
    if tget(tags, "railway") not in ("station", "halt"):
        return False
    return not is_subway_explicit(tags)


def keep_basic_props(tags, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    props: Dict[str, Any] = {}
    for k in ("name", "ref", "operator", "network", "brand", "wikidata", "wikipedia"):
        v = tget(tags, k)
        if v is not None:
            props[k] = v

    for k in (
        "highway", "railway", "public_transport", "amenity", "tourism", "leisure",
        "natural", "waterway", "boundary", "admin_level", "place", "station",
        "subway", "tram", "bus", "train", "diplomatic", "office", "water", "landuse"
    ):
        v = tget(tags, k)
        if v is not None:
            props[k] = v

    if extra:
        props.update(extra)
    return props


def is_inland_water(tags) -> bool:
    # Exclusions (avoid coastal / marine)
    if tget(tags, "natural") in ("bay", "sea"):
        return False
    if tget(tags, "water") in ("sea",):
        return False
    if tget(tags, "place") in ("sea",):
        return False

    natural = tget(tags, "natural")
    water = tget(tags, "water")
    waterway = tget(tags, "waterway")
    landuse = tget(tags, "landuse")

    if natural == "water":
        return True
    if water in ("lake", "pond", "reservoir", "basin"):
        return True
    if landuse == "reservoir":
        return True
    if waterway == "riverbank":
        return True

    return False


# =========================
# Area centroid (no geometry factory)
# =========================

def _ring_to_lonlat(ring: Iterable) -> list[Tuple[float, float]]:
    pts: list[Tuple[float, float]] = []
    for nref in ring:
        try:
            pts.append((float(nref.lon), float(nref.lat)))
        except Exception:
            continue
    return pts


def _ensure_closed(coords: list[Tuple[float, float]]) -> list[Tuple[float, float]]:
    if len(coords) < 3:
        return coords
    if coords[0] != coords[-1]:
        return coords + [coords[0]]
    return coords


def _ring_area_and_centroid(coords: list[Tuple[float, float]]) -> Tuple[float, Optional[Tuple[float, float]]]:
    coords = _ensure_closed(coords)
    if len(coords) < 4:
        return 0.0, None

    area2 = 0.0
    cx = 0.0
    cy = 0.0

    for i in range(len(coords) - 1):
        x0, y0 = coords[i]
        x1, y1 = coords[i + 1]
        cross = x0 * y1 - x1 * y0
        area2 += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross

    if area2 == 0.0:
        return 0.0, None

    cx /= (3.0 * area2)
    cy /= (3.0 * area2)
    return abs(area2 * 0.5), (cx, cy)


def centroid_point_from_area(a: osmium.osm.Area) -> Optional[Point]:
    best_area = 0.0
    best_centroid: Optional[Tuple[float, float]] = None
    try:
        outers = a.outer_rings()
    except Exception:
        return None

    for ring in outers:
        coords = _ring_to_lonlat(ring)
        area, cent = _ring_area_and_centroid(coords)
        if cent and area > best_area:
            best_area = area
            best_centroid = cent

    if not best_centroid:
        return None
    x, y = best_centroid
    return Point(x, y)


# =========================
# Layer names + schema
# =========================

POINT_LAYER_NAMES = [
    "points_bus_stops",
    "points_tram_stops",
    "points_subway_stops",
    "points_train_stations",
    "poi_parks",
    "poi_mountains",
    "poi_hospitals",
    "poi_foreign_missions",
    "poi_cinemas",
    "poi_bodies_of_water",
    "poi_amusement_parks",
    "poi_aquariums",
    "poi_libraries",
    "poi_golf_courses",
    "poi_museums",
]

LINE_LAYER_NAMES = [
    "lines_rivers",
    "lines_canals",
    "lines_streams",
    "lines_coastline",
]

ADMIN_LAYER = "admin_regions"

FIELDS = [
    "osm_id", "osm_type", "name", "ref", "operator", "network", "brand", "wikidata", "wikipedia",
    "highway", "railway", "public_transport", "amenity", "tourism", "leisure",
    "natural", "waterway", "boundary", "admin_level", "place", "station",
    "subway", "tram", "bus", "train", "diplomatic", "office", "water", "landuse"
]


# =========================
# GUI pickers
# =========================

def pick_file() -> str:
    if tk is None or filedialog is None:
        return ""
    root = tk.Tk()
    root.withdraw()
    return filedialog.askopenfilename(
        title="Select OSM PBF file",
        filetypes=[("OSM PBF files", "*.pbf"), ("All files", "*.*")]
    )


def pick_output_dir() -> str:
    if tk is None or filedialog is None:
        return ""
    root = tk.Tk()
    root.withdraw()
    return filedialog.askdirectory(title="Select output folder")
