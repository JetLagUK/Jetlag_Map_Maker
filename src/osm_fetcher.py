import threading
import queue
import random
import time
import socket
from urllib.parse import urlparse
from pathlib import Path
from typing import Callable, Optional, Dict, List, Tuple

import pandas as pd
import overpy
import config

# Local data + coverage
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, box
from shapely.ops import unary_union

# Your extractor constants (layer names)
from screens.shared.osm_extract_common import POINT_LAYER_NAMES


# ---------------------------
# Error helpers
# ---------------------------
def _is_timeout_error(e: Exception) -> bool:
    s = str(e).lower()
    return (
        isinstance(e, TimeoutError)
        or isinstance(e, socket.timeout)
        or "timed out" in s
        or "10060" in s  # WinError 10060 (connect timeout)
    )


def _is_overloaded_error(e: Exception) -> bool:
    s = str(e).lower()
    return (
        "server load too high" in s
        or "too busy" in s
        or "429" in s
        or "rate limit" in s
    )


def _is_bus(type_name: str) -> bool:
    return str(type_name).strip().lower() == "bus"


def _run_with_timeout(func, timeout=8):
    q = queue.Queue()

    def wrapper():
        try:
            q.put(func())
        except Exception as e:
            q.put(e)

    t = threading.Thread(target=wrapper, daemon=True)
    t.start()

    try:
        result = q.get(timeout=timeout)
    except queue.Empty:
        raise TimeoutError("Overpass request timed out")

    if isinstance(result, Exception):
        raise result

    return result


# ---------------------------
# AOI / Overpass clause helpers
# ---------------------------
def _area_clause_from_config():
    """
    Returns an Overpass area clause string:
      - (poly:"lat lon lat lon ...")
      - (south,west,north,east)
      - None
    """
    poly = getattr(config, "overpass_poly", None)
    if poly:
        return f'(poly:"{poly}")'

    bb = getattr(config, "bound_box", None)
    if bb:
        south, west, north, east = bb
        return f"({south},{west},{north},{east})"

    return None


def _aoi_geom_from_config():
    """
    AOI as shapely geometry in EPSG:4326.
    - config.overpass_poly is "lat lon lat lon ..."
    - config.bound_box is [south, west, north, east]
    """
    poly = getattr(config, "overpass_poly", None)
    if poly:
        parts = [p for p in str(poly).replace(",", " ").split() if p]
        if len(parts) >= 6 and len(parts) % 2 == 0:
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


def _polygon_to_overpass_poly(p: Polygon) -> str:
    coords = list(p.exterior.coords)
    parts = [f"{y} {x}" for (x, y) in coords]
    return f'(poly:"{" ".join(parts)}")'


def _missing_area_clauses(missing) -> List[str]:
    if missing is None or missing.is_empty:
        return []
    if isinstance(missing, Polygon):
        return [_polygon_to_overpass_poly(missing)]
    if isinstance(missing, MultiPolygon):
        return [_polygon_to_overpass_poly(p) for p in missing.geoms]
    return []


# ---------------------------
# Coverage / local dataset selection
# ---------------------------
def _short_host(url: str) -> str:
    try:
        return urlparse(url).netloc or url
    except Exception:
        return url


def _local_base_dir() -> Path:
    # prefer your config.LOCAL_DATA_DIR if present
    base = getattr(config, "LOCAL_DATA_DIR", None)
    if base:
        return Path(base)
    return Path("local_data_outputs")


def _datasets_intersecting_aoi(aoi) -> List[Path]:
    """
    Return list of local dataset folders whose coverage.geojson intersects AOI.
    Each dataset folder is expected to contain:
      - coverage.geojson
      - layers_clean.gpkg or layers.gpkg
    """
    base = _local_base_dir()
    if not base.exists():
        return []

    out_dirs: List[Path] = []
    for cov_path in base.rglob("coverage.geojson"):
        try:
            gdf = gpd.read_file(cov_path)
            if gdf.empty:
                continue
            cov_geom = unary_union([g for g in gdf.geometry if g is not None])
            if cov_geom is None or cov_geom.is_empty:
                continue
            if cov_geom.intersects(aoi):
                out_dirs.append(cov_path.parent)
        except Exception:
            continue

    return out_dirs


def _coverage_union(out_dirs: List[Path]):
    geoms = []
    for d in out_dirs:
        cov = d / "coverage.geojson"
        try:
            gdf = gpd.read_file(cov)
            for g in gdf.geometry:
                if g is not None:
                    geoms.append(g)
        except Exception:
            pass
    return unary_union(geoms) if geoms else None


def _pick_gpkg(out_dir: Path) -> Optional[Path]:
    clean = out_dir / "layers_clean.gpkg"
    raw = out_dir / "layers.gpkg"
    if clean.exists():
        return clean
    if raw.exists():
        return raw
    return None


# ---------------------------
# Local read for transit layers
# ---------------------------
TYPE_TO_LAYER: Dict[str, str] = {
    "Bus": "points_bus_stops",
    "Tram": "points_tram_stops",
    "Subway": "points_subway_stops",
    "Train": "points_train_stations",
}


def _local_fetch_points(gpkg: Path, layer: str, aoi) -> pd.DataFrame:
    minx, miny, maxx, maxy = aoi.bounds
    try:
        gdf = gpd.read_file(gpkg, layer=layer, bbox=(minx, miny, maxx, maxy))
    except Exception:
        return pd.DataFrame(columns=["Name", "Type", "Latitude", "Longitude"])

    if gdf is None or gdf.empty or "geometry" not in gdf:
        return pd.DataFrame(columns=["Name", "Type", "Latitude", "Longitude"])

    # clip precisely
    try:
        gdf = gdf[gdf.geometry.intersects(aoi)]
    except Exception:
        pass

    if gdf.empty:
        return pd.DataFrame(columns=["Name", "Type", "Latitude", "Longitude"])

    names = gdf["name"] if "name" in gdf.columns else pd.Series(["Unnamed"] * len(gdf))
    cent = gdf.geometry.centroid

    return pd.DataFrame({
        "Name": names.fillna("Unnamed").astype(str),
        "Latitude": cent.y.astype(float),
        "Longitude": cent.x.astype(float),
    })


def _merge_points(local_df: Optional[pd.DataFrame], over_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if local_df is None or local_df.empty:
        return over_df if over_df is not None and not over_df.empty else None
    if over_df is None or over_df.empty:
        return local_df

    merged = pd.concat([local_df, over_df], ignore_index=True)

    # light dedupe: ~10m grid + name
    merged["_k"] = (
        merged["Latitude"].round(4).astype(str)
        + "|"
        + merged["Longitude"].round(4).astype(str)
        + "|"
        + merged["Name"].fillna("").astype(str)
        + "|"
        + merged["Type"].fillna("").astype(str)
    )
    merged = merged.drop_duplicates("_k").drop(columns=["_k"]).reset_index(drop=True)
    return merged


# ---------------------------
# Your existing bbox entry parsing
# ---------------------------
def _parse_lat_lon(text: str):
    s = text.strip().replace(",", " ")
    parts = [p for p in s.split() if p]
    if len(parts) != 2:
        raise ValueError("Expected: lat lon")
    return float(parts[0]), float(parts[1])


def _save_bounding_box(point1_entry, point2_entry):
    try:
        lat1, lon1 = _parse_lat_lon(point1_entry.get())
        lat2, lon2 = _parse_lat_lon(point2_entry.get())

        config.bound_box = [
            min(lat1, lat2),  # south
            min(lon1, lon2),  # west
            max(lat1, lat2),  # north
            max(lon1, lon2),  # east
        ]
        return True
    except Exception:
        return False


# ---------------------------
# MAIN: fetch_osm_data (now coverage-aware)
# ---------------------------
def fetch_osm_data(osm_filter, type_name, progress_cb, point1_entry, point2_entry, area_clause_override=None):
    """
    Transit stops fetcher (Bus/Tram/Subway/Train):
      - If AOI is fully covered by saved local coverages:
            -> read from local gpkg(s) only, skip Overpass
      - If partially covered:
            -> local for AOI + Overpass only for missing polygons
      - If no coverages intersect:
            -> behave exactly like before (Overpass full AOI)

    area_clause_override is used internally when querying missing polygons.
    """
    def say(msg: str):
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    # Ensure config stores exist
    if not hasattr(config, "all_data") or config.all_data is None:
        config.all_data = {"Train": None, "Subway": None, "Tram": None, "Bus": None}

    # Ensure AOI exists (keeps your old bbox entry behavior)
    if _area_clause_from_config() is None and area_clause_override is None:
        if not _save_bounding_box(point1_entry, point2_entry):
            say("No area set. Set a hiding zone or enter a bounding box.")
            return None

    # If we are being asked to run over a specific area clause (missing polygon),
    # do NOT re-run hybrid logic here (avoid recursion / loops).
    if area_clause_override is None:
        aoi = _aoi_geom_from_config()
        if aoi is not None:
            # Determine local datasets that intersect AOI
            dirs = _datasets_intersecting_aoi(aoi)
            cov_union = _coverage_union(dirs) if dirs else None
            missing = aoi if cov_union is None else aoi.difference(cov_union)
            clauses = _missing_area_clauses(missing)

            # Local fetch (merge across all intersecting datasets)
            layer = TYPE_TO_LAYER.get(type_name)
            local_df = None

            if layer and layer in POINT_LAYER_NAMES and dirs:
                parts = []
                for d in dirs:
                    gpkg = _pick_gpkg(d)
                    if not gpkg:
                        continue
                    dfp = _local_fetch_points(gpkg, layer, aoi)
                    if not dfp.empty:
                        parts.append(dfp)
                if parts:
                    local_df = pd.concat(parts, ignore_index=True)
                    local_df["Type"] = type_name
                    local_df = local_df[["Name", "Type", "Latitude", "Longitude"]]
                    say(f"Local: {len(local_df)} {type_name} from {len(dirs)} dataset(s).")
                else:
                    say("Local: 0 hits (or no matching local layer).")

            # Fully covered -> skip overpass
            if dirs and not clauses:
                say("Coverage: AOI fully covered locally — skipping Overpass.")
                return local_df if local_df is not None and not local_df.empty else None

            # Partial coverage -> run overpass for missing polygons only
            if dirs and clauses:
                # prevent too many polygon calls
                if len(clauses) > 8:
                    say(f"Coverage: missing area fragmented ({len(clauses)} parts) — using one Overpass call for full AOI.")
                    over_df = _fetch_overpass_full(osm_filter, type_name, progress_cb, point1_entry, point2_entry)
                    return _merge_points(local_df, over_df)

                say(f"Coverage: fetching missing area from Overpass ({len(clauses)} part(s))…")
                over_parts = []
                for c in clauses:
                    df = fetch_osm_data(osm_filter, type_name, progress_cb, point1_entry, point2_entry, area_clause_override=c)
                    if df is not None and not df.empty:
                        over_parts.append(df)

                over_df = pd.concat(over_parts, ignore_index=True) if over_parts else None
                return _merge_points(local_df, over_df)

            # No local coverage intersecting -> fall through to old behaviour

    # Old behaviour (Overpass for area_clause_override OR full AOI)
    return _fetch_overpass(osm_filter, type_name, progress_cb, point1_entry, point2_entry, area_clause_override=area_clause_override)


def _fetch_overpass_full(osm_filter, type_name, progress_cb, point1_entry, point2_entry):
    # Full AOI from config
    return _fetch_overpass(osm_filter, type_name, progress_cb, point1_entry, point2_entry, area_clause_override=None)


def _fetch_overpass(osm_filter, type_name, progress_cb, point1_entry, point2_entry, area_clause_override=None):
    def say(msg: str):
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    # Area clause
    area_clause = area_clause_override or _area_clause_from_config()
    if area_clause is None:
        say("No area set (missing polygon/bounding box).")
        return None

    # Filters (support list)
    filters = osm_filter if isinstance(osm_filter, (list, tuple)) else [osm_filter]

    # Build query
    blocks = []
    for f in filters:
        blocks.append(f"nwr[{f}]{area_clause};")

    query = f"""
    [out:json][timeout:50];
    (
      {''.join(blocks)}
    );
    out center;
    """

    is_bus = _is_bus(type_name)
    if is_bus:
        print("\n================ BUS FETCH DEBUG ================")
        print("Area clause:", area_clause)
        print("Filters:", filters)
        print("Overpass query:")
        print(query)
        print("=================================================\n")

    mirrors = list(getattr(config, "overpass_mirrors", []))
    if not mirrors:
        say("No Overpass mirrors configured.")
        return None

    random.shuffle(mirrors)

    last_error = None
    dead = set()
    backoffs = [1.5, 3.0, 5.0]
    backoff_i = 0

    for round_i in range(2):
        for url in mirrors:
            if url in dead:
                continue

            host = _short_host(url)
            try:
                say(f"Trying {host}...")

                api = overpy.Overpass(url=url)
                result = _run_with_timeout(lambda: api.query(query), timeout=15)

                say(f"{host}: nodes={len(getattr(result,'nodes',[]))} ways={len(getattr(result,'ways',[]))} rels={len(getattr(result,'relations',[]))}")

                rows = []

                # Nodes
                for n in getattr(result, "nodes", []):
                    rows.append({
                        "Name": n.tags.get("name", "Unnamed"),
                        "Type": type_name,
                        "Latitude": float(n.lat),
                        "Longitude": float(n.lon),
                    })

                # Ways (center)
                for w in getattr(result, "ways", []):
                    lat = getattr(w, "center_lat", None)
                    lon = getattr(w, "center_lon", None)
                    if lat is None or lon is None:
                        continue
                    rows.append({
                        "Name": w.tags.get("name", "Unnamed"),
                        "Type": type_name,
                        "Latitude": float(lat),
                        "Longitude": float(lon),
                    })

                # Relations (center)
                for r in getattr(result, "relations", []):
                    lat = getattr(r, "center_lat", None)
                    lon = getattr(r, "center_lon", None)
                    if lat is None or lon is None:
                        continue
                    rows.append({
                        "Name": r.tags.get("name", "Unnamed"),
                        "Type": type_name,
                        "Latitude": float(lat),
                        "Longitude": float(lon),
                    })

                if not rows:
                    return None

                df = pd.DataFrame(rows)
                say(f"Fetched {len(df)} {type_name} points.")
                return df

            except Exception as e:
                last_error = e

                if _is_overloaded_error(e):
                    say(f"{host}: Server load too high (backing off...)")
                    time.sleep(backoffs[min(backoff_i, len(backoffs) - 1)])
                    backoff_i += 1
                    continue

                if _is_timeout_error(e):
                    say(f"Timeout on {host}")
                    dead.add(url)
                    continue

                say(f"Error on {host}: {e}")
                dead.add(url)
                continue

        say(f"Retrying mirrors (round {round_i + 2}/2)...")

    say(f"Failed to fetch {type_name} (all servers). Last error: {last_error}")
    return None