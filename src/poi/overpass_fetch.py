import threading
import queue
import random
import pandas as pd
import overpy
import time
import socket
import config
from pathlib import Path
from typing import Optional, List, Dict

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, box
from shapely.ops import unary_union

from screens.shared.osm_extract_common import POINT_LAYER_NAMES, LINE_LAYER_NAMES

from .utils import clean_name, norm_str, parse_int_tag
from .filters import (
    is_excluded_park,
    is_excluded_golf_course,
    is_non_building_museum,
    is_private_hospital,
    is_excluded_hospital,
    merge_nearby_hospitals,
)

# ============================================================
# Timeout helper
# ============================================================
def _is_timeout_error(e: Exception) -> bool:
    s = str(e).lower()
    return (
        isinstance(e, TimeoutError)
        or isinstance(e, socket.timeout)
        or "timed out" in s
        or "10060" in s
    )

def _is_overload_error(e: Exception) -> bool:
    s = str(e).lower()
    return (
        "server load too high" in s
        or "too busy" in s
        or "rate limit" in s
        or "429" in s
    )

def _is_blocked_or_bad_endpoint(e: Exception) -> bool:
    s = str(e).lower()
    return (
        "status code: 403" in s
        or "status code: 405" in s
        or " 403" in s
        or " 405" in s
        or "forbidden" in s
        or "method not allowed" in s
    )

def run_with_timeout(func, timeout=12):
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


# ============================================================
# AOI / coverage helpers
# ============================================================
def area_clause_from_config():
    poly = getattr(config, "overpass_poly", None)
    if poly:
        return f'(poly:"{poly}")'

    bb = getattr(config, "bound_box", None) or getattr(config, "saved_bound_box", None)
    if bb:
        south, west, north, east = bb
        return f"({south},{west},{north},{east})"

    return None


def _aoi_geom_from_config():
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

    bb = getattr(config, "bound_box", None) or getattr(config, "saved_bound_box", None)
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


def _local_base_dir() -> Path:
    base = getattr(config, "LOCAL_DATA_DIR", None)
    if base:
        return Path(base)
    return Path("local_data_outputs")


def _datasets_intersecting_aoi(aoi) -> List[Path]:
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


# ============================================================
# Local layer mapping (exact names from extractor)
# ============================================================
POI_TYPE_TO_LAYER: Dict[str, str] = {
    "Park": "poi_parks",
    "Mountain": "poi_mountains",
    "Hospital": "poi_hospitals",
    "Foreign mission": "poi_foreign_missions",
    "Cinema": "poi_cinemas",
    "Body of water": "poi_bodies_of_water",
    "Amusement park": "poi_amusement_parks",
    "Aquarium": "poi_aquariums",
    "Library": "poi_libraries",
    "Golf course": "poi_golf_courses",
    "Museum": "poi_museums",
}

# For line-based local layers:
LINE_TYPE_TO_LAYER: Dict[str, str] = {
    "Coastline": "lines_coastline",
    # "Body of water" lines come from these:
    "Rivers": "lines_rivers",
    "Canals": "lines_canals",
    "Streams": "lines_streams",
}


def _local_fetch_points(gpkg: Path, layer: str, aoi) -> gpd.GeoDataFrame:
    minx, miny, maxx, maxy = aoi.bounds
    try:
        gdf = gpd.read_file(gpkg, layer=layer, bbox=(minx, miny, maxx, maxy))
    except Exception:
        return gpd.GeoDataFrame(columns=["name", "geometry"])

    if gdf is None or gdf.empty or "geometry" not in gdf:
        return gpd.GeoDataFrame(columns=["name", "geometry"])

    try:
        gdf = gdf[gdf.geometry.intersects(aoi)]
    except Exception:
        pass

    return gdf


def _local_fetch_lines(gpkg: Path, layer: str, aoi) -> gpd.GeoDataFrame:
    # bbox prefilter
    minx, miny, maxx, maxy = aoi.bounds
    try:
        gdf = gpd.read_file(gpkg, layer=layer, bbox=(minx, miny, maxx, maxy))
    except Exception:
        return gpd.GeoDataFrame(columns=["name", "geometry"])

    if gdf is None or gdf.empty or "geometry" not in gdf:
        return gpd.GeoDataFrame(columns=["name", "geometry"])

    try:
        gdf = gdf[gdf.geometry.intersects(aoi)]
    except Exception:
        pass

    return gdf


def _linestring_to_latlon_list(geom) -> Optional[list]:
    if geom is None:
        return None
    try:
        coords = list(geom.coords)
        # convert (lon,lat) -> (lat,lon)
        pts = [(float(y), float(x)) for (x, y) in coords]
        return pts if len(pts) >= 2 else None
    except Exception:
        return None


# ============================================================
# Main fetch (now coverage-aware)
# ============================================================
def fetch_pois(osm_filter, type_name: str, status_label):
    """
    POI fetcher:
      - If fully covered: local only, skip Overpass
      - If partial: local + Overpass only for missing polygons
      - Else: original Overpass behaviour

    NOTE: this keeps your original Overpass logic almost untouched.
    """
    aoi = _aoi_geom_from_config()
    area_clause = area_clause_from_config()
    if not area_clause or aoi is None:
        status_label.config(text="No area set. Go back and set a boundary first.")
        return None

    mirrors = list(getattr(config, "overpass_mirrors", []))
    if not mirrors:
        status_label.config(text="No Overpass mirrors configured.")
        return None

    random.shuffle(mirrors)

    def short_host(url: str) -> str:
        try:
            return url.split("/")[2] if "://" in url else url
        except Exception:
            return url

    type_key = " ".join(str(type_name).split()).lower()

    # Determine local coverage + missing polygons
    dirs = _datasets_intersecting_aoi(aoi)
    cov_union = _coverage_union(dirs) if dirs else None
    missing = aoi if cov_union is None else aoi.difference(cov_union)
    missing_clauses = _missing_area_clauses(missing)

    # -------------------------
    # Local fetch (if we have intersecting datasets)
    # -------------------------
    local_df = None

    # Special: Coastline (lines)
    if type_key == "coastline" and dirs:
        parts = []
        for d in dirs:
            gpkg = _pick_gpkg(d)
            if not gpkg:
                continue
            layer = "lines_coastline"
            if layer not in LINE_LAYER_NAMES:
                continue

            gdf = _local_fetch_lines(gpkg, layer, aoi)
            if gdf.empty:
                continue

            rows = []
            for _, row in gdf.iterrows():
                pts = _linestring_to_latlon_list(row.get("geometry"))
                if not pts:
                    continue
                rows.append({
                    "Name": "",               # coastline usually unnamed
                    "Type": "Coastline",
                    "Kind": "coastline",
                    "Geometry": pts,
                })
            if rows:
                parts.append(pd.DataFrame(rows))

        if parts:
            local_df = pd.concat(parts, ignore_index=True)
            status_label.config(text=f"Local: {len(local_df)} coastline segments.")
        else:
            status_label.config(text="Local: 0 coastline segments.")

    # Special: Body of water (points + lines)
    is_water = (type_key == "body of water")
    if not is_water:
        f_joined = " ".join(map(str, osm_filter if isinstance(osm_filter, (list, tuple)) else [osm_filter])).lower()
        if "waterway=" in f_joined or "natural=water" in f_joined or "water=" in f_joined:
            is_water = True

    if is_water and dirs:
        # points from poi_bodies_of_water
        point_parts = []
        for d in dirs:
            gpkg = _pick_gpkg(d)
            if not gpkg:
                continue
            layer = "poi_bodies_of_water"
            if layer not in POINT_LAYER_NAMES:
                continue

            gdf = _local_fetch_points(gpkg, layer, aoi)
            if gdf.empty:
                continue

            cent = gdf.geometry.centroid
            rows = []
            for i, row in gdf.iterrows():
                name = clean_name(str(row.get("name") or "").strip())
                if not name:
                    continue

                # derive kind similarly to your overpass water points logic
                natural = norm_str(row.get("natural"))
                water = norm_str(row.get("water"))
                landuse = norm_str(row.get("landuse"))

                kind = "water"
                if landuse == "reservoir" or water == "reservoir":
                    kind = "reservoir"
                elif water in ("lake", "pond"):
                    kind = water
                elif natural == "water":
                    kind = water or "water"

                rows.append({
                    "Name": name,
                    "Type": "Body of water",
                    "Kind": kind,
                    "Latitude": float(cent.loc[i].y),
                    "Longitude": float(cent.loc[i].x),
                })
            if rows:
                point_parts.append(pd.DataFrame(rows))

        df_points = pd.concat(point_parts, ignore_index=True) if point_parts else pd.DataFrame()

        # lines from rivers/canals/streams
        line_parts = []
        for d in dirs:
            gpkg = _pick_gpkg(d)
            if not gpkg:
                continue

            for layer in ("lines_rivers", "lines_canals", "lines_streams"):
                if layer not in LINE_LAYER_NAMES:
                    continue
                gdf = _local_fetch_lines(gpkg, layer, aoi)
                if gdf.empty:
                    continue

                rows = []
                kind = layer.replace("lines_", "")
                for _, row in gdf.iterrows():
                    pts = _linestring_to_latlon_list(row.get("geometry"))
                    if not pts:
                        continue
                    name = clean_name(str(row.get("name") or "").strip())
                    # keep unnamed out (your overpass lines require [name] anyway)
                    if not name:
                        continue
                    rows.append({
                        "Name": name,
                        "Type": "Body of water",
                        "Kind": kind[:-1] if kind.endswith("s") else kind,  # rivers->river etc
                        "Geometry": pts,
                    })
                if rows:
                    line_parts.append(pd.DataFrame(rows))

        df_lines = pd.concat(line_parts, ignore_index=True) if line_parts else pd.DataFrame()

        if df_points.empty and df_lines.empty:
            local_df = None
            status_label.config(text="Local: 0 water features.")
        else:
            if df_points.empty:
                local_df = df_lines
            elif df_lines.empty:
                local_df = df_points
            else:
                local_df = pd.concat([df_points, df_lines], ignore_index=True)
            status_label.config(text=f"Local: {len(local_df)} water features.")

    # Generic POI points (parks, hospitals, etc)
    if local_df is None and dirs:
        # Map type_name to exact local layer name (case-insensitive match)
        tn = " ".join(str(type_name).split()).strip()
        layer = None
        for k, v in POI_TYPE_TO_LAYER.items():
            if k.lower() == tn.lower():
                layer = v
                break

        if layer and layer in POINT_LAYER_NAMES:
            parts = []
            for d in dirs:
                gpkg = _pick_gpkg(d)
                if not gpkg:
                    continue
                gdf = _local_fetch_points(gpkg, layer, aoi)
                if gdf.empty:
                    continue

                cent = gdf.geometry.centroid
                rows = []

                for i, row in gdf.iterrows():
                    name = clean_name(str(row.get("name") or "").strip())

                    # Apply the same “named” requirement you use for generic overpass
                    if not name:
                        continue

                    # Minimal type-specific exclusions that don’t need Overpass tags beyond schema
                    tags = row  # row has schema fields like amenity/leisure/etc

                    if type_key == "park" and is_excluded_park(tags, name):
                        continue
                    if type_key == "golf course" and is_excluded_golf_course(tags, name):
                        continue
                    if type_key == "hospital":
                        if is_private_hospital(tags) or is_excluded_hospital(tags, name):
                            continue
                        beds = parse_int_tag(tags, "beds") or parse_int_tag(tags, "capacity")
                        rows.append({
                            "Name": name,
                            "Type": type_name,
                            "Latitude": float(cent.loc[i].y),
                            "Longitude": float(cent.loc[i].x),
                            "Beds": beds,
                        })
                    else:
                        rows.append({
                            "Name": name,
                            "Type": type_name,
                            "Latitude": float(cent.loc[i].y),
                            "Longitude": float(cent.loc[i].x),
                        })

                if rows:
                    parts.append(pd.DataFrame(rows))

            if parts:
                local_df = pd.concat(parts, ignore_index=True)
                if type_key == "hospital":
                    before = len(local_df)
                    local_df = merge_nearby_hospitals(local_df, radius_m=500.0)
                    merged = before - len(local_df)
                    if merged > 0:
                        status_label.config(text=f"Local: {len(local_df)} hospitals (merged {merged} nearby).")
                    else:
                        status_label.config(text=f"Local: {len(local_df)} hospitals.")
                else:
                    status_label.config(text=f"Local: {len(local_df)} {type_name}.")
            else:
                status_label.config(text=f"Local: 0 {type_name}.")

    # If fully covered -> skip overpass
    if dirs and not missing_clauses:
        # return local if any, else None
        if local_df is not None and not local_df.empty:
            return local_df
        return None

    # If partially covered -> Overpass only for missing polygons
    if dirs and missing_clauses:
        # If too many polygon pieces, fall back to single Overpass call on full AOI for reliability
        if len(missing_clauses) > 8:
            status_label.config(text=f"Coverage partial ({len(missing_clauses)} parts) — using one Overpass call.")
            over_df = _fetch_pois_overpass(osm_filter, type_name, status_label, mirrors, short_host, area_clause)
            return _merge_local_overpass(local_df, over_df)

        status_label.config(text=f"Coverage partial — fetching missing area ({len(missing_clauses)} parts)…")
        over_parts = []
        for c in missing_clauses:
            df = _fetch_pois_overpass(osm_filter, type_name, status_label, mirrors, short_host, c)
            if df is not None and not df.empty:
                over_parts.append(df)
        over_df = pd.concat(over_parts, ignore_index=True) if over_parts else None
        return _merge_local_overpass(local_df, over_df)

    # No intersecting local coverage -> original Overpass behaviour
    return _fetch_pois_overpass(osm_filter, type_name, status_label, mirrors, short_host, area_clause)


def _merge_local_overpass(local_df: Optional[pd.DataFrame], over_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if local_df is None or local_df.empty:
        return over_df if over_df is not None and not over_df.empty else None
    if over_df is None or over_df.empty:
        return local_df

    merged = pd.concat([local_df, over_df], ignore_index=True)

    # Dedup:
    # - For point rows: use rounded lat/lon + name + type
    # - For line rows (Geometry): use name+kind+len
    if "Latitude" in merged.columns and "Longitude" in merged.columns:
        merged["_k"] = (
            merged.get("Latitude", pd.Series()).round(4).astype(str).fillna("")
            + "|"
            + merged.get("Longitude", pd.Series()).round(4).astype(str).fillna("")
            + "|"
            + merged.get("Name", pd.Series()).astype(str).fillna("")
            + "|"
            + merged.get("Type", pd.Series()).astype(str).fillna("")
            + "|"
            + merged.get("Kind", pd.Series()).astype(str).fillna("")
        )
    else:
        merged["_k"] = (
            merged.get("Name", pd.Series()).astype(str).fillna("")
            + "|"
            + merged.get("Type", pd.Series()).astype(str).fillna("")
            + "|"
            + merged.get("Kind", pd.Series()).astype(str).fillna("")
            + "|"
            + merged.get("Geometry", pd.Series()).astype(str).fillna("")
        )

    merged = merged.drop_duplicates("_k").drop(columns=["_k"]).reset_index(drop=True)
    return merged


# ============================================================
# Overpass portion (mostly your original logic, with area clause injected)
# ============================================================
def _fetch_pois_overpass(osm_filter, type_name: str, status_label, mirrors, short_host, area_clause: str):
    filters = osm_filter if isinstance(osm_filter, (list, tuple)) else [osm_filter]
    type_key = " ".join(str(type_name).split()).lower()

    # Water special
    is_water = (type_key == "body of water")
    if not is_water:
        f_joined = " ".join(map(str, filters)).lower()
        if "waterway=" in f_joined or "natural=water" in f_joined or "water=" in f_joined:
            is_water = True

    if is_water:
        df = fetch_body_of_water(area_clause, status_label, mirrors, short_host)
        return df

    if type_key == "coastline":
        return fetch_coastline_lines(area_clause, status_label, mirrors, short_host)

    # Generic POI query (points only using center)
    blocks = []
    for f in filters:
        blocks.append(f'node[{f}]{area_clause};')
        blocks.append(f'way[{f}]{area_clause};')
        blocks.append(f'relation[{f}]{area_clause};')

    query = f"""
    [out:json][timeout:50];
    (
      {"".join(blocks)}
    );
    out center;
    """

    for url in mirrors:
        try:
            status_label.config(text=f"Trying {short_host(url)}...")
            status_label.update_idletasks()

            api = overpy.Overpass(url=url)
            result = run_with_timeout(lambda: api.query(query), timeout=12)

            rows = []

            def maybe_add(name, tags, lat, lon):
                name = clean_name(name)

                # Cinema fallback
                if not name and type_key == "cinema":
                    name = clean_name(
                        tags.get("brand")
                        or tags.get("operator")
                        or tags.get("short_name")
                        or tags.get("name:en")
                        or tags.get("ref")
                    )
                    if not name:
                        name = "Cinema (unnamed)"

                if not name:
                    return

                name_l = norm_str(name)

                if type_key == "foreign mission":
                    if "residence of" in name_l or "ambassador's residence" in name_l:
                        return
                    BAD_KEYWORDS = (
                        "consular section", "consular department", "consulate general",
                        "consulate of", "visa office", "passport", "trade", "commercial",
                        "defence", "defense", "military", "attache", "education section",
                        "cultural", "medical office", "student department",
                        "science & technology", "naval", "delegation of",
                    )
                    if any(k in name_l for k in BAD_KEYWORDS):
                        return
                    ALLOWED_KEYWORDS = (
                        "embassy of", "high commission of", "royal embassy",
                        "delegation of the european union",
                    )
                    if not any(k in name_l for k in ALLOWED_KEYWORDS):
                        return

                if "house of " in name_l and name_l not in ("house of commons", "house of lords"):
                    return
                if "official residence of" in name_l:
                    return

                if type_key == "park" and is_excluded_park(tags, name):
                    return
                if type_key == "golf course" and is_excluded_golf_course(tags, name):
                    return
                if type_key == "museum":
                    if not norm_str(tags.get("building")) and not norm_str(tags.get("building:part")):
                        return
                    if is_non_building_museum(tags, name):
                        return
                if type_key == "hospital":
                    if is_private_hospital(tags):
                        return
                    if is_excluded_hospital(tags, name):
                        return
                    beds = parse_int_tag(tags, "beds") or parse_int_tag(tags, "capacity")
                    rows.append({
                        "Name": name,
                        "Type": type_name,
                        "Latitude": float(lat),
                        "Longitude": float(lon),
                        "Beds": beds,
                    })
                    return

                rows.append({
                    "Name": name,
                    "Type": type_name,
                    "Latitude": float(lat),
                    "Longitude": float(lon),
                })

            for n in result.nodes:
                maybe_add(n.tags.get("name"), n.tags, n.lat, n.lon)

            for w in result.ways:
                lat = getattr(w, "center_lat", None)
                lon = getattr(w, "center_lon", None)
                if lat is None or lon is None:
                    continue
                maybe_add(w.tags.get("name"), w.tags, lat, lon)

            for r in result.relations:
                lat = getattr(r, "center_lat", None)
                lon = getattr(r, "center_lon", None)
                if lat is None or lon is None:
                    continue
                maybe_add(r.tags.get("name"), r.tags, lat, lon)

            if not rows:
                status_label.config(text=f"No named {type_name} found.")
                return None

            df = pd.DataFrame(rows)

            if type_key == "hospital":
                before = len(df)
                df = merge_nearby_hospitals(df, radius_m=500.0)
                merged = before - len(df)
                if merged > 0:
                    status_label.config(text=f"Fetched {len(df)} hospitals (merged {merged} nearby).")

            if "Name" in df.columns and not df.empty:
                df = df[df["Name"].astype(str).str.strip().ne("")]
                df = df[df["Name"].astype(str).str.lower().ne("unnamed")]

            status_label.config(text=f"Fetched {len(df)} named {type_name}.")
            return df

        except TimeoutError:
            status_label.config(text=f"Timeout on {short_host(url)}")
            status_label.update_idletasks()
        except Exception as e:
            status_label.config(text=f"Error on {short_host(url)}: {e}")
            status_label.update_idletasks()

    status_label.config(text=f"Failed to fetch {type_name} (all servers).")
    return None


# ============================================================
# Your existing water/coastline functions (unchanged)
# ============================================================
def fetch_body_of_water(area_clause, status_label, mirrors, short_host):
    df_points = fetch_water_points(area_clause, status_label, mirrors, short_host)
    if df_points is None:
        df_points = pd.DataFrame()

    df_lines = fetch_water_lines(area_clause, status_label, mirrors, short_host)
    if df_lines is None:
        return df_points if not df_points.empty else None

    if df_points.empty:
        return df_lines

    return pd.concat([df_points, df_lines], ignore_index=True)


def fetch_water_points(area_clause, status_label, mirrors, short_host):
    df = None

    q_points = f"""
    [out:json][timeout:80][maxsize:1073741824];
    (
      node[natural=water][name]{area_clause};
      way[natural=water][name]{area_clause};
      relation[natural=water][name]{area_clause};

      node[water~"^(lake|pond|reservoir)$"][name]{area_clause};
      way[water~"^(lake|pond|reservoir)$"][name]{area_clause};
      relation[water~"^(lake|pond|reservoir)$"][name]{area_clause};

      way[landuse=reservoir][name]{area_clause};
      relation[landuse=reservoir][name]{area_clause};
    );
    out body center;
    """

    for url in mirrors:
        try:
            status_label.config(text=f"Trying {short_host(url)} (water points)...")
            status_label.update_idletasks()

            api = overpy.Overpass(url=url)
            res = run_with_timeout(lambda: api.query(q_points), timeout=35)

            rows = []

            def add_point(tags, lat, lon):
                name = clean_name(tags.get("name") or tags.get("name:en"))
                if not name:
                    return

                natural = norm_str(tags.get("natural"))
                water = norm_str(tags.get("water"))
                landuse = norm_str(tags.get("landuse"))

                kind = "water"
                if landuse == "reservoir" or water == "reservoir":
                    kind = "reservoir"
                elif water in ("lake", "pond"):
                    kind = water
                elif natural == "water":
                    kind = water or "water"

                rows.append({
                    "Name": name,
                    "Type": "Body of water",
                    "Kind": kind,
                    "Latitude": float(lat),
                    "Longitude": float(lon),
                })

            for n in res.nodes:
                add_point(n.tags, n.lat, n.lon)

            for w in res.ways:
                lat = getattr(w, "center_lat", None)
                lon = getattr(w, "center_lon", None)
                if lat is None or lon is None:
                    continue
                add_point(w.tags, lat, lon)

            for r in res.relations:
                lat = getattr(r, "center_lat", None)
                lon = getattr(r, "center_lon", None)
                if lat is None or lon is None:
                    continue
                add_point(r.tags, lat, lon)

            if not rows:
                status_label.config(text="No named water points found.")
                return None

            df = pd.DataFrame(rows)
            status_label.config(text=f"Fetched {len(df)} water points.")
            return df

        except TimeoutError:
            status_label.config(text=f"Timeout on {short_host(url)} (water points)")
            status_label.update_idletasks()
        except Exception as e:
            status_label.config(text=f"Error on {short_host(url)} (water points): {e}")
            status_label.update_idletasks()

    return df


def fetch_water_lines(area_clause, status_label, mirrors, short_host):
    # (your existing function unchanged)
    # ... keep your current implementation ...
    df = None
    # NOTE: keep your existing fetch_water_lines body here
    return df


def fetch_coastline_lines(area_clause, status_label, mirrors, short_host):
    # (your existing function unchanged)
    # ... keep your current implementation ...
    df = None
    # NOTE: keep your existing fetch_coastline_lines body here
    return df