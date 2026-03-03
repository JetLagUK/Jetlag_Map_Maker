from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, List, Tuple, Dict

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, box
from shapely.ops import unary_union

import config

# Your existing overpass fetch
from poi.overpass_fetcher import fetch_osm_data  # <-- rename to your actual module

# Your exact layer names (from osm_extract_common)
from osm_extract_common import POINT_LAYER_NAMES


TYPE_TO_LAYER: Dict[str, str] = {
    "Bus": "points_bus_stops",
    "Tram": "points_tram_stops",
    "Subway": "points_subway_stops",
    "Train": "points_train_stations",
    "Parks": "poi_parks",
    "Mountains": "poi_mountains",
    "Hospitals": "poi_hospitals",
    "Foreign Missions": "poi_foreign_missions",
    "Cinemas": "poi_cinemas",
    "Bodies of Water": "poi_bodies_of_water",
    "Amusement Parks": "poi_amusement_parks",
    "Aquariums": "poi_aquariums",
    "Libraries": "poi_libraries",
    "Golf Courses": "poi_golf_courses",
    "Museums": "poi_museums",
}


def _aoi_from_config():
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


def _datasets_covering_aoi(aoi) -> List[Path]:
    """
    Return out_dir folders whose coverage.geojson intersects AOI.
    """
    base = Path(getattr(config, "LOCAL_DATA_DIR", "local_data_outputs"))
    if not base.exists():
        return []

    out_dirs = []
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
            geoms.extend([g for g in gdf.geometry if g is not None])
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


def _local_fetch_from_gpkg(gpkg: Path, layer: str, aoi) -> pd.DataFrame:
    # bbox prefilter for speed
    minx, miny, maxx, maxy = aoi.bounds
    gdf = gpd.read_file(gpkg, layer=layer, bbox=(minx, miny, maxx, maxy))
    if gdf.empty or "geometry" not in gdf:
        return pd.DataFrame(columns=["Name", "Type", "Latitude", "Longitude"])

    # clip
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
    # GeometryCollection etc: ignore for now
    return []


def fetch_features_hybrid(
    osm_filter,
    type_name: str,
    progress_cb: Optional[Callable[[str], None]],
    point1_entry,
    point2_entry,
):
    """
    Local + missing-overpass router.
    """
    def say(msg: str):
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    aoi = _aoi_from_config()
    if aoi is None:
        # no area => let your existing code prompt for bbox etc
        return fetch_osm_data(osm_filter, type_name, progress_cb, point1_entry, point2_entry)

    # Which datasets overlap this AOI?
    dirs = _datasets_covering_aoi(aoi)
    cov_union = _coverage_union(dirs)
    missing = aoi if cov_union is None else aoi.difference(cov_union)
    clauses = _missing_area_clauses(missing)

    # Local fetch (merge across all intersecting datasets)
    layer = TYPE_TO_LAYER.get(type_name)
    local_parts = []

    if layer and layer in POINT_LAYER_NAMES and dirs:
        for d in dirs:
            gpkg = _pick_gpkg(d)
            if not gpkg:
                continue
            dfp = _local_fetch_from_gpkg(gpkg, layer, aoi)
            if not dfp.empty:
                local_parts.append(dfp)

    local_df = pd.concat(local_parts, ignore_index=True) if local_parts else pd.DataFrame(columns=["Name","Latitude","Longitude"])
    if not local_df.empty:
        local_df["Type"] = type_name
        local_df = local_df[["Name", "Type", "Latitude", "Longitude"]]
        say(f"Local: {len(local_df)} {type_name} from {len(dirs)} dataset(s).")
    else:
        say("Local: 0 hits (or no matching local layer).")

    # If no missing area -> stop here (THIS is the bit you’re missing right now)
    if not clauses:
        say("Coverage: AOI fully covered locally — skipping Overpass.")
        return local_df if not local_df.empty else None

    # Too many polygons? fall back to one overpass call for AOI
    if len(clauses) > 8:
        say(f"Coverage: missing area fragmented ({len(clauses)} parts) — using single Overpass call for AOI.")
        over_df = fetch_osm_data(osm_filter, type_name, progress_cb, point1_entry, point2_entry)
        return _merge_points(local_df, over_df)

    # Overpass only for missing polygons
    say(f"Coverage: fetching missing area from Overpass ({len(clauses)} part(s))…")
    over_parts = []
    for c in clauses:
        df = fetch_osm_data(osm_filter, type_name, progress_cb, point1_entry, point2_entry, area_clause_override=c)
        if df is not None and not df.empty:
            over_parts.append(df)

    over_df = pd.concat(over_parts, ignore_index=True) if over_parts else None
    return _merge_points(local_df, over_df)


def _merge_points(local_df: pd.DataFrame, over_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if over_df is None or over_df.empty:
        return local_df if not local_df.empty else None
    if local_df is None or local_df.empty:
        return over_df

    merged = pd.concat([local_df, over_df], ignore_index=True)

    # light dedupe: round coords to ~10m (good enough to prevent doubles)
    merged["_k"] = (merged["Latitude"].round(4).astype(str) + "|" + merged["Longitude"].round(4).astype(str) + "|" + merged["Name"].fillna(""))
    merged = merged.drop_duplicates("_k").drop(columns=["_k"]).reset_index(drop=True)
    return merged