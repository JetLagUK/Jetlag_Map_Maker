import requests
from typing import Optional

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

def search_osm_regions(query: str, *, limit: int = 10, country_codes: Optional[str] = None):
    """
    Returns a list of dicts from Nominatim search.

    Each item typically includes:
      display_name, type, class, osm_type, osm_id,
      boundingbox [south, north, west, east] as strings,
      geojson (if polygon_geojson=1)
    """
    params = {
        "q": query,
        "format": "jsonv2",
        "limit": str(limit),
        "polygon_geojson": "1",
        "addressdetails": "0",
        # bias to boundary-ish results; still returns useful POIs sometimes, we can filter lightly in UI
    }
    if country_codes:
        params["countrycodes"] = country_codes

    headers = {
        # Important per Nominatim usage policy: identify your app
        "User-Agent": "JetlagMapMaker/1.0 (contact: ryancomet@gmail.com)",
        "Accept": "application/json"
    }

    r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


def geojson_to_latlon_rings(geojson: dict):
    """
    Convert GeoJSON Polygon/MultiPolygon to list of rings in (lat, lon).
    Returns list[list[(lat, lon)]]
    """
    if not geojson or "type" not in geojson:
        return []

    gtype = geojson["type"]
    coords = geojson.get("coordinates", [])

    rings = []

    def ring_lonlat_to_latlon(r):
        # r is [(lon, lat), ...]
        return [(float(lat), float(lon)) for lon, lat in r]

    if gtype == "Polygon":
        # coords: [ outer_ring, hole1, hole2, ...]
        if coords:
            rings.append(ring_lonlat_to_latlon(coords[0]))  # outer ring only
    elif gtype == "MultiPolygon":
        # coords: [ [poly1_rings], [poly2_rings], ... ]
        for poly in coords:
            if poly and poly[0]:
                rings.append(ring_lonlat_to_latlon(poly[0]))  # outer ring only per polygon

    return rings
