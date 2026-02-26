import threading
import queue
import random
import time
import socket

import pandas as pd
import overpy
from typing import Callable, Optional
import config

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


def _area_clause_from_config():
    """
    Returns an Overpass area clause string:
      - poly:"lat lon lat lon ..."   (preferred)
      - (south,west,north,east)      (bbox)
      - None                         (no area known)
    """
    poly = getattr(config, "overpass_poly", None)
    if poly:
        # Overpass wants: (poly:"lat lon ...")
        return f'(poly:"{poly}")'

    bb = getattr(config, "bound_box", None)
    if bb:
        south, west, north, east = bb
        return f"({south},{west},{north},{east})"

    return None


def _parse_lat_lon(text: str):
    s = text.strip().replace(",", " ")
    parts = [p for p in s.split() if p]
    if len(parts) != 2:
        raise ValueError("Expected: lat lon")
    return float(parts[0]), float(parts[1])


def _save_bounding_box(point1_entry, point2_entry):
    """
    Read bounding box from two Entry-like objects and store in config.bound_box.
    Accepts 'lat, lon' OR 'lat lon'
    """
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


from urllib.parse import urlparse

def _short_host(url: str) -> str:
    try:
        return urlparse(url).netloc or url
    except Exception:
        return url

def fetch_osm_data(osm_filter, type_name, progress_cb: Optional[Callable[[str], None]], point1_entry, point2_entry):
    """
    Fetch OSM data using Overpass.

    - Accepts osm_filter as string OR list/tuple of strings
    - Uses nwr (nodes + ways + relations)
    - Uses out center (so ways/relations get a point)
    - Tk-safe via progress_cb
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

    # Area
    area_clause = _area_clause_from_config()
    if area_clause is None:
        if not _save_bounding_box(point1_entry, point2_entry):
            say("No area set. Set a hiding zone or enter a bounding box.")
            return None
        area_clause = _area_clause_from_config()

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
                if is_bus:
                    print(f"\n[BUS DEBUG] Mirror: {host}")
                    print(f"[BUS DEBUG] Nodes: {len(getattr(result, 'nodes', []))}")
                    print(f"[BUS DEBUG] Ways:  {len(getattr(result, 'ways', []))}")
                    print(f"[BUS DEBUG] Rels:  {len(getattr(result, 'relations', []))}")
                # DEBUG counters (super helpful)
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
                    say(f"No {type_name} found in area.")
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
   