import threading
import queue
import random
import pandas as pd
import overpy
import time
import socket
import config

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
        or "10060" in s  # WinError 10060
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
# Area clause helper
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


# ============================================================
# Main fetch
# ============================================================
def fetch_pois(osm_filter, type_name: str, status_label):
    area_clause = area_clause_from_config()
    if not area_clause:
        status_label.config(text="No area set. Go back and set a boundary first.")
        return None

    filters = osm_filter if isinstance(osm_filter, (list, tuple)) else [osm_filter]

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

    # ------------------------------------------------------------
    # Special case: Body of water
    #   - points first (lakes/ponds/reservoir/etc)
    #   - then lines (rivers/streams/canals)
    # ------------------------------------------------------------
    is_water = (type_key == "body of water")
    if not is_water:
        f_joined = " ".join(map(str, filters)).lower()
        if "waterway=" in f_joined or "natural=water" in f_joined or "water=" in f_joined:
            is_water = True

    if is_water:
        df = fetch_body_of_water(area_clause, status_label, mirrors, short_host)
        if df is not None:
            print(f"[FETCH_POIS RETURN] {type_name} -> columns = {list(df.columns)} rows = {len(df)}")
        return df

    # ------------------------------------------------------------
    # Special case: Coastline (lines)
    # ------------------------------------------------------------

    if type_key == "coastline":
        df = fetch_coastline_lines(area_clause, status_label, mirrors, short_host)
        if df is not None:
            print(f"[FETCH_POIS RETURN] {type_name} -> columns = {list(df.columns)} rows = {len(df)}")
        return df    
    # ------------------------------------------------------------
    # Generic POI query (points only using center for ways/relations)
    # ------------------------------------------------------------
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

                # Cinema fallback: OSM often lacks name= for cinemas
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

                # For other types, keep strict: must be named
                if not name:
                    return

                name_l = norm_str(name)

                # Foreign mission cleanup (ONLY for missions)
                if type_key == "foreign mission":
                    if "residence of" in name_l or "ambassador's residence" in name_l:
                        return

                    BAD_KEYWORDS = (
                        "consular section",
                        "consular department",
                        "consulate general",
                        "consulate of",
                        "visa office",
                        "passport",
                        "trade",
                        "commercial",
                        "defence",
                        "defense",
                        "military",
                        "attache",
                        "education section",
                        "cultural",
                        "medical office",
                        "student department",
                        "science & technology",
                        "naval",
                        "delegation of",
                    )
                    if any(k in name_l for k in BAD_KEYWORDS):
                        return

                    ALLOWED_KEYWORDS = (
                        "embassy of",
                        "high commission of",
                        "royal embassy",
                        "delegation of the european union",
                    )
                    if not any(k in name_l for k in ALLOWED_KEYWORDS):
                        return

                # Gameplay name blacklist (all types)
                if "house of " in name_l and name_l not in ("house of commons", "house of lords"):
                    return

                if "official residence of" in name_l:
                    return

                # Type-specific filters
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
            print(f"[FETCH_POIS RETURN] {type_name} -> columns = {list(df.columns)} rows = {len(df)}")
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
# Water fetching (split: points first, then lines)
# ============================================================
def fetch_body_of_water(area_clause, status_label, mirrors, short_host):
    """
    Fetch water in two stages:
      1) named still-water points (lakes/ponds/reservoir/etc)
      2) named moving-water lines (rivers/streams/canals) as WAYS with geom
    If stage (2) fails, return stage (1) results.
    """
    df_points = fetch_water_points(area_clause, status_label, mirrors, short_host)
    if df_points is None:
        df_points = pd.DataFrame()

    df_lines = fetch_water_lines(area_clause, status_label, mirrors, short_host)
    if df_lines is None:
        # If lines fail, still return points if we have any
        return df_points if not df_points.empty else None

    if df_points.empty:
        return df_lines

    return pd.concat([df_points, df_lines], ignore_index=True)


def fetch_water_points(area_clause, status_label, mirrors, short_host):
    df = None  # <-- key fix

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
            print(f"[WATER POINTS ERROR] {short_host(url)}: {e}")

    return df  # will be None unless a mirror succeeded

def fetch_water_lines(area_clause, status_label, mirrors, short_host):
    """
    Rivers/streams/canals as LINES, staged for reliability:
      Stage 1: named rivers + named canals (WAYS only)  ✅ much cheaper
      Stage 2: named streams (WAYS only)               ✅ optional, only if stage 1 worked

    Geometry is rebuilt from returned nodes: (._;>;); out body;
    """
    df = None

    def _fetch_lines_for_kinds(kinds, stage_label, timeout_s):
        """
        kinds: iterable like ("river","canal")
        Returns list[dict] rows or None if mirror loop totally failed.
        Returns [] if query succeeded but produced no rows.
        """
        kinds_re = "|".join(kinds)

        # WAYS ONLY + [name] keeps it sane.
        q = f"""
        [out:json][timeout:80][maxsize:1073741824];
        (
          way[waterway~"^({kinds_re})$"][name]{area_clause};
        );
        (._;>;);
        out body;
        """

        overload_backoffs = [1.5, 3.0, 5.0]
        backoff_i = 0

        # Try mirrors twice: helps when one mirror is momentarily overloaded.
        last_err = None
        for round_i in range(2):
            for url in mirrors:
                host = short_host(url)
                try:
                    print(f"\n[WATER LINES:{stage_label}] Trying mirror: {host}")
                    status_label.config(text=f"Trying {host} ({stage_label})...")
                    status_label.update_idletasks()

                    api = overpy.Overpass(url=url)
                    res = run_with_timeout(lambda: api.query(q), timeout=timeout_s)

                    print(f"[WATER LINES:{stage_label}] Raw result:")
                    print(f"  Ways:  {len(res.ways)}")
                    print(f"  Nodes: {len(res.nodes)}")

                    # Node id -> (lat, lon)
                    node_ll = {}
                    for n in res.nodes:
                        try:
                            node_ll[int(n.id)] = (float(n.lat), float(n.lon))
                        except Exception:
                            continue

                    def way_to_geom(w):
                        pts = []
                        for n in getattr(w, "nodes", []) or []:
                            ll = node_ll.get(int(n.id))
                            if ll:
                                pts.append(ll)
                        return pts if len(pts) >= 2 else None

                    rows = []
                    total = 0
                    ok = 0

                    for w in res.ways:
                        ww = norm_str(w.tags.get("waterway"))
                        if ww not in kinds:
                            continue
                        total += 1

                        geom = way_to_geom(w)
                        if not geom:
                            continue
                        ok += 1

                        name = clean_name(w.tags.get("name") or w.tags.get("name:en")) or ""
                        rows.append({
                            "Name": name,              # named by query constraint, but keep safe
                            "Type": "Body of water",
                            "Kind": ww,
                            "Geometry": geom,
                        })

                    print(f"[WATER LINES:{stage_label}] Ways found: {total}")
                    print(f"[WATER LINES:{stage_label}] Ways with rebuilt geometry: {ok}")
                    print(f"[WATER LINES:{stage_label}] Total segments collected: {len(rows)}")

                    # Success (even if empty)
                    return rows

                except Exception as e:
                    last_err = e

                    if _is_blocked_or_bad_endpoint(e):
                        print(f"[WATER LINES:{stage_label}] 🚫 Blocked/bad endpoint on {host}: {e}")
                        status_label.config(text=f"Blocked on {host} (skipping)")
                        status_label.update_idletasks()
                        continue

                    if _is_overload_error(e):
                        print(f"[WATER LINES:{stage_label}] 💤 Overloaded on {host}: {e}")
                        status_label.config(text=f"{host} overloaded (waiting...)")
                        status_label.update_idletasks()
                        time.sleep(overload_backoffs[min(backoff_i, len(overload_backoffs) - 1)])
                        backoff_i += 1
                        continue

                    if _is_timeout_error(e):
                        print(f"[WATER LINES:{stage_label}] ⏱ Timeout on {host}")
                        status_label.config(text=f"Timeout on {host} ({stage_label})")
                        status_label.update_idletasks()
                        continue

                    msg = str(e).lower()
                    if "unknown content type" in msg and "text/html" in msg:
                        print(f"[WATER LINES:{stage_label}] ⚠ HTML returned by {host} — skipping")
                        status_label.config(text=f"Mirror returned HTML (skipping {host})")
                        status_label.update_idletasks()
                        continue

                    print(f"[WATER LINES:{stage_label}] 💥 Error on {host}: {e}")
                    status_label.config(text=f"Error on {host} ({stage_label}): {e}")
                    status_label.update_idletasks()
                    continue

            print(f"[WATER LINES:{stage_label}] Round {round_i+1}/2 done, retrying mirrors...")

        print(f"[WATER LINES:{stage_label}] ❌ All mirrors failed. Last error: {last_err}")
        return None

    # -------------------------
    # Stage 1: rivers + canals (reliable)
    # -------------------------
    stage1 = _fetch_lines_for_kinds(("river", "canal"), stage_label="water lines (rivers+canals)", timeout_s=25)
    if stage1 is None:
        return None  # mirrors fully failed for stage 1
    if not stage1:
        status_label.config(text="No usable river/canal lines produced.")
        print("[WATER LINES] ❌ Stage 1 produced no usable geometry.")
        return None

    status_label.config(text=f"Fetched {len(stage1)} river/canal line segments. Fetching streams…")
    status_label.update_idletasks()

    # -------------------------
    # Stage 2: streams (optional, only if stage 1 worked)
    # -------------------------
    stage2 = _fetch_lines_for_kinds(("stream",), stage_label="water lines (streams)", timeout_s=30)
    # If streams fail due to mirrors, we still return stage 1.
    if stage2 is None:
        print("[WATER LINES] ⚠ Stage 2 (streams) failed; returning rivers/canals only.")
        df = pd.DataFrame(stage1)
        status_label.config(text=f"Fetched {len(df)} water line segments (streams unavailable).")
        return df

    # Merge stage 1 + stage 2
    rows = stage1 + stage2
    if not rows:
        return None

    df = pd.DataFrame(rows)
    if "Name" in df.columns:
        df["Name"] = df["Name"].astype(str).str.strip()

    status_label.config(text=f"Fetched {len(df)} water line segments (rivers/canals + streams).")
    print(f"[WATER LINES] ✅ Returning {len(df)} line segments (stage1={len(stage1)}, stage2={len(stage2)})")
    return df
def fetch_coastline_lines(area_clause, status_label, mirrors, short_host):
    """
    Coastline as LINES.
    Rebuild geometry from nodes (same approach as water lines).
    Coastlines are usually unnamed, so we do NOT require name.
    """
    df = None

    q = f"""
    [out:json][timeout:80][maxsize:1073741824];
    (
      way[natural=coastline]{area_clause};
      relation[natural=coastline]{area_clause};
    );
    (._;>;);
    out body;
    """

    for url in mirrors:
        try:
            print(f"\n[COASTLINE] Trying mirror: {short_host(url)}")
            status_label.config(text=f"Trying {short_host(url)} (coastline)...")
            status_label.update_idletasks()

            api = overpy.Overpass(url=url)
            res = run_with_timeout(lambda: api.query(q), timeout=45)

            print(f"[COASTLINE] Raw result:")
            print(f"  Relations: {len(res.relations)}")
            print(f"  Ways:      {len(res.ways)}")
            print(f"  Nodes:     {len(res.nodes)}")

            # Node id -> (lat, lon)
            node_ll = {}
            for n in res.nodes:
                try:
                    node_ll[int(n.id)] = (float(n.lat), float(n.lon))
                except Exception:
                    continue

            def way_to_geom(w):
                pts = []
                for n in getattr(w, "nodes", []) or []:
                    ll = node_ll.get(int(n.id))
                    if ll:
                        pts.append(ll)
                return pts if len(pts) >= 2 else None

            rows = []
            total = 0
            ok = 0

            for w in res.ways:
                if norm_str(w.tags.get("natural")) != "coastline":
                    continue

                total += 1
                geom = way_to_geom(w)
                if not geom:
                    continue

                ok += 1
                rows.append({
                    "Name": "",              # usually unnamed
                    "Type": "Coastline",
                    "Kind": "coastline",
                    "Geometry": geom,
                })

            print(f"[COASTLINE] Ways found: {total}")
            print(f"[COASTLINE] Ways with rebuilt geometry: {ok}")
            print(f"[COASTLINE] Total coastline segments collected: {len(rows)}")

            if not rows:
                status_label.config(text="No usable coastline lines produced.")
                print("[COASTLINE] ❌ No usable coastline geometry produced.")
                return None

            df = pd.DataFrame(rows)
            status_label.config(text=f"Fetched {len(df)} coastline segments.")
            print(f"[COASTLINE] ✅ Returning {len(df)} coastline segments")
            return df

        except TimeoutError:
            print(f"[COASTLINE] ⏱ Timeout on {short_host(url)}")
            status_label.config(text=f"Timeout on {short_host(url)} (coastline)")
            status_label.update_idletasks()

        except Exception as e:
            print(f"[COASTLINE] 💥 Error on {short_host(url)}: {e}")
            status_label.config(text=f"Error on {short_host(url)} (coastline): {e}")
            status_label.update_idletasks()

    print("[COASTLINE] ❌ All mirrors failed")
    return df