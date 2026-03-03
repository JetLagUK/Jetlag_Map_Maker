from __future__ import annotations

import json
import time
from typing import Dict, Any

import osmium
from shapely.geometry import shape, Point, LineString

from osm_extract_common import (
    tget, is_truthy, is_named,
    is_subway_explicit, is_train_station,
    keep_basic_props, is_inland_water,
    centroid_point_from_area,
    ADMIN_LAYER,
)


# =========================
# Pass 1: POINTS (locations=False)
# =========================

class PointsPass(osmium.SimpleHandler):
    def __init__(self, writer, print_every: int = 2_000_000) -> None:
        super().__init__()
        self.writer = writer
        self.start_time = time.time()
        self.print_every = print_every
        self.n_nodes = 0
        self.layer_counts: Dict[str, int] = {}

    def _maybe_print(self) -> None:
        if self.n_nodes % self.print_every != 0:
            return
        elapsed = time.time() - self.start_time
        rate = self.n_nodes / elapsed if elapsed else 0.0
        print(f"[POINTS] nodes={self.n_nodes:,} elapsed={elapsed:,.1f}s rate={rate:,.0f} obj/s")

    def _bump(self, layer: str, every: int = 50_000) -> None:
        self.layer_counts[layer] = self.layer_counts.get(layer, 0) + 1
        c = self.layer_counts[layer]
        if c == 1:
            print(f"[POINTS] layer {layer}: started")
        elif c % every == 0:
            print(f"[POINTS] layer {layer}: {c:,} features")

    def node(self, n: osmium.osm.Node) -> None:
        self.n_nodes += 1
        self._maybe_print()

        if not n.tags:
            return
        tags = n.tags

        # fast skip
        if (
            tget(tags, "highway") is None
            and tget(tags, "railway") is None
            and tget(tags, "public_transport") is None
            and tget(tags, "amenity") is None
            and tget(tags, "tourism") is None
            and tget(tags, "leisure") is None
            and tget(tags, "natural") is None
            and tget(tags, "place") is None
            and tget(tags, "boundary") is None
            and tget(tags, "water") is None
            and tget(tags, "diplomatic") is None
            and tget(tags, "office") is None
            and tget(tags, "station") is None
            and tget(tags, "subway") is None
            and tget(tags, "tram") is None
            and tget(tags, "bus") is None
            and tget(tags, "train") is None
            and tget(tags, "landuse") is None
            and tget(tags, "waterway") is None
        ):
            return

        matched: list[str] = []

        # Transport (unnamed allowed)
        if tget(tags, "highway") == "bus_stop" or (
            tget(tags, "public_transport") in ("platform", "stop_position") and is_truthy(tags, "bus")
        ):
            matched.append("points_bus_stops")

        if tget(tags, "railway") == "tram_stop" or tget(tags, "station") == "tram" or (
            tget(tags, "public_transport") in ("platform", "stop_position") and is_truthy(tags, "tram")
        ):
            matched.append("points_tram_stops")

        if is_subway_explicit(tags):
            matched.append("points_subway_stops")

        if is_train_station(tags):
            matched.append("points_train_stations")

        # POIs (named only)
        if (tget(tags, "leisure") == "park" or tget(tags, "boundary") == "national_park") and is_named(tags):
            matched.append("poi_parks")

        if (tget(tags, "natural") == "peak" or tget(tags, "place") == "mountain") and is_named(tags):
            matched.append("poi_mountains")

        if tget(tags, "amenity") == "hospital" and is_named(tags):
            matched.append("poi_hospitals")

        if (
            (tget(tags, "amenity") == "embassy")
            or (tget(tags, "diplomatic") in ("embassy", "consulate"))
            or (tget(tags, "office") == "diplomatic")
            or (tget(tags, "embassy") is not None)
        ):
            if is_named(tags):
                matched.append("poi_foreign_missions")

        if tget(tags, "amenity") == "cinema" and is_named(tags):
            matched.append("poi_cinemas")

        # (you decided not to require name here in your current script)
        if is_inland_water(tags):
            matched.append("poi_bodies_of_water")

        if (tget(tags, "tourism") == "theme_park" or tget(tags, "leisure") == "amusement_park") and is_named(tags):
            matched.append("poi_amusement_parks")

        if tget(tags, "tourism") == "aquarium" and is_named(tags):
            matched.append("poi_aquariums")

        if tget(tags, "amenity") == "library" and is_named(tags):
            matched.append("poi_libraries")

        if tget(tags, "leisure") == "golf_course" and is_named(tags):
            matched.append("poi_golf_courses")

        if tget(tags, "tourism") == "museum" and is_named(tags):
            matched.append("poi_museums")

        if not matched:
            return

        try:
            geom = Point(float(n.location.lon), float(n.location.lat))
        except Exception:
            return

        props = keep_basic_props(tags, extra={"osm_id": int(n.id), "osm_type": "node"})
        for layer in matched:
            self.writer.add(layer, geom, props)
            self._bump(layer)


# =========================
# Pass 2: LINES (locations=True)
# =========================

class LinesPass(osmium.SimpleHandler):
    def __init__(self, writer, print_every: int = 500_000) -> None:
        super().__init__()
        self.writer = writer
        self.start_time = time.time()
        self.print_every = print_every
        self.n_ways = 0
        self.layer_counts: Dict[str, int] = {}

    def _maybe_print(self) -> None:
        if self.n_ways % self.print_every != 0:
            return
        elapsed = time.time() - self.start_time
        rate = self.n_ways / elapsed if elapsed else 0.0
        print(f"[LINES] ways={self.n_ways:,} elapsed={elapsed:,.1f}s rate={rate:,.0f} obj/s")

    def _bump(self, layer: str, every: int = 20_000) -> None:
        self.layer_counts[layer] = self.layer_counts.get(layer, 0) + 1
        c = self.layer_counts[layer]
        if c == 1:
            print(f"[LINES] layer {layer}: started")
        elif c % every == 0:
            print(f"[LINES] layer {layer}: {c:,} features")

    def way(self, w: osmium.osm.Way) -> None:
        self.n_ways += 1
        self._maybe_print()

        if not w.tags:
            return
        tags = w.tags
        waterway = tget(tags, "waterway")
        natural = tget(tags, "natural")

        layer = None
        if waterway == "river":
            layer = "lines_rivers"
        elif waterway == "canal":
            layer = "lines_canals"
        elif waterway == "stream":
            layer = "lines_streams"
        elif natural == "coastline":
            layer = "lines_coastline"

        if not layer:
            return

        coords = []
        for nref in w.nodes:
            try:
                coords.append((float(nref.lon), float(nref.lat)))
            except Exception:
                pass
        if len(coords) < 2:
            return

        geom = LineString(coords)
        props = keep_basic_props(tags, extra={"osm_id": int(w.id), "osm_type": "way"})
        self.writer.add(layer, geom, props)
        self._bump(layer)


# =========================
# Pass 3: ADMIN
# =========================

from osmium.geom import GeoJSONFactory as _GJF
_admin_gjf = _GJF()


class AdminPass(osmium.SimpleHandler):
    def __init__(self, writer, print_every: int = 200_000) -> None:
        super().__init__()
        self.writer = writer
        self.start_time = time.time()
        self.print_every = print_every
        self.n_rels = 0
        self.layer_counts: Dict[str, int] = {}

    def _maybe_print(self) -> None:
        if self.n_rels % self.print_every != 0:
            return
        elapsed = time.time() - self.start_time
        rate = self.n_rels / elapsed if elapsed else 0.0
        print(f"[ADMIN] rels={self.n_rels:,} elapsed={elapsed:,.1f}s rate={rate:,.0f} obj/s")

    def _bump(self, every: int = 2_000) -> None:
        c = self.layer_counts.get(ADMIN_LAYER, 0) + 1
        self.layer_counts[ADMIN_LAYER] = c
        if c == 1:
            print(f"[ADMIN] layer {ADMIN_LAYER}: started")
        elif c % every == 0:
            print(f"[ADMIN] layer {ADMIN_LAYER}: {c:,} features")

    def relation(self, r: osmium.osm.Relation) -> None:
        self.n_rels += 1
        self._maybe_print()

        if not r.tags:
            return
        if tget(r.tags, "boundary") != "administrative":
            return

        try:
            gj = json.loads(_admin_gjf.create_multipolygon(r))
            geom = shape(gj)
        except Exception:
            return

        props = keep_basic_props(
            r.tags,
            extra={"osm_id": int(r.id), "osm_type": "relation", "admin_level": tget(r.tags, "admin_level")},
        )
        self.writer.add(ADMIN_LAYER, geom, props)
        self._bump()


# =========================
# Pass 4: POI AREAS -> CENTROID POINTS
# =========================

class POIAreasCentroidPass(osmium.SimpleHandler):
    def __init__(self, writer, print_every: int = 200_000) -> None:
        super().__init__()
        self.writer = writer
        self.start_time = time.time()
        self.print_every = print_every
        self.n_areas = 0
        self.layer_counts: Dict[str, int] = {}

    def _maybe_print(self) -> None:
        if self.n_areas % self.print_every != 0:
            return
        elapsed = time.time() - self.start_time
        rate = self.n_areas / elapsed if elapsed else 0.0
        print(f"[POI_AREAS] areas={self.n_areas:,} elapsed={elapsed:,.1f}s rate={rate:,.0f} areas/s")

    def _bump(self, layer: str, every: int = 10_000) -> None:
        c = self.layer_counts.get(layer, 0) + 1
        self.layer_counts[layer] = c
        if c == 1:
            print(f"[POI_AREAS] layer {layer}: started")
        elif c % every == 0:
            print(f"[POI_AREAS] layer {layer}: {c:,} features")

    def area(self, a: osmium.osm.Area) -> None:
        self.n_areas += 1
        self._maybe_print()

        if not a.tags:
            return
        tags = a.tags

        if not is_named(tags):
            return

        matched: list[str] = []

        if tget(tags, "leisure") == "park" or tget(tags, "boundary") == "national_park":
            matched.append("poi_parks")
        if tget(tags, "amenity") == "hospital":
            matched.append("poi_hospitals")
        if (
            tget(tags, "amenity") == "embassy"
            or tget(tags, "diplomatic") in ("embassy", "consulate")
            or tget(tags, "office") == "diplomatic"
            or tget(tags, "embassy") is not None
        ):
            matched.append("poi_foreign_missions")
        if tget(tags, "amenity") == "cinema":
            matched.append("poi_cinemas")
        if is_inland_water(tags) and is_named(tags):
            matched.append("poi_bodies_of_water")
        if tget(tags, "tourism") == "theme_park" or tget(tags, "leisure") == "amusement_park":
            matched.append("poi_amusement_parks")
        if tget(tags, "tourism") == "aquarium":
            matched.append("poi_aquariums")
        if tget(tags, "amenity") == "library":
            matched.append("poi_libraries")
        if tget(tags, "leisure") == "golf_course":
            matched.append("poi_golf_courses")
        if tget(tags, "tourism") == "museum":
            matched.append("poi_museums")

        if not matched:
            return

        pt = centroid_point_from_area(a)
        if pt is None:
            return

        props = keep_basic_props(tags, extra={"osm_id": int(a.id), "osm_type": "area"})
        for layer in matched:
            self.writer.add(layer, pt, props)
            self._bump(layer)
