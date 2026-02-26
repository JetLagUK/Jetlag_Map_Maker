import tkinter as tk
import tkinter.filedialog as filedialog
import tkinter.messagebox as messagebox

import config
from ui_layout import build_header, build_body
from map_utils import embed_map, make_map_container

from shapely.geometry import Polygon, MultiPolygon, GeometryCollection
from shapely.ops import unary_union

from screens.shared.game_area_section import build_game_area_section
from image_loader import load_image

import xml.etree.ElementTree as ET


def kml_screen(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)

    # Import here to avoid circular imports
    from screens.main_menu import main_menu

    build_header(
        frame,
        "Custom KML Polygon",
        back_command=lambda: show_screen(main_menu),
        photo=photo
    )

    left, right = build_body(frame, config.BG)

    map_container = make_map_container(right)
    map_widget = embed_map(map_container, center=config.DEFAULT_MAP_CENTER, zoom=config.DEFAULT_MAP_ZOOM)

    # Ensure transport store exists too (needed by shared section)
    if not hasattr(config, "all_data") or config.all_data is None:
        config.all_data = {"Train": None, "Subway": None, "Tram": None, "Bus": None}

    # ------------------------------
    # Shared icons (same as bbox screen)
    # ------------------------------
    ICON_SIZE = (28, 28)
    icons = {
        "Train": load_image("train.png", size=ICON_SIZE),
        "Tram": load_image("tram.png", size=ICON_SIZE),
        "Bus": load_image("bus.png", size=ICON_SIZE),
        "Subway": load_image("subway.png", size=ICON_SIZE),
    }
    TRANSPARENT_ICON = tk.PhotoImage(master=root, width=1, height=1)

    # ------------------------------
    # State
    # ------------------------------
    kml_state = {
        "geom": None,          # shapely geometry
        "shape_objs": [],      # map draw objects
        "locked": False,
    }

    def _set_status(text: str):
        status_lbl.config(text=text)

    def _delete_shape(obj):
        if obj is None:
            return
        try:
            obj.delete()
        except Exception:
            pass

    def _clear_draw():
        for obj in kml_state["shape_objs"]:
            _delete_shape(obj)
        kml_state["shape_objs"] = []

    def _geom_to_overpass_poly(g):
        """
        Overpass expects poly:"lat lon lat lon ..."
        Uses the largest exterior ring if multiple polygons exist.
        """
        if g is None or g.is_empty:
            return None

        polys = []
        if isinstance(g, Polygon):
            polys = [g]
        elif isinstance(g, MultiPolygon):
            polys = list(g.geoms)
        elif isinstance(g, GeometryCollection):
            for part in g.geoms:
                if isinstance(part, Polygon):
                    polys.append(part)
                elif isinstance(part, MultiPolygon):
                    polys.extend(list(part.geoms))

        if not polys:
            return None

        biggest = max(polys, key=lambda p: p.area)
        coords = list(biggest.exterior.coords)  # (lon, lat)

        parts = []
        for lon, lat in coords:
            parts.append(f"{lat} {lon}")
        return " ".join(parts)

    def _draw_geom(g):
        _clear_draw()
        if g is None or g.is_empty:
            return

        # Draw exterior(s) + holes if present
        rings = []

        def add_polygon(p: Polygon):
            rings.append([(lat, lon) for lon, lat in list(p.exterior.coords)])
            for interior in p.interiors:
                rings.append([(lat, lon) for lon, lat in list(interior.coords)])

        if isinstance(g, Polygon):
            add_polygon(g)
        elif isinstance(g, MultiPolygon):
            for p in g.geoms:
                add_polygon(p)
        elif isinstance(g, GeometryCollection):
            for part in g.geoms:
                if isinstance(part, Polygon):
                    add_polygon(part)
                elif isinstance(part, MultiPolygon):
                    for p in part.geoms:
                        add_polygon(p)

        for ring in rings:
            if ring and ring[0] != ring[-1]:
                ring = ring + [ring[0]]

            if hasattr(map_widget, "set_path"):
                kml_state["shape_objs"].append(map_widget.set_path(ring, width=3))
            elif hasattr(map_widget, "set_polygon"):
                kml_state["shape_objs"].append(map_widget.set_polygon(ring, border_width=3))

    def _fit_to_geom(g):
        if g is None or g.is_empty:
            return
        minx, miny, maxx, maxy = g.bounds  # west, south, east, north

        if hasattr(map_widget, "fit_bounding_box"):
            map_widget.fit_bounding_box((maxy, minx), (miny, maxx))  # (north, west), (south, east)
        else:
            map_widget.set_position((miny + maxy) / 2, (minx + maxx) / 2)
            map_widget.set_zoom(config.DEFAULT_MAP_ZOOM)

    def _parse_kml_file(path: str):
        """
        Minimal KML polygon parser:
        - Finds all <coordinates> under <Polygon> boundaries
        - Builds shapely Polygons from outer rings
        - Unions them into one geometry
        """
        tree = ET.parse(path)
        root_el = tree.getroot()

        # Handle namespaces (KML often has them)
        def strip_ns(tag):
            return tag.split("}", 1)[-1] if "}" in tag else tag

        polygons = []

        # Find any element named "Polygon" and get its outer boundary coordinates
        for poly_el in root_el.iter():
            if strip_ns(poly_el.tag) != "Polygon":
                continue

            # outerBoundaryIs/LinearRing/coordinates
            coords_texts = []
            for child in poly_el.iter():
                if strip_ns(child.tag) == "coordinates" and child.text:
                    coords_texts.append(child.text.strip())

            # A Polygon may contain multiple coordinate blocks (outer + inner rings).
            # We'll treat the FIRST as outer ring (good enough for this use).
            if not coords_texts:
                continue

            outer_text = coords_texts[0]
            pts = []
            for token in outer_text.replace("\n", " ").split():
                # token format: lon,lat or lon,lat,alt
                parts = token.split(",")
                if len(parts) < 2:
                    continue
                lon = float(parts[0])
                lat = float(parts[1])
                pts.append((lon, lat))

            if len(pts) >= 4:
                try:
                    polygons.append(Polygon(pts))
                except Exception:
                    pass

        if not polygons:
            return None

        # Union all polygons into one geometry
        try:
            return unary_union(polygons)
        except Exception:
            return polygons[0]

    # ------------------------------
    # UI
    # ------------------------------
    tk.Label(
        left,
        text="Load a KML Boundary",
        bg=config.BG,
        fg=config.FG,
        font=config.SUBTITLE_FONT
    ).grid(row=0, column=0, columnspan=2, pady=(10, 8), padx=10)

    status_lbl = tk.Label(
        left,
        text="No KML loaded yet.",
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT,
        wraplength=320,
        justify="left"
    )
    status_lbl.grid(row=1, column=0, columnspan=2, sticky="w", padx=10, pady=(0, 8))

    def load_kml():
        if kml_state["locked"]:
            _set_status("Hiding Zone already set — KML loading locked.")
            return

        path = filedialog.askopenfilename(
            title="Select KML file",
            filetypes=[("KML files", "*.kml"), ("All files", "*.*")]
        )
        if not path:
            return

        try:
            g = _parse_kml_file(path)
            if g is None or g.is_empty:
                messagebox.showerror("KML", "No usable Polygon found in that KML.")
                return

            kml_state["geom"] = g
            config.kml_geom = g  # optional: store for later use

            _draw_geom(g)
            _fit_to_geom(g)

            set_hiding_btn.config(state="normal")

            _set_status(f"Loaded KML ✅  Boundary ready. ({path.split('/')[-1]})")
        except Exception as e:
            messagebox.showerror("KML", f"Failed to load KML:\n{e}")

    load_btn = tk.Button(
        left,
        text="Load KML",
        bg=config.BTN,
        fg=config.FG,
        width=22,
        command=load_kml
    )
    load_btn.grid(row=2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

    def set_hiding_zone():
        g = kml_state["geom"]
        if g is None or g.is_empty:
            _set_status("No KML boundary loaded yet.")
            return

        # Lock
        kml_state["locked"] = True
        load_btn.config(state="disabled")
        set_hiding_btn.config(state="disabled")

        # Store bbox + poly for Overpass fetching
        minx, miny, maxx, maxy = g.bounds
        config.bound_box = [miny, minx, maxy, maxx]
        config.overpass_poly = _geom_to_overpass_poly(g)

        _set_status("Hiding Zone set ✅  KML boundary locked.")

    set_hiding_btn = tk.Button(
        left,
        text="Set Hiding Zone",
        bg=config.BTN,
        fg=config.FG,
        width=22,
        state="disabled",
        command=set_hiding_zone
    )
    set_hiding_btn.grid(row=3, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

    # ------------------------------
    # Shared tools section below
    # ------------------------------
    def go_next():
        from screens.points_of_interest import points_of_interest
        show_screen(points_of_interest)

    build_game_area_section(
        left=left,
        root=root,
        map_widget=map_widget,
        icons=icons,
        transparent_icon=TRANSPARENT_ICON,
        point1_entry=None,
        point2_entry=None,
        go_next_callback=go_next,
        start_row=4,  # directly below "Set Hiding Zone"
    )

    left.grid_columnconfigure(0, weight=1)
    left.grid_columnconfigure(1, weight=1)

    return frame
