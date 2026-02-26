import tkinter as tk
import threading
import pandas as pd
import config
from PIL import Image, ImageTk
import os
from tkinter import filedialog, messagebox

from poi.kml_merge import merge_pois_into_existing_kml
from ui_layout import build_header, build_body
from map_utils import embed_map, make_map_container
from poi.overpass_fetch import fetch_pois
from poi.boundary_draw import draw_bbox, draw_poly, fit_to_area

from shapely.geometry import LineString, Polygon, MultiLineString, box


def points_of_interest(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)

    # Local import to avoid circular imports
    from screens.bbox_screen import bbox_screen

    build_header(
        frame,
        "Points of Interest",
        back_command=lambda: show_screen(bbox_screen),
        photo=photo
    )

    left, right = build_body(frame, config.BG)

    map_container = make_map_container(right)
    map_widget = embed_map(map_container, center=config.DEFAULT_MAP_CENTER, zoom=config.DEFAULT_MAP_ZOOM)

    # ---------------------------
    # POI icon loading (CUSTOM ICONS)
    # ---------------------------
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    POI_ICON_DIR = os.path.join(PROJECT_ROOT, "assets", "poi icons")

    ICON_FILE_BY_TYPE = {
        "Commercial airport": "Airport.png",
        "Mountain": "Mountain.png",
        "Park": "Park.png",
        "Amusement park": "Theme Park.png",
        "Aquarium": "Aquarium.png",
        "Golf course": "icon_042.png",
        "Museum": "Museum.png",
        "Hospital": "Hospital.png",
        "Library": "Library.png",
        "Foreign mission": "icon_042.png",
        "Cinema": "Cinema.png",
        "Body of water": "Body of Water.png",
        "Coastline": "Coastline.png",
        "Zoo": "Zoo.png",
    }

    poi_icons = {}

    def get_poi_icon(type_name: str):
        if type_name in poi_icons:
            return poi_icons[type_name]

        fname = ICON_FILE_BY_TYPE.get(type_name, f"{type_name}.png")
        path = os.path.join(POI_ICON_DIR, fname)

        if not os.path.exists(path):
            print(f"[POI ICON] Missing for '{type_name}': {path}")
            poi_icons[type_name] = None
            return None

        img = Image.open(path).convert("RGBA")
        img = img.resize((28, 28), Image.LANCZOS)
        icon = ImageTk.PhotoImage(img)

        poi_icons[type_name] = icon
        return icon

    # ---------------------------
    # Boundary drawing
    # ---------------------------
    boundary_objs = []

    def _get_boundary_polygon():
        poly_str = getattr(config, "overpass_poly", None)
        if poly_str:
            parts = [p for p in str(poly_str).strip().split() if p]
            coords = []
            for i in range(0, len(parts) - 1, 2):
                lat = float(parts[i])
                lon = float(parts[i + 1])
                coords.append((lon, lat))

            if len(coords) >= 3:
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                return Polygon(coords)

        bb = getattr(config, "bound_box", None) or getattr(config, "saved_bound_box", None)
        if bb:
            south, west, north, east = bb
            return box(west, south, east, north)

        return None

    def _clip_latlon_path_to_boundary(latlon_path, boundary_poly):
        if not boundary_poly:
            return [latlon_path]

        if not latlon_path or len(latlon_path) < 2:
            return []

        line = LineString([(lon, lat) for (lat, lon) in latlon_path])
        clipped = line.intersection(boundary_poly)
        if clipped.is_empty:
            return []

        segments = []

        def add_linestring(ls):
            coords = list(ls.coords)
            if len(coords) >= 2:
                segments.append([(lat, lon) for (lon, lat) in coords])

        if isinstance(clipped, LineString):
            add_linestring(clipped)
        elif isinstance(clipped, MultiLineString):
            for ls in clipped.geoms:
                add_linestring(ls)
        else:
            try:
                for g in clipped.geoms:
                    if isinstance(g, LineString):
                        add_linestring(g)
                    elif isinstance(g, MultiLineString):
                        for ls in g.geoms:
                            add_linestring(ls)
            except Exception:
                pass

        return segments

    def _delete_obj(o):
        try:
            o.delete()
        except Exception:
            pass

    status_top = tk.Label(
        left,
        text="",
        bg=config.BG,
        fg=config.FG,
        font=config.SUBTITLE_FONT,
        anchor="w",
        justify="left",
        wraplength=320,
    )
    status_top.pack(fill="x", padx=10, pady=(10, 6))

    status_lbl = tk.Label(
        left,
        text="Choose a POI type to fetch.",
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT,
        anchor="w",
        justify="left",
        wraplength=320,
    )
    status_lbl.pack(fill="x", padx=10, pady=(0, 10))

    def redraw_boundary():
        nonlocal boundary_objs
        for o in boundary_objs:
            _delete_obj(o)
        boundary_objs = []

        poly = getattr(config, "overpass_poly", None)
        bb = getattr(config, "bound_box", None) or getattr(config, "saved_bound_box", None)

        if poly:
            boundary_objs = draw_poly(map_widget, poly, width=3)
            status_top.config(text="Boundary: polygon")
        elif bb:
            boundary_objs = draw_bbox(map_widget, bb, width=3)
            status_top.config(text="Boundary: bounding box")
        else:
            status_top.config(text="Boundary: (none set)")

        fit_to_area(map_widget)

    # ---------------------------
    # Markers / Paths
    # ---------------------------
    markers_by_type = {}

    marker_label_text = {}   # marker_obj -> "Name"
    marker_label_on = set()  # markers currently showing text

    def toggle_marker_label(marker):
        try:
            if marker in marker_label_on:
                marker.set_text("")
                marker_label_on.remove(marker)
            else:
                marker.set_text(marker_label_text.get(marker, ""))
                marker_label_on.add(marker)
        except Exception:
            pass

    def clear_markers(type_name=None):
        if type_name is None:
            for t in list(markers_by_type.keys()):
                clear_markers(t)
            return

        for m in markers_by_type.get(type_name, []):
            try:
                marker_label_text.pop(m, None)
                marker_label_on.discard(m)
                m.delete()
            except Exception:
                pass
        markers_by_type[type_name] = []

    def plot_df(type_name, df: pd.DataFrame):
        clear_markers(type_name)
        if df is None or df.empty:
            return

        # Coastline: draw line geometry only (no markers)
        if str(type_name).strip().lower() == "coastline" and "Geometry" in df.columns:
            objs = []
            boundary_poly = _get_boundary_polygon()

            for _, row in df.iterrows():
                geom = row.get("Geometry", None)
                if not isinstance(geom, (list, tuple)) or len(geom) < 2:
                    continue

                clipped_segments = _clip_latlon_path_to_boundary(geom, boundary_poly)
                for seg in clipped_segments:
                    if hasattr(map_widget, "set_path"):
                        objs.append(map_widget.set_path(seg, width=3, color="#66ccff"))

            markers_by_type[type_name] = objs
            return

        # Body of water: lines for rivers/streams/canals; markers for lakes/ponds/etc
        if "Kind" in df.columns:
            objs = []
            has_geom = "Geometry" in df.columns
            has_latlon = ("Latitude" in df.columns and "Longitude" in df.columns)
            boundary_poly = _get_boundary_polygon()
            icon = get_poi_icon(type_name)

            for _, row in df.iterrows():
                kind = str(row.get("Kind", "")).strip().lower()
                name = str(row.get("Name", "")).strip()

                # Lines (rivers/streams/canals)
                if has_geom and kind in ("river", "stream", "canal"):
                    geom = row.get("Geometry", None)
                    if isinstance(geom, (list, tuple)) and len(geom) >= 2 and hasattr(map_widget, "set_path"):
                        clipped_segments = _clip_latlon_path_to_boundary(geom, boundary_poly)
                        for seg in clipped_segments:
                            objs.append(map_widget.set_path(seg, width=3, color="#66ccff"))
                    continue

                # Points (lakes/ponds/etc)
                if has_latlon and pd.notna(row.get("Latitude")) and pd.notna(row.get("Longitude")):
                    if kind in ("river", "stream", "canal"):
                        continue

                    lat = float(row["Latitude"])
                    lon = float(row["Longitude"])

                    if hasattr(map_widget, "set_marker"):
                        mk = map_widget.set_marker(
                            lat,
                            lon,
                            text="",  # hidden by default
                            icon=icon,
                            command=toggle_marker_label
                        )
                        marker_label_text[mk] = name
                        objs.append(mk)

            markers_by_type[type_name] = objs
            return

        # Normal POIs (markers)
        if "Name" in df.columns:
            name_s = df["Name"].astype(str).str.strip()
            df = df[name_s.ne("")]
            df = df[~name_s.str.lower().isin({"unnamed", "water (unnamed)", "river/canal (unnamed)"})]

        objs = []
        icon = get_poi_icon(type_name)

        for _, row in df.iterrows():
            name = str(row.get("Name", "")).strip()
            if not name:
                continue
            lat = float(row["Latitude"])
            lon = float(row["Longitude"])

            if hasattr(map_widget, "set_marker"):
                mk = map_widget.set_marker(
                    lat,
                    lon,
                    text="",  # hidden by default
                    icon=icon,
                    command=toggle_marker_label
                )
                marker_label_text[mk] = name
                objs.append(mk)

        markers_by_type[type_name] = objs

    # ---------------------------
    # POI types
    # ---------------------------
    POIS = [
        ("Commercial airport", "aeroway=aerodrome", "Commercial airport"),
        ("Mountain", "natural=peak][prominence>=150", "Mountain"),
        ("Park (major)", [
            "leisure=park][wikidata",
            "leisure=park][wikipedia",
            "landuse=recreation_ground][wikidata",
            "landuse=recreation_ground][wikipedia",
        ], "Park"),
        ("Amusement park", "tourism=theme_park", "Amusement park"),
        ("Aquarium", "tourism=aquarium", "Aquarium"),
        ("Golf course", "leisure=golf_course", "Golf course"),
        ("Museum", "tourism=museum", "Museum"),
        ("Hospital (state)", ["amenity=hospital", "healthcare=hospital"], "Hospital"),
        ("Library", "amenity=library", "Library"),
        ("Foreign mission", ["office=diplomatic][diplomatic=embassy", "office=diplomatic][diplomatic=consulate"], "Foreign mission"),
        ("Cinema", "amenity=cinema", "Cinema"),
        ("Body of water", [
            "natural=water",
            "water=lake",
            "water=pond",
            "water=reservoir",
            "waterway=river",
            "waterway=stream",
            "waterway=canal",
        ], "Body of water"),
        ("Coastline", "natural=coastline", "Coastline"),
        ("Zoo", "tourism=zoo", "Zoo"),
    ]

    # ---------------------------
    # Utility buttons
    # ---------------------------
    btn_row = tk.Frame(left, bg=config.BG)
    btn_row.pack(fill="x", padx=10, pady=(0, 10))

    tk.Button(
        btn_row,
        text="Redraw boundary",
        bg=config.BTN,
        fg=config.FG,
        command=redraw_boundary,
    ).pack(side="left", fill="x", expand=True, padx=(0, 6))

    tk.Button(
        btn_row,
        text="Clear POIs",
        bg=config.BTN,
        fg=config.FG,
        command=lambda: clear_markers(None),
    ).pack(side="left", fill="x", expand=True, padx=(6, 0))

    fetch_all_btn = tk.Button(left, text="FETCH ALL", bg=config.BTN, fg=config.FG)
    fetch_all_btn.pack(fill="x", padx=10, pady=(0, 10))

    # ---------------------------
    # Per-type status + fetch
    # ---------------------------
    grid_frame = tk.Frame(left, bg=config.BG)
    grid_frame.pack(fill="x", padx=10)

    status_by_type = {}

    def fetch_one(osm_filter, tname, status_for_btn):
        def worker():
            try:
                df = fetch_pois(osm_filter, tname, status_for_btn)
                if df is None or df.empty:
                    return

                config.poi_data = getattr(config, "poi_data", {}) or {}
                config.poi_data[tname] = df

                root.after(0, lambda tt=tname, d=df: plot_df(tt, d))
            except Exception as e:
                root.after(0, lambda err=e: status_for_btn.config(text=f"Error: {err}"))

        threading.Thread(target=worker, daemon=True).start()

    def fetch_all():
        fetch_all_btn.config(state="disabled")
        status_lbl.config(text="Fetching all POIs…")

        def worker():
            try:
                for label, osm_filter, tname in POIS:
                    st = status_by_type.get(tname)
                    if not st:
                        continue

                    root.after(0, lambda s=st, l=label: s.config(text=f"Queueing {l}…"))

                    df = fetch_pois(osm_filter, tname, st)
                    if df is None or df.empty:
                        continue

                    config.poi_data = getattr(config, "poi_data", {}) or {}
                    config.poi_data[tname] = df

                    root.after(0, lambda tt=tname, d=df: plot_df(tt, d))

                root.after(0, lambda: status_lbl.config(text="Fetch all complete ✅"))
            finally:
                root.after(0, lambda: fetch_all_btn.config(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    fetch_all_btn.config(command=fetch_all)

    # Make two equal-width columns
    grid_frame.grid_columnconfigure(0, weight=1, uniform="poi_cols")
    grid_frame.grid_columnconfigure(1, weight=1, uniform="poi_cols")

    for i, (label, osm_filter, tname) in enumerate(POIS):
        r = i // 2
        c = i % 2

        cell = tk.Frame(grid_frame, bg=config.BG)
        cell.grid(row=r, column=c, sticky="ew", padx=(0, 6) if c == 0 else (6, 0), pady=4)

        btn = tk.Button(cell, text=f"Fetch {label}", bg=config.BTN, fg=config.FG)
        btn.pack(fill="x")

        st = tk.Label(
            cell,
            text="",
            bg=config.BG,
            fg=config.FG,
            font=config.BODY_FONT,
            anchor="w",
            justify="left",
            wraplength=150,
        )
        st.pack(fill="x", pady=(2, 6))

        status_by_type[tname] = st
        btn.config(command=lambda f=osm_filter, tt=tname, s=st: fetch_one(f, tt, s))

    # ---------------------------
    # Export (BOTTOM)
    # ---------------------------
    export_frame = tk.Frame(left, bg=config.BG)
    export_frame.pack(side="bottom", fill="x", padx=10, pady=10)

    export_kml_btn = tk.Button(
        export_frame,
        text="EXPORT TO REGIONS KML",
        bg=config.BTN,
        fg=config.FG
    )
    export_kml_btn.pack(fill="x")

    def export_to_regions_kml():
        existing = (
            getattr(config, "regions_kml_path", None)
            or getattr(config, "region_kml_path", None)
            or getattr(config, "export_kml_path", None)
            or getattr(config, "kml_path", None)
        )

        if not existing or not os.path.exists(existing):
            existing = filedialog.askopenfilename(
                title="Select the Regions KML to add POIs into",
                filetypes=[("KML files", "*.kml"), ("All files", "*.*")]
            )
            if not existing:
                return

        out = filedialog.asksaveasfilename(
            title="Save merged KML",
            defaultextension=".kml",
            initialfile=os.path.basename(existing),
            filetypes=[("KML files", "*.kml"), ("All files", "*.*")]
        )
        if not out:
            return

        poi_data = getattr(config, "poi_data", {}) or {}
        if not poi_data:
            messagebox.showinfo("No POIs", "No POI data found to export. Fetch some POIs first.")
            return

        export_kml_btn.config(state="disabled")
        status_lbl.config(text="Exporting POIs into Regions KML…")

        def worker():
            try:
                merge_pois_into_existing_kml(
                    existing_kml_path=existing,
                    out_kml_path=out,
                    poi_data=poi_data,
                    icon_file_by_type=ICON_FILE_BY_TYPE,
                    poi_icon_dir=POI_ICON_DIR,
                )
                root.after(0, lambda: status_lbl.config(text="Export complete ✅"))
            except Exception as e:
                root.after(0, lambda: messagebox.showerror("KML Export Failed", str(e)))
                root.after(0, lambda: status_lbl.config(text="Export failed ❌"))
            finally:
                root.after(0, lambda: export_kml_btn.config(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    export_kml_btn.config(command=export_to_regions_kml)

    redraw_boundary()
    return frame
