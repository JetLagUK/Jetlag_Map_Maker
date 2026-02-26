import tkinter as tk
import tkinter.messagebox as messagebox
import threading

import config
from shapely.geometry import shape, Polygon, MultiPolygon, GeometryCollection

from screens.shared.osm_regions import search_osm_regions
from image_loader import load_image


# ------------------------------
# Setup helpers
# ------------------------------
def build_icons(root):
    ICON_SIZE = (28, 28)
    icons = {
        "Train": load_image("train.png", size=ICON_SIZE),
        "Tram": load_image("tram.png", size=ICON_SIZE),
        "Bus": load_image("bus.png", size=ICON_SIZE),
        "Subway": load_image("subway.png", size=ICON_SIZE),
    }
    transparent = tk.PhotoImage(master=root, width=1, height=1)
    return icons, transparent


def init_geo_state():
    if not hasattr(config, "game_areas"):
        config.game_areas = []

    if not hasattr(config, "all_data") or config.all_data is None:
        config.all_data = {"Train": None, "Subway": None, "Tram": None, "Bus": None}

    return {
        "region_geoms": {},                 # osm_key -> shapely geometry
        "combined_geom": {"geom": None},    # shapely
        "combined_shapes": {"objs": []},    # map draw objects
        "hiding_zone_locked": {"locked": False},
    }


# ------------------------------
# Geometry + drawing helpers
# ------------------------------
def make_geom_helpers(map_widget, state, set_status):
    region_geoms = state["region_geoms"]
    combined_geom = state["combined_geom"]
    combined_shapes = state["combined_shapes"]

    def area_key(item):
        return f"{item.get('osm_type')}:{item.get('osm_id')}"

    def delete_shape(obj):
        if obj is None:
            return
        try:
            obj.delete()
        except Exception:
            pass

    def geojson_to_shapely(gj: dict):
        if not gj or "type" not in gj:
            return None
        try:
            return shape(gj)
        except Exception:
            return None

    def clear_combined_draw():
        for obj in combined_shapes["objs"]:
            delete_shape(obj)
        combined_shapes["objs"] = []

    def shapely_to_rings_latlon(geom):
        rings = []

        def add_polygon(poly: Polygon):
            ext = [(lat, lon) for (lon, lat) in list(poly.exterior.coords)]
            rings.append(ext)
            for interior in poly.interiors:
                hole = [(lat, lon) for (lon, lat) in list(interior.coords)]
                rings.append(hole)

        def add_any(g):
            if g is None or g.is_empty:
                return
            if isinstance(g, Polygon):
                add_polygon(g)
            elif isinstance(g, MultiPolygon):
                for p in g.geoms:
                    add_polygon(p)
            elif isinstance(g, GeometryCollection):
                for part in g.geoms:
                    add_any(part)

        add_any(geom)
        return rings

    def draw_combined():
        clear_combined_draw()

        geom = combined_geom["geom"]
        if geom is None or geom.is_empty:
            set_status("Game Area is empty.")
            return

        rings = shapely_to_rings_latlon(geom)
        if not rings:
            set_status("Game Area has no drawable boundary.")
            return

        for ring in rings:
            if ring and ring[0] != ring[-1]:
                ring = ring + [ring[0]]

            if hasattr(map_widget, "set_path"):
                combined_shapes["objs"].append(map_widget.set_path(ring, width=3))
            elif hasattr(map_widget, "set_polygon"):
                combined_shapes["objs"].append(map_widget.set_polygon(ring, border_width=3))

    def zoom_to_bbox(bb):
        south = float(bb[0])
        north = float(bb[1])
        west = float(bb[2])
        east = float(bb[3])

        if hasattr(map_widget, "fit_bounding_box"):
            map_widget.fit_bounding_box((north, west), (south, east))
        else:
            map_widget.set_position((south + north) / 2, (west + east) / 2)
            map_widget.set_zoom(config.DEFAULT_MAP_ZOOM)

    def geom_to_overpass_poly(g):
        """
        Overpass expects: poly:"lat lon lat lon ..."
        Uses exteriors only (holes ignored).
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
        coords = list(biggest.exterior.coords)

        parts = []
        for (lon, lat) in coords:
            parts.append(f"{lat} {lon}")

        return " ".join(parts)

    return {
        "area_key": area_key,
        "geojson_to_shapely": geojson_to_shapely,
        "draw_combined": draw_combined,
        "zoom_to_bbox": zoom_to_bbox,
        "geom_to_overpass_poly": geom_to_overpass_poly,
        "region_geoms": region_geoms,
        "combined_geom": combined_geom,
    }


# ------------------------------
# UI builders (search + dropdown)
# ------------------------------
def build_search_ui(left):
    header = tk.Label(left, text="Search OSM Region", bg=config.BG, fg=config.FG, font=config.SUBTITLE_FONT)
    header.grid(row=0, column=0, columnspan=2, pady=(10, 8), padx=10)

    query_entry = tk.Entry(left)
    query_entry.grid(row=1, column=0, sticky="ew", padx=(10, 6))

    search_btn = tk.Button(left, text="Search", bg=config.BTN, fg=config.FG, width=10)
    search_btn.grid(row=1, column=1, sticky="ew", padx=(6, 10))

    status_lbl = tk.Label(
        left, text="", bg=config.BG, fg=config.FG, font=config.BODY_FONT,
        wraplength=320, justify="left"
    )
    status_lbl.grid(row=2, column=0, columnspan=2, sticky="w", padx=10, pady=(6, 6))

    selected_lbl = tk.Label(
        left, text="", bg=config.BG, fg=config.FG, font=config.BODY_FONT,
        wraplength=320, justify="left"
    )
    selected_lbl.grid(row=3, column=0, columnspan=2, sticky="w", padx=10, pady=(0, 8))

    results_panel = tk.Frame(left, bg=config.BG, bd=1, relief="solid")

    results_title = tk.Label(results_panel, text="Results", bg=config.BG, fg=config.FG, font=config.BODY_FONT)
    results_title.grid(row=0, column=0, columnspan=3, sticky="w", padx=8, pady=(6, 4))

    results_canvas = tk.Canvas(results_panel, bg=config.BG, highlightthickness=0, height=240)
    results_scroll = tk.Scrollbar(results_panel, orient="vertical", command=results_canvas.yview)
    results_canvas.configure(yscrollcommand=results_scroll.set)

    results_canvas.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=(6, 0), pady=(0, 6))
    results_scroll.grid(row=1, column=2, sticky="ns", padx=(0, 6), pady=(0, 6))

    results_panel.grid_columnconfigure(0, weight=1)
    results_panel.grid_rowconfigure(1, weight=1)

    results_rows = tk.Frame(results_canvas, bg=config.BG)
    results_window = results_canvas.create_window((0, 0), window=results_rows, anchor="nw")

    def on_results_configure(_evt=None):
        results_canvas.configure(scrollregion=results_canvas.bbox("all"))
        results_canvas.itemconfigure(results_window, width=results_canvas.winfo_width())

    results_rows.bind("<Configure>", on_results_configure)
    results_canvas.bind("<Configure>", on_results_configure)

    left.grid_columnconfigure(0, weight=1)
    left.grid_columnconfigure(1, weight=0)

    def hide_results_dropdown():
        results_panel.grid_forget()

    def show_results_dropdown():
        results_panel.grid(row=4, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

    return {
        "query_entry": query_entry,
        "search_btn": search_btn,
        "status_lbl": status_lbl,
        "selected_lbl": selected_lbl,
        "results_panel": results_panel,
        "results_rows": results_rows,
        "show_dropdown": show_results_dropdown,
        "hide_dropdown": hide_results_dropdown,
        "refresh_canvas": on_results_configure,
    }


# ------------------------------
# Search + selection logic
# ------------------------------
def make_search_handlers(root, ui, geom, state, set_hiding_btn):
    query_entry = ui["query_entry"]
    search_btn = ui["search_btn"]
    results_rows = ui["results_rows"]

    area_key = geom["area_key"]
    geojson_to_shapely = geom["geojson_to_shapely"]
    draw_combined = geom["draw_combined"]
    zoom_to_bbox = geom["zoom_to_bbox"]
    region_geoms = geom["region_geoms"]
    combined_geom = geom["combined_geom"]

    def set_status(text):
        ui["status_lbl"].config(text=text)

    def is_selected(item):
        k = area_key(item)
        return any(area_key(a) == k for a in config.game_areas)

    def update_selected_summary():
        n = len(config.game_areas)
        if n == 0:
            ui["selected_lbl"].config(text="Game Area: (none selected)")
            set_hiding_btn.config(state="disabled")
            return

        names = [a.get("display_name", "Unnamed") for a in config.game_areas[:3]]
        extra = "" if n <= 3 else f" (+{n - 3} more)"
        ui["selected_lbl"].config(text=f"Game Area: {n} selected — " + "; ".join(names) + extra)

        if not state["hiding_zone_locked"]["locked"]:
            set_hiding_btn.config(state="normal")

    def add_to_game_area(item):
        k = area_key(item)

        if k not in region_geoms:
            g = geojson_to_shapely(item.get("geojson") or {})
            if g is None:
                set_status("No polygon for this region — can't add as an area.")
                return
            region_geoms[k] = g

        if not is_selected(item):
            config.game_areas.append({
                "display_name": item.get("display_name"),
                "osm_type": item.get("osm_type"),
                "osm_id": item.get("osm_id"),
                "class": item.get("class"),
                "type": item.get("type"),
                "boundingbox": item.get("boundingbox"),
                "geojson": item.get("geojson"),
            })

        g = region_geoms[k]
        if combined_geom["geom"] is None or combined_geom["geom"].is_empty:
            combined_geom["geom"] = g
        else:
            combined_geom["geom"] = combined_geom["geom"].union(g)

        bb = item.get("boundingbox")
        if bb:
            zoom_to_bbox(bb)

        draw_combined()
        update_selected_summary()

    def remove_from_game_area(item):
        k = area_key(item)

        if k not in region_geoms:
            g = geojson_to_shapely(item.get("geojson") or {})
            if g is None:
                set_status("No polygon for this region — can't subtract it.")
                return
            region_geoms[k] = g

        if combined_geom["geom"] is None or combined_geom["geom"].is_empty:
            set_status("Nothing selected yet — remove did nothing.")
            return

        g = region_geoms[k]
        combined_geom["geom"] = combined_geom["geom"].difference(g)

        config.game_areas = [a for a in config.game_areas if area_key(a) != k]

        draw_combined()
        update_selected_summary()

    def populate_results(items):
        for w in results_rows.winfo_children():
            w.destroy()

        if not items:
            tk.Label(
                results_rows, text="No results.",
                bg=config.BG, fg=config.FG, font=config.BODY_FONT, anchor="w"
            ).grid(row=0, column=0, sticky="ew", padx=6, pady=6)
            ui["show_dropdown"]()
            ui["refresh_canvas"]()
            return

        for r, it in enumerate(items):
            name = it.get("display_name", "Unnamed")
            label_text = f"{it.get('class','?')}/{it.get('type','?')} — {name}"

            row = tk.Frame(results_rows, bg=config.BG)
            row.grid(row=r, column=0, sticky="ew", pady=2)
            row.grid_columnconfigure(0, weight=1, minsize=220)
            row.grid_columnconfigure(1, weight=0)
            row.grid_columnconfigure(2, weight=0)

            tk.Label(
                row,
                text=label_text,
                bg=config.BG,
                fg=config.FG,
                font=config.BODY_FONT,
                wraplength=180,
                justify="left",
                anchor="w"
            ).grid(row=0, column=0, sticky="w", padx=(6, 6), pady=2)

            add_btn = tk.Button(row, text="Add", bg=config.BTN, fg=config.FG, width=6)
            rem_btn = tk.Button(row, text="Remove", bg=config.BTN, fg=config.FG, width=8)

            add_btn.grid(row=0, column=1, padx=(0, 6))
            rem_btn.grid(row=0, column=2, padx=(0, 6))

            def refresh_buttons(item=it):
                selected = is_selected(item)
                add_btn.config(state="disabled" if selected else "normal")
                rem_btn.config(state="normal" if selected else "disabled")

            def on_add(item=it):
                set_status("Adding region…")
                add_to_game_area(item)
                set_status(f"Added ✅  ({len(config.game_areas)} in game area)")
                refresh_buttons(item)

            def on_remove(item=it):
                set_status("Removing region…")
                remove_from_game_area(item)
                set_status(f"Removed ✅  ({len(config.game_areas)} in game area)")
                refresh_buttons(item)

            add_btn.config(command=on_add)
            rem_btn.config(command=on_remove)
            refresh_buttons(it)

        ui["show_dropdown"]()
        ui["refresh_canvas"]()

    def do_search():
        if state["hiding_zone_locked"]["locked"]:
            set_status("Hiding Zone is already set — search is locked.")
            return

        q = query_entry.get().strip()
        if not q:
            messagebox.showerror("Search", "Type a place/region name first.")
            return

        set_status("Searching…")
        search_btn.config(state="disabled")

        for w in results_rows.winfo_children():
            w.destroy()

        tk.Label(
            results_rows, text="Searching…",
            bg=config.BG, fg=config.FG, font=config.BODY_FONT, anchor="w"
        ).grid(row=0, column=0, sticky="ew", padx=6, pady=6)

        ui["show_dropdown"]()
        ui["refresh_canvas"]()

        def worker():
            try:
                items = search_osm_regions(q, limit=12)
                items_sorted = sorted(items, key=lambda it: 0 if it.get("class") in ("boundary", "place") else 1)
                root.after(0, lambda: populate_results(items_sorted))
                root.after(0, lambda: set_status(
                    f"Found {len(items_sorted)} results. Use Add / Remove to update game area."
                ))
            except Exception as e:
                err = str(e)
                root.after(0, lambda: populate_results([]))
                root.after(0, lambda err=err: set_status(f"Search failed: {err}"))
            finally:
                root.after(0, lambda: search_btn.config(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    return {
        "do_search": do_search,
        "populate_results": populate_results,
        "update_selected_summary": update_selected_summary,
        "set_status": set_status,
    }

