import tkinter as tk
import math
import tkinter.messagebox as messagebox

def parse_lat_lon(text: str):
    s = text.strip().replace(",", " ")
    parts = [p for p in s.split() if p]
    if len(parts) != 2:
        raise ValueError("Expected: lat lon")
    return float(parts[0]), float(parts[1])

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def circle_points(lat, lon, radius_m, segments=36):
    if radius_m <= 0:
        return []
    lat = float(lat)
    lon = float(lon)

    m_per_deg_lat = 111_320.0
    m_per_deg_lon = 111_320.0 * math.cos(math.radians(lat))
    if m_per_deg_lon == 0:
        return []

    dlat = radius_m / m_per_deg_lat
    dlon = radius_m / m_per_deg_lon

    pts = []
    for i in range(segments + 1):
        a = 2 * math.pi * i / segments
        pts.append((lat + dlat * math.sin(a), lon + dlon * math.cos(a)))
    return pts

def draw_hiding_zone(map_widget, lat, lon, radius_m):
    if radius_m <= 200:
        seg = 18
    elif radius_m <= 600:
        seg = 24
    else:
        seg = 32

    pts = circle_points(lat, lon, radius_m, segments=seg)
    if not pts:
        return None

    if hasattr(map_widget, "set_path"):
        return map_widget.set_path(pts, width=1)
    elif hasattr(map_widget, "set_polygon"):
        return map_widget.set_polygon(pts, border_width=1)
    return None

def build_hiding_zones_ui(*, left, root, map_widget, config, row=6):
    hide_zone_shapes = []
    hide_zone_data = []

    def clear_hiding_zones():
        for obj in hide_zone_shapes:
            try:
                obj.delete()
            except Exception:
                pass
        hide_zone_shapes.clear()
        hide_zone_data.clear()

    use_smaller_zones_var = tk.BooleanVar(master=root, value=False)
    large_zone_radius_var = tk.IntVar(master=root, value=500)
    small_zone_radius_var = tk.IntVar(master=root, value=200)
    distance_from_city_km_var = tk.IntVar(master=root, value=3)

    smaller_zone_entries = []

    zones_frame = tk.Frame(left, bg=config.BG)
    zones_frame.grid(row=row, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))
    zones_frame.grid_columnconfigure(0, weight=1)

    smaller_frame = tk.Frame(zones_frame, bg=config.BG)
    smaller_frame.grid_columnconfigure(0, weight=1)

    def toggle_smaller_ui():
        if use_smaller_zones_var.get():
            smaller_frame.grid(row=3, column=0, columnspan=2, sticky="ew")
        else:
            smaller_frame.grid_remove()

    tk.Checkbutton(
        zones_frame,
        text="Use smaller zones",
        variable=use_smaller_zones_var,
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT,          # ✅ add this
        selectcolor=config.BG,
        activebackground=config.BG,
        activeforeground=config.FG,
        command=toggle_smaller_ui
    ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 6))

    tk.Label(zones_frame, text="Large zone radius (m):", bg=config.BG, fg=config.FG, font=config.BODY_FONT)\
        .grid(row=1, column=0, sticky="w")

    large_val_lbl = tk.Label(zones_frame, text=f"{large_zone_radius_var.get()} m", bg=config.BG, fg=config.FG, width=7, anchor="e")
    large_val_lbl.grid(row=1, column=1, sticky="e")

    tk.Scale(
        zones_frame, from_=0, to=2000, resolution=100, orient="horizontal",
        variable=large_zone_radius_var, length=240, showvalue=False,
        bg=config.BG, fg=config.FG, highlightthickness=0, troughcolor="#2A3B57",
        command=lambda val: large_val_lbl.config(text=f"{int(float(val))} m")
    ).grid(row=2, column=0, columnspan=2, sticky="ew", pady=(2, 8))

    smaller_frame.grid_remove()

    tk.Label(smaller_frame, text="Small zone radius (m):", bg=config.BG, fg=config.FG, font=config.BODY_FONT)\
        .grid(row=0, column=0, sticky="w")
    small_val_lbl = tk.Label(smaller_frame, text=f"{small_zone_radius_var.get()} m", bg=config.BG, fg=config.FG, width=7, anchor="e")
    small_val_lbl.grid(row=0, column=1, sticky="e")

    tk.Scale(
        smaller_frame, from_=0, to=1000, resolution=50, orient="horizontal",
        variable=small_zone_radius_var, length=240, showvalue=False,
        bg=config.BG, fg=config.FG, highlightthickness=0, troughcolor="#2A3B57",
        command=lambda val: small_val_lbl.config(text=f"{int(float(val))} m")
    ).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(2, 8))

    points_container = tk.Frame(smaller_frame, bg=config.BG)
    points_container.grid(row=2, column=0, columnspan=2, sticky="ew")
    points_container.grid_columnconfigure(0, weight=1)

    def add_smaller_point_row():
        idx = len(smaller_zone_entries) + 1
        rowf = tk.Frame(points_container, bg=config.BG)
        rowf.grid(row=idx - 1, column=0, sticky="ew", pady=2)
        rowf.grid_columnconfigure(1, weight=1)

        tk.Label(rowf, text=f"Smaller zones point {idx}", bg=config.BG, fg=config.FG, width=18, anchor="w")\
            .grid(row=0, column=0, sticky="w")

        entry = tk.Entry(rowf)
        entry.grid(row=0, column=1, sticky="ew", padx=(6, 6))

        tk.Button(rowf, text="Add another", bg=config.BTN, fg=config.FG, command=add_smaller_point_row)\
            .grid(row=0, column=2, sticky="e")

        smaller_zone_entries.append(entry)

    add_smaller_point_row()

    tk.Label(smaller_frame, text="Distance from City (km):", bg=config.BG, fg=config.FG, font=config.BODY_FONT)\
        .grid(row=3, column=0, sticky="w", pady=(8, 0))

    dist_val_lbl = tk.Label(smaller_frame, text=f"{distance_from_city_km_var.get()} km", bg=config.BG, fg=config.FG, width=7, anchor="e")
    dist_val_lbl.grid(row=3, column=1, sticky="e", pady=(8, 0))

    tk.Scale(
        smaller_frame, from_=1, to=10, resolution=1, orient="horizontal",
        variable=distance_from_city_km_var, length=240, showvalue=False,
        bg=config.BG, fg=config.FG, highlightthickness=0, troughcolor="#2A3B57",
        command=lambda val: dist_val_lbl.config(text=f"{int(float(val))} km")
    ).grid(row=4, column=0, columnspan=2, sticky="ew", pady=(2, 8))

    def create_hiding_zones():
        clear_hiding_zones()

        large_r = int(large_zone_radius_var.get())
        if large_r <= 0:
            return

        use_small = bool(use_smaller_zones_var.get())
        small_r = int(small_zone_radius_var.get()) if use_small else 0
        dist_limit_m = int(distance_from_city_km_var.get()) * 1000 if use_small else 0

        city_points = []
        if use_small:
            for ent in smaller_zone_entries:
                txt = ent.get().strip()
                if not txt:
                    continue
                try:
                    city_points.append(parse_lat_lon(txt))
                except Exception:
                    messagebox.showerror(
                        "Smaller zones point",
                        "One of your smaller-zones points is invalid.\nUse:\n55.7, -3.9\nor\n55.7 -3.9"
                    )
                    return

        for t in ("Train", "Subway", "Tram", "Bus"):
            df = config.all_data.get(t)
            if df is None or df.empty:
                continue

            for row in df.itertuples(index=False):
                lat = float(row.Latitude)
                lon = float(row.Longitude)

                radius = large_r
                if use_small and city_points and small_r > 0 and dist_limit_m > 0:
                    for clat, clon in city_points:
                        if haversine_m(lat, lon, clat, clon) <= dist_limit_m:
                            radius = small_r
                            break

                obj = draw_hiding_zone(map_widget, lat, lon, radius)
                if obj is not None:
                    hide_zone_shapes.append(obj)
                    hide_zone_data.append((lat, lon, radius))

    tk.Button(zones_frame, text="Generate zones", bg=config.BTN, fg=config.FG, width=18, command=create_hiding_zones)\
        .grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 0))

    toggle_smaller_ui()

    return {
        "hide_zone_data": hide_zone_data,
        "clear_hiding_zones": clear_hiding_zones,
        "circle_points": circle_points,  # exported for KML
    }
