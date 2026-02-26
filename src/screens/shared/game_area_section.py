# game_area_section.py (FIXED)

import tkinter as tk
import tkinter.filedialog as filedialog
import tkinter.messagebox as messagebox  # ✅ FIX: you used messagebox but didn't import it
import threading
import queue
import traceback

import config
from osm_fetcher import fetch_osm_data
from screens.shared.map_markers import MapMarkers
from screens.shared.dedup import deduplicate_all_by_priority
from screens.shared.hiding_zones import build_hiding_zones_ui
from screens.shared.kml_export import export_game_area_kml


class _EntryProxy:
    def __init__(self, which: int):
        self.which = which

    def get(self):
        bb = getattr(config, "bound_box", None)
        if not bb:
            return ""
        south, west, north, east = bb
        if self.which == 1:
            return f"{south}, {west}"
        return f"{north}, {east}"


def _run_in_background(root, work_fn, on_success=None, on_error=None, on_finally=None, poll_ms=60):
    q = queue.Queue()

    def worker():
        try:
            q.put(("ok", work_fn()))
        except Exception as e:
            q.put(("err", (e, traceback.format_exc())))

    threading.Thread(target=worker, daemon=True).start()

    def poll():
        try:
            status, payload = q.get_nowait()
        except queue.Empty:
            root.after(poll_ms, poll)
            return

        if status == "ok":
            if on_success:
                on_success(payload)
        else:
            err, tb = payload
            if on_error:
                on_error(err, tb)
            else:
                messagebox.showerror("Fetch failed", f"{err}\n\n{tb}")

        if on_finally:
            on_finally()

    root.after(poll_ms, poll)


def build_game_area_section(
    *,
    left: tk.Frame,
    root: tk.Tk,
    map_widget,
    icons: dict,
    transparent_icon: tk.PhotoImage,
    point1_entry=None,
    point2_entry=None,
    go_next_callback=None,
    start_row: int = 4,
):
    if point1_entry is None:
        point1_entry = _EntryProxy(1)
    if point2_entry is None:
        point2_entry = _EntryProxy(2)

    base = start_row

    markers = MapMarkers(root=root, map_widget=map_widget, icons=icons, transparent_icon=transparent_icon)

    # ---- Fetch Buttons ----
    fetch_frame = tk.Frame(left, bg=config.BG)
    fetch_frame.grid(row=base + 0, column=0, columnspan=2, pady=15)

    buttons = [
        ("Fetch Train", 'railway=station][station!=subway', "Train"),
        ("Fetch Tram", 'railway=tram_stop', "Tram"),
        ("Fetch Bus", [
            "highway=bus_stop",
            "public_transport=platform",
            "public_transport=stop_position",
        ], "Bus"),
        ("Fetch Subway", 'station=subway', "Subway"),
    ]

    def fetch_and_plot_async(osm_filter, type_name, status_label, btn: tk.Button):
        # UI updates (main thread)
        btn.config(state="disabled")
        status_label.config(text="Fetching...")

        def work():
            # Background thread: DO NOT touch Tk or map_widget here

            # ✅ FIX: fetch_osm_data expects a progress callback, not a Label
            def progress(msg: str):
                root.after(0, lambda m=msg: status_label.config(text=m))

            df = fetch_osm_data(osm_filter, type_name, progress, point1_entry, point2_entry)
            return df

        def success(df):
            # Back on main thread: safe to touch Tk + map
            if df is not None and not df.empty:
                config.all_data[type_name] = df
                markers.plot_points(type_name, df)
                status_label.config(text=f"{len(df)} found")
            else:
                config.all_data[type_name] = df
                markers.clear_markers(type_name)
                status_label.config(text="0 found")

        def error(err, tb):
            status_label.config(text="Fetch failed")
            messagebox.showerror("Fetch failed", f"{type_name}: {err}")

        def finally_():
            btn.config(state="normal")

        _run_in_background(root, work_fn=work, on_success=success, on_error=error, on_finally=finally_)

    for i, (text, osm_filter, type_name) in enumerate(buttons):
        r = (i // 2) * 2
        c = i % 2

        status = tk.Label(
            fetch_frame,
            text="",
            bg=config.BG,
            fg=config.FG,
            wraplength=180,
            justify="left",
            anchor="w"
        )

        btn = tk.Button(
            fetch_frame,
            text=text,
            width=15,
            bg=config.BTN,
            fg=config.FG,
        )
        btn.config(command=lambda f=osm_filter, t=type_name, l=status, b=btn: fetch_and_plot_async(f, t, l, b))
        btn.grid(row=r, column=c, padx=6, pady=4)

        status.grid(row=r + 1, column=c, padx=6, pady=(0, 10), sticky="w")

    fetch_frame.grid_columnconfigure(0, weight=1)
    fetch_frame.grid_columnconfigure(1, weight=1)

    # ---- Dedup UI ----
    def run_dedup():
        threshold = int(dedup_slider.get())
        removed_by_type, total_removed = deduplicate_all_by_priority(config.all_data, threshold)

        for t in ("Train", "Subway", "Tram", "Bus"):
            df = config.all_data.get(t)
            if df is not None and not df.empty:
                markers.plot_points(t, df)
            else:
                markers.clear_markers(t)

        dedup_value_label.config(text=f"{threshold} m")
        if total_removed > 0:
            dedup_removed_label.config(
                text=f"Removed {total_removed} (Bus {removed_by_type['Bus']}, Tram {removed_by_type['Tram']}, "
                     f"Subway {removed_by_type['Subway']}, Train {removed_by_type['Train']})"
            )
        else:
            dedup_removed_label.config(text="Removed 0")

    dedup_frame = tk.Frame(left, bg=config.BG)
    dedup_frame.grid(row=base + 1, column=0, columnspan=2, sticky="ew", padx=10, pady=(5, 8))
    dedup_frame.grid_columnconfigure(0, weight=1)

    tk.Label(dedup_frame, text="Deduplicate distance (m):", bg=config.BG, fg=config.FG, font=config.BODY_FONT)\
        .grid(row=0, column=0, columnspan=3, sticky="w")

    dedup_slider = tk.Scale(
        dedup_frame, from_=0, to=1000, resolution=100, orient="horizontal",
        length=170, showvalue=False, bg=config.BG, fg=config.FG,
        highlightthickness=0, troughcolor="#2A3B57",
        command=lambda val: dedup_value_label.config(text=f"{int(float(val))} m")
    )
    dedup_slider.grid(row=1, column=0, sticky="ew")

    dedup_value_label = tk.Label(dedup_frame, text="0 m", bg=config.BG, fg=config.FG, width=6, anchor="w")
    dedup_value_label.grid(row=1, column=1, sticky="w", padx=(6, 6))

    tk.Button(dedup_frame, text="Deduplicate", bg=config.BTN, fg=config.FG, width=12, command=run_dedup)\
        .grid(row=1, column=2, sticky="e")

    dedup_removed_label = tk.Label(
        dedup_frame, text="", bg=config.BG, fg=config.FG,
        font=config.BODY_FONT, wraplength=260, justify="left"
    )
    dedup_removed_label.grid(row=2, column=0, columnspan=3, sticky="w", pady=(4, 0))

    # ---- Hiding zones ----
    zones = build_hiding_zones_ui(left=left, root=root, map_widget=map_widget, config=config, row=base + 2)

    # ---- Export + Next ----
    def save_to_kml(next_btn):
        path = filedialog.asksaveasfilename(
            defaultextension=".kml",
            filetypes=[("KML files", "*.kml")],
            title="Save game area"
        )
        if not path:
            return

        export_game_area_kml(
            path=path,
            config=config,
            hide_zone_data=zones["hide_zone_data"],
            circle_points=zones["circle_points"],
        )

        config.last_export_path = path
        config.saved_bound_box = list(config.bound_box) if config.bound_box else None
        next_btn.config(state="normal")

        stops_count = sum(
            len(config.all_data[t]) for t in ("Train", "Subway", "Tram", "Bus")
            if config.all_data.get(t) is not None
        )

        messagebox.showinfo(
            "Export complete",
            f"Saved:\n{path}\n\nStops: {stops_count}\nZones: {len(zones['hide_zone_data'])}"
        )

    save_btn = tk.Button(
        left,
        text="EXPORT GAME AREA (KML)",
        bg=config.BTN, fg=config.FG,
        font=("Segoe UI", 12, "bold"),
        height=2
    )
    save_btn.grid(row=base + 3, column=0, sticky="ew", padx=(10, 5), pady=(10, 15))

    next_btn = tk.Button(
        left,
        text="NEXT ▶",
        bg=config.BTN, fg=config.FG,
        font=("Segoe UI", 12, "bold"),
        height=2,
        state="disabled",
        command=(go_next_callback if go_next_callback else (lambda: None))
    )
    next_btn.grid(row=base + 3, column=1, sticky="ew", padx=(5, 10), pady=(10, 15))

    save_btn.config(command=lambda: save_to_kml(next_btn))

    return {"next_btn": next_btn, "save_btn": save_btn, "zones": zones, "markers": markers}
