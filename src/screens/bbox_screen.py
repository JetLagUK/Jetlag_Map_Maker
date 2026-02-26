import tkinter as tk
import config
from image_loader import load_image
from ui_layout import build_header, build_body
from map_utils import embed_map, make_map_container
import tkinter.messagebox as messagebox

from screens.shared.game_area_section import build_game_area_section  # ✅ NEW


def bbox_screen(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)

    if not hasattr(config, "all_data") or config.all_data is None:
        config.all_data = {"Train": None, "Subway": None, "Tram": None, "Bus": None}
    if not hasattr(config, "bound_box"):
        config.bound_box = None

    from screens.main_menu import main_menu

    build_header(
        frame,
        "Bounding Box",
        back_command=lambda: show_screen(main_menu),
        photo=photo
    )

    left, right = build_body(frame, config.BG)

    map_container = make_map_container(right)
    map_widget = embed_map(map_container)

    ICON_SIZE = (28, 28)
    icons = {
        "Train": load_image("train.png", size=ICON_SIZE),
        "Tram": load_image("tram.png", size=ICON_SIZE),
        "Bus": load_image("bus.png", size=ICON_SIZE),
        "Subway": load_image("subway.png", size=ICON_SIZE),
    }
    TRANSPARENT_ICON = tk.PhotoImage(master=root, width=1, height=1)

    bbox_shape = {"obj": None}

    def parse_lat_lon(text: str):
        s = text.strip().replace(",", " ")
        parts = [p for p in s.split() if p]
        if len(parts) != 2:
            raise ValueError("Expected: lat lon")
        return float(parts[0]), float(parts[1])

    # ----------------------------
    # Centered Bounding Box block
    # ----------------------------
    bbox_frame = tk.Frame(left, bg=config.BG)
    # occupies rows 0-3 so the shared section can still start at row 4
    bbox_frame.grid(row=0, column=0, columnspan=2, rowspan=4, sticky="ew")
    bbox_frame.grid_columnconfigure(0, weight=1)  # left spacer
    bbox_frame.grid_columnconfigure(1, weight=0)  # label col
    bbox_frame.grid_columnconfigure(2, weight=0)  # entry col
    bbox_frame.grid_columnconfigure(3, weight=1)  # right spacer

    tk.Label(
        bbox_frame,
        text="Bounding Box",
        bg=config.BG,
        fg=config.FG,
        font=config.SUBTITLE_FONT
    ).grid(row=0, column=0, columnspan=4, pady=(10, 10))

    tk.Label(bbox_frame, text="Point 1 (lat, lon):", bg=config.BG, fg=config.FG)\
        .grid(row=1, column=1, sticky="e", pady=2, padx=(0, 6))
    point1_entry = tk.Entry(bbox_frame, width=25)
    point1_entry.grid(row=1, column=2, sticky="w", pady=2)

    tk.Label(bbox_frame, text="Point 2 (lat, lon):", bg=config.BG, fg=config.FG)\
        .grid(row=2, column=1, sticky="e", pady=2, padx=(0, 6))
    point2_entry = tk.Entry(bbox_frame, width=25)
    point2_entry.grid(row=2, column=2, sticky="w", pady=2)

    def _delete_shape(obj):
        if obj is None:
            return
        try:
            obj.delete()
        except Exception:
            pass

    def set_bounding_box():
        try:
            lat1, lon1 = parse_lat_lon(point1_entry.get())
            lat2, lon2 = parse_lat_lon(point2_entry.get())

            south = min(lat1, lat2)
            north = max(lat1, lat2)
            west = min(lon1, lon2)
            east = max(lon1, lon2)
        except Exception:
            messagebox.showerror("Bounding box", "Enter as:\n55.7, -3.9\nor\n55.7 -3.9")
            return

        config.bound_box = [south, west, north, east]

        for ent in (point1_entry, point2_entry):
            ent.config(
                state="disabled",
                disabledforeground=config.FG,
                disabledbackground=config.BG,
                relief="flat"
            )

        if hasattr(map_widget, "fit_bounding_box"):
            map_widget.fit_bounding_box((north, west), (south, east))
        else:
            map_widget.set_position((south + north) / 2, (west + east) / 2)
            map_widget.set_zoom(config.DEFAULT_MAP_ZOOM)

        corners = [
            (north, west),
            (north, east),
            (south, east),
            (south, west),
            (north, west),
        ]

        _delete_shape(bbox_shape["obj"])
        bbox_shape["obj"] = None

        if hasattr(map_widget, "set_path"):
            bbox_shape["obj"] = map_widget.set_path(corners, width=2)
        elif hasattr(map_widget, "set_polygon"):
            bbox_shape["obj"] = map_widget.set_polygon(corners, border_width=2)

        set_bbox_btn.config(state="disabled", text="Bounding Box Set")

    set_bbox_btn = tk.Button(
        bbox_frame,
        text="Set Bounding Box",
        bg=config.BTN,
        fg=config.FG,
        width=22,
        command=set_bounding_box
    )
    set_bbox_btn.grid(row=3, column=0, columnspan=4, pady=(10, 5))

    # ✅ EVERYTHING AFTER THIS LINE IS NOW REUSED
    def go_next():
        from screens.points_of_interest import points_of_interest
        show_screen(points_of_interest)

    build_game_area_section(
        left=left,
        root=root,
        map_widget=map_widget,
        icons=icons,
        transparent_icon=TRANSPARENT_ICON,
        point1_entry=point1_entry,
        point2_entry=point2_entry,
        go_next_callback=go_next,
    )

    return frame
