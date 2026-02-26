import tkinter as tk

import config
from ui_layout import build_header, build_body
from map_utils import embed_map, make_map_container

from screens.shared.game_area_section import build_game_area_section
from screens.shared.geo_area_helpers import (
    build_icons,
    init_geo_state,
    build_search_ui,
    make_geom_helpers,
    make_search_handlers,
)


def geo_screen(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)

    from screens.main_menu import main_menu

    build_header(
        frame,
        "Geographic Area",
        back_command=lambda: show_screen(main_menu),
        photo=photo
    )

    left, right = build_body(frame, config.BG)

    map_container = make_map_container(right)
    map_widget = embed_map(map_container, center=config.DEFAULT_MAP_CENTER, zoom=config.DEFAULT_MAP_ZOOM)

    icons, TRANSPARENT_ICON = build_icons(root)
    state = init_geo_state()

    ui = build_search_ui(left)

    def set_status(text):
        ui["status_lbl"].config(text=text)

    geom = make_geom_helpers(map_widget, state, set_status)

    # Button under dropdown
    def lock_hiding_zone():
        state["hiding_zone_locked"]["locked"] = True
        ui["hide_dropdown"]()
        ui["query_entry"].config(state="disabled", disabledbackground="#3a3a3a", disabledforeground="#bdbdbd")
        ui["search_btn"].config(state="disabled")
        set_hiding_btn.config(state="disabled")

        g = state["combined_geom"]["geom"]
        if g is not None and (not g.is_empty):
            minx, miny, maxx, maxy = g.bounds
            config.bound_box = [miny, minx, maxy, maxx]
            config.overpass_poly = geom["geom_to_overpass_poly"](g)

        set_status(f"Hiding Zone set ✅  ({len(config.game_areas)} regions). Search locked.")

    set_hiding_btn = tk.Button(
        left,
        text="Set Hiding Zone",
        bg=config.BTN,
        fg=config.FG,
        state="disabled",
        command=lock_hiding_zone
    )
    set_hiding_btn.grid(row=5, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))

    handlers = make_search_handlers(root, ui, geom, state, set_hiding_btn)

    ui["search_btn"].config(command=handlers["do_search"])
    ui["query_entry"].bind("<Return>", lambda _e: handlers["do_search"]())

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
        start_row=6,
    )

    handlers["update_selected_summary"]()
    set_status("Search for a city, county, country, or region (e.g. “Scotland”, “Edinburgh”).")

    return frame
