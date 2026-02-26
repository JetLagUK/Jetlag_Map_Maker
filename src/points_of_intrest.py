import tkinter as tk
import config
from ui_layout import build_header, build_body
from screens.points_of_interest import points_of_interest

def points_of_interest(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)

    # Back goes to bounding box screen
    from screens.bbox_screen import bbox_screen

    build_header(
        frame,
        "Points of Interest",
        back_command=lambda: show_screen(bbox_screen),
        photo=photo
    )

    left, right = build_body(frame, config.BG)

    # Debug / confirmation info (can remove later)
    bbox = config.saved_bound_box
    export_path = config.last_export_path

    tk.Label(
        left,
        text="Game data loaded:",
        bg=config.BG,
        fg=config.FG,
        font=config.SUBTITLE_FONT
    ).pack(anchor="w", padx=10, pady=(10, 4))

    tk.Label(
        left,
        text=f"Exported KML:\n{export_path}\n\nBounding Box:\n{bbox}",
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT,
        justify="left",
        anchor="w",
        wraplength=320
    ).pack(fill="x", padx=10)

    return frame
