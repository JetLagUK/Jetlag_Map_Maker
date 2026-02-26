import tkinter as tk
import config


def main_menu(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)

    if photo:
        logo = tk.Label(frame, image=photo, bg=config.BG)
        logo.image = photo
        logo.pack(pady=20)

    tk.Label(
        frame,
        text="Jetlag UK Map Maker",
        bg=config.BG,
        fg=config.FG,
        font=("Segoe UI", 22, "bold")
    ).pack(pady=(0, 20))

    tk.Label(
        frame,
        text="I'm going to set my area by:",
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT
    ).pack(pady=(0, 20))

    # Local imports avoid circular imports
    from screens.bbox_screen import bbox_screen
    from screens.geo_screen import geo_screen
    from screens.kml_screen import kml_screen

    tk.Button(
        frame,
        text="Bounding Box",
        width=25,
        bg=config.BTN,
        fg=config.FG,
        command=lambda: show_screen(bbox_screen)
    ).pack(pady=5)

    tk.Button(
        frame,
        text="Geographic Area",
        width=25,
        bg=config.BTN,
        fg=config.FG,
        command=lambda: show_screen(geo_screen)
    ).pack(pady=5)

    tk.Button(
        frame,
        text="Custom KML Polygon",
        width=25,
        bg=config.BTN,
        fg=config.FG,
        command=lambda: show_screen(kml_screen)
    ).pack(pady=5)

    return frame
