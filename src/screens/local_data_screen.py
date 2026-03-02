import tkinter as tk
import config

def local_data_screen(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)

    # Local import to avoid circular imports
    from screens.main_menu import main_menu

    tk.Label(
        frame,
        text="Get Local Data",
        bg=config.BG,
        fg=config.FG,
        font=("Segoe UI", 22, "bold")
    ).pack(pady=(20, 10))

    tk.Label(
        frame,
        text="Coming soon: download / load local GeoPackage or PBF data.",
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT
    ).pack(pady=(0, 20))

    tk.Button(
        frame,
        text="Back",
        width=25,
        bg=config.BTN,
        fg=config.FG,
        command=lambda: show_screen(main_menu)
    ).pack(pady=10)

    return frame