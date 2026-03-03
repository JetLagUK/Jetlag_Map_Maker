import threading
import tkinter as tk
from tkinter import messagebox

import config
from .shared.geofabrik_index import fetch_geofabrik_index, flatten_geofabrik_index
from .shared.local_data_left_panel import LocalDataLeftPanel
from screens.shared.local_data_right_panel import LocalDataRightPanel


def local_data_screen(root, show_screen, photo):
    frame = tk.Frame(root, bg=config.BG)
    from screens.main_menu import main_menu

    state = {
        "nodes": None,
        "current_parent_id": "world",
        "selected_id": None,
        "index_loaded": False,
        "busy": False,
    }

    # Layout
    frame.grid_rowconfigure(2, weight=1)
    frame.grid_columnconfigure(0, weight=0)
    frame.grid_columnconfigure(1, weight=1)

    tk.Label(
        frame,
        text="Get Local Data",
        bg=config.BG,
        fg=config.FG,
        font=("Segoe UI", 22, "bold"),
    ).grid(row=0, column=0, columnspan=2, sticky="w", padx=16, pady=(16, 6))

    parent_label = tk.Label(
        frame,
        text="Loading…",
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT,
    )
    parent_label.grid(row=1, column=0, columnspan=2, sticky="w", padx=16, pady=(0, 10))

    # Left + right columns
    left_host = tk.Frame(frame, bg=config.BG)
    left_host.grid(row=2, column=0, sticky="nsew", padx=(16, 10), pady=(0, 10))
    left_host.grid_rowconfigure(0, weight=1)
    left_host.grid_columnconfigure(0, weight=1)

    right = tk.Frame(frame, bg=config.BG)
    right.grid(row=2, column=1, sticky="nsew", padx=(0, 16), pady=(0, 10))
    right.grid_rowconfigure(0, weight=1)
    right.grid_columnconfigure(0, weight=1)
    right_panel = LocalDataRightPanel(
        right,
        local_data_root=config.LOCAL_DATA_DIR  # or whatever your folder constant is
    )
    right_panel.grid(row=0, column=0, sticky="nsew")
    # Bottom bar
    bottom = tk.Frame(frame, bg=config.BG)
    bottom.grid(row=3, column=0, columnspan=2, sticky="ew", padx=16, pady=(0, 16))
    bottom.grid_columnconfigure(1, weight=1)

    tk.Button(
        bottom,
        text="Back",
        width=18,
        bg=config.BTN,
        fg=config.FG,
        command=lambda: show_screen(main_menu),
    ).grid(row=0, column=0, sticky="w")

    status_label = tk.Label(
        bottom,
        text="Status: loading index…",
        bg=config.BG,
        fg=config.FG,
        font=config.BODY_FONT,
        anchor="w",
    )
    status_label.grid(row=0, column=1, sticky="ew", padx=(10, 0))

    def set_status(text: str):
        status_label.config(text=f"Status: {text}")

    # Left panel controller
    left_panel = LocalDataLeftPanel(
        left_host,
        root=root,
        state=state,
        set_status=set_status,
        parent_label=parent_label,
    )
    left_panel.widget().grid(row=0, column=0, sticky="nsew")

    # Index load
    def load_index():
        def worker():
            try:
                data = fetch_geofabrik_index()
                nodes, _ = flatten_geofabrik_index(data)

                state["nodes"] = nodes
                state["current_parent_id"] = "world"
                state["selected_id"] = None
                state["index_loaded"] = True

                root.after(0, lambda: set_status("Index loaded."))
                root.after(0, lambda: left_panel.set_download_enabled(None))
                root.after(0, left_panel.render_parent)

            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                root.after(0, lambda: set_status(err))
                root.after(0, lambda: messagebox.showerror("Geofabrik Index", err))

        threading.Thread(target=worker, daemon=True).start()

    load_index()
    return frame