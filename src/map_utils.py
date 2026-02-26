import tkinter as tk
import config
import tkintermapview 

def embed_map(parent: tk.Widget, center=None, zoom=None):
    if center is None:
        center = config.DEFAULT_MAP_CENTER
    if zoom is None:
        zoom = config.DEFAULT_MAP_ZOOM

    map_widget = tkintermapview.TkinterMapView(parent, corner_radius=0)
    map_widget.pack(expand=True, fill="both")

    map_widget.set_position(center[0], center[1])
    map_widget.set_zoom(zoom)

    return map_widget


def make_map_container(parent: tk.Widget):
    """
    Creates a padded container for the map that expands naturally.
    """
    outer = tk.Frame(parent, bg=config.BG)
    outer.pack(expand=True, fill="both", padx=10, pady=10)

    inner = tk.Frame(outer, bg=config.BG)
    inner.pack(expand=True, fill="both", padx=8, pady=8)

    return inner