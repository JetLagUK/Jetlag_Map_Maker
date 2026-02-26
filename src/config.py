"""
Global configuration and shared state for Jetlag UK Map Maker
"""

# ----------------- COLOURS -----------------
BG = "#1B2A40"        # Background
FG = "#FFFFFF"        # Foreground / text
BTN = "#F68B1F"       # Buttons / accents
DANGER = "#D21F2D"    # Errors / destructive actions

# ----------------- LAYOUT -----------------
HEADER_HEIGHT = 80
LEFT_PANEL_WIDTH = 420
MAP_HEIGHT = 350

# ----------------- FONTS -----------------
TITLE_FONT = ("Segoe UI", 18, "bold")
SUBTITLE_FONT = ("Segoe UI", 14, "bold")
BODY_FONT = ("Segoe UI", 12)

# ----------------- MAP DEFAULTS -----------------
DEFAULT_MAP_CENTER = (55.86, -4.25)  # Glasgow-ish
DEFAULT_MAP_ZOOM = 10

# ----------------- OSM / DATA -----------------
overpass_mirrors = [
     # Primary, reliable when not overloaded
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",

    # Often succeeds when .de is overloaded
    "https://overpass.kumi.systems/api/interpreter",

    # Good fallback, frequently underused
    "https://overpass.private.coffee/api/interpreter",

    # Asia-based mirror, surprisingly reliable for big queries
    "https://overpass.nchc.org.tw/api/interpreter",
]

# Shared runtime state (kept simple for now)
bound_box = None

all_data = {
    "Train": None,
    "Subway": None,
    "Tram": None,
    "Bus": None
}

dedup_valid = False
last_export_path = None
saved_bound_box = None