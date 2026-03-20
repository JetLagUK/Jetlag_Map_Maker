"""
Global configuration and shared state for Jetlag UK Map Maker
"""
from pathlib import Path
import platform

# Project root = folder where config.py lives
BASE_DIR = Path(__file__).resolve().parents[1]

# Local data folder inside project
LOCAL_DATA_DIR = BASE_DIR / "local_data_outputs"

# Make sure it exists
LOCAL_DATA_DIR.mkdir(exist_ok=True)

# ----------------- COLOURS -----------------
BG = "#1B2A40" if platform.system() != "Darwin" else "#ececec"       # Background
FG = "#FFFFFF" if platform.system() != "Darwin" else "#000000"       # Foreground / text
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

COVERAGE_BUFFER_M = 300 
COVERAGE_MIN_MISSING_KM2 = 1.0 
COVERAGE_MIN_MISSING_RATIO = 0.005