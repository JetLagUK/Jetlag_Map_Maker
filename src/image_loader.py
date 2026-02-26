from pathlib import Path
from typing import Optional, Tuple, Dict
from PIL import Image, ImageTk

# Base assets directory (.../your_project/assets)
ASSETS_DIR = Path(__file__).parent / "assets"

# Cache to avoid reloading + prevent Tkinter PhotoImage GC issues
# Key is (filename, size) where size is (w, h) or None
_image_cache: Dict[Tuple[str, Optional[Tuple[int, int]]], ImageTk.PhotoImage] = {}


def load_image(filename: str, size: Optional[Tuple[int, int]] = None) -> Optional[ImageTk.PhotoImage]:
    """
    Load an image from the assets folder and return a Tkinter PhotoImage.
    Results are cached by (filename, size).

    :param filename: e.g. "logo.png"
    :param size: (width, height) or None
    :return: ImageTk.PhotoImage or None if missing/unloadable
    """
    key = (filename, size)
    if key in _image_cache:
        return _image_cache[key]

    path = ASSETS_DIR / filename
    if not path.exists():
        print("[image_loader] Missing image:", path)
        return None

    try:
        img = Image.open(path)
        if size is not None:
            img = img.resize(size)
        photo = ImageTk.PhotoImage(img)
    except Exception as e:
        print(f"[image_loader] Failed to load {path}: {e}")
        return None

    _image_cache[key] = photo
    return photo
