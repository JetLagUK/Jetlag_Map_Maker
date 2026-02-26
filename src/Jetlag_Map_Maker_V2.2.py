import tkinter as tk

from config import BG  # or set BG here if you haven't made config.py yet
from image_loader import load_image

from screens.main_menu import main_menu
from screens.bbox_screen import bbox_screen
from screens.geo_screen import geo_screen
from screens.kml_screen import kml_screen

root = tk.Tk()
root.title("Jetlag UK Map Maker")
root.geometry("1000x600")
root.configure(bg=BG)

# Load logo once (cached)
logo_photo = load_image("logo.png", size=(100, 100))

current_screen = None

def show_screen(screen_func):
    """Destroy current screen frame and show a new one."""
    global current_screen
    if current_screen is not None:
        current_screen.destroy()

    # Every screen must accept: (root, show_screen, logo_photo)
    current_screen = screen_func(root, show_screen, logo_photo)
    current_screen.pack(expand=True, fill="both")


# Start app on main menu
show_screen(main_menu)
root.mainloop()
