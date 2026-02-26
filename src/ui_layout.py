import tkinter as tk
import config
from image_loader import load_image


def build_header(parent: tk.Widget, title_text: str, back_command, photo=None):
    header = tk.Frame(parent, height=config.HEADER_HEIGHT, bg=config.BG)
    header.pack(fill="x")
    header.pack_propagate(False)

    back_btn = tk.Button(
        header,
        text="← Back",
        bg=config.BTN,
        fg=config.FG,
        command=back_command
    )
    back_btn.grid(row=0, column=0, padx=10, sticky="w")

    title_lbl = tk.Label(
        header,
        text=title_text if title_text else "Jetlag Map Maker",
        bg=config.BG,
        fg=config.FG,
        font=config.TITLE_FONT
    )
    title_lbl.grid(row=0, column=1, padx=10, sticky="n")

    if photo:
        font_size = config.TITLE_FONT[1]
        logo_size = (font_size + 20, font_size + 20)

        logo = load_image("logo.png", size=logo_size)
        if logo:
            logo_lbl = tk.Label(header, image=logo, bg=config.BG)
            logo_lbl.image = logo  # keep reference
            logo_lbl.grid(row=0, column=2, padx=10, sticky="e")

    header.grid_columnconfigure(1, weight=1)


def make_scrollable_left(parent, bg):
    """
    Returns (outer_frame, scrollable_frame)

    outer_frame goes in your grid
    scrollable_frame is where you put all left-side widgets
    """
    outer = tk.Frame(parent, bg=bg)
    outer.grid_rowconfigure(0, weight=1)
    outer.grid_columnconfigure(0, weight=1)

    canvas = tk.Canvas(
        outer,
        bg=bg,
        highlightthickness=0,
        bd=0
    )
    canvas.grid(row=0, column=0, sticky="nsew")

    scrollbar = tk.Scrollbar(
        outer,
        orient="vertical",
        command=canvas.yview
    )
    scrollbar.grid(row=0, column=1, sticky="ns")

    canvas.configure(yscrollcommand=scrollbar.set)

    scrollable = tk.Frame(canvas, bg=bg)
    window_id = canvas.create_window((0, 0), window=scrollable, anchor="nw")

    def _on_configure(_event=None):
        canvas.configure(scrollregion=canvas.bbox("all"))

    scrollable.bind("<Configure>", _on_configure)

    def _on_canvas_configure(event):
        canvas.itemconfig(window_id, width=event.width)

    canvas.bind("<Configure>", _on_canvas_configure)

    # Safe mouse wheel (no bind_all; works over children)
    def _scroll_units(units: int):
        canvas.yview_scroll(units, "units")
        return "break"

    def _on_mousewheel(event):
        delta = getattr(event, "delta", 0)
        if delta:
            return _scroll_units(int(-1 * (delta / 120)))
        return None

    def _on_linux_wheel_up(_event):
        return _scroll_units(-1)

    def _on_linux_wheel_down(_event):
        return _scroll_units(1)

    def _bind_wheel(_e=None):
        for w in (outer, canvas, scrollable):
            w.bind("<MouseWheel>", _on_mousewheel)
            w.bind("<Button-4>", _on_linux_wheel_up)
            w.bind("<Button-5>", _on_linux_wheel_down)

    def _safe_unbind(widget, sequence: str):
        try:
            if widget is not None and widget.winfo_exists():
                widget.unbind(sequence)
        except Exception:
            pass

    def _unbind_wheel(_e=None):
        for w in (outer, canvas, scrollable):
            _safe_unbind(w, "<MouseWheel>")
            _safe_unbind(w, "<Button-4>")
            _safe_unbind(w, "<Button-5>")

    outer.bind("<Enter>", _bind_wheel)
    outer.bind("<Leave>", _unbind_wheel)

    def _on_outer_destroy(event):
        if event.widget is outer:
            _unbind_wheel()

    outer.bind("<Destroy>", _on_outer_destroy)

    return outer, scrollable


def build_body(parent, bg):
    """
    PACK-only body builder.
    Left side scrolls; right side (map) stays fixed.
    """
    body = tk.Frame(parent, bg=bg)
    body.pack(fill="both", expand=True)

    # Right panel (map)
    right = tk.Frame(body, bg=bg)
    right.pack(side="right", fill="both", expand=True)

    # Left panel (scrollable)
    left_outer = tk.Frame(body, bg=bg, highlightthickness=0, bd=0)
    left_outer.pack(side="left", fill="y")

    left_outer.configure(width=360)
    left_outer.pack_propagate(False)

    left_canvas = tk.Canvas(
        left_outer,
        bg=bg,
        highlightthickness=0,
        bd=0
    )
    left_canvas.pack(side="left", fill="both", expand=True)

    left_scrollbar = tk.Scrollbar(
        left_outer,
        orient="vertical",
        command=left_canvas.yview,
        bg=config.BG,
        activebackground=config.BTN,
        troughcolor=config.BG,
        highlightthickness=0,
        bd=0
    )
    left_scrollbar.pack(side="right", fill="y")

    left_canvas.configure(yscrollcommand=left_scrollbar.set)

    left = tk.Frame(left_canvas, bg=bg)
    window_id = left_canvas.create_window((0, 0), window=left, anchor="nw")

    def _on_left_configure(_event=None):
        left_canvas.configure(scrollregion=left_canvas.bbox("all"))

    left.bind("<Configure>", _on_left_configure)

    def _on_canvas_configure(event):
        left_canvas.itemconfig(window_id, width=event.width)

    left_canvas.bind("<Configure>", _on_canvas_configure)

     # --- Mouse wheel scrolling (reliable): bind on the toplevel while hovered ---

    toplevel = parent.winfo_toplevel()
    _wheel_bound = {"on": False}

    def _scroll_units(units: int):
        try:
            if left_canvas.winfo_exists():
                left_canvas.yview_scroll(units, "units")
        except Exception:
            pass
        return "break"

    def _on_mousewheel(event):
        delta = getattr(event, "delta", 0)
        if delta:
            return _scroll_units(int(-1 * (delta / 120)))
        return None

    def _on_linux_wheel_up(_event):
        return _scroll_units(-1)

    def _on_linux_wheel_down(_event):
        return _scroll_units(1)

    def _bind_wheel(_e=None):
        if _wheel_bound["on"]:
            return
        _wheel_bound["on"] = True
        # bind on the whole window so child widgets still trigger scrolling
        toplevel.bind_all("<MouseWheel>", _on_mousewheel)
        toplevel.bind_all("<Button-4>", _on_linux_wheel_up)
        toplevel.bind_all("<Button-5>", _on_linux_wheel_down)

    def _unbind_wheel(_e=None):
        if not _wheel_bound["on"]:
            return
        _wheel_bound["on"] = False
        try:
            toplevel.unbind_all("<MouseWheel>")
            toplevel.unbind_all("<Button-4>")
            toplevel.unbind_all("<Button-5>")
        except Exception:
            pass

    # Bind/unbind when mouse enters/leaves ANY part of the left panel (including children)
    for w in (left_outer, left_canvas, left):
        w.bind("<Enter>", _bind_wheel)
        w.bind("<Leave>", _unbind_wheel)

    # Clean up when this screen is destroyed (prevents old-canvas errors)
    def _on_body_destroy(event):
        if event.widget is body:
            _unbind_wheel()

    body.bind("<Destroy>", _on_body_destroy)

    return left, right
