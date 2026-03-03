import tkinter as tk


class ScrollableFrame(tk.Frame):
    """Canvas-based scrollable frame with a reliable reset_view() for navigation UIs."""

    def __init__(self, parent, bg, *args, **kwargs):
        super().__init__(parent, bg=bg, *args, **kwargs)

        self.canvas = tk.Canvas(self, bg=bg, highlightthickness=0, bd=0)
        self.scrollbar = tk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = tk.Frame(self.canvas, bg=bg)

        self.inner_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.inner.bind("<Configure>", self._on_inner_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        self._bind_mousewheel(self.canvas)

    def _on_inner_configure(self, _evt):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, evt):
        # Make inner match the visible canvas width (important for 2-column grids)
        try:
            self.canvas.itemconfigure(self.inner_id, width=evt.width)
        except tk.TclError:
            pass

    def clear(self):
        for w in self.inner.winfo_children():
            w.destroy()

    def reset_view(self):
        # Critical fix: when switching from long -> short lists, reset scroll state.
        self.inner.update_idletasks()
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        self.canvas.yview_moveto(0)

    def _bind_mousewheel(self, widget):
        def _on_mousewheel_windows(e):
            self.canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")

        def _on_mousewheel_linux(e):
            if e.num == 4:
                self.canvas.yview_scroll(-1, "units")
            elif e.num == 5:
                self.canvas.yview_scroll(1, "units")

        widget.bind_all("<MouseWheel>", _on_mousewheel_windows)
        widget.bind_all("<Button-4>", _on_mousewheel_linux)
        widget.bind_all("<Button-5>", _on_mousewheel_linux)
