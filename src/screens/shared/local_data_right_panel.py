# screens/shared/local_data_right_panel.py
from __future__ import annotations

import os
import shutil
import tkinter as tk
from tkinter import ttk, messagebox
from dataclasses import dataclass
from typing import List, Optional

import config


def _human_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def _country_from_folder(folder_name: str) -> str:
    base = folder_name.replace("_local_data", "")
    base = base.replace("-", " ").replace("_", " ").strip()
    return base.title()


@dataclass
class LocalDataRow:
    folder_path: str
    folder_name: str
    country_display: str
    size_bytes: int


def scan_local_data_folders(local_data_root: str) -> List[LocalDataRow]:
    rows: List[LocalDataRow] = []
    if not local_data_root or not os.path.isdir(local_data_root):
        return rows

    for name in sorted(os.listdir(local_data_root)):
        full = os.path.join(local_data_root, name)
        if not os.path.isdir(full):
            continue
        if not name.endswith("_local_data"):
            continue

        size_b = _dir_size_bytes(full)
        rows.append(
            LocalDataRow(
                folder_path=full,
                folder_name=name,
                country_display=_country_from_folder(name),
                size_bytes=size_b,
            )
        )

    rows.sort(key=lambda r: r.size_bytes, reverse=True)
    return rows


def _hex_brighten(hex_color: str, amt: int = 18) -> str:
    """Lighten a #RRGGBB colour slightly for zebra rows."""
    hc = hex_color.lstrip("#")
    if len(hc) != 6:
        return hex_color
    r = min(255, int(hc[0:2], 16) + amt)
    g = min(255, int(hc[2:4], 16) + amt)
    b = min(255, int(hc[4:6], 16) + amt)
    return f"#{r:02X}{g:02X}{b:02X}"


class LocalDataRightPanel(ttk.Frame):
    """
    Right-side panel: table of downloaded local datasets.
    Columns: Country | Size | Remove
    Remove is clickable text styled to match the app.
    """

    def __init__(self, parent, *, local_data_root: str, bg: Optional[str] = None):
        super().__init__(parent)

        self.local_data_root = str(local_data_root)
        self._iid_to_row: dict[str, LocalDataRow] = {}

        # App colours
        app_bg = config.BG
        app_fg = config.FG
        accent = config.BTN          # orange
        danger = config.DANGER       # red (not used unless you switch it)

        zebra_bg = _hex_brighten(app_bg, 14)

        # ---- ttk styling ----
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("LocalData.TFrame", background=app_bg)
        style.configure("LocalData.TLabel", background=app_bg, foreground=app_fg)

        style.configure(
            "LocalData.Treeview",
            background=app_bg,
            foreground=app_fg,
            fieldbackground=app_bg,
            borderwidth=0,
            rowheight=28,
        )
        style.configure(
            "LocalData.Treeview.Heading",
            background=accent,
            foreground=app_fg,
            font=("Segoe UI", 10, "bold"),
        )
        style.map(
            "LocalData.Treeview",
            background=[("selected", accent)],
            foreground=[("selected", app_fg)],
        )

        self.configure(style="LocalData.TFrame")

        # Tags
        self._tag_even = "even"
        self._tag_odd = "odd"
        self._tag_remove = "remove"

        # Header
        title = ttk.Label(self, text="Downloaded Data", style="LocalData.TLabel", font=("Segoe UI", 12, "bold"))
        title.pack(anchor="w", padx=10, pady=(10, 6))

        # Table host (pack host, grid inside)
        table_host = tk.Frame(self, bg=app_bg)
        table_host.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        table_host.grid_rowconfigure(0, weight=1)
        table_host.grid_columnconfigure(0, weight=1)

        cols = ("country", "size", "remove")
        self.tree = ttk.Treeview(
            table_host,
            columns=cols,
            show="headings",
            height=12,
            style="LocalData.Treeview",
        )
        self.tree.heading("country", text="Country")
        self.tree.heading("size", text="Size")
        self.tree.heading("remove", text="Remove")

        self.tree.column("country", width=260, anchor="w")
        self.tree.column("size", width=110, anchor="e")
        self.tree.column("remove", width=100, anchor="center")

        self.tree.tag_configure(self._tag_even, background=app_bg, foreground=app_fg)
        self.tree.tag_configure(self._tag_odd, background=zebra_bg, foreground=app_fg)

        # Orange remove text (switch to danger if you want destructive = red)
        self.tree.tag_configure(self._tag_remove, foreground=accent)   # <- change to danger for red

        yscroll = ttk.Scrollbar(table_host, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")

        # Click + hover
        self.tree.bind("<Button-1>", self._on_click)
        self.tree.bind("<Motion>", self._on_motion)
        self.tree.bind("<Leave>", lambda _e: self.tree.configure(cursor=""))

        # Bottom buttons (use tk.Button to match your existing style exactly)
        btns = tk.Frame(self, bg=app_bg)
        btns.pack(fill="x", padx=10, pady=(0, 10))

        refresh_btn = tk.Button(
            btns,
            text="Refresh",
            bg=accent,
            fg=app_fg,
            activebackground=accent,
            activeforeground=app_fg,
            relief="flat",
            command=self.refresh,
        )
        refresh_btn.pack(side="left")

        self.after(50, self.refresh)

    def refresh(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self._iid_to_row.clear()

        rows = scan_local_data_folders(self.local_data_root)

        if not rows:
            iid = self.tree.insert("", "end", values=("No local data downloaded yet", "", ""))
            self._iid_to_row[iid] = LocalDataRow("", "", "No local data downloaded yet", 0)
            return

        for i, r in enumerate(rows):
            base_tag = self._tag_even if (i % 2 == 0) else self._tag_odd
            iid = self.tree.insert(
                "",
                "end",
                values=(r.country_display, _human_size(r.size_bytes), "Remove"),
                tags=(base_tag, self._tag_remove),
            )
            self._iid_to_row[iid] = r

    def _on_motion(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            self.tree.configure(cursor="")
            return

        col = self.tree.identify_column(event.x)
        self.tree.configure(cursor="hand2" if col == "#3" else "")

    def _on_click(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        col = self.tree.identify_column(event.x)  # "#1" "#2" "#3"
        row_iid = self.tree.identify_row(event.y)
        if not row_iid or col != "#3":
            return

        row = self._iid_to_row.get(row_iid)
        if not row or not row.folder_path or not os.path.isdir(row.folder_path):
            return

        ok = messagebox.askyesno(
            "Remove local data",
            f"Delete local data for:\n\n{row.country_display}\n\nThis will remove:\n{row.folder_path}",
            icon="warning",
        )
        if not ok:
            return

        try:
            shutil.rmtree(row.folder_path)
        except Exception as e:
            messagebox.showerror("Delete failed", f"Could not delete:\n{row.folder_path}\n\n{e}")
            return

        self.refresh()