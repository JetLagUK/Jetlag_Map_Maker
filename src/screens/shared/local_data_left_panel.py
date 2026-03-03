import os
import threading
import tkinter as tk
from tkinter import ttk
from typing import Callable, Dict, Optional

import config
from .ui_scrollable import ScrollableFrame
from .geofabrik_index import breadcrumb
from .local_data_pipeline import run_local_data_pipeline


def human_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


class LocalDataLeftPanel:
    """
    Owns the entire left-side UI:
      - browse mode (tree buttons)
      - run mode (progress + live logs + summary)
      - pipeline start + UI updates
    """

    def __init__(
        self,
        parent: tk.Widget,
        *,
        root: tk.Tk,
        state: dict,
        set_status: Callable[[str], None],
        parent_label: tk.Label,
    ):
        self.root = root
        self.state = state
        self.set_status = set_status
        self.parent_label = parent_label

        # Container
        self.container = tk.Frame(parent, bg=config.BG)
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)

        # ---------------------------
        # Browse mode
        # ---------------------------
        self.browse_frame = tk.Frame(self.container, bg=config.BG)
        self.browse_frame.grid(row=0, column=0, sticky="nsew")
        self.browse_frame.grid_rowconfigure(1, weight=1)
        self.browse_frame.grid_columnconfigure(0, weight=1)

        self.up_btn = tk.Button(
            self.browse_frame,
            text="⬅ Up",
            bg=config.BTN,
            fg=config.FG,
            command=self.go_up,
        )
        self.up_btn.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        self.nav_list = ScrollableFrame(self.browse_frame, bg=config.BG)
        self.nav_list.grid(row=1, column=0, sticky="nsew")

        self.download_btn = tk.Button(
            self.browse_frame,
            text="Download + Build Dataset",
            bg=config.BTN,
            fg=config.FG,
            state="disabled",
            command=self.start_pipeline,
        )
        self.download_btn.grid(row=2, column=0, sticky="ew", pady=(10, 0))

        # ---------------------------
        # Run mode
        # ---------------------------
        self.run_frame = tk.Frame(self.container, bg=config.BG)
        self.run_frame.grid(row=0, column=0, sticky="nsew")
        self.run_frame.grid_rowconfigure(2, weight=1)
        self.run_frame.grid_columnconfigure(0, weight=1)

        self.run_title = tk.Label(
            self.run_frame,
            text="Building local dataset…",
            bg=config.BG,
            fg=config.FG,
            font=config.BODY_FONT,
        )
        self.run_title.grid(row=0, column=0, sticky="w", pady=(0, 8))

        self.progress = ttk.Progressbar(self.run_frame, mode="determinate")
        self.progress.grid(row=1, column=0, sticky="ew")

        self.log_box = tk.Text(
            self.run_frame,
            wrap="word",
            bg=config.BG,
            fg=config.FG,
            insertbackground=config.FG,
            relief="flat",
            height=12,
        )
        self.log_box.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        self.log_box.config(state="disabled")

        # start in browse mode
        self.run_frame.grid_remove()

    # ============================================================
    # Public accessors
    # ============================================================
    def widget(self) -> tk.Frame:
        return self.container

    # ============================================================
    # Browse/run mode helpers
    # ============================================================
    def _append_log(self, line: str) -> None:
        self.log_box.config(state="normal")
        self.log_box.insert("end", line + "\n")
        self.log_box.see("end")
        self.log_box.config(state="disabled")

    def _set_summary(self, text: str) -> None:
        self.log_box.config(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.insert("end", text)
        self.log_box.see("1.0")
        self.log_box.config(state="disabled")

    def show_run_mode(self) -> None:
        self.browse_frame.grid_remove()
        self.run_frame.grid()
        self.run_title.config(text="Building local dataset…")

        self.progress.stop()
        self.progress.config(mode="determinate")
        self.progress["value"] = 0
        self.progress["maximum"] = 100

        self.log_box.config(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.config(state="disabled")

    def show_browse_mode(self) -> None:
        self.run_frame.grid_remove()
        self.browse_frame.grid()

    # ============================================================
    # Tree rendering / navigation
    # ============================================================
    def clear_nav(self) -> None:
        self.nav_list.clear()

    def set_download_enabled(self, region_id: Optional[str]) -> None:
        nodes = self.state.get("nodes")
        if not nodes or not region_id:
            self.download_btn.config(state="disabled")
            return

        n = nodes.get(region_id)
        if not n or not n.get("pbf_url") or self.state.get("busy"):
            self.download_btn.config(state="disabled")
            return

        self.download_btn.config(state="normal")

    def go_up(self) -> None:
        nodes = self.state.get("nodes")
        if not nodes:
            return
        cur = self.state["current_parent_id"]
        parent_id = nodes.get(cur, {}).get("parent_id")
        if parent_id:
            self.state["current_parent_id"] = parent_id
            self.render_parent()

    def on_click(self, region_id: str) -> None:
        self.state["selected_id"] = region_id
        self.state["current_parent_id"] = region_id
        self.set_download_enabled(region_id)
        self.render_parent()

    def render_parent(self) -> None:
        nodes = self.state.get("nodes")
        if not nodes:
            return

        pid = self.state["current_parent_id"]
        self.parent_label.config(text=breadcrumb(nodes, pid))

        parent = nodes.get(pid)
        children = (parent.get("children_ids") or []) if parent else []

        # Up visibility
        is_root = (pid == "world") or (parent and parent.get("parent_id") is None)
        if is_root:
            self.up_btn.grid_remove()
        else:
            self.up_btn.grid()

        self.clear_nav()

        if not children:
            tk.Label(
                self.nav_list.inner,
                text="No sub-regions available.",
                bg=config.BG,
                fg=config.FG,
                font=config.BODY_FONT,
                anchor="w",
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=8)
            self.nav_list.reset_view()
            return

        self.nav_list.inner.grid_columnconfigure(0, weight=1)
        self.nav_list.inner.grid_columnconfigure(1, weight=1)

        for i, cid in enumerate(children):
            child = nodes.get(cid)
            if not child:
                continue

            r = i // 2
            c = i % 2

            tk.Button(
                self.nav_list.inner,
                text=child["name"],
                bg=config.BTN,
                fg=config.FG,
                command=lambda cc=cid: self.on_click(cc),
            ).grid(row=r, column=c, sticky="ew", padx=(0 if c == 0 else 8, 0), pady=4)

        self.nav_list.reset_view()

    # ============================================================
    # Pipeline runner (download + scrape + summary)
    # ============================================================
    def start_pipeline(self) -> None:
        nodes = self.state.get("nodes")
        sid = self.state.get("selected_id")
        if not nodes or not sid:
            return

        node = nodes.get(sid)
        if not node or not node.get("pbf_url"):
            return

        pbf_url = node["pbf_url"]
        region_id = node["id"]

        self.state["busy"] = True
        self.set_download_enabled(sid)

        self.show_run_mode()
        self.set_status("Starting…")

        def on_progress(done: int, total: Optional[int]):
            if total and total > 0:
                pct = int(done * 100 / total)
                self.progress.config(mode="determinate")
                self.progress["value"] = max(0, min(100, pct))
            else:
                if str(self.progress["mode"]) != "indeterminate":
                    self.progress.config(mode="indeterminate")
                    self.progress.start(10)

        def worker():
            try:
                result = run_local_data_pipeline(
                    region_id=region_id,
                    pbf_url=pbf_url,
                    on_status=lambda m: self.root.after(0, lambda: self.set_status(m)),
                    on_progress=lambda d, t: self.root.after(0, lambda: on_progress(d, t)),
                    on_log=lambda s: self.root.after(0, lambda: self._append_log(s)),
                )

                def finish_ok():
                    self.progress.stop()
                    self.progress.config(mode="determinate")
                    self.progress["value"] = 100

                    size_txt = human_size(result.gpkg_bytes)

                    summary_lines = [
                        "Local data ready ✅",
                        "",
                        f"Output folder: {result.out_dir}",
                        f"Output file: {os.path.basename(result.gpkg_path)} ({size_txt})",
                        "",
                        "Counts by layer:",
                    ]
                    for layer, cnt in sorted(result.layer_counts.items()):
                        summary_lines.append(f"  • {layer}: {cnt:,}")

                    self.run_title.config(text="Finished")
                    self._set_summary("\n".join(summary_lines))
                    self.set_status("Complete.")

                    self.state["busy"] = False
                    self.set_download_enabled(self.state.get("selected_id"))

                self.root.after(0, finish_ok)

            except Exception as e:
                err = str(e)

                def finish_err(err_msg=err):
                    self.progress.stop()
                    self.progress.config(mode="determinate")
                    self.progress["value"] = 0

                    self.run_title.config(text="Failed")
                    self._append_log(f"\nERROR: {err_msg}")
                    self.set_status(err_msg)

                    self.state["busy"] = False
                    self.set_download_enabled(self.state.get("selected_id"))

                self.root.after(0, finish_err)

        threading.Thread(target=worker, daemon=True).start()
