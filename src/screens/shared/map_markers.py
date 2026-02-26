import tkinter as tk

class MapMarkers:
    def __init__(self, *, root, map_widget, icons, transparent_icon: tk.PhotoImage):
        self.root = root
        self.map_widget = map_widget
        self.icons = icons
        self.transparent_icon = transparent_icon

        self.markers_by_type = {"Train": [], "Tram": [], "Bus": [], "Subway": []}
        self.label_marker = {"obj": None}
        self._label_after_id = {"id": None}

        # Clicking empty map clears label
        if hasattr(self.map_widget, "add_left_click_map_command"):
            self.map_widget.add_left_click_map_command(lambda _coords: self.schedule_clear_label())

    def _delete_label_marker(self):
        if self.label_marker["obj"] is not None:
            try:
                self.label_marker["obj"].delete()
            except Exception:
                pass
            self.label_marker["obj"] = None

    def schedule_clear_label(self):
        if self._label_after_id["id"] is not None:
            try:
                self.root.after_cancel(self._label_after_id["id"])
            except Exception:
                pass
            self._label_after_id["id"] = None
        self._label_after_id["id"] = self.root.after(0, self._delete_label_marker)

    def schedule_show_label(self, lat, lon, name):
        if self._label_after_id["id"] is not None:
            try:
                self.root.after_cancel(self._label_after_id["id"])
            except Exception:
                pass
            self._label_after_id["id"] = None

        def _apply():
            self._delete_label_marker()
            lat_offset = 0.00035
            self.label_marker["obj"] = self.map_widget.set_marker(
                float(lat) + lat_offset,
                float(lon),
                text=name,
                icon=self.transparent_icon,
                icon_anchor="center"
            )

        self._label_after_id["id"] = self.root.after(0, _apply)

    def clear_markers(self, type_name: str):
        for m in self.markers_by_type.get(type_name, []):
            try:
                m.delete()
            except Exception:
                pass
        self.markers_by_type[type_name] = []

    def _make_marker_click(self, lat, lon, name):
        def _handler(*_args, **_kwargs):
            self.schedule_show_label(lat, lon, name)
        return _handler

    def plot_points(self, type_name, df, limit=500):
        self.clear_markers(type_name)

        if df is None or df.empty:
            return

        icon = self.icons.get(type_name)

        count = 0
        for row in df.itertuples(index=False):
            on_click = self._make_marker_click(row.Latitude, row.Longitude, row.Name)
            try:
                marker = self.map_widget.set_marker(
                    row.Latitude,
                    row.Longitude,
                    icon=icon,
                    icon_anchor="center",
                    command=on_click
                )
            except TypeError:
                marker = self.map_widget.set_marker(
                    row.Latitude,
                    row.Longitude,
                    icon=icon,
                    command=on_click
                )

            self.markers_by_type[type_name].append(marker)
            count += 1
            if count >= limit:
                break
