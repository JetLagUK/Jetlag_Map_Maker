import config

def draw_bbox(map_widget, bbox, width=3):
    south, west, north, east = bbox
    corners = [
        (north, west),
        (north, east),
        (south, east),
        (south, west),
        (north, west),
    ]
    if hasattr(map_widget, "set_path"):
        return [map_widget.set_path(corners, width=width)]
    if hasattr(map_widget, "set_polygon"):
        return [map_widget.set_polygon(corners, border_width=width)]
    return []


def poly_string_to_ring(poly_str: str):
    parts = poly_str.strip().split()
    if len(parts) < 6 or len(parts) % 2 != 0:
        return []
    ring = []
    for i in range(0, len(parts), 2):
        lat = float(parts[i])
        lon = float(parts[i + 1])
        ring.append((lat, lon))
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def draw_poly(map_widget, poly_str, width=3):
    ring = poly_string_to_ring(poly_str)
    if not ring:
        return []
    if hasattr(map_widget, "set_path"):
        return [map_widget.set_path(ring, width=width)]
    if hasattr(map_widget, "set_polygon"):
        return [map_widget.set_polygon(ring, border_width=width)]
    return []


def fit_to_area(map_widget):
    poly = getattr(config, "overpass_poly", None)
    bb = getattr(config, "bound_box", None) or getattr(config, "saved_bound_box", None)

    if poly:
        ring = poly_string_to_ring(poly)
        if ring:
            lats = [p[0] for p in ring]
            lons = [p[1] for p in ring]
            south, north = min(lats), max(lats)
            west, east = min(lons), max(lons)
            if hasattr(map_widget, "fit_bounding_box"):
                map_widget.fit_bounding_box((north, west), (south, east))
            else:
                map_widget.set_position((south + north) / 2, (west + east) / 2)
                map_widget.set_zoom(config.DEFAULT_MAP_ZOOM)
            return

    if bb:
        south, west, north, east = bb
        if hasattr(map_widget, "fit_bounding_box"):
            map_widget.fit_bounding_box((north, west), (south, east))
        else:
            map_widget.set_position((south + north) / 2, (west + east) / 2)
            map_widget.set_zoom(config.DEFAULT_MAP_ZOOM)
