import math
import pandas as pd

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def deduplicate_all_by_priority(all_data: dict, threshold_m: int):
    if threshold_m <= 0:
        return {"Train": 0, "Subway": 0, "Tram": 0, "Bus": 0}, 0

    priority_keep_order = ["Train", "Subway", "Tram", "Bus"]

    dfs = [all_data.get(t) for t in priority_keep_order]
    dfs = [d for d in dfs if d is not None and not d.empty]
    if not dfs:
        return {"Train": 0, "Subway": 0, "Tram": 0, "Bus": 0}, 0

    all_concat = pd.concat(dfs, ignore_index=True)
    mean_lat = float(all_concat["Latitude"].mean())

    m_per_deg_lat = 111_320.0
    m_per_deg_lon = 111_320.0 * math.cos(math.radians(mean_lat))
    cell_size_m = float(threshold_m)

    def cell_key(lat, lon):
        x = lon * m_per_deg_lon
        y = lat * m_per_deg_lat
        return (int(x // cell_size_m), int(y // cell_size_m))

    kept_points = []
    grid = {}

    def too_close_to_kept(lat, lon):
        cx, cy = cell_key(lat, lon)
        for nx in (cx - 1, cx, cx + 1):
            for ny in (cy - 1, cy, cy + 1):
                for kp_i in grid.get((nx, ny), []):
                    klat, klon = kept_points[kp_i]
                    if haversine_m(lat, lon, klat, klon) <= threshold_m:
                        return True
        return False

    def add_kept(lat, lon):
        cx, cy = cell_key(lat, lon)
        kept_points.append((lat, lon))
        idx = len(kept_points) - 1
        grid.setdefault((cx, cy), []).append(idx)

    removed_counts = {"Train": 0, "Subway": 0, "Tram": 0, "Bus": 0}

    for t in priority_keep_order:
        df = all_data.get(t)
        if df is None or df.empty:
            continue

        keep_rows = []
        df2 = df.reset_index(drop=True)

        for i, row in df2.iterrows():
            lat = float(row["Latitude"])
            lon = float(row["Longitude"])
            if too_close_to_kept(lat, lon):
                removed_counts[t] += 1
                continue
            keep_rows.append(i)
            add_kept(lat, lon)

        all_data[t] = df2.iloc[keep_rows].reset_index(drop=True)

    total_removed = sum(removed_counts.values())
    return removed_counts, total_removed
