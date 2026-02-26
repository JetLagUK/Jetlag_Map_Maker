# screens/shared/kml_export.py
import simplekml

def export_game_area_kml(*, path, config, hide_zone_data, circle_points):
    kml = simplekml.Kml()

    game_folder = kml.newfolder(name="Game area")
    zones_folder = kml.newfolder(name="Hiding zones")

    ICON_URLS = {
        "Train":  "https://raw.githubusercontent.com/JetLagUK/Jetlag_Map_Maker_V2.2/91abe3f21ac49551a2346a55bd3d956a3b1cf3e5/Jetlag_Map_Maker_V2.2/assets/train.png",
        "Subway": "https://raw.githubusercontent.com/JetLagUK/Jetlag_Map_Maker_V2.2/91abe3f21ac49551a2346a55bd3d956a3b1cf3e5/Jetlag_Map_Maker_V2.2/assets/subway.png",
        "Tram":   "https://raw.githubusercontent.com/JetLagUK/Jetlag_Map_Maker_V2.2/91abe3f21ac49551a2346a55bd3d956a3b1cf3e5/Jetlag_Map_Maker_V2.2/assets/tram.png",
        "Bus":    "https://raw.githubusercontent.com/JetLagUK/Jetlag_Map_Maker_V2.2/91abe3f21ac49551a2346a55bd3d956a3b1cf3e5/Jetlag_Map_Maker_V2.2/assets/bus.png",
    }

    styles = {}
    for t, url in ICON_URLS.items():
        s = simplekml.Style()
        s.iconstyle.icon.href = url
        styles[t] = s

    # Stops
    for t in ("Train", "Subway", "Tram", "Bus"):
        df = config.all_data.get(t)
        if df is None or df.empty:
            continue

        for row in df.itertuples(index=False):
            name = getattr(row, "Name", "Unnamed") or "Unnamed"
            lat = float(row.Latitude)
            lon = float(row.Longitude)

            p = game_folder.newpoint(name=name, coords=[(lon, lat)])
            p.extendeddata.newdata(name="Type", value=t)
            if t in styles:
                p.style = styles[t]

    # Zones
    for i, (lat, lon, radius_m) in enumerate(hide_zone_data, start=1):
        pts = circle_points(lat, lon, radius_m, segments=36)
        if not pts:
            continue

        ring = [(lo, la) for la, lo in pts]
        if ring[0] != ring[-1]:
            ring.append(ring[0])

        pol = zones_folder.newpolygon(name=f"Hiding zone {i}")
        pol.outerboundaryis = ring
        pol.tessellate = 1
        pol.style.linestyle.width = 1
        pol.style.linestyle.color = simplekml.Color.red
        pol.style.polystyle.fill = 1
        pol.style.polystyle.color = simplekml.Color.changealphaint(60, simplekml.Color.red)
        pol.extendeddata.newdata(name="Radius_m", value=str(int(radius_m)))

    # Bounding box
    if config.bound_box:
        south, west, north, east = config.bound_box
        bbox_coords = [
            (west, north),
            (east, north),
            (east, south),
            (west, south),
            (west, north),
        ]
        bbox_poly = kml.newpolygon(name="Bounding Box")
        bbox_poly.outerboundaryis = bbox_coords
        bbox_poly.tessellate = 1
        bbox_poly.style.linestyle.width = 2
        bbox_poly.style.linestyle.color = simplekml.Color.blue
        bbox_poly.style.polystyle.fill = 0
        bbox_poly.extendeddata.newdata(name="Type", value="Bounding Box")

    kml.save(path)
