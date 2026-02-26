# poi/kml_merge.py
from __future__ import annotations

import os
import pandas as pd
import xml.etree.ElementTree as ET


KML_NS = "http://www.opengis.net/kml/2.2"
NS = {"kml": KML_NS}
ET.register_namespace("", KML_NS)


def _k(tag: str) -> str:
    return f"{{{KML_NS}}}{tag}"


def _abspath_file_href(path: str) -> str:
    # Windows-friendly file:// URL
    ap = os.path.abspath(path).replace("\\", "/")
    return "file:///" + ap


def _kml_color_from_hex_rgb(rgb: str, alpha: int = 255) -> str:
    # "#RRGGBB" -> "AABBGGRR"
    rgb = rgb.strip().lstrip("#")
    if len(rgb) != 6:
        return "ff0000ff"
    r = int(rgb[0:2], 16)
    g = int(rgb[2:4], 16)
    b = int(rgb[4:6], 16)
    return f"{alpha:02x}{b:02x}{g:02x}{r:02x}"


def _ensure_document(tree: ET.ElementTree) -> ET.Element:
    root = tree.getroot()
    doc = root.find("kml:Document", NS)
    if doc is None:
        doc = ET.SubElement(root, _k("Document"))
    return doc


def _remove_existing_folder(doc: ET.Element, folder_name: str) -> None:
    # Remove top-level Folder with matching <name>
    for folder in list(doc.findall("kml:Folder", NS)):
        name_el = folder.find("kml:name", NS)
        if name_el is not None and (name_el.text or "").strip() == folder_name:
            doc.remove(folder)


def _ensure_style_icon(doc: ET.Element, style_id: str, icon_href: str) -> None:
    # If style already exists, skip
    for s in doc.findall("kml:Style", NS):
        if s.get("id") == style_id:
            return

    style = ET.SubElement(doc, _k("Style"), id=style_id)
    icon_style = ET.SubElement(style, _k("IconStyle"))
    ET.SubElement(icon_style, _k("scale")).text = "1.0"
    icon = ET.SubElement(icon_style, _k("Icon"))
    ET.SubElement(icon, _k("href")).text = icon_href


def _ensure_style_line(doc: ET.Element, style_id: str, rgb: str = "#66ccff", width: int = 3) -> None:
    for s in doc.findall("kml:Style", NS):
        if s.get("id") == style_id:
            return

    style = ET.SubElement(doc, _k("Style"), id=style_id)
    ls = ET.SubElement(style, _k("LineStyle"))
    ET.SubElement(ls, _k("color")).text = _kml_color_from_hex_rgb(rgb, 255)
    ET.SubElement(ls, _k("width")).text = str(width)


def _add_point(folder: ET.Element, name: str, lat: float, lon: float, style_url: str | None) -> None:
    pm = ET.SubElement(folder, _k("Placemark"))
    ET.SubElement(pm, _k("name")).text = name or ""
    if style_url:
        ET.SubElement(pm, _k("styleUrl")).text = style_url

    pt = ET.SubElement(pm, _k("Point"))
    ET.SubElement(pt, _k("coordinates")).text = f"{lon},{lat},0"


def _add_line(folder: ET.Element, name: str, latlon_path: list[tuple[float, float]], style_url: str | None) -> None:
    # latlon_path: [(lat, lon), ...]
    if not latlon_path or len(latlon_path) < 2:
        return

    pm = ET.SubElement(folder, _k("Placemark"))
    ET.SubElement(pm, _k("name")).text = name or ""
    if style_url:
        ET.SubElement(pm, _k("styleUrl")).text = style_url

    ls = ET.SubElement(pm, _k("LineString"))
    ET.SubElement(ls, _k("tessellate")).text = "1"
    coords = " ".join([f"{lon},{lat},0" for (lat, lon) in latlon_path])
    ET.SubElement(ls, _k("coordinates")).text = coords


def merge_pois_into_existing_kml(
    existing_kml_path: str,
    out_kml_path: str,
    poi_data: dict[str, pd.DataFrame],
    icon_file_by_type: dict[str, str],
    poi_icon_dir: str,
    *,
    top_folder_name: str = "Points of Interest",
    line_rgb: str = "#66ccff",
) -> None:
    """
    Load an existing KML (from Regions screen), add/replace a top-level folder with POIs.
    """
    tree = ET.parse(existing_kml_path)
    doc = _ensure_document(tree)

    # Replace previous POI folder if present
    _remove_existing_folder(doc, top_folder_name)

    # Shared line style for coastline + water lines
    _ensure_style_line(doc, "poi_line_style", rgb=line_rgb, width=3)

    # Create POI top folder
    top = ET.SubElement(doc, _k("Folder"))
    ET.SubElement(top, _k("name")).text = top_folder_name

    # One folder per type
    for tname, df in (poi_data or {}).items():
        if df is None or df.empty:
            continue

        type_folder = ET.SubElement(top, _k("Folder"))
        ET.SubElement(type_folder, _k("name")).text = tname

        # icon style for point types (if icon exists)
        style_url = None
        fname = icon_file_by_type.get(tname)
        if fname:
            icon_path = os.path.join(poi_icon_dir, fname)
            if os.path.exists(icon_path):
                style_id = f"poi_icon_{tname}".replace(" ", "_").replace("/", "_")
                _ensure_style_icon(doc, style_id, _abspath_file_href(icon_path))
                style_url = f"#{style_id}"

        # Decide what rows are points vs lines
        has_geom = "Geometry" in df.columns
        has_latlon = ("Latitude" in df.columns and "Longitude" in df.columns)

        for _, row in df.iterrows():
            name = str(row.get("Name", "") or "").strip()

            # Lines (coastline / rivers / canals / streams)
            if has_geom:
                geom = row.get("Geometry", None)
                if isinstance(geom, (list, tuple)) and len(geom) >= 2:
                    # Give a sensible line name if blank
                    kind = str(row.get("Kind", "") or "").strip()
                    line_name = name if name else f"{tname} {kind}".strip()
                    _add_line(type_folder, line_name, list(geom), style_url="#poi_line_style")
                    continue

            # Points
            if has_latlon:
                try:
                    lat = float(row["Latitude"])
                    lon = float(row["Longitude"])
                except Exception:
                    continue

                # If label is blank, still export
                _add_point(type_folder, name or tname, lat, lon, style_url=style_url)

    tree.write(out_kml_path, encoding="utf-8", xml_declaration=True)
