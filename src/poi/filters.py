import pandas as pd

from .utils import norm_str, parse_int_tag, haversine_m


def is_excluded_park(tags: dict, name: str) -> bool:
    name_l = norm_str(name)

    amenity = norm_str(tags.get("amenity"))
    landuse = norm_str(tags.get("landuse"))
    leisure = norm_str(tags.get("leisure"))
    cemetery = norm_str(tags.get("cemetery"))
    historic = norm_str(tags.get("historic"))

    if amenity in ("grave_yard", "cemetery"):
        return True

    if landuse in ("cemetery", "religious"):
        if cemetery in ("churchyard", "graveyard") or "church" in name_l:
            return True
        if "cemetery" in name_l or "graveyard" in name_l or "churchyard" in name_l:
            return True

    if cemetery == "churchyard":
        return True
    if historic == "churchyard":
        return True
    if "churchyard" in name_l or "graveyard" in name_l or "cemetery" in name_l:
        return True

    if leisure == "garden":
        return True
    if " garden" in name_l or name_l.endswith(" garden") or " gardens" in name_l:
        return True

    return False


def is_non_building_museum(tags: dict, name: str) -> bool:
    amenity = norm_str(tags.get("amenity"))
    tourism = norm_str(tags.get("tourism"))
    building = norm_str(tags.get("building"))
    museum = norm_str(tags.get("museum"))
    heritage = norm_str(tags.get("heritage"))
    name_l = norm_str(name)

    if museum == "open_air":
        return True
    if tourism == "attraction" and "museum" in name_l:
        return True

    if tourism == "archaeological_site":
        return True
    if heritage and heritage != "yes":
        return True

    if "open air" in name_l or "open-air" in name_l:
        return True
    if "heritage site" in name_l:
        return True
    if "outdoor" in name_l:
        return True

    if amenity == "museum" and building in ("no", "roof", "tent"):
        return True

    return False


def is_excluded_golf_course(tags: dict, name: str) -> bool:
    name_l = norm_str(name)

    golf = norm_str(tags.get("golf"))
    leisure = norm_str(tags.get("leisure"))

    # Explicit non-courses / practice
    if golf in ("driving_range", "practice"):
        return True

    # Mini golf
    if leisure == "miniature_golf":
        return True
    if golf in ("miniature", "miniature_golf", "minigolf"):
        return True

    # Pitch & putt / par-3 (tag-based)
    if golf in ("pitch_and_putt", "pitch_putt", "par3", "par_3", "par3_course", "par_3_course"):
        return True

    course = norm_str(tags.get("course"))
    course_type = norm_str(tags.get("course:type"))
    if course in ("pitch_and_putt", "par3", "par_3") or course_type in ("pitch_and_putt", "par3", "par_3"):
        return True

    # Small courses by holes
    holes = parse_int_tag(tags, "holes") or parse_int_tag(tags, "golf:holes")
    if holes > 0 and holes < 18:
        return True

    # Name-based fallback
    if "pitch and putt" in name_l:
        return True
    if "pitch & putt" in name_l:
        return True
    if "par 3" in name_l or "par-3" in name_l:
        return True
    if "mini golf" in name_l or "minigolf" in name_l:
        return True

    return False


def is_private_hospital(tags: dict) -> bool:
    op_type = norm_str(tags.get("operator:type"))
    ownership = norm_str(tags.get("ownership"))
    access = norm_str(tags.get("access"))

    if op_type == "private":
        return True
    if ownership == "private":
        return True
    if access == "private":
        return True

    return False


def is_excluded_hospital(tags: dict, name: str) -> bool:
    amenity = norm_str(tags.get("amenity"))
    healthcare = norm_str(tags.get("healthcare"))
    speciality = norm_str(tags.get("healthcare:speciality"))
    hospice_flag = norm_str(tags.get("hospice"))
    name_l = norm_str(name)

    # Hospices
    if amenity == "hospice" or healthcare == "hospice":
        return True
    if hospice_flag in ("yes", "true", "1"):
        return True
    if "hospice" in name_l:
        return True

    # Research centres
    if amenity == "research_institute":
        return True
    if "research centre" in name_l or "research center" in name_l:
        return True
    if "research institute" in name_l:
        return True

    # Resource centres / rehab-type units
    if "resource centre" in name_l or "resource center" in name_l:
        return True
    if "resource unit" in name_l:
        return True
    if amenity in ("social_facility", "community_centre"):
        return True
    if healthcare in ("rehabilitation", "physiotherapy", "occupational_therapy"):
        return True
    if speciality in ("rehabilitation", "physiotherapy", "occupational_therapy"):
        return True

    # Day hospitals / outpatient units
    if "day hospital" in name_l:
        return True
    if "day unit" in name_l:
        return True
    if "outpatient" in name_l or "out-patient" in name_l:
        return True
    if healthcare in ("day_care", "outpatient", "clinic"):
        return True

    return False


def merge_nearby_hospitals(df: pd.DataFrame, radius_m: float = 500.0) -> pd.DataFrame:
    """
    Merge/cluster hospitals within radius_m.
    Keeps the "largest" as anchor (Beds desc), discards nearby smaller ones.
    """
    if df is None or df.empty:
        return df

    work = df.copy()
    if "Beds" not in work.columns:
        work["Beds"] = 0

    work["Beds"] = pd.to_numeric(work["Beds"], errors="coerce").fillna(0).astype(int)
    work = work.sort_values(by=["Beds", "Name"], ascending=[False, True]).reset_index(drop=True)

    kept_rows = []
    removed = [False] * len(work)

    for i in range(len(work)):
        if removed[i]:
            continue

        anchor = work.iloc[i]
        kept_rows.append(anchor)

        a_lat = float(anchor["Latitude"])
        a_lon = float(anchor["Longitude"])

        for j in range(i + 1, len(work)):
            if removed[j]:
                continue
            cand = work.iloc[j]
            d = haversine_m(a_lat, a_lon, float(cand["Latitude"]), float(cand["Longitude"]))
            if d <= radius_m:
                removed[j] = True

    return pd.DataFrame(kept_rows).reset_index(drop=True)
