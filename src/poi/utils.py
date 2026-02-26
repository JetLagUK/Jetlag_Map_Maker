import math

def norm_str(v) -> str:
    return str(v).strip().lower() if v is not None else ""

def clean_name(name) -> str:
    if name is None:
        return ""
    s = str(name).strip()
    if not s:
        return ""
    if s.lower() == "unnamed":
        return ""
    return s

def parse_int_tag(tags: dict, key: str) -> int:
    raw = tags.get(key)
    if raw is None:
        return 0
    s = str(raw).strip()
    if not s:
        return 0

    for part in s.replace(";", " ").replace(",", " ").split():
        try:
            return int(float(part))
        except Exception:
            continue
    return 0

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))
