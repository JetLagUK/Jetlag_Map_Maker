
from __future__ import annotations

from typing import Optional, Tuple, Dict, Any
import requests

GEOFABRIK_INDEX_URL = "https://download.geofabrik.de/index-v1-nogeom.json"


def pretty_from_id(node_id: str) -> str:
    leaf = (node_id or "").split("/")[-1]
    return leaf.replace("-", " ").strip().title() if leaf else (node_id or "Unknown")


def fetch_geofabrik_index(url: str = GEOFABRIK_INDEX_URL) -> dict:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def flatten_geofabrik_index(index_json: dict) -> Tuple[Dict[str, Dict[str, Any]], str]:
    """
    Build nodes from FLAT FeatureCollection using properties.parent.

    Fixes:
      - Use existing 'us' node for United States of America
      - Force 'us/<state>' children under 'us'
      - Display US state names without 'us/' prefix
      - Force UK regions under 'united-kingdom'
      - Rebuild children lists from scratch to avoid duplicates
    """

    def norm_id(x: Optional[str]) -> Optional[str]:
        if not x:
            return None
        return str(x).strip().replace("\\", "/").strip("/")

    nodes: Dict[str, Dict[str, Any]] = {
        "world": {
            "id": "world",
            "name": "World",
            "parent_id": None,
            "children_ids": [],
            "pbf_url": None,
        }
    }

    features = index_json.get("features", [])
    if not isinstance(features, list):
        features = []

    # 1) Create all nodes
    for f in features:
        props = f.get("properties", {}) if isinstance(f, dict) else {}
        node_id = norm_id(props.get("id"))
        if not node_id:
            continue

        parent_id = norm_id(props.get("parent"))
        name = props.get("name") or pretty_from_id(node_id)

        # Clean US state display names
        if node_id.startswith("us/"):
            name = pretty_from_id(node_id)

        urls = props.get("urls") if isinstance(props.get("urls"), dict) else {}
        pbf_url = urls.get("pbf")

        nodes[node_id] = {
            "id": node_id,
            "name": name,
            "parent_id": parent_id,
            "children_ids": [],
            "pbf_url": pbf_url,
        }

    # 2) Build basename map for resolving short parents
    basename_map = {}
    for nid in nodes.keys():
        if nid == "world":
            continue
        base = nid.split("/")[-1]
        basename_map.setdefault(base, []).append(nid)

    def resolve_parent(pid: Optional[str]) -> Optional[str]:
        if not pid:
            return None
        if pid in nodes:
            return pid

        base = pid.split("/")[-1]
        cands = basename_map.get(base, [])
        if not cands:
            return None

        # pick the "closest to root" (fewest slashes)
        cands.sort(key=lambda s: (s.count("/"), len(s)))
        return cands[0]

    uk_id = "united-kingdom" if "united-kingdom" in nodes else resolve_parent("united-kingdom")
    uk_children = {"england", "scotland", "wales", "northern-ireland", "bermuda", "falklands"}

    # 3) Assign final parent_id for each node (no children built yet)
    for nid, node in nodes.items():
        if nid == "world":
            continue

        raw_parent = node.get("parent_id")
        pid = resolve_parent(raw_parent)

        # Force US states under existing USA node id "us"
        if nid.startswith("us/") and "us" in nodes:
            pid = "us"

        # Force UK regions under UK node
        if uk_id:
            if raw_parent == "united-kingdom":
                pid = uk_id
            if nid in uk_children:
                pid = uk_id
            elif isinstance(raw_parent, str) and raw_parent.replace("\\", "/").strip("/").endswith("united-kingdom"):
                pid = uk_id

        if pid and pid in nodes and pid != nid:
            node["parent_id"] = pid
        else:
            node["parent_id"] = "world"

    # 4) Rebuild children lists from scratch
    for nid in nodes:
        nodes[nid]["children_ids"] = []

    for nid, node in nodes.items():
        if nid == "world":
            continue

        pid = node.get("parent_id") or "world"
        if pid not in nodes or pid == nid:
            pid = "world"
            node["parent_id"] = "world"

        nodes[pid]["children_ids"].append(nid)

    # 5) Sort children for stable UI
    for nid in nodes:
        nodes[nid]["children_ids"].sort(key=lambda cid: nodes[cid]["name"].lower())

    return nodes, "world"


def breadcrumb(nodes: dict, current_id: str) -> str:
    parts = []
    cur = current_id
    while cur:
        n = nodes.get(cur)
        if not n:
            break
        parts.append(n["name"])
        cur = n["parent_id"]
    parts.reverse()
    return " → ".join(parts)