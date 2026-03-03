from __future__ import annotations

import os
import sys
import time
import sqlite3
import subprocess
from dataclasses import dataclass
from typing import Callable, Dict, Optional

import requests


@dataclass
class PipelineResult:
    out_dir: str
    gpkg_path: str
    gpkg_bytes: int
    layer_counts: Dict[str, int]


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _project_root_from_this_file() -> str:
    """
    Assuming: src/screens/shared/local_data_pipeline.py
    project_root is three levels up from this file's folder.
    Adjust if your structure differs.
    """
    here = os.path.dirname(os.path.abspath(__file__))  # .../src/screens/shared
    return os.path.abspath(os.path.join(here, "..", "..", ".."))  # .../project_root


def _download_with_progress(
    url: str,
    dest_path: str,
    on_progress: Callable[[int, Optional[int]], None],
    on_log: Callable[[str], None],
    chunk_size: int = 1024 * 256,
    timeout: int = 60,
) -> None:
    """
    Downloads url -> dest_path
    Calls on_progress(downloaded_bytes, total_bytes_or_None)
    """
    _ensure_dir(os.path.dirname(dest_path))

    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        total = r.headers.get("Content-Length")
        total_bytes = int(total) if total and total.isdigit() else None

        tmp_path = dest_path + ".part"
        downloaded = 0

        on_log(f"[DOWNLOAD] {url}")
        if total_bytes:
            on_log(f"[DOWNLOAD] Total: {total_bytes:,} bytes")

        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                on_progress(downloaded, total_bytes)

        os.replace(tmp_path, dest_path)
        on_log(f"[DOWNLOAD] Saved -> {dest_path}")


def _run_scraper_subprocess(
    pbf_path: str,
    out_dir: str,
    on_log: Callable[[str], None],
    project_root: str,
) -> None:
    """
    Runs your 4-pass pipeline via Data_Packeger.py (CLI mode) and streams output.
    """
    this_dir = os.path.dirname(os.path.abspath(__file__))  # .../src/screens/shared
    packager_py = os.path.join(this_dir, "Data_Packeger.py")

    if not os.path.exists(packager_py):
        raise FileNotFoundError(f"Missing scraper entrypoint: {packager_py}")

    cmd = [sys.executable, packager_py, pbf_path, out_dir]
    on_log("[SCRAPER] " + " ".join(cmd))

    # Stream stdout+stderr together so you see prints and tracebacks live
    p = subprocess.Popen(
        cmd,
        cwd=project_root,  # ✅ ensure project root on sys.path for imports
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    assert p.stdout is not None
    for line in p.stdout:
        line = line.rstrip("\n")
        if line:
            on_log(line)

    rc = p.wait()
    if rc != 0:
        raise RuntimeError(f"Scraper failed (exit code {rc}). See log output above.")


def _gpkg_layer_counts(gpkg_path: str) -> Dict[str, int]:
    """
    Fast counts via SQLite (GeoPackage is a SQLite DB).
    Uses gpkg_contents to list layers, then COUNT(*) each table.
    """
    if not os.path.exists(gpkg_path):
        return {}

    counts: Dict[str, int] = {}
    con = sqlite3.connect(gpkg_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT table_name FROM gpkg_contents;")
        tables = [row[0] for row in cur.fetchall()]

        for t in tables:
            cur.execute(f'SELECT COUNT(*) FROM "{t}";')
            counts[t] = int(cur.fetchone()[0])
    finally:
        con.close()

    return counts


def run_local_data_pipeline(
    *,
    region_id: str,
    region_name: Optional[str] = None,
    pbf_url: str,
    on_status: Callable[[str], None],
    on_progress: Callable[[int, Optional[int]], None],
    on_log: Optional[Callable[[str], None]] = None,
) -> PipelineResult:
    """
    Main pipeline: download -> scrape -> stats -> coverage -> cleanup
    Intended to run in a background thread.
    """
    if on_log is None:
        on_log = lambda _msg: None  # noqa: E731

    project_root = _project_root_from_this_file()

    # Put outputs in a predictable project folder
    out_base = os.path.join(project_root, "local_data_outputs")
    safe_region = region_id.replace("/", "_")
    out_dir = os.path.join(out_base, f"{safe_region}_local_data")

    # If it already exists, wipe it completely
    if os.path.exists(out_dir):
        import shutil
        shutil.rmtree(out_dir)

    _ensure_dir(out_dir)

    pbf_path = os.path.join(out_dir, f"{safe_region}.osm.pbf")

    on_status("Downloading PBF…")
    _download_with_progress(pbf_url, pbf_path, on_progress, on_log)

    on_status("Running scraper…")
    _run_scraper_subprocess(pbf_path, out_dir, on_log, project_root)

    gpkg_clean = os.path.join(out_dir, "layers_clean.gpkg")
    gpkg_raw = os.path.join(out_dir, "layers.gpkg")

    gpkg_path = gpkg_clean if os.path.exists(gpkg_clean) else gpkg_raw
    gpkg_bytes = os.path.getsize(gpkg_path) if os.path.exists(gpkg_path) else 0

    on_status("Computing summary…")
    layer_counts = _gpkg_layer_counts(gpkg_path)

    # ---- Save coverage (Geofabrik .poly -> coverage.geojson) ----
    on_status("Saving coverage…")
    from pathlib import Path
    from .coverage_utils import (
        poly_url_from_pbf_url, download_text, parse_geofabrik_poly, save_coverage_geojson
    )

    rn = region_name or region_id
    poly_url = poly_url_from_pbf_url(pbf_url)
    poly_txt_path = Path(out_dir) / "coverage.poly"

    try:
        download_text(poly_url, poly_txt_path)
        geom = parse_geofabrik_poly(poly_txt_path.read_text(encoding="utf-8", errors="ignore"))
        save_coverage_geojson(Path(out_dir), geom, region_id=region_id, region_name=rn)
        on_log(f"[COVERAGE] Saved coverage.geojson for {rn}")
    except Exception as e:
        on_log(f"[COVERAGE] Failed to save coverage: {e}")

    # Cleanup: delete the downloaded PBF
    try:
        if os.path.exists(pbf_path):
            os.remove(pbf_path)
            on_log(f"[CLEANUP] Deleted PBF: {pbf_path}")
    except Exception as e:
        on_log(f"[CLEANUP] Could not delete PBF: {e}")

    on_status("Done.")
    return PipelineResult(
        out_dir=out_dir,
        gpkg_path=gpkg_path,
        gpkg_bytes=gpkg_bytes,
        layer_counts=layer_counts,
    )