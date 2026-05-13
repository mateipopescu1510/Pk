"""
Scrape ACMI replay files from a Lardoon/Growling Sidewinder endpoint.

State is checkpointed to data/state/gs_state.json after each download so
interrupted runs can resume without re-downloading files.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://tacviewnew.growlingsidewinder.com"
DEFAULT_MISSION_FILTER = "Operation_Urban_Thunder"


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_json_write(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _load_state(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"downloaded": {}, "last_run_utc": None}


def _request(session: requests.Session, url: str, stream: bool = False, retries: int = 5) -> requests.Response:
    delay = 1.0
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = session.request("GET", url, timeout=(10, 120), stream=stream)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                log.warning("Retry %d/%d for %s: %s", attempt + 1, retries, url, e)
                time.sleep(delay)
                delay *= 2
    raise last_exc  # type: ignore[misc]


def _extract_acmi(part: Path, dest: Path) -> bool:
    """Move/extract part file to dest. Returns True if it was a zip."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if zipfile.is_zipfile(part):
        with zipfile.ZipFile(part) as z:
            member = next((n for n in z.namelist() if n.lower().endswith(".acmi")), z.namelist()[0])
            with z.open(member) as src, dest.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        part.unlink()
        return True
    os.replace(part, dest)
    return False


def ingest(
    base_url: str = DEFAULT_BASE_URL,
    out_dir: str = "data/raw",
    state_path: str = "data/state/gs_state.json",
    mission_filter: str = DEFAULT_MISSION_FILTER,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> int:
    """Download new ACMI files. Returns count of newly downloaded files."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    state_file = Path(state_path)
    state = _load_state(state_file)
    downloaded: Dict[str, Any] = state.get("downloaded", {})

    with requests.Session() as session:
        replays: List[dict] = _request(session, f"{base_url}/api/replay").json()
        replays = [r for r in replays if isinstance(r, dict) and mission_filter in str(r.get("title", ""))]
        replays.sort(key=lambda r: (int(r["id"]) if str(r.get("id", "")).isdigit() else 0))
        if limit:
            replays = replays[:limit]

        new = 0
        for r in replays:
            rid = str(r["id"])
            if rid in downloaded:
                continue
            dest = out / f"{rid}.txt.acmi"
            if dest.exists() and dest.stat().st_size > 0:
                downloaded[rid] = {"note": "preexisted", "downloaded_utc": _now_iso()}
                continue

            log.info("Downloading id=%s title=%s", rid, r.get("title"))
            if dry_run:
                new += 1
                continue

            part = out / f".{rid}.part"
            resp = _request(session, f"{base_url}/api/replay/{rid}/download", stream=True)
            with part.open("wb") as f:
                for chunk in resp.iter_content(1 << 18):
                    if chunk:
                        f.write(chunk)

            _extract_acmi(part, dest)
            downloaded[rid] = {"title": r.get("title"), "downloaded_utc": _now_iso(), "sha256": _sha256(dest)}
            new += 1

            state["downloaded"] = downloaded
            state["last_run_utc"] = _now_iso()
            _atomic_json_write(state_file, state)

    state["downloaded"] = downloaded
    state["last_run_utc"] = _now_iso()
    if not dry_run:
        _atomic_json_write(state_file, state)

    log.info("Done. new=%d total_tracked=%d", new, len(downloaded))
    return new


def main() -> None:
    p = argparse.ArgumentParser(description="Download ACMI replays from Growling Sidewinder.")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p.add_argument("--out-dir", default="data/raw")
    p.add_argument("--state", default="data/state/gs_state.json")
    p.add_argument("--mission-filter", default=DEFAULT_MISSION_FILTER)
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = p.parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s: %(message)s")
    ingest(args.base_url, args.out_dir, args.state, args.mission_filter, args.limit, args.dry_run)


if __name__ == "__main__":
    main()
