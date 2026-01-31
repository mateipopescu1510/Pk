from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import shutil
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


log = logging.getLogger("ingest.gs_lardoon")


DEFAULT_BASE_URL = "https://tacviewnew.growlingsidewinder.com"
DEFAULT_MISSION_NAME = "Operation_Urban_Thunder"
DEFAULT_TIMEOUT = (10, 120)  # (connect, read) seconds


@dataclass(frozen=True)
class ReplayMeta:
    id: str
    title: str
    raw: Dict[str, Any]


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _load_state(state_path: Path) -> Dict[str, Any]:
    if not state_path.exists():
        return {"downloaded": {}, "last_run_utc": None}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        # If state is corrupted, do not silently overwrite; keep going with empty state.
        return {"downloaded": {}, "last_run_utc": None, "_warning": "state_corrupted"}


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: Tuple[int, int] = DEFAULT_TIMEOUT,
    stream: bool = False,
    max_retries: int = 5,
    backoff_s: float = 1.0,
) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, timeout=timeout, stream=stream)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            if attempt == max_retries:
                break
            sleep_s = backoff_s * (2 ** (attempt - 1))
            log.warning("Request failed (%s). Retry %d/%d in %.1fs: %s", url, attempt, max_retries, sleep_s, e)
            time.sleep(sleep_s)
    assert last_err is not None
    raise last_err


def list_replays(session: requests.Session, base_url: str) -> List[ReplayMeta]:
    url = f"{base_url.rstrip('/')}/api/replay"
    resp = _request_with_retries(session, "GET", url, stream=False)
    data = resp.json()

    out: List[ReplayMeta] = []
    if not isinstance(data, list):
        raise ValueError(f"Unexpected /api/replay payload type: {type(data)}")
    for item in data:
        if not isinstance(item, dict):
            continue
        rid = str(item.get("id", "")).strip()
        title = str(item.get("title", "")).strip()
        if not rid or not title:
            continue
        out.append(ReplayMeta(id=rid, title=title, raw=item))
    return out


def _pick_zip_member(z: zipfile.ZipFile) -> str:
    # Prefer an .acmi-like member, otherwise first member.
    names = z.namelist()
    for n in names:
        ln = n.lower()
        if ln.endswith(".acmi") or ln.endswith(".zip.acmi") or ln.endswith(".txt.acmi"):
            return n
    return names[0]


def _download_to_part_file(
    session: requests.Session,
    url: str,
    part_path: Path,
    *,
    timeout: Tuple[int, int],
    max_retries: int,
) -> None:
    _ensure_dir(part_path.parent)
    if part_path.exists():
        part_path.unlink()

    resp = _request_with_retries(
        session, "GET", url, timeout=timeout, stream=True, max_retries=max_retries
    )
    with part_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)


def _materialize_acmi(part_path: Path, final_path: Path) -> Dict[str, Any]:
    """
    Convert .part file to final .acmi:
      - If ZIP: extract chosen member to final_path
      - Else: move bytes to final_path
    Returns extraction metadata.
    """
    _ensure_dir(final_path.parent)

    meta: Dict[str, Any] = {
        "is_zip": False,
        "zip_member": None,
    }

    # zipfile.is_zipfile accepts a file path.
    if zipfile.is_zipfile(part_path):
        meta["is_zip"] = True
        with zipfile.ZipFile(part_path, "r") as z:
            member = _pick_zip_member(z)
            meta["zip_member"] = member
            with z.open(member) as src, final_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        part_path.unlink(missing_ok=True)
    else:
        # Not a zip: just move to final.
        os.replace(part_path, final_path)

    return meta


def ingest(
    *,
    base_url: str,
    out_dir: Path,
    state_path: Path,
    mission_filter: Optional[str],
    limit: Optional[int],
    timeout: Tuple[int, int],
    max_retries: int,
    dry_run: bool,
) -> int:
    _ensure_dir(out_dir)
    state = _load_state(state_path)
    downloaded: Dict[str, Any] = state.get("downloaded", {}) if isinstance(state.get("downloaded"), dict) else {}

    with requests.Session() as session:
        replays = list_replays(session, base_url)

        # Keep stable order: try numeric id, else lexicographic.
        def _sort_key(r: ReplayMeta) -> Tuple[int, str]:
            try:
                return (int(r.id), r.id)
            except Exception:
                return (sys.maxsize, r.id)

        replays.sort(key=_sort_key, reverse=False)

        selected: List[ReplayMeta] = []
        for r in replays:
            if mission_filter and mission_filter not in r.title:
                continue
            selected.append(r)
            if limit is not None and len(selected) >= limit:
                break

        new_count = 0
        for r in selected:
            if r.id in downloaded:
                continue

            # Always write as "{id}.txt.acmi" like your prototype, but keep room to change later.
            final_path = out_dir / f"{r.id}.txt.acmi"
            if final_path.exists() and final_path.stat().st_size > 0:
                # If the file exists, treat it as downloaded even if state was lost.
                downloaded[r.id] = {
                    "title": r.title,
                    "downloaded_utc": _utc_now_iso(),
                    "path": str(final_path),
                    "note": "file_preexisted",
                }
                log.info("Skip (already on disk): id=%s title=%s", r.id, r.title)
                continue

            download_url = f"{base_url.rstrip('/')}/api/replay/{r.id}/download"

            log.info("Fetch: id=%s title=%s", r.id, r.title)
            if dry_run:
                new_count += 1
                continue

            part_path = out_dir / f".{r.id}.part"
            _download_to_part_file(session, download_url, part_path, timeout=timeout, max_retries=max_retries)
            extract_meta = _materialize_acmi(part_path, final_path)
            sha = _sha256_file(final_path)

            downloaded[r.id] = {
                "title": r.title,
                "downloaded_utc": _utc_now_iso(),
                "path": str(final_path),
                "sha256": sha,
                "source": {
                    "base_url": base_url,
                    "download_url": download_url,
                },
                "zip": extract_meta,
                "api_meta": r.raw,  # keep original fields (size, date, etc if present)
            }
            new_count += 1

            # Persist state after each successful file (checkpointing).
            state["downloaded"] = downloaded
            state["last_run_utc"] = _utc_now_iso()
            _atomic_write_json(state_path, state)

    # Final persist (covers dry-run + ensures last_run_utc updates)
    state["downloaded"] = downloaded
    state["last_run_utc"] = _utc_now_iso()
    if not dry_run:
        _atomic_write_json(state_path, state)

    log.info("Done. New=%d TotalTracked=%d", new_count, len(downloaded))
    return new_count


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest Tacview replays from a Lardoon/Growling Sidewinder endpoint.")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p.add_argument("--mission-filter", default="Operation_Urban_Thunder")
    p.add_argument("--out-dir", default="data/raw")
    p.add_argument("--state", default="data/state/gs_lardoon_state.json")
    p.add_argument("--limit", type=int, default=None, help="Process at most N matching replays (debugging)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--max-retries", type=int, default=5)
    p.add_argument("--timeout-connect", type=int, default=10)
    p.add_argument("--timeout-read", type=int, default=120)
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return ingest(
        base_url=args.base_url,
        out_dir=Path(args.out_dir),
        state_path=Path(args.state),
        mission_filter=args.mission_filter,
        limit=args.limit,
        timeout=(args.timeout_connect, args.timeout_read),
        max_retries=args.max_retries,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
