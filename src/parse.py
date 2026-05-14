"""
Parse ACMI text files into SQLite objects and samples tables.

The T= field is variable-length; the field count determines the layout:
  3: lon|lat|alt                  5: lon|lat|alt|U|V
  6: lon|lat|alt|roll|pitch|yaw   9: lon|lat|alt|roll|pitch|yaw|U|V|hdg
Only lon|lat|alt are present in every variant, so we ignore the U/V slots
(their index shifts between layouts) and project lon/lat to local meters
ourselves using the file's ReferenceLongitude/Latitude header. Empty
field = carry forward.
"""
from __future__ import annotations

import math
import re
import sqlite3
from pathlib import Path
from typing import Optional

MISSILE_NAMES = (
    "aim_120", "aim-120", "aim_9", "aim-9", "aim_7", "aim-7",
    "aim_54", "aim-54", "p_27", "r-27", "p_73", "r-73",
    "p_77", "r-77", "pl-12", "pl_12", "sd-10", "sd_10",
)

_OBJ_ID_RE = re.compile(r"^[0-9a-fA-F]+$")
_TIME_RE = re.compile(r"^#([+-]?\d+(?:\.\d+)?)$")

# Velocity derivation guards. ACMI samples at ~5 Hz (dt ~0.2 s); a dt below
# DT_FLOOR means a near-duplicate timestamp, and a derived speed above SPD_CAP
# means a position teleport (object respawn). Either way the velocity is
# meaningless, so leave it at 0.0 rather than emitting a garbage value.
DT_FLOOR = 0.05      # s
SPD_CAP = 2000.0     # m/s — above any aircraft or whitelisted missile

_METERS_PER_DEG = 111319.49  # WGS84 mean; exact value cancels in relative geometry


def _project(lon: float, lat: float, ref_lon: float, ref_lat: float) -> tuple[float, float]:
    """Equirectangular lon/lat -> (East, North) meters from the reference point."""
    u = (lon - ref_lon) * _METERS_PER_DEG * math.cos(math.radians(ref_lat))
    v = (lat - ref_lat) * _METERS_PER_DEG
    return u, v


def _init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS objects (
            run_id    TEXT,
            obj_id    TEXT,
            obj_class TEXT,
            name      TEXT,
            pilot     TEXT,
            coalition TEXT,
            first_t   REAL,
            last_t    REAL,
            removed_t REAL,
            PRIMARY KEY (run_id, obj_id)
        );
        CREATE TABLE IF NOT EXISTS samples (
            run_id TEXT,
            obj_id TEXT,
            t      REAL,
            u      REAL,
            v      REAL,
            alt    REAL,
            vx     REAL,
            vy     REAL,
            spd    REAL,
            mach   REAL
        );
        CREATE INDEX IF NOT EXISTS idx_samples ON samples(run_id, obj_id, t);
    """)
    conn.commit()


def _classify(obj_type: str, name: str) -> Optional[str]:
    t = obj_type.lower()
    n = name.lower()
    if t == "air+fixedwing":
        return "aircraft"
    if t == "weapon+missile" and any(w in n for w in MISSILE_NAMES):
        return "missile"
    return None


def _sos(alt_m: float) -> float:
    h = max(0.0, alt_m)
    T = 288.15 - 0.0065 * h if h <= 11000.0 else 216.65
    return math.sqrt(1.4 * 287.05287 * T)


def parse_file(acmi_path: Path, db_path: Path) -> tuple[int, int]:
    """Parse one ACMI file into the DB. Returns (objects_written, samples_written).
    Skips the file if run_id already exists in objects table (idempotent).
    """
    run_id = acmi_path.stem

    conn = sqlite3.connect(db_path)
    _init_db(conn)

    if conn.execute("SELECT 1 FROM objects WHERE run_id=? LIMIT 1", (run_id,)).fetchone():
        conn.close()
        return 0, 0

    # carry[obj_id]: [lon, lat, alt] T= state (None = not yet seen)
    carry: dict[str, list] = {}
    # meta[obj_id]: classification + lifecycle info
    meta: dict[str, dict] = {}
    # prev[obj_id]: (t, u, v, alt) for velocity derivation
    prev: dict[str, tuple] = {}
    # global reference point (from object "0" header), set once near file start
    ref_lon: Optional[float] = None
    ref_lat: Optional[float] = None

    buf: list[tuple] = []

    def flush() -> None:
        conn.executemany("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?,?)", buf)
        conn.commit()
        buf.clear()

    current_t = 0.0
    n_samples = 0

    with acmi_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("FileType") or line.startswith("FileVersion"):
                continue

            m = _TIME_RE.match(line)
            if m:
                current_t = float(m.group(1))
                continue

            if line.startswith("-"):
                obj_id = line[1:].strip()
                if obj_id in meta:
                    meta[obj_id]["removed_t"] = current_t
                    meta[obj_id]["last_t"] = current_t
                continue

            comma = line.find(",")
            if comma == -1:
                continue
            obj_id = line[:comma]
            if not _OBJ_ID_RE.match(obj_id):
                continue

            t_val = None
            kv: dict[str, str] = {}
            for token in line[comma + 1:].split(","):
                if "=" not in token:
                    continue
                k, _, v = token.partition("=")
                k = k.strip()
                if k == "T":
                    t_val = v.strip()
                else:
                    kv[k] = v.strip()

            if obj_id == "0":
                if "ReferenceLongitude" in kv:
                    ref_lon = float(kv["ReferenceLongitude"])
                if "ReferenceLatitude" in kv:
                    ref_lat = float(kv["ReferenceLatitude"])
                continue

            if obj_id not in meta:
                obj_class = _classify(kv.get("Type", ""), kv.get("Name", ""))
                meta[obj_id] = {
                    "class": obj_class,
                    "name": kv.get("Name", ""),
                    "pilot": kv.get("Pilot", ""),
                    "coalition": kv.get("Coalition", ""),
                    "first_t": current_t,
                    "last_t": current_t,
                    "removed_t": None,
                }
                carry[obj_id] = [None, None, None]
            else:
                meta[obj_id]["last_t"] = current_t

            if meta[obj_id]["class"] is None or t_val is None:
                continue
            if ref_lon is None or ref_lat is None:
                continue

            # First 3 T= fields are lon|lat|alt in every layout variant.
            state = carry[obj_id]
            for i, s in enumerate(t_val.split("|")[:3]):
                if s.strip():
                    try:
                        state[i] = float(s)
                    except ValueError:
                        pass

            lon, lat, alt = state[0], state[1], state[2]
            if lon is None or lat is None or alt is None:
                continue
            u, v = _project(lon, lat, ref_lon, ref_lat)

            vx = vy = spd = mach = 0.0
            if obj_id in prev:
                pt, pu, pv, _ = prev[obj_id]
                dt = current_t - pt
                if dt >= DT_FLOOR:
                    cvx = (u - pu) / dt
                    cvy = (v - pv) / dt
                    cspd = math.hypot(cvx, cvy)
                    if cspd <= SPD_CAP:
                        vx, vy, spd = cvx, cvy, cspd
                        mach = spd / _sos(alt)

            prev[obj_id] = (current_t, u, v, alt)
            buf.append((run_id, obj_id, current_t, u, v, alt, vx, vy, spd, mach))
            n_samples += 1
            if len(buf) >= 1000:
                flush()

    if buf:
        flush()

    obj_rows = [
        (run_id, oid, m["class"], m["name"], m["pilot"], m["coalition"],
         m["first_t"], m["last_t"], m["removed_t"])
        for oid, m in meta.items()
        if m["class"] is not None
    ]
    conn.executemany("INSERT OR IGNORE INTO objects VALUES (?,?,?,?,?,?,?,?,?)", obj_rows)
    conn.commit()
    conn.close()

    return len(obj_rows), n_samples


def parse_all(raw_dir: Path, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for path in sorted(raw_dir.glob("*.acmi")):
        n_obj, n_samp = parse_file(path, db_path)
        if n_obj == 0 and n_samp == 0:
            print(f"{path.name}: skipped (already parsed)")
        else:
            print(f"{path.name}: {n_obj} objects, {n_samp} samples")


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--raw-dir", default="data/raw")
    p.add_argument("--db", default="data/pk.db")
    args = p.parse_args()
    parse_all(Path(args.raw_dir), Path(args.db))


if __name__ == "__main__":
    main()
