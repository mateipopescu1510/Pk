"""Build the shots table: one row per whitelisted missile launch with inferred
launcher, inferred target (CPA), kinematic features at t0, and hit label.

See CLAUDE.md → Key Algorithms for the launcher / target / hit-label rules.
"""
from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

MISSILE_NAMES = (
    "aim_120", "aim-120", "aim_9", "aim-9", "aim_7", "aim-7",
    "aim_54", "aim-54", "p_27", "r-27", "p_73", "r-73",
    "p_77", "r-77", "pl-12", "pl_12", "sd-10", "sd_10",
)

LAUNCHER_RADIUS = 3000.0       # m, max launcher-missile dist at t0
CPA_THRESHOLD   = 10000.0      # m, max CPA to count as engagement
HIT_DISTANCE    = 1000.0       # m, terminal miss-distance for HIT
HIT_DT          = 8.0          # s, |missile.removed_t - target.removed_t| for HIT
MISS_SURVIVAL   = 15.0         # s, target.last_t - missile.removed_t for MISS


def _whitelisted(name: str) -> bool:
    n = name.lower()
    return any(w in n for w in MISSILE_NAMES)


def _guidance(name: str) -> Optional[str]:
    """Guidance category: ARH (active radar), IR, or SARH (semiactive radar).

    DCS missile names mix hyphens and underscores almost at random, so we
    normalise separators and match on substrings. The R-27 family splits by
    variant: a 'T' in the suffix means the IR seeker, otherwise semiactive.
    """
    n = name.lower().replace("-", "_").replace(" ", "_")
    if any(k in n for k in ("aim_120", "aim_54", "pl_12", "sd_10", "r_77", "p_77")):
        return "ARH"
    if any(k in n for k in ("aim_9", "r_73", "p_73")):
        return "IR"
    if "aim_7" in n:
        return "SARH"
    if "27" in n:  # R-27 family
        return "IR" if "t" in n.split("27", 1)[1] else "SARH"
    return None


def _init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        DROP TABLE IF EXISTS shots;
        CREATE TABLE shots (
            run_id        TEXT,
            missile_id    TEXT,
            missile_name  TEXT,
            launcher_id   TEXT,
            target_id     TEXT,
            t0            REAL,
            range_lt      REAL,
            alt_l         REAL,
            alt_t         REAL,
            alt_delta     REAL,
            spd_l         REAL,
            spd_t         REAL,
            mach_l        REAL,
            mach_t        REAL,
            ata           REAL,
            ta            REAL,
            cpa           REAL,
            hit           INTEGER,
            label_conf    TEXT,
            guidance      TEXT,
            PRIMARY KEY (run_id, missile_id)
        );
    """)
    conn.commit()


def _angle(ax: float, ay: float, bx: float, by: float) -> float:
    na, nb = math.hypot(ax, ay), math.hypot(bx, by)
    if na < 1e-3 or nb < 1e-3:
        return float("nan")
    c = max(-1.0, min(1.0, (ax * bx + ay * by) / (na * nb)))
    return math.degrees(math.acos(c))


def _state_at(traj: pd.DataFrame, t: float) -> Optional[dict]:
    ts = traj["t"].to_numpy()
    if t < ts[0] or t > ts[-1]:
        return None
    return {
        "u":    float(np.interp(t, ts, traj["u"].to_numpy())),
        "v":    float(np.interp(t, ts, traj["v"].to_numpy())),
        "alt":  float(np.interp(t, ts, traj["alt"].to_numpy())),
        "vx":   float(np.interp(t, ts, traj["vx"].to_numpy())),
        "vy":   float(np.interp(t, ts, traj["vy"].to_numpy())),
        "spd":  float(np.interp(t, ts, traj["spd"].to_numpy())),
        "mach": float(np.interp(t, ts, traj["mach"].to_numpy())),
    }


def _find_launcher(m_state: dict, m_coalition: str, ac_meta: pd.DataFrame,
                   traj: dict, t0: float) -> Optional[str]:
    cand = ac_meta[(ac_meta["coalition"] == m_coalition) &
                   (ac_meta["first_t"] <= t0) &
                   (ac_meta["last_t"] >= t0)]
    best_id, best_d = None, LAUNCHER_RADIUS
    for oid in cand["obj_id"]:
        s = _state_at(traj[oid], t0)
        if s is None:
            continue
        d = math.hypot(s["u"] - m_state["u"], s["v"] - m_state["v"])
        if d < best_d:
            best_id, best_d = oid, d
    return best_id


def _find_target(m_traj: pd.DataFrame, m_coalition: str,
                 ac_meta: pd.DataFrame, traj: dict) -> tuple[Optional[str], float]:
    """Pick enemy aircraft with smallest CPA. Returns (target_id, cpa)."""
    t0, tT = float(m_traj["t"].iloc[0]), float(m_traj["t"].iloc[-1])
    mt = m_traj["t"].to_numpy()
    mu = m_traj["u"].to_numpy()
    mv = m_traj["v"].to_numpy()
    ma = m_traj["alt"].to_numpy()

    enemies = ac_meta[(ac_meta["coalition"] != m_coalition) &
                      (ac_meta["coalition"] != "") &
                      (ac_meta["first_t"] <= tT) &
                      (ac_meta["last_t"] >= t0)]

    best_id, best_cpa = None, CPA_THRESHOLD
    for oid in enemies["obj_id"]:
        a = traj[oid]
        ats = a["t"].to_numpy()
        lo, hi = max(t0, ats[0]), min(tT, ats[-1])
        if lo >= hi:
            continue
        mask = (mt >= lo) & (mt <= hi)
        if not mask.any():
            continue
        au = np.interp(mt[mask], ats, a["u"].to_numpy())
        av = np.interp(mt[mask], ats, a["v"].to_numpy())
        ah = np.interp(mt[mask], ats, a["alt"].to_numpy())
        d = np.sqrt((mu[mask] - au) ** 2 + (mv[mask] - av) ** 2 + (ma[mask] - ah) ** 2)
        cpa = float(d.min())
        if cpa < best_cpa:
            best_id, best_cpa = oid, cpa
    return best_id, best_cpa


def _hit_label(missile: pd.Series, target: pd.Series, m_terminal: dict,
               t_at_terminal: Optional[dict]) -> tuple[Optional[int], str]:
    m_rem = missile["removed_t"]
    if m_rem is None or pd.isna(m_rem):
        return None, "low"            # missile never expired in the recording

    t_rem = target["removed_t"]
    if t_rem is not None and not pd.isna(t_rem) and t_at_terminal is not None:
        d = math.hypot(m_terminal["u"] - t_at_terminal["u"],
                       m_terminal["v"] - t_at_terminal["v"])
        if abs(t_rem - m_rem) <= HIT_DT and d <= HIT_DISTANCE:
            return 1, "high"

    if target["last_t"] - m_rem > MISS_SURVIVAL:
        return 0, "high"

    return None, "low"


def process_run(conn: sqlite3.Connection, run_id: str) -> list[tuple]:
    objs = pd.read_sql("SELECT * FROM objects WHERE run_id=?", conn, params=(run_id,))
    samples = pd.read_sql(
        "SELECT obj_id, t, u, v, alt, vx, vy, spd, mach FROM samples WHERE run_id=?",
        conn, params=(run_id,),
    )
    if samples.empty or objs.empty:
        return []

    samples = samples.sort_values(["obj_id", "t"])
    traj = {oid: g.reset_index(drop=True) for oid, g in samples.groupby("obj_id")}

    ac_meta = objs[objs["obj_class"] == "aircraft"].reset_index(drop=True)
    missiles = objs[objs["obj_class"] == "missile"]
    obj_by_id = {o["obj_id"]: o for _, o in objs.iterrows()}

    rows = []
    for _, m in missiles.iterrows():
        if not _whitelisted(m["name"]) or not m["coalition"]:
            continue
        m_traj = traj.get(m["obj_id"])
        if m_traj is None or len(m_traj) < 2:
            continue

        t0 = float(m_traj["t"].iloc[0])
        m0 = m_traj.iloc[0].to_dict()

        launcher_id = _find_launcher(m0, m["coalition"], ac_meta, traj, t0)
        if launcher_id is None:
            continue

        target_id, cpa = _find_target(m_traj, m["coalition"], ac_meta, traj)
        if target_id is None:
            continue

        l = _state_at(traj[launcher_id], t0)
        t = _state_at(traj[target_id], t0)
        if l is None or t is None:
            continue

        range_lt = math.hypot(t["u"] - l["u"], t["v"] - l["v"])
        ata = _angle(l["vx"], l["vy"], t["u"] - l["u"], t["v"] - l["v"])
        ta  = _angle(t["vx"], t["vy"], l["u"] - t["u"], l["v"] - t["v"])

        m_term = m_traj.iloc[-1].to_dict()
        t_term = _state_at(traj[target_id], float(m_term["t"]))
        hit, conf = _hit_label(m, obj_by_id[target_id], m_term, t_term)
        if hit is None:
            continue

        rows.append((
            run_id, m["obj_id"], m["name"], launcher_id, target_id, t0,
            range_lt, l["alt"], t["alt"], t["alt"] - l["alt"],
            l["spd"], t["spd"], l["mach"], t["mach"],
            ata, ta, cpa, hit, conf, _guidance(m["name"]),
        ))
    return rows


def build_shots(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    _init_db(conn)
    runs = [r[0] for r in conn.execute("SELECT DISTINCT run_id FROM objects").fetchall()]
    total_hits = total = 0
    for i, run_id in enumerate(runs, 1):
        rows = process_run(conn, run_id)
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO shots VALUES (" + ",".join(["?"] * 20) + ")",
                rows,
            )
            conn.commit()
        n_hit = sum(r[17] for r in rows)
        total += len(rows)
        total_hits += n_hit
        print(f"[{i}/{len(runs)}] {run_id}: {len(rows)} shots ({n_hit} hits)")
    print(f"\nTotal: {total} shots, {total_hits} hits ({100*total_hits/max(total,1):.1f}%)")
    conn.close()


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/pk.db")
    args = p.parse_args()
    build_shots(Path(args.db))


if __name__ == "__main__":
    main()
