"""Validate the rebuilt shots table before training.

Run after `python -m src.shots`. Compares against pre-fix baselines so each
check is concrete. Pure ASCII output (Windows cp1252 console).
"""
from collections import Counter
from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd

from src.shots import (_whitelisted, _state_at, _find_launcher,
                       _find_target, _hit_label)

DB = Path("data/pk.db")
FUNNEL_RUN = "5600.txt"          # representative run for the instrumented funnel
OLD = dict(shots=59212, hit_rate=26.6, runs=119)   # pre-fix build, for comparison


def hr(title: str) -> None:
    print("\n==== " + title + " ====")


# ---- Tier 1: did the velocity fix land? --------------------------------------

def tier1_velocity(shots: pd.DataFrame) -> None:
    hr("T1.1 velocity ranges (pre-fix: mach_l max 124.5, mach_t max 66.6)")
    for col in ["spd_l", "spd_t", "mach_l", "mach_t"]:
        s = shots[col]
        print(f"  {col:8s} min={s.min():8.2f}  p50={s.median():8.2f}  "
              f"p99={s.quantile(0.99):8.2f}  max={s.max():8.2f}")
    for col in ["mach_l", "mach_t"]:
        n = (shots[col] > 3.0).sum()
        print(f"  {col} > 3.0 (beyond fighter perf): {n} ({100*n/len(shots):.2f}%)")


def tier1_nulls(shots: pd.DataFrame) -> None:
    hr("T1.2 ata/ta nulls (pre-fix: 950 / 2397, ~4%)")
    for col in ["ata", "ta"]:
        n = shots[col].isna().sum()
        print(f"  {col} nulls: {n} ({100*n/len(shots):.2f}%)")


def tier1_caught_rows(shots: pd.DataFrame) -> None:
    hr("T1.3 rows we caught corrupt (5939.txt was mach_t=24.9, 5641.txt was 13.8)")
    for run in ["5939.txt", "5641.txt"]:
        sub = shots[shots.run_id == run]
        if sub.empty:
            print(f"  {run}: no shots")
            continue
        print(f"  {run}: {len(sub)} shots, mach_t max={sub.mach_t.max():.2f}, "
              f"mach_l max={sub.mach_l.max():.2f}")


# ---- Tier 2: did the fix break anything else? --------------------------------

def tier2_coverage(shots: pd.DataFrame) -> None:
    hr("T2.4 coverage vs old baseline")
    n, hits = len(shots), int(shots.hit.sum())
    print(f"  shots:    {n}        (old {OLD['shots']})")
    print(f"  hit rate: {100*hits/n:.1f}%      (old {OLD['hit_rate']}%)")
    print(f"  runs:     {shots.run_id.nunique()}/138    (old {OLD['runs']})")


def tier2_funnel(conn: sqlite3.Connection) -> None:
    hr(f"T2.5 filter funnel ({FUNNEL_RUN}; pre-fix: 481 -> 267)")
    objs = pd.read_sql("SELECT * FROM objects WHERE run_id=?", conn, params=(FUNNEL_RUN,))
    samples = pd.read_sql(
        "SELECT obj_id,t,u,v,alt,vx,vy,spd,mach FROM samples WHERE run_id=?",
        conn, params=(FUNNEL_RUN,))
    samples = samples.sort_values(["obj_id", "t"])
    traj = {oid: g.reset_index(drop=True) for oid, g in samples.groupby("obj_id")}
    ac_meta = objs[objs.obj_class == "aircraft"].reset_index(drop=True)
    missiles = objs[objs.obj_class == "missile"]
    obj_by_id = {o["obj_id"]: o for _, o in objs.iterrows()}

    f: Counter = Counter()
    for _, m in missiles.iterrows():
        f["00_total"] += 1
        if not _whitelisted(m["name"]) or not m["coalition"]:
            f["01_no_wl_or_coalition"] += 1
            continue
        mt = traj.get(m["obj_id"])
        if mt is None or len(mt) < 2:
            f["02_no_trajectory"] += 1
            continue
        t0 = float(mt["t"].iloc[0])
        m0 = mt.iloc[0].to_dict()
        lid = _find_launcher(m0, m["coalition"], ac_meta, traj, t0)
        if lid is None:
            f["03_no_launcher"] += 1
            continue
        tid, _cpa = _find_target(mt, m["coalition"], ac_meta, traj)
        if tid is None:
            f["04_no_target_cpa_gt_10km"] += 1
            continue
        l = _state_at(traj[lid], t0)
        t = _state_at(traj[tid], t0)
        if l is None or t is None:
            f["05_no_state"] += 1
            continue
        m_term = mt.iloc[-1].to_dict()
        t_term = _state_at(traj[tid], float(m_term["t"]))
        hit, _conf = _hit_label(m, obj_by_id[tid], m_term, t_term)
        if hit is None:
            f["06_ambiguous"] += 1
            continue
        f["07_labeled"] += 1
    for k in sorted(f):
        print(f"  {k:26s} {f[k]}")


def tier2_range(shots: pd.DataFrame) -> None:
    hr("T2.6 range_lt sanity (>160km is implausible for A2A)")
    for thr in [120_000, 160_000, 200_000]:
        n = (shots.range_lt > thr).sum()
        print(f"  range_lt > {thr/1000:.0f}km: {n} ({100*n/len(shots):.2f}%)")


def tier2_integrity(shots: pd.DataFrame) -> None:
    hr("T2.7 integrity")
    for col in ["target_id", "launcher_id", "hit", "range_lt", "cpa"]:
        print(f"  {col} nulls: {shots[col].isna().sum()}")
    dup = len(shots) - shots[["run_id", "missile_id"]].drop_duplicates().shape[0]
    print(f"  duplicate (run_id,missile_id): {dup}")
    num = shots.select_dtypes(include=[np.number]).to_numpy()
    print(f"  inf values across numeric cols: {int(np.isinf(num).sum())}")


# ---- Tier 3: target correlation & label quality ------------------------------

def tier3_cpa(shots: pd.DataFrame) -> None:
    hr("T3.8 CPA distribution (pre-fix: p50 ~2km)")
    c = shots.cpa
    print(f"  min={c.min():.1f}  p50={c.median():.1f}  "
          f"p90={c.quantile(0.9):.1f}  max={c.max():.1f}")
    hr("T3.9 label leakage: hits with high CPA (pre-fix: 3% >200m, 1.6% >500m)")
    h = shots[shots.hit == 1]
    for thr in [200, 500, 1000]:
        n = (h.cpa > thr).sum()
        print(f"  hits with cpa > {thr}m: {n} ({100*n/len(h):.1f}%)")


def tier3_trajectories(conn: sqlite3.Connection, shots: pd.DataFrame) -> None:
    hr("T3.10 raw trajectory spot-check (3 hits, 3 misses)")
    for label, hv in [("HIT ", 1), ("MISS", 0)]:
        for _, r in shots[shots.hit == hv].head(3).iterrows():
            mlast = pd.read_sql(
                "SELECT t,u,v,alt FROM samples WHERE run_id=? AND obj_id=? "
                "ORDER BY t DESC LIMIT 1", conn, params=(r.run_id, r.missile_id))
            if mlast.empty:
                continue
            mt = mlast.iloc[0]
            tnear = pd.read_sql(
                "SELECT t,u,v,alt FROM samples WHERE run_id=? AND obj_id=? "
                "ORDER BY ABS(t-?) LIMIT 1",
                conn, params=(r.run_id, r.target_id, float(mt.t)))
            if tnear.empty:
                continue
            tn = tnear.iloc[0]
            d = ((mt.u - tn.u) ** 2 + (mt.v - tn.v) ** 2 + (mt.alt - tn.alt) ** 2) ** 0.5
            print(f"  {label} {r.run_id:10s} {r.missile_name:12s} "
                  f"terminal_sep={d:8.0f}m  cpa={r.cpa:8.0f}m")


def tier3_launcher(conn: sqlite3.Connection) -> None:
    hr("T3.11 launcher type distribution (expect fighters, not tankers/AWACS)")
    q = pd.read_sql(
        "SELECT o.name, COUNT(*) n FROM shots s "
        "JOIN objects o ON s.run_id=o.run_id AND s.launcher_id=o.obj_id "
        "GROUP BY o.name ORDER BY n DESC LIMIT 12", conn)
    for _, r in q.iterrows():
        print(f"  {r['name']:22s} {r['n']}")


# ---- Tier 4: training readiness ----------------------------------------------

def tier4_hit_by_type(shots: pd.DataFrame) -> None:
    hr("T4.12 hit rate by missile type (expect WVR > short BVR > long BVR)")
    g = shots.groupby("missile_name").agg(n=("hit", "size"), hits=("hit", "sum"))
    g["rate"] = (100 * g.hits / g.n).round(1)
    for name, row in g.sort_values("n", ascending=False).head(12).iterrows():
        print(f"  {name:20s} {int(row.hits):4d}/{int(row.n):<6d} ({row.rate}%)")


def tier4_balance(shots: pd.DataFrame) -> None:
    hr("T4.13 class & type balance")
    print(f"  hit/miss: {int(shots.hit.sum())} / {int((shots.hit == 0).sum())}")
    for name, n in shots.missile_name.value_counts().head(3).items():
        print(f"  {name}: {n} ({100*n/len(shots):.1f}%)")


def tier4_monotonicity(shots: pd.DataFrame) -> None:
    hr("T4.14 feature/label monotonicity (hit rate should fall with range)")
    for col, edges in [("range_lt", [0, 10_000, 20_000, 40_000, 80_000, 250_000]),
                       ("ta", [0, 30, 60, 90, 120, 180])]:
        print(f"  {col}:")
        cats = pd.cut(shots[col], bins=edges)
        g = shots.groupby(cats, observed=True).agg(n=("hit", "size"), hits=("hit", "sum"))
        for interval, row in g.iterrows():
            rate = 100 * row.hits / row.n if row.n else 0.0
            print(f"    {str(interval):24s} n={int(row.n):6d}  hit={rate:.1f}%")


def main() -> None:
    conn = sqlite3.connect(DB)
    shots = pd.read_sql("SELECT * FROM shots", conn)
    print(f"Loaded {len(shots)} shots from {DB}")

    tier1_velocity(shots)
    tier1_nulls(shots)
    tier1_caught_rows(shots)
    tier2_coverage(shots)
    tier2_funnel(conn)
    tier2_range(shots)
    tier2_integrity(shots)
    tier3_cpa(shots)
    tier3_trajectories(conn, shots)
    tier3_launcher(conn)
    tier4_hit_by_type(shots)
    tier4_balance(shots)
    tier4_monotonicity(shots)

    conn.close()
    print("\nDone. Review each section against the pre-fix baselines noted inline.")


if __name__ == "__main__":
    main()
