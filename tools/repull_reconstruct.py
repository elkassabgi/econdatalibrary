"""tools/repull_reconstruct.py — build the CORRECT upload parquet for each re-pulled
PxWeb subject by combining, per table:

  * BACKUP rows for CLEAN tables (backup min obs-year >= 1500) — these were already
    correct on R2, and the whole-subject re-ingest would AGGREGATE the big ones
    (cube now > MAX_CELLS) and lose detail. Keeping backup preserves that detail.
  * RE-INGEST rows for CORRUPT tables (backup min obs-year < 1500 — municipality /
    period codes mis-read as years) and for RECOVERED tables (present only in the
    re-ingest). The re-ingest fixed the date axis for these.
  * RETRY-FETCH rows for CORRUPT tables the re-ingest DROPPED to a transient
    ConnectionReset (in backup-corrupt, absent from re-ingest) — re-fetched live with
    the wired ingester so a network blip doesn't leave a corrupt table unfixed.

Writes reconstructed subjects to a STAGING dir (never touches the live data tree until
you verify). Run:
    python tools/repull_reconstruct.py            # build to staging + self-verify
Output staging dir printed at the end; promote it only after the verify block is clean.
"""
from __future__ import annotations
import importlib.util
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = r"D:/research/econfindatalibrary/data/clean_full"
BACKUP = r"D:/temp/claude/D--research-hfdatalibrary/5bda36f5-59a1-4804-b441-06c56c3755da/scratchpad/repull_backup"
STAGE = r"D:/temp/claude/D--research-hfdatalibrary/5bda36f5-59a1-4804-b441-06c56c3755da/scratchpad/reconstructed"
TARGETS = {"scb": ["AA", "AM", "BE", "BO", "FM", "HA", "HE", "JO"],
           "statfin": ["ntp", "tjt", "tkke", "tyokay", "tyonv", "velk", "vtutk"]}
GARBAGE_LT = 1500


def _prefix(series_key: str) -> str:
    return ":".join(x for x in series_key.split(":") if "=" not in x)


def _load(path):
    """Return (table_or_None, {prefix: [rows, min_year]})."""
    if not os.path.exists(path):
        return None, {}
    t = pq.read_table(path)
    ks = t.column("series_key").to_pylist()
    ds = t.column("obs_date").to_pylist()
    info: dict[str, list] = {}
    for k, d in zip(ks, ds):
        p = _prefix(k)
        e = info.setdefault(p, [0, 9999])
        e[0] += 1
        if d is not None and d.year < e[1]:
            e[1] = d.year
    return t, info


def _load_scb_ingester():
    path = os.path.join(ROOT, "jobs", "ingest_scb.py")
    spec = importlib.util.spec_from_file_location("_ing_scb_recon", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def retry_dropped(dropped_by_src: dict[str, list[str]]):
    """Re-fetch corrupt tables the re-ingest dropped. dropped_by_src: {src: [prefix,...]}.
    Returns {(src, prefix): pa.Table of (series_key,obs_date,value)} for those that fetch."""
    out = {}
    if not any(dropped_by_src.values()):
        return out
    # all current drops are scb; load the scb ingester + catalog and query each by path.
    ing = _load_scb_ingester()
    cat = {t["path"].replace("/", ":"): t for t in ing.crawl_catalog()}
    for src, prefixes in dropped_by_src.items():
        if src != "scb":
            print(f"  RETRY: {src} drops not handled by scb ingester — skipping {prefixes}")
            continue
        for pref in prefixes:
            entry = cat.get(pref)
            if entry is None:
                print(f"  RETRY {pref}: no catalog entry (path {pref.replace(':','/')}) — GONE upstream")
                continue
            try:
                rows = ing.query_table(entry)
            except Exception as e:
                print(f"  RETRY {pref}: fetch error {type(e).__name__}: {str(e)[:80]} — leaving dropped")
                continue
            if not rows:
                print(f"  RETRY {pref}: 0 rows (empty/removed) — leaving dropped")
                continue
            ks = [r[0] for r in rows]; dsv = [r[1] for r in rows]; vs = [r[2] for r in rows]
            bad = sum(1 for d in dsv if d.year < GARBAGE_LT)
            out[(src, pref)] = pa.table({"series_key": pa.array(ks, pa.string()),
                                         "obs_date": pa.array(dsv, pa.date32()),
                                         "value": pa.array(vs, pa.float64())})
            print(f"  RETRY {pref}: re-fetched {len(rows)} rows, {bad} garbage-year "
                  f"({'OK' if bad == 0 else 'STILL CORRUPT!'})")
    return out


def _filter_by_prefix(table, keep_prefixes: set, keep: bool):
    """Rows whose prefix is (keep=True: in / keep=False: not in) keep_prefixes."""
    ks = table.column("series_key").to_pylist()
    mask = pa.array([( _prefix(k) in keep_prefixes) == keep for k in ks], pa.bool_())
    return table.filter(mask)


def main() -> int:
    os.makedirs(STAGE, exist_ok=True)
    # pass 1: classify + collect the dropped corrupt tables
    dropped: dict[str, list[str]] = {s: [] for s in TARGETS}
    plans = {}
    for src, subs in TARGETS.items():
        for s in subs:
            bt, bi = _load(os.path.join(BACKUP, src, f"{s}.parquet"))
            rt, ri = _load(os.path.join(DATA, src, f"{s}.parquet"))
            corrupt = {p for p, v in bi.items() if v[1] < GARBAGE_LT}
            clean = {p for p, v in bi.items() if v[1] >= GARBAGE_LT}
            drop = [p for p in corrupt if p not in ri]
            dropped[src] += [f"{src}:{p}" if False else p for p in drop]  # keep bare prefix
            # store the actual prefix strings for retry mapped per src below
            plans[(src, s)] = dict(bt=bt, rt=rt, corrupt=corrupt, clean=clean, drop=drop)
    # retry needs {src: [prefix]}
    retry_in = {src: [p for (s2, s), pl in plans.items() if s2 == src for p in pl["drop"]]
                for src in TARGETS}
    print("=== retry-fetch dropped corrupt tables ===")
    retried = retry_dropped(retry_in)
    # pass 2: build each subject
    print("\n=== reconstruct + verify per subject ===")
    problems = []
    for (src, s), pl in plans.items():
        os.makedirs(os.path.join(STAGE, src), exist_ok=True)
        parts = []
        if pl["bt"] is not None:
            parts.append(_filter_by_prefix(pl["bt"], pl["clean"], keep=True))   # backup clean
        if pl["rt"] is not None:
            parts.append(_filter_by_prefix(pl["rt"], pl["clean"], keep=False))  # reingest corrupt+recovered
        for p in pl["drop"]:
            rt = retried.get((src, p))
            if rt is not None:
                parts.append(rt)
        parts = [p for p in parts if p is not None and p.num_rows > 0]
        recon = pa.concat_tables(parts) if parts else None
        outp = os.path.join(STAGE, src, f"{s}.parquet")
        if recon is None:
            print(f"  {src}/{s}: EMPTY reconstruction — SKIP"); problems.append(f"{src}/{s} empty"); continue
        pq.write_table(recon, outp, compression="zstd")
        # verify: (1) no garbage years, (2) no clean table smaller than backup
        yrs = [d.year for d in recon.column("obs_date").to_pylist()]
        n_bad = sum(1 for y in yrs if y < GARBAGE_LT)
        _, ri2 = _load(outp)
        _, bi = _load(os.path.join(BACKUP, src, f"{s}.parquet"))
        shrunk = [p for p in pl["clean"] if p in ri2 and p in bi and ri2[p][0] < bi[p][0]]
        tag = "OK"
        if n_bad: tag = f"GARBAGE x{n_bad}"; problems.append(f"{src}/{s} {n_bad} garbage")
        if shrunk: tag += f" | {len(shrunk)} clean shrank"; problems.append(f"{src}/{s} clean shrank {shrunk[:3]}")
        print(f"  {src}/{s:<8} recon={recon.num_rows:>10,} rows  [{tag}]")
    print("\n=== VERDICT ===")
    print("  CLEAN — reconstruction safe to promote" if not problems
          else "  PROBLEMS:\n   " + "\n   ".join(problems))
    print(f"  staging: {STAGE}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
