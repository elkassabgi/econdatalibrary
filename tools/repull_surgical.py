"""tools/repull_surgical.py — SURGICAL per-table PxWeb data fix (R27-safe).

Per affected table (from the worklist):
  * CORRUPT  -> re-fetch via the source's wired ingester; REPLACE its rows.
  * TWO_AXIS -> re-fetch; if the set of NON-time dims that land IN THE KEY changed
               (old vs new), the time axis MOVED => DIVERGENT => REPLACE (so on-disk
               matches the live fetcher, no merge duplication). If unchanged => ALIGNED
               => KEEP on-disk rows verbatim (preserves detail an over-MAX_CELLS
               re-fetch would aggregate away — R27).
Every other table is passed through byte-for-byte. Only MODIFIED subjects are staged to
scratchpad/wave2_fixed/<src>/<subject>. Never touches the live tree or R2.

Per-source re-fetch adapters (key-prefix -> ingester call):
  ssb            "SSB:<id>"           -> query_table(<id>)
  stat_slovenia  "SI:<id>"            -> query_table(<id>.px | <id>)
  dst            "DST:<id>"           -> query_table(<id>, tableinfo(id).variables)
  stat_latvia    "LV:<db>:<path:>"    -> query_table(catalog dict for that db/path)
  scb/statfin    "<path:>"            -> query_table(catalog dict for that path)

Run:  python tools/repull_surgical.py <worklist.json> [src1 src2 ...]
"""
from __future__ import annotations
import importlib.util
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Derived, not hardcoded — the store moved D: -> E: in the workstation cutover, and every
# consumer of the stale root silently reported "no data" as "no defects". See R330.
DATA = os.environ.get("ECONDL_CLEAN_FULL") or os.path.join(ROOT, "data", "clean_full")
if not os.path.isdir(DATA):
    raise SystemExit(f"repull_surgical: clean_full root not found: {DATA!r}\n"
                     f"Set ECONDL_CLEAN_FULL. Refusing to run against an absent tree — a "
                     f"re-pull tool that sees nothing must not conclude nothing needs repair.")
STAGE = r"D:/temp/claude/D--research-hfdatalibrary/5bda36f5-59a1-4804-b441-06c56c3755da/scratchpad/wave2_fixed"
WAVE2 = ["ssb", "stat_latvia", "stat_slovenia", "dst"]


def _prefix(k): return ":".join(x for x in k.split(":") if "=" not in x)
def _key_dims(keys): return {p.split("=", 1)[0] for k in keys for p in k.split(":") if "=" in p}
def _year(d):
    try: return d.year
    except AttributeError: return None


def _load(src):
    spec = importlib.util.spec_from_file_location(f"_ing_{src}", os.path.join(ROOT, "jobs", f"ingest_{src}.py"))
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m


def make_refetch(src, ing):
    if src == "ssb":
        return lambda pref: ing.query_table(pref[len("SSB:"):])
    if src == "stat_slovenia":
        def rf(pref):
            tid = pref[len("SI:"):]
            r = ing.query_table(tid + ".px")
            return r if r else ing.query_table(tid)
        return rf
    if src == "dst":
        def rf(pref):
            tid = pref[len("DST:"):]
            meta = ing.get_json(f"{ing.BASE}/tableinfo?id={tid}&lang=en")
            return ing.query_table(tid, meta.get("variables", [])) if isinstance(meta, dict) else None
        return rf
    if src == "stat_latvia":
        cat = {f"LV:{e['db']}:{e['path'].replace('/', ':')}": e for e in ing.crawl_catalog()}
        return lambda pref: (ing.query_table(cat[pref]) if pref in cat else None)
    cat = {t["path"].replace("/", ":"): t for t in ing.crawl_catalog()}   # scb/statfin
    return lambda pref: (ing.query_table(cat[pref]) if pref in cat else None)


def process_source(src, src_wl):
    ing = _load(src); refetch = make_refetch(src, ing)
    os.makedirs(os.path.join(STAGE, src), exist_ok=True)
    s = {"corrupt_fixed": 0, "divergent_fixed": 0, "aligned_kept": 0, "unmapped": 0, "err": 0, "subjects_changed": 0}
    for subject, wl in sorted(src_wl.items()):
        onp = os.path.join(DATA, src, subject)
        if not os.path.exists(onp): print(f"  {src}/{subject}: MISSING — skip"); continue
        t = pq.read_table(onp); ks = t.column("series_key").to_pylist()
        by_pref = {}
        for i, k in enumerate(ks): by_pref.setdefault(_prefix(k), []).append(i)
        replace = {}
        for kind in ("corrupt", "two_axis"):
            for pref in wl.get(kind, []):
                try:
                    rows = refetch(pref)
                except Exception as e:
                    s["err"] += 1; print(f"    {src} {pref}: fetch-err {type(e).__name__} {str(e)[:50]} — AS-IS"); continue
                if rows is None: s["unmapped"] += 1; print(f"    {src} {pref}: unmapped — AS-IS"); continue
                if not rows: print(f"    {src} {pref}: 0 rows — AS-IS"); continue
                nk = [r[0] for r in rows]
                new_tbl = pa.table({"series_key": pa.array(nk, pa.string()),
                                    "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                                    "value": pa.array([r[2] for r in rows], pa.float64())})
                if kind == "corrupt":
                    replace[pref] = new_tbl; s["corrupt_fixed"] += 1; continue
                old_dims = _key_dims(ks[i] for i in by_pref.get(pref, []))
                if old_dims != _key_dims(nk):
                    replace[pref] = new_tbl; s["divergent_fixed"] += 1
                else:
                    s["aligned_kept"] += 1
        if not replace: continue
        keep = pa.array([_prefix(k) not in replace for k in ks], pa.bool_())
        out = pa.concat_tables([t.filter(keep)] + list(replace.values()))
        pq.write_table(out, os.path.join(STAGE, src, subject), compression="zstd")
        s["subjects_changed"] += 1
        bad = sum(1 for d in out.column("obs_date").to_pylist() if (y := _year(d)) is not None and not 1500 <= y <= 2100)
        print(f"  {src}/{subject}: replaced {len(replace)} -> {t.num_rows}->{out.num_rows} rows" + (f"  <-- {bad} GARBAGE!" if bad else ""))
    return s


def main():
    wl = json.load(open(sys.argv[1], encoding="utf-8"))
    for src in (sys.argv[2:] or WAVE2):
        if src not in wl: print(f"=== {src}: nothing in worklist ==="); continue
        print(f"=== {src} ===")
        r = process_source(src, wl[src])
        print(f"  -> corrupt_fixed={r['corrupt_fixed']} divergent_fixed={r['divergent_fixed']} "
              f"aligned_kept={r['aligned_kept']} unmapped={r['unmapped']} err={r['err']} changed={r['subjects_changed']}")
    print(f"\nstaged -> {STAGE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
