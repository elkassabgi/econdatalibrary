"""tools/repull_worklist.py — the authoritative, NETWORK-FREE work-list for the
value-first-resolver clean re-pull.

Reuses the verified categoriser from tools/pxweb_regression_live.py (parse_key +
categorise) so this and the migration proof can never disagree. For every PxWeb
source it buckets each on-disk TABLE (series_key prefix) into
clean / corrupt / two_axis / false_alarm, then emits:

  * a per-(source, subject-parquet) count table, and
  * a JSON worklist  {source: {subject_file: {"corrupt": [prefix...],
                                               "two_axis": [prefix...]}}}
    listing exactly the tables a re-pull must rebuild (corrupt = wrong dates on
    disk NOW; two_axis = a genuine 2nd date axis whose tie-break must be re-pulled
    so on-disk matches what the live value-first fetcher will produce).

Clean / false_alarm tables are NOT in the worklist: the byte-identical proofs show
the new resolver reproduces their exact (series_key, obs_date), so their on-disk
rows are already correct and are kept verbatim during the rebuild.

Run:  python tools/repull_worklist.py  [out.json]
"""
from __future__ import annotations
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import pyarrow.parquet as pq  # noqa: E402
from pxweb_regression_live import parse_key, categorise, _year, DATA, SOURCES  # noqa: E402


def scan_source(src: str):
    """Return {subject_file: {prefix: (cat, risk_dim, rate)}} for one source, or None."""
    d = os.path.join(DATA, src)
    if not os.path.isdir(d):
        return None
    out: dict[str, dict] = {}
    for f in sorted(os.listdir(d)):
        if not f.endswith(".parquet"):
            continue
        pf = pq.ParquetFile(os.path.join(d, f))
        tables: dict[str, dict] = {}
        seen: set[str] = set()
        for batch in pf.iter_batches(columns=["series_key", "obs_date"], batch_size=250_000):
            ks = batch.column("series_key").to_pylist()
            ds = batch.column("obs_date").to_pylist()
            for k, od in zip(ks, ds):
                if not k or k in seen:
                    continue
                seen.add(k)
                pref, dims = parse_key(k)
                tt = tables.setdefault(pref, {"dims": {}, "years": set()})
                yr = _year(od)
                if yr is not None:
                    tt["years"].add(yr)
                for dim, codes in dims.items():
                    tt["dims"].setdefault(dim, set()).update(codes)
        subj: dict[str, tuple] = {}
        for pref, tt in tables.items():
            subj[pref] = categorise(tt["dims"], tt["years"])   # (cat, dim, rate)
        out[f] = subj
    return out


def main() -> int:
    out_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(ROOT), "repull_worklist.json")
    worklist: dict[str, dict] = {}
    grand = {"clean": 0, "corrupt": 0, "two_axis": 0, "false_alarm": 0}
    print(f"  {'source':<14} {'subject.parquet':<34} {'clean':>6} {'corr':>5} {'2ax':>5} {'false':>6}")
    for src in SOURCES:
        scanned = scan_source(src)
        if scanned is None:
            print(f"  {src:<14} (no on-disk data — skipped)")
            continue
        src_wl: dict[str, dict] = {}
        for subj_file, tables in sorted(scanned.items()):
            c = {"clean": 0, "corrupt": 0, "two_axis": 0, "false_alarm": 0}
            corrupt, two_axis = [], []
            for pref, (cat, _dim, _rate) in tables.items():
                c[cat] += 1
                if cat == "corrupt":
                    corrupt.append(pref)
                elif cat == "two_axis":
                    two_axis.append(pref)
            for k in grand:
                grand[k] += c[k]
            if corrupt or two_axis:
                src_wl[subj_file] = {"corrupt": sorted(corrupt), "two_axis": sorted(two_axis)}
                print(f"  {src:<14} {subj_file:<34} {c['clean']:>6} {c['corrupt']:>5} "
                      f"{c['two_axis']:>5} {c['false_alarm']:>6}")
        if src_wl:
            worklist[src] = src_wl
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(worklist, fh, indent=1)
    n_corrupt = sum(len(s["corrupt"]) for src in worklist.values() for s in src.values())
    n_two = sum(len(s["two_axis"]) for src in worklist.values() for s in src.values())
    n_subj = sum(len(src) for src in worklist.values())
    print()
    print(f"  GRAND: clean={grand['clean']} corrupt={grand['corrupt']} "
          f"two_axis={grand['two_axis']} false_alarm={grand['false_alarm']}")
    print(f"  RE-PULL WORKLIST: {n_corrupt} corrupt + {n_two} two_axis tables "
          f"across {n_subj} subject parquets in {len(worklist)} source(s)")
    print(f"  written -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
