"""Derive a VALUE-VERIFIED element/item crosswalk for a frozen fao_* source (#19).

WHY. FAOSTAT re-coded element (and some item) codes when it merged the DBnomics-era
domains (R180: fao_gt's 7231->723113 AR5 re-code took id reproduction 27%->79%).
The re-code is legible in the element NAMES, but a name match alone can pick the
wrong successor when several candidates share a name (current QCL carries TWO
'Yield' codes and TWO 'Yield/Carcass Weight' codes). So every candidate mapping is
verified on VALUES: the old series' stored observations must agree with the
candidate new key's bulk observations on their shared years.

METHOD, per missing old id (old ids reproduce as <Item>.<Area>.<Element>):
  1. candidates = same area, x item in {old item + name-matched re-coded items},
     x element in {every current element} — restricted to keys present in the bulk.
  2. score = fraction of shared (year) points where values agree to 6 sig figs
     (FAO revisions make exact-100% rare; the qcl repair measured 92.2% agreement
     on truly-identical keys, so the acceptance floor is deliberately below that).
  3. accept the best candidate iff score >= --floor (default 0.90), it has >= 3
     shared points, AND the runner-up scores < best - 0.05 (uniqueness margin —
     refuse ambiguous winners rather than guess).
The output is a JSON crosswalk {old_suffix: new_suffix} plus a per-mapping report;
nothing is written to the store/catalog by this tool (measure first, apply later).

    python tools/fao_element_crosswalk.py --source fao_ql --code QCL \
        --emit updater/strategies/fetchers/_faostat_maps/fao_ql.crosswalk.json
"""
from __future__ import annotations

import argparse
import collections
import csv
import io
import json
import math
import os
import sqlite3
import sys
import urllib.request
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BULK_INDEX = "https://bulks-faostat.fao.org/production/datasets_E.json"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}


def _get(url, timeout=900):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                  timeout=timeout).read()


def close(a, b):
    if a == b:
        return True
    if a == 0 or b == 0:
        return abs(a - b) < 1e-9
    return math.isclose(a, b, rel_tol=5e-4)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--code", required=True, help="FAOSTAT DatasetCode, e.g. QCL")
    ap.add_argument("--floor", type=float, default=0.90)
    ap.add_argument("--emit")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    ours = {r[0].split(":", 2)[2] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (a.source,))
        if r[0].count(":") >= 2}
    print(f"{a.source}: {len(ours):,} published ids")

    idx = json.loads(_get(BULK_INDEX, 180).decode("utf-8-sig"))
    entry = next(x for x in idx["Datasets"]["Dataset"]
                 if x["DatasetCode"] == a.code.upper())
    z = zipfile.ZipFile(io.BytesIO(_get(entry["FileLocation"])))
    name = next(n for n in z.namelist() if "All_Data" in n)

    # bulk: key -> {year: value}; plus code->name maps
    bulk = collections.defaultdict(dict)
    elem_names, item_names = {}, {}
    with z.open(name) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")):
            i, ar, el = row["Item Code"], row["Area Code"], row["Element Code"]
            elem_names.setdefault(el, row["Element"])
            item_names.setdefault(i, row["Item"])
            try:
                v = float(row["Value"])
            except (TypeError, ValueError):
                continue
            bulk[f"{i}.{ar}.{el}"][row["Year"]] = v
    print(f"bulk {a.code}: {len(bulk):,} keys")

    miss = sorted(k for k in ours if k not in bulk)
    print(f"missing ids: {len(miss):,}")

    # store: old key -> {year: value}
    import pyarrow.parquet as pq
    store = collections.defaultdict(dict)
    need = set(miss)
    pf = pq.ParquetFile(os.path.join(ROOT, "data", "clean_full", a.source,
                                     a.source + ".parquet"))
    for b in pf.iter_batches(columns=["series_key", "obs_date", "value"],
                             batch_size=500_000):
        for k, d, v in zip(b.column(0).to_pylist(), b.column(1).to_pylist(),
                           b.column(2).to_pylist()):
            suf = k.split(":", 1)[1] if ":" in k else k
            # store keys carry the FAO_XX: prefix form; normalize to bare suffix
            suf = suf.split(":", 1)[1] if ":" in suf else suf
            if suf in need and v is not None:
                store[suf][str(d.year)] = v
    print(f"store series matched to missing ids: {len(store):,}")

    # index bulk keys by (area) for candidate generation
    by_area = collections.defaultdict(list)
    for k in bulk:
        i, ar, el = k.split(".")
        by_area[ar].append(k)

    xwalk, report = {}, collections.Counter()
    for old in miss:
        oi, oar, oel = old.split(".")
        obs = store.get(old)
        if not obs:
            report["no_store_values"] += 1
            continue
        scored = []
        for cand in by_area.get(oar, []):
            shared = obs.keys() & bulk[cand].keys()
            if len(shared) < 3:
                continue
            hits = sum(1 for y in shared if close(obs[y], bulk[cand][y]))
            scored.append((hits / len(shared), len(shared), cand))
        scored.sort(reverse=True)
        if not scored or scored[0][0] < a.floor:
            report["no_candidate_above_floor"] += 1
            continue
        if len(scored) > 1 and scored[1][0] > scored[0][0] - 0.05:
            report["ambiguous"] += 1
            continue
        xwalk[old] = scored[0][2]
        report["mapped"] += 1

    print("\nRESULT:", dict(report))
    # consistency: do the mappings form a coherent code-level story?
    pair_counts = collections.Counter()
    for old, new in xwalk.items():
        oi, _, oel = old.split(".")
        ni, _, nel = new.split(".")
        pair_counts[((oi, oel), (ni, nel))] += 1
    print("\ntop (item,element) -> (item,element) mappings:")
    for (o, n), c in pair_counts.most_common(15):
        print(f"  {o} -> {n}  x{c}   "
              f"[{item_names.get(n[0], '?')} / {elem_names.get(n[1], '?')}]")

    if a.emit:
        with open(a.emit, "w", encoding="utf-8") as f:
            json.dump({"source_id": a.source, "code": a.code.upper(),
                       "floor": a.floor, "mapped": len(xwalk),
                       "report": dict(report), "crosswalk": xwalk},
                      f, indent=1, sort_keys=True)
        print(f"\nwrote {a.emit} ({len(xwalk):,} mappings)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
