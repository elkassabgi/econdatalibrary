"""Which sources hold store regions that NO RUNNING FETCHER WRITES? Read-only, listing only.

THE DEFECT CLASS. `tools/audit_store_vs_catalog.py` reports UNCATALOGUED (data present, no
catalogue row), PARTIAL, and ORPHAN (catalogue row, no store key). There is a fourth outcome
nothing looks for:

    CATALOGUED, PRESENT, DOWNLOADABLE - AND FROZEN,
    because no running fetcher writes that part of the store.

bea is the worked example (ledger R268, R282, R762): its fetcher iterates 2 of 12 dataset
directories and writes ONE file, so the rest of the tree is first-pass `jobs/ingest_bea_full.py`
output that no running code merges. `_tree_frontier` takes the MAX over the tree, so the source's
reported `last_obs_date` comes from the freshest file and is structurally blind to the rest.

WHY LastModified AND NOT obs_date. 50-queue.md records that LastModified has ~zero precision for
CSV STALENESS, because a merge rewrites a whole parquet for a one-row change. That warning is
about a different question. For "does anything ever write this object", the object's own write
time IS the instrument, and the skill uses it that way: ecb was cleared by "of 540 objects only 15
are more than 30 days old", and the class lesson reads "run notes tell you a sweep is truncated;
they do NOT tell you what was never fetched. Only the store answers that."

TWO THINGS THIS TOOL LEARNED THE HARD WAY, both worth keeping:

1. **Rank by the gap INSIDE a source, never by an absolute age.** The first version asked for
   objects ">= 90 days old" and returned ZERO hits fleet-wide, on a fleet where bea's frozen tree
   was sitting in plain sight. Nothing in this bucket is older than ~67 days (see 2), so the
   threshold could never fire. "0 defects" from a threshold that cannot fire is iron-rule 3's
   "0 defects in 0 files examined". The signal is a source that wrote something TODAY while other
   parts of its own store have not been written in months.

2. **There is a CENSORING BOUND and the tool prints it.** Measured 2026-09-05: the oldest object
   among 39,556 under `clean_full/` is 67 days and the oldest under `clean_grouped/` is 66, i.e.
   nothing in the bucket predates ~2026-06-30. Some fleet-wide write reset every timestamp then.
   So an object at the bound has not been written for AT LEAST that long - the age is a LOWER
   BOUND on the freeze, and the freeze DATE is unknowable from this instrument. Never quote the
   bound as "last written on <date>".

It is a SCREEN, not a verdict. An object can be old because (a) nothing writes it, or (b) upstream
published nothing for it. Only reading the fetcher's write paths separates those, so every hit
must be confirmed in code before it is called a defect (R282: scope every aggregate by which code
path WRITES those files).

Listing only - no parquet reads, so it cannot repeat R690's unbounded-read crash.

    python tools/audit_unwritten_store_regions.py [--source X ...] [--gap-days N] [--out FILE]
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _last_success():
    from updater import config
    out = {}
    try:
        con = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True)
        for sid, ok in con.execute(
                "SELECT source_id, MAX(last_success_utc) FROM unit_state GROUP BY source_id"):
            out[sid] = ok
    except Exception as ex:                                          # noqa: BLE001
        print(f"  (state unreadable: {type(ex).__name__}: {ex})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append", default=None)
    ap.add_argument("--gap-days", type=int, default=30,
                    help="flag a source when part of its store is this far behind its own newest write")
    ap.add_argument("--frozen-days", type=int, default=30,
                    help="flag a source whose NEWEST object is at least this old (wholly frozen)")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    os.environ.setdefault("AQUEDUCT_BACKEND", "r2")
    from updater import blob, config, registry                       # noqa: E402

    print(f"read at {_stamp()}   backend {config.BACKEND}")
    r2 = blob._r2_routed()
    if r2 is None:
        print("NOT ROUTED TO R2 - refusing. The local tree is a scratch mirror of the last run only "
              "(R296/R36) and would give the opposite answer.")
        return 2
    print(f"bucket {r2.bucket}")

    reg = registry.load()
    entries = {e["source_id"]: e for e in reg.get("sources", []) if e.get("source_id")}
    ids = sorted(entries)
    if a.source:
        ids = [s for s in ids if s in set(a.source)]
    succ = _last_success()
    now = dt.datetime.now(dt.timezone.utc)

    zero, single, multi = [], [], []
    for sid in ids:
        prefix = blob._path_to_key(config.source_dir(sid)).rstrip("/") + "/"
        items = []
        try:
            for page in r2.client.get_paginator("list_objects_v2").paginate(
                    Bucket=r2.bucket, Prefix=prefix):
                for o in page.get("Contents", []):
                    nm = o["Key"][len(prefix):]
                    if nm.endswith(".parquet"):
                        items.append((nm, (now - o["LastModified"]).days))
        except Exception as ex:                                      # noqa: BLE001
            print(f"  {sid:<30} LIST FAILED {type(ex).__name__}: {str(ex)[:60]}")
            continue
        rec = dict(sid=sid, n=len(items), items=items, live=entries[sid].get("live"),
                   cadence=entries[sid].get("cadence"), last_success=succ.get(sid))
        (zero if not items else single if len(items) == 1 else multi).append(rec)

    bound = max((a2 for r in (single + multi) for _, a2 in r["items"]), default=0)
    print(f"\nregistered sources screened : {len(ids)}")
    print(f"  ZERO parquets on R2       : {len(zero)}   (a different class - tools/audit_store_present.py)")
    print(f"  exactly ONE parquet       : {len(single)}   (cannot hold a frozen REGION; can be wholly frozen)")
    print(f"  TWO or more parquets      : {len(multi)}   <- the only population where this defect is possible")
    print(f"\nCENSORING BOUND: the oldest object seen anywhere is {bound}d. A fleet-wide write reset")
    print(f"every timestamp around then, so an object AT the bound has gone unwritten for AT LEAST")
    print(f"{bound} days and the freeze DATE cannot be recovered from this instrument.")
    if zero:
        print("\nNO OBJECTS ON R2: " + ", ".join(r["sid"] for r in zero))

    for r in multi:
        ages = [x for _, x in r["items"]]
        r["youngest"], r["oldest"] = min(ages), max(ages)
        r["gap"] = r["oldest"] - r["youngest"]
        hist = collections.Counter(ages)
        r["at_old_end"] = sum(n for age, n in hist.items() if age >= r["oldest"] - 3)
        r["frac_old"] = r["at_old_end"] / r["n"]
        if r["gap"] < a.gap_days:
            r["shape"] = "uniform"
        elif r["frac_old"] >= 0.5:
            r["shape"] = "CLUSTER at old end"
        elif len(hist) >= 8:
            r["shape"] = "continuous tail"
        else:
            r["shape"] = "discrete batches"
        r["groups"] = dict(collections.Counter(
            n.split("/")[0] if "/" in n else "(flat)" for n, _ in r["items"]))

    hits = [r for r in multi if r["gap"] >= a.gap_days]
    hits.sort(key=lambda r: (-r["frac_old"], -r["gap"]))
    print(f"\n=== PART OF THE STORE >= {a.gap_days}d BEHIND THAT SOURCE'S OWN NEWEST WRITE ({len(hits)}) ===")
    print(f"{'source':<24}{'files':>6}{'newest':>8}{'oldest':>8}{'at old end':>11}  {'shape':<20}last success")
    for r in hits:
        print(f"{r['sid']:<24}{r['n']:>6}{r['youngest']:>7}d{r['oldest']:>7}d"
              f"{r['at_old_end']:>7}/{r['n']:<4} {r['shape']:<20}{str(r['last_success'])[:10]}")

    clusters = [r for r in hits if r["shape"] == "CLUSTER at old end"]
    print(f"\n=== THE bea SHAPE - most of the store at ONE old timestamp ({len(clusters)}) ===")
    print("A single shared timestamp across hundreds of files is ONE bulk write, not per-file merges.")
    for r in clusters:
        print(f"  {r['sid']:<22} {r['at_old_end']} of {r['n']} objects at ~{r['oldest']}d"
              f"   dirs: {', '.join(f'{g}={n}' for g, n in sorted(r['groups'].items()))[:90]}")

    frozen = [r for r in (single + multi) if min(x for _, x in r["items"]) >= a.frozen_days]
    print(f"\n=== WHOLLY FROZEN - NEWEST object >= {a.frozen_days}d old ({len(frozen)}) ===")
    print("Annual and static publishers belong here legitimately; read the cadence column.")
    print(f"{'source':<24}{'files':>6}{'newest':>8}  {'cadence':<11}{'live':<7}last success")
    for r in sorted(frozen, key=lambda r: -min(x for _, x in r["items"])):
        print(f"{r['sid']:<24}{r['n']:>6}{min(x for _, x in r['items']):>7}d  "
              f"{str(r['cadence']):<11}{str(r['live']):<7}{str(r['last_success'])[:10]}")

    clean = [r["sid"] for r in multi if r["gap"] < a.gap_days
             and min(x for _, x in r["items"]) < a.frozen_days]
    print(f"\n=== CLEAN: whole store written recently and together ({len(clean)}) ===")
    print("  " + ", ".join(sorted(clean)))

    print("\nTHIS IS A SCREEN, NOT A VERDICT. Confirm every hit by reading that fetcher's own write")
    print("paths before calling it a defect. An old object means EITHER nothing writes it OR the")
    print("publisher had nothing new, and only the code tells you which.")

    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump([{k: v for k, v in r.items() if k != "items"} for r in multi], f, indent=1, default=str)
        print(f"\nwrote {a.out}")
    print(f"\ndone {_stamp()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
