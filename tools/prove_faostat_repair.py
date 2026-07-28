"""Can a fao_* source be repaired in place from FAOSTAT's own bulk distribution?

Our 25 fao_* sources (136,754 series) arrive via DBnomics, whose FAO index was last
refreshed 2022-04-05. FAOSTAT itself publishes a bulk API of 69 datasets, several
updated within the last month. The data is current at the publisher and four years
stale in our copy (ledger R73).

The repair turns out to need no translation table: the DBnomics-era keys ARE
FAOSTAT's own codes. `FAO_QCL:5111.1.1016` is element 5111 (Stocks), area 1
(Armenia), item 1016 (Goats) — exactly what the series title says. What is NOT known
in advance is WHICH code columns, in WHICH order, a given dataset's ids were built
from: QCL uses Element/Area/Item, AE exposes Indicator/Cost Category/Institution/
Area instead.

So the ordering is discovered rather than guessed. Every ordering of the dataset's
code columns is scored by the only thing that matters — how many of our PUBLISHED
ids it reproduces exactly — and the winner is emitted as a config the fetcher reads.
A template that reproduces few ids is refused rather than shipped, because a wrong
one does not fail loudly: it mints a parallel id space beside the live series and
reports success.

Usage:
  python tools/prove_faostat_repair.py --source fao_qcl --code QCL [--emit path.json]
"""
from __future__ import annotations

import argparse
import collections
import csv
import io
import itertools
import json
import os
import sqlite3
import sys
import urllib.request
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BULK_INDEX = "https://bulks-faostat.fao.org/production/datasets_E.json"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
# Code columns that never take part in a series identity.
SKIP_CODE_COLS = {"Year Code", "Area Code (M49)", "Item Code (CPC)",
                  "Item Code (FBS)", "Item Code (SDG)"}


def _get(url, timeout=900):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                  timeout=timeout).read()


def entry(code):
    idx = json.loads(_get(BULK_INDEX, timeout=180).decode("utf-8-sig"))
    for x in idx["Datasets"]["Dataset"]:
        if (x.get("DatasetCode") or "").upper() == code.upper():
            return x
    return None


def published_ids(source_id):
    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    return {r[0].split(":", 2)[2] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (source_id,))
        if r[0].count(":") >= 2}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", required=True)
    ap.add_argument("--code", required=True, help="FAOSTAT DatasetCode, e.g. QCL")
    ap.add_argument("--emit", help="write the winning template as JSON")
    a = ap.parse_args()

    ours = published_ids(a.source)
    print(f"{a.source}: {len(ours):,} published ids")
    e = entry(a.code)
    if not e:
        print(f"FAOSTAT has no dataset {a.code}")
        return 1
    print(f"FAOSTAT {a.code} — {e.get('DatasetName')}  updated {str(e.get('DateUpdate'))[:10]}")

    raw = _get(e["FileLocation"])
    print(f"downloaded {len(raw):,} bytes", flush=True)
    z = zipfile.ZipFile(io.BytesIO(raw))
    member = next(n for n in z.namelist() if n.lower().endswith("(normalized).csv"))
    text = z.read(member).decode("utf-8-sig", errors="replace")

    rd = csv.DictReader(io.StringIO(text))
    cols = [c for c in (rd.fieldnames or [])
            if c.endswith("Code") and c not in SKIP_CODE_COLS]
    print(f"candidate code columns: {cols}")
    rows = list(rd)
    print(f"rows: {len(rows):,}")

    # Score every ordering by exact reproduction of PUBLISHED ids. Nothing else is
    # evidence: a template can look plausible and still address a different series.
    best = None
    for r in range(min(len(cols), 4), 0, -1):
        for perm in itertools.permutations(cols, r):
            built = {".".join((row.get(c) or "").strip() for c in perm)
                     for row in rows}
            hit = len(built & ours) / max(len(ours), 1)
            if best is None or hit > best[0]:
                best = (hit, perm, len(built))
        if best and best[0] >= 0.95:
            break

    hit, perm, nbuilt = best
    print(f"\nBEST TEMPLATE: {' . '.join(perm)}")
    print(f"  reproduces {hit * 100:.1f}% of our {len(ours):,} published ids")
    print(f"  upstream distinct series: {nbuilt:,}  ({nbuilt / max(len(ours), 1):.1f}x ours)")

    years = [int((row.get("Year Code") or row.get("Year") or "0")[:4])
             for row in rows if (row.get("Year Code") or row.get("Year") or "0")[:4].isdigit()]
    print(f"  upstream year range: {min(years)}–{max(years)}")
    print()
    ok = hit >= 0.95
    print("VERDICT: " + ("REPAIRABLE IN PLACE — ids survive, gains freshness and breadth"
                        if ok else
                        f"NOT a clean repair — best template reproduces only {hit * 100:.1f}%"))
    if a.emit:
        if not ok:
            print(f"\nREFUSING to emit a config for a {hit * 100:.1f}% template — a "
                  f"fetcher built on it would mint a parallel id space.")
            return 1
        prefix = f"FAO_{a.code.upper()}"
        cfg = {"source_id": a.source, "code": a.code.upper(), "key_prefix": prefix,
               "key_columns": list(perm), "date_convention": "start",
               "derived_from": {"id_reproduction_pct": round(hit * 100, 2),
                                "published_ids": len(ours),
                                "upstream_series": nbuilt,
                                "upstream_updated": str(e.get("DateUpdate"))[:10]}}
        os.makedirs(os.path.dirname(a.emit), exist_ok=True)
        io.open(a.emit, "w", encoding="utf-8").write(
            json.dumps(cfg, indent=1, sort_keys=True))
        print(f"\nwrote {a.emit}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
