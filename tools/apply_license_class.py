"""Point a source's catalog rows at a licence class declared in configs/sources.yaml.

WHY THIS EXISTS (2026-08-05): the 2026-07-16 DeFiLlama un-gating (written NC grant from
support@defillama.com, REDISTRIBUTION_COMPLIANCE.md row 'defillama') updated the denylist,
D1 and the worker — but LOCAL catalog.db kept the pre-audit 'defillama-open' row with
commercial_ok=1. An NC grant advertised as commercial-OK is the exact defect the
ei_statreview compliance entry warns about ("un-gating alone would have advertised EI data
as commercial-OK with no attribution"), and the R38 two-store rule extends to licence rows:
catalog.db and D1 must carry the SAME class or the site/API lie about terms.

The class flags are READ from configs/sources.yaml `licenses:` — one source of truth,
never re-typed here. The tool refuses classes sources.yaml does not declare, prints
before/after, and is idempotent. After it, run core/sync_catalog_d1.py --source <sid>
and tools/refresh_r2_catalog.py so all three stores agree (R38).

Usage:  python tools/apply_license_class.py <source_id> <license_class> [--apply]
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source_id")
    ap.add_argument("license_class")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    cfg = yaml.safe_load(open(os.path.join(ROOT, "configs", "sources.yaml"),
                              encoding="utf-8"))
    classes = cfg.get("licenses") or {}
    if a.license_class not in classes:
        print(f"licence class {a.license_class!r} not declared in configs/sources.yaml — "
              f"refusing (declare it there first; that file is the source of truth)")
        return 1
    src_row = (cfg.get("sources") or {}).get(a.source_id) or {}
    declared = src_row.get("license")
    if declared and declared != a.license_class:
        print(f"configs/sources.yaml declares {a.source_id} license: {declared!r}, "
              f"not {a.license_class!r} — refusing (align sources.yaml first)")
        return 1

    c = classes[a.license_class]
    flags = (1 if c.get("reservable") is True else 0,
             1 if c.get("commercial_ok") is True else 0,
             1 if c.get("attribution") == "required" else 0,
             1 if c.get("no_modify") is True else 0,
             c.get("url") or "")

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=120)
    con.execute("PRAGMA busy_timeout=120000")
    before_src = con.execute("SELECT license_id FROM source WHERE source_id=?",
                             (a.source_id,)).fetchone()
    before_series = con.execute(
        "SELECT license_id, COUNT(*) FROM series WHERE source_id=? GROUP BY 1",
        (a.source_id,)).fetchall()
    print(f"before: source={before_src}  series={before_series}")
    print(f"target: {a.license_class} -> reservable={flags[0]} commercial_ok={flags[1]} "
          f"attribution_required={flags[2]} no_modify={flags[3]}")
    if not a.apply:
        print("(dry run — pass --apply to write)")
        return 0

    con.execute("INSERT OR REPLACE INTO license(license_id,name,reservable,commercial_ok,"
                "attribution_required,no_modify,url) VALUES(?,?,?,?,?,?,?)",
                (a.license_class, a.license_class, *flags))
    con.execute("UPDATE source SET license_id=? WHERE source_id=?",
                (a.license_class, a.source_id))
    n = con.execute("UPDATE series SET license_id=? WHERE source_id=?",
                    (a.license_class, a.source_id)).rowcount
    con.commit()
    after = con.execute("SELECT license_id, COUNT(*) FROM series WHERE source_id=? GROUP BY 1",
                        (a.source_id,)).fetchall()
    print(f"applied: source + {n} series rows -> {after}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
