"""For each served-but-unscheduled source: is a fetcher the missing piece, or is the UPSTREAM dead?

WHY THIS MATTERS MORE THAN IT SOUNDS. Progress on "make everything auto-update" was being
reported as "N of 202 sources scheduled", which invites the reading that the remaining sources
are all fetcher work. They are not. Most of this library's long tail arrived through DBnomics,
and DBnomics stopped re-indexing several providers years ago. For those sources there is
nothing newer to fetch: an updater written against DBnomics would run daily, succeed, and
transfer no new data — the worst kind of green.

Bringing one of those current is not a fetcher. It is a re-derivation from the real publisher
PLUS reproducing our published series ids exactly (or minting new ids and retiring the old
ones, which breaks every live link). tools/prove_faostat_repair.py exists precisely because
that id-reproduction step is hard and fails silently: a wrong key template does not error, it
mints a parallel id space beside the live series and reports success.

WHAT THIS PRINTS is the newest index date DBnomics itself reports per provider, and the source
counts behind it. Deliberately NOT a pass/fail: "live" and "frozen" are a judgement about how
stale is too stale, and that judgement belongs to whoever reads it, so the DATE is the output.
One probe per provider, not per source.

Usage:  python tools/audit_upstream_liveness.py [--json out.json]
"""
from __future__ import annotations
import argparse
import importlib.util
import json
import os
import sys
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
API = "https://api.db.nomics.world/v22/datasets/{code}?limit=200"


def _get(url):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=120).read())


def _coverage():
    spec = importlib.util.spec_from_file_location(
        "cov", os.path.join(ROOT, "tools", "audit_schedule_coverage.py"))
    cov = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cov)
    return cov


def _provider_code(source_id: str) -> str:
    """The DBnomics provider this source came from, per its own _provider.json."""
    p = os.path.join(ROOT, "data", "clean_full", source_id, "_provider.json")
    code = None
    if os.path.exists(p):
        try:
            code = (json.load(open(p, encoding="utf-8")).get("provider_code") or "") or None
        except Exception:                                    # noqa: BLE001
            code = None
    return (code or source_id).split("_")[0].upper()


def _newest(code):
    """(newest_index_date, n_datasets) for a DBnomics provider; (None, 0) if not one."""
    try:
        d = _get(API.format(code=code))
    except urllib.error.HTTPError as e:
        return (None, 0) if e.code == 404 else (f"HTTP {e.code}", 0)
    except Exception as e:                                   # noqa: BLE001
        return (f"error {str(e)[:30]}", 0)
    ds = d.get("datasets", {})
    docs = ds.get("docs") or []
    if not docs:
        return (None, 0)
    stamps = [x.get("indexed_at") or x.get("updated_at") or "" for x in docs]
    return (max(stamps) if stamps else None, ds.get("num_found") or len(docs))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="also write the full table here")
    a = ap.parse_args()

    cov = _coverage()
    counts = cov.catalog_counts()
    supported = cov.supported_sources()
    sched, _ = cov.scheduled_sources()
    gap = {s: counts[s] for s in counts if s in supported and s not in sched}
    print(f"served + unscheduled: {len(gap)} sources / {sum(gap.values()):,} series\n")

    probes, rows = {}, []
    for s, n in sorted(gap.items(), key=lambda kv: -kv[1]):
        pc = _provider_code(s)
        if pc not in probes:
            probes[pc] = _newest(pc)
        newest, ndatasets = probes[pc]
        rows.append({"source": s, "series": n, "provider": pc,
                     "dbnomics_newest_index": newest, "dbnomics_datasets": ndatasets})

    by_prov = {}
    for r in rows:
        p = by_prov.setdefault(r["provider"], {"sources": 0, "series": 0,
                                               "newest": r["dbnomics_newest_index"]})
        p["sources"] += 1
        p["series"] += r["series"]

    print(f"{'provider':<12s} {'sources':>7s} {'series':>10s}  newest DBnomics index")
    for p, v in sorted(by_prov.items(), key=lambda kv: -kv[1]["series"]):
        newest = v["newest"] or "-- not a DBnomics provider --"
        print(f"{p:<12s} {v['sources']:>7d} {v['series']:>10,}  {newest}")

    print("\nRead this as: where 'newest DBnomics index' is old, a DBnomics-based updater "
          "cannot make the source current — there is nothing newer behind it. Those need a\n"
          "re-derivation from the real publisher WITH id reproduction (see "
          "tools/prove_faostat_repair.py), not a fetcher.")

    if a.json:
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump(rows, fh, indent=1)
        print(f"\nwrote {a.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
