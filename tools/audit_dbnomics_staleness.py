"""How much of the library is frozen behind a stale DBnomics index?

WHY: imf_commodity is LIVE, reports `no_change` every single day, last succeeded
today — and serves commodity prices 12 months out of date. Nothing in our pipeline
is broken. DBnomics simply stopped indexing IMF/PCPS on 2025-07-16, and our change
signal is DBnomics' own dataset hash, so "nothing changed" is true of the relay and
false of the publisher.

That failure is invisible by construction: a frozen relay and a genuinely quiet
publisher produce byte-identical evidence on our side. The only way to tell them
apart is to ask DBnomics when it last re-indexed each dataset.

`indexed_at` is DBnomics' own field, so this is their claim about their own
freshness, not an inference of ours. We re-probe it live rather than reading the
crawl-time checkpoints, because the whole question is what is true NOW.

Mapping: DBnomics-relayed source ids are "<provider>_<dataset>" lowercased
(IMF/AFRREO -> imf_afrreo), which is how the bulk ingest named them. Sources that
do not match a checkpointed dataset are reported as unmapped rather than assumed
fresh -- silence is not evidence.

Usage:  python tools/audit_dbnomics_staleness.py [--days 180] [--json out.json]
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import io
import json
import os
import sqlite3
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CKPT = os.path.join(ROOT, "data", "raw", "dbnomics", "_ckpt_datasets")
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
API = "https://api.db.nomics.world/v22/series/{prov}/{ds}?limit=1&observations=0"

# Sources whose id does not follow "<provider>_<dataset>". Verified individually
# against the fetcher's own hard-coded URL, not guessed from the name.
OVERRIDES = {
    "imf_commodity": ("IMF", "PCPS"),   # updater/strategies/fetchers/imf_commodity.py
    "imf_fsi": ("IMF", "FSI"),          # jobs/ingest_imf_fsi.py
}


def checkpoint_datasets() -> dict:
    """{(PROVIDER, DATASET): crawl-time indexed_at} for every dataset we crawled."""
    out = {}
    if not os.path.isdir(CKPT):
        return out
    for f in sorted(os.listdir(CKPT)):
        if not f.endswith(".json"):
            continue
        try:
            rows = json.load(io.open(os.path.join(CKPT, f), encoding="utf-8"))
        except Exception:                                     # noqa: BLE001
            continue
        for r in rows if isinstance(rows, list) else []:
            p, d = r.get("provider_code"), r.get("dataset_code")
            if p and d:
                out[(p, d)] = r.get("indexed_at")
    return out


def has_direct_fetcher(sid: str) -> bool:
    """Does this source now pull from the PUBLISHER instead of DBnomics?

    An adversarial review found 17 of this audit's 88 "stale relayed" sources were
    not relayed at all — 11 had dedicated direct fetchers, including imf_commodity,
    the very source whose 12-month freeze motivated writing this tool. It was
    migrated to api.imf.org that same morning and left hard-coded in OVERRIDES as
    relayed, so the audit went on reporting its own fixed example as broken.

    A source is judged by what its fetcher actually does TODAY, not by a list.
    """
    f = os.path.join(ROOT, "updater", "strategies", "fetchers", f"{sid}.py")
    if not os.path.exists(f):
        return os.path.exists(os.path.join(ROOT, "updater", "strategies",
                                           "fetchers", f"{sid}_direct.py"))
    src = io.open(f, encoding="utf-8", errors="replace").read()
    if "db.nomics.world" in src or "DBNOMICS" in src:
        return False                       # still a relay
    return any(m in src for m in ("_imf_direct", "_imf_mapped", "_faostat",
                                  "api.imf.org", "bulks-faostat", "DIRECT"))


def store_sources() -> set:
    """Every source that exists on disk — NOT just those with catalog rows.

    Selecting from catalog.db missed 20 genuinely relayed sources (fao_tp, imf_cdis,
    imf_cpis, several unctad_* and unesco_*), 13 of them over a year stale, because
    a source can hold data without yet being catalogued. The store is the population;
    the catalog is a projection of part of it.
    """
    base = os.path.join(ROOT, "data", "clean_full")
    if not os.path.isdir(base):
        return set()
    return {d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))}


def probe(item):
    """Live indexed_at from DBnomics. (sid, prov, ds, indexed_at, error)."""
    sid, prov, ds = item
    try:
        req = urllib.request.Request(API.format(prov=prov, ds=ds), headers=UA)
        with urllib.request.urlopen(req, timeout=90) as r:
            d = json.load(r)
    except Exception as e:                                    # noqa: BLE001
        return sid, prov, ds, None, type(e).__name__
    dsi = d.get("dataset") if isinstance(d.get("dataset"), dict) else {}
    return sid, prov, ds, dsi.get("indexed_at"), None


def age_days(idx, now):
    if not idx:
        return None
    try:
        return (now - dt.datetime.fromisoformat(idx.replace("Z", "+00:00"))).days
    except Exception:                                         # noqa: BLE001
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--days", type=int, default=180,
                    help="flag datasets not re-indexed within this many days")
    ap.add_argument("--json", help="write the full result set here")
    a = ap.parse_args()

    ck = checkpoint_datasets()
    print(f"checkpointed DBnomics datasets: {len(ck):,}")

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    counts = {r[0]: r[1] for r in con.execute(
        "SELECT source_id, COUNT(*) FROM series GROUP BY source_id")}

    todo, unmapped, migrated = [], [], []
    # Population = what is ON DISK, union what is catalogued. Either alone is a
    # projection: the catalog omits uncatalogued sources, the store omits nothing.
    universe = sorted(store_sources() | set(counts))
    for sid in universe:
        if has_direct_fetcher(sid):
            migrated.append(sid)           # no longer a relay — not this audit's business
            continue
        if sid in OVERRIDES:
            todo.append((sid,) + OVERRIDES[sid])
            continue
        hit = next((k for k in ck if f"{k[0]}_{k[1]}".lower() == sid), None)
        if hit:
            todo.append((sid, hit[0], hit[1]))
        else:
            unmapped.append(sid)

    print(f"our sources relayed via DBnomics: {len(todo)}   "
          f"(not DBnomics-relayed / unmapped: {len(unmapped)})")
    if migrated:
        print(f"EXCLUDED — now fetch direct from the publisher, no longer relayed: "
              f"{len(migrated)}  ({', '.join(migrated[:8])}"
              f"{', ...' if len(migrated) > 8 else ''})")
    print(f"series behind the relay: "
          f"{sum(counts.get(s, 0) for s, _, _ in todo):,}")
    print("re-probing DBnomics for current indexed_at ...")
    print()

    now = dt.datetime.now(dt.timezone.utc)
    rows, errs = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for sid, prov, ds, idx, err in ex.map(probe, todo):
            if err:
                errs.append((sid, f"{prov}/{ds}", err))
                continue
            rows.append({"source_id": sid, "dataset": f"{prov}/{ds}",
                         "indexed_at": idx, "age_days": age_days(idx, now),
                         "series": counts.get(sid, 0)})

    dated = [r for r in rows if r["age_days"] is not None]
    stale = sorted([r for r in dated if r["age_days"] > a.days],
                   key=lambda r: -r["age_days"])
    fresh = [r for r in dated if r["age_days"] <= a.days]

    print(f"probed OK: {len(rows)}    probe errors: {len(errs)}")
    print(f"re-indexed within {a.days}d : {len(fresh):>3}  "
          f"{sum(r['series'] for r in fresh):>10,} series")
    print(f"STALE (>{a.days}d)          : {len(stale):>3}  "
          f"{sum(r['series'] for r in stale):>10,} series")
    print()
    if stale:
        print("%-26s %-18s %-12s %6s %11s"
              % ("our source", "dbnomics dataset", "indexed_at", "days", "series"))
        print("-" * 78)
        for r in stale:
            print("%-26s %-18s %-12s %6d %11s"
                  % (r["source_id"], r["dataset"], (r["indexed_at"] or "")[:10],
                     r["age_days"], format(r["series"], ",")))
    if errs:
        print()
        print("probe errors (%d):" % len(errs))
        for sid, dsn, e in errs[:12]:
            print("   %-26s %-18s %s" % (sid, dsn, e))

    if a.json:
        io.open(a.json, "w", encoding="utf-8").write(json.dumps(
            {"generated_utc": now.isoformat(), "threshold_days": a.days,
             "rows": rows, "errors": errs, "unmapped": unmapped}, indent=1))
        print(f"\nwrote {a.json}")

    print()
    print("A stale relay is INVISIBLE to our monitoring: the vintage signal is")
    print("DBnomics' own hash, so a frozen dataset reports no_change forever while")
    print("the health gate sees a source that succeeds every single day.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
