"""Cursor-grain audit — defect (a) of the changed-set contract (PHASE3 brief v2 step 1).

QUESTION, per source that persists series_cursors: can the §5.7 mapper turn this
source's ACTUAL stored cursor keys into catalogue ids? A source whose keys cannot map
never re-derives a CSV through §5.7 — norgesbank's live 23-day coherence gap, noaa's
disease at smaller scale. The instrument is the REAL `_catalog_ids_for` on a random
sample of the REAL stored keys — not a re-implementation, which could only agree with
itself (the verify_derive_parity lesson).

TWO DELIBERATE MEASUREMENT CHOICES:
  * The mapper runs under **r2 semantics** (config.BACKEND forced to "r2" for the
    call): locally, a source with <= _DERIVE_ALL_CAP catalogued ids falls into the
    derive-all rescue whenever ANY key is unmapped, which answers "is coherence saved"
    rather than "do the keys map" — and it is the CLOUD runners, which have no rescue,
    where an unmappable grain silently starves CSVs. The rescue is reported in its own
    column instead (`derive_all` = catalogued 1.._DERIVE_ALL_CAP), read beside
    run_location.
  * Everything is LOCAL: state.db (pulled fresh) + catalog.db. Zero D1.

Also reported per source, for defects (b)/(c) triage: cursor count vs catalogue count
(a cursor set far above a full-scope catalogue is the seed-inflation smell) and
CURSOR_CAP saturation (a truncated changed-set, R497).

Usage: py tools/audit_cursor_grain.py [--sample 200] [--source X ...]
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import config, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--source", nargs="*", default=None)
    ap.add_argument("--json-out", default=None)
    a = ap.parse_args()

    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    st = StateStore()
    srcs = [r[0] for r in st.db.execute(
        "SELECT source_id, COUNT(*) FROM series_cursor GROUP BY source_id ORDER BY source_id")]
    if a.source:
        srcs = [s for s in srcs if s in set(a.source)]

    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    ccon = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)

    # r2 semantics for the mapper — see module docstring. Restored on exit.
    _saved_backend = config.BACKEND
    config.BACKEND = "r2"
    rows = []
    try:
        for sid in srcs:
            cursors = st.series_cursors(sid)
            n_cur = len(cursors)
            n_cat = ccon.execute(
                "SELECT COUNT(*) FROM series WHERE series_id >= ? AND series_id < ?",
                (sid + ":", sid + ";")).fetchone()[0]
            rnd = random.Random(20260831)
            keys = list(cursors)
            # Small cursor sets run in FULL regardless of --sample: an EXPANSION
            # source's id-coverage is only fair over the whole key set (ecb's 540
            # keys sampled at 200 read idcov 51% while the full set claims 35/35),
            # and 2,000 keys is still cheap (indexed seeks).
            full_bound = max(a.sample, 2000)
            sample = keys if len(keys) <= full_bound else rnd.sample(keys, a.sample)
            ids, unmapped = orchestrate._catalog_ids_for(sid, sample)
            mapped_keys = len(sample) - len(unmapped)
            pct = (100.0 * mapped_keys / len(sample)) if sample else 0.0
            cap = getattr(config, "CURSOR_CAP", 50_000)
            # ID-COVERAGE (WU-9): key-mapped% is the WRONG KPI for one-to-many
            # EXPANSION sources — ecb's 0.5% is correct geometry (only 4 of 540 store
            # files contain catalogued series) while its expansion claims 35/35 ids.
            # The mapper only ever returns catalogue hits, so coverage is simply
            # |ids_returned| / n_catalogued. Meaningful when the sample could plausibly
            # reach the whole catalogue; a 200-key sample against norgesbank's 35k ids
            # reads ~0 and can never fake an OK-EXPANSION.
            id_cov = (100.0 * len(ids) / n_cat) if n_cat else 0.0
            klass = ("UNCATALOGUED" if n_cat == 0 else
                     "OK" if pct >= 99.5 else
                     "OK-EXPANSION" if id_cov >= 99.5 else
                     "GRAIN-MISMATCH" if pct == 0.0 else
                     "PARTIAL")
            e = reg.get(sid, {})
            rows.append({
                "source": sid, "class": klass, "n_cursors": n_cur, "n_catalogued": n_cat,
                "sample_n": len(sample), "mapped_keys": mapped_keys,
                "mapped_pct": round(pct, 1),
                "ids_returned": len(ids),
                "id_coverage_pct": round(id_cov, 1),
                "cap_saturated": n_cur >= cap,
                "derive_all_rescues_locally": 0 < n_cat <= orchestrate._DERIVE_ALL_CAP,
                "catalog_scope": str(e.get("catalog_scope", "full")),
                "run_location": e.get("run_location") or "cloud",
                "live": bool(e.get("live", False)),
                "cursor_to_catalog_ratio": round(n_cur / n_cat, 2) if n_cat else None,
                "sample_keys": sample[:3],
            })
            print(f"{klass:<15} {sid:<26} cursors={n_cur:>9,} cat={n_cat:>10,} "
                  f"mapped {mapped_keys}/{len(sample)} ({pct:5.1f}%) idcov={id_cov:5.1f}%"
                  f"{'  CAP-SATURATED' if n_cur >= cap else ''}"
                  f"{'  derive-all-rescue(local)' if 0 < n_cat <= orchestrate._DERIVE_ALL_CAP else ''}",
                  flush=True)
    finally:
        config.BACKEND = _saved_backend
        ccon.close()

    order = {"GRAIN-MISMATCH": 0, "PARTIAL": 1, "UNCATALOGUED": 2, "OK-EXPANSION": 3, "OK": 4}
    rows.sort(key=lambda r: (order.get(r["class"], 9), r["source"]))
    summary = {}
    for r in rows:
        summary[r["class"]] = summary.get(r["class"], 0) + 1
    print("\nsummary:", json.dumps(summary))
    # A FILTERED run must not clobber the fleet baseline (AR-032's note: the WU-1
    # pilot's exit-gate run silently replaced the 260-source baseline with a 1-source
    # file, and the "before" numbers survived only as spec text). --source runs land
    # in a .partial.json unless --json-out says otherwise; only full runs own the
    # baseline path.
    default_name = ("cursor_grain_audit.json" if not a.source
                    else "cursor_grain_audit.partial.json")
    out = a.json_out or os.path.join(ROOT, "data", default_name)
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"sample_per_source": a.sample, "summary": summary, "rows": rows},
                  f, indent=1)
    print("wrote", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
