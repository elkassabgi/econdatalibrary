"""Which sources have NO parquet on the backend their fetcher reads?

One reported example is a class (R208/R256). statcan reported `no_change` for 18 days because its store
was deleted from R2 while its runs read r2 — every changed cube was skipped by a silent scope check and
the tally stayed empty. The same cleanup deleted other things, and any source whose fetcher is written
"refresh what the store already has" fails the same silent way.

This lists, per registered source: parquets visible on the R2 store, parquets on the local mirror, and
its last recorded status. A source with ZERO on R2 and many locally is the statcan shape. Read-only.

    python tools/audit_store_present.py [--backend r2] [--source X ...]
"""
from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", default="r2")
    ap.add_argument("--source", action="append", default=None)
    a = ap.parse_args()
    os.environ["AQUEDUCT_BACKEND"] = a.backend

    from updater import config, registry, blob
    reg = registry.load()
    sources = [e["source_id"] for e in reg.get("sources", [])]
    if a.source:
        sources = [s for s in sources if s in set(a.source)]
    print(f"backend resolved: {config.BACKEND}   sources: {len(sources)}")

    # The STORE is shared (one R2 bucket) but state.db is not: this reads the LOCAL lineage, which for a
    # cloud-run source is not the state CI wrote (R340 — two independent DBs over one store; reconciling
    # them is a merge, never a freshness comparison). The status column below is therefore a hint about
    # what THIS machine last saw, not a verdict about the source; the parquet counts are the measurement.
    print("NOTE: the parquet counts are the measurement; the status column is the LOCAL state lineage,")
    print("      which for a cloud-run source is not the state CI wrote (R340).")
    state = {}
    try:
        c = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True)
        for sid, st, att in c.execute(
                "select source_id, status, last_attempt_utc from unit_state"):
            state[sid] = (st, att)
    except Exception as ex:                                        # noqa: BLE001
        print(f"(state unreadable: {type(ex).__name__}: {ex})")

    rows = []
    for s in sources:
        d = config.source_dir(s)
        try:
            remote = len([f for f in blob.list_parquets(d)
                          if not os.path.basename(f).startswith("_")])
        except Exception as ex:                                    # noqa: BLE001
            remote = f"ERR {type(ex).__name__}"
        local = len([f for f in glob.glob(os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "data", "clean_full", s, "*.parquet"))
            if not os.path.basename(f).startswith("_")])
        st, att = state.get(s, ("(no state)", ""))
        rows.append((s, remote, local, st, (att or "")[:10]))
        print(f"  {s:28} r2 {str(remote):>7}   local {local:>7}   {st:<16} {(att or '')[:10]}", flush=True)

    print()
    empty = [r for r in rows if r[1] == 0]
    silent = [r for r in empty if r[2] > 0]
    print(f"sources with ZERO parquets on {config.BACKEND}: {len(empty)}")
    if silent:
        print(f"  of which the LOCAL mirror still holds files — the statcan shape, a store the cloud "
              f"cannot see: {len(silent)}")
        for s, rem, loc, st, att in silent:
            print(f"     {s:28} local {loc:>7} files, last status {st} ({att})")
    only_remote = [r for r in rows if isinstance(r[1], int) and r[1] > 0 and r[2] == 0]
    print(f"sources present on {config.BACKEND} but absent locally (normal for cloud-run sources): {len(only_remote)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
