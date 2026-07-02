"""NON-DESTRUCTIVE self-test for S4 giant_changed_units (eurostat + oecd).

Copies ONE existing production parquet for each giant into an ISOLATED temp dir,
points a per-flow Unit at the temp dir, and exercises the real fetcher end to end:
  - catalogue download + diff selects the (changed-by-empty-state) flow,
  - incremental startPeriod fetch,
  - per-flow merge (dedup + never-shrink),
  - STABLE series_key check (no 'LAST UPDATE' / no per-release token),
  - idempotency: a SECOND run must not grow the file (no duplication),
  - honest status.

PRODUCTION parquet is NEVER opened for write — only read once to seed the temp copy.
Run:  python -m updater.strategies._giant_selftest
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

import pyarrow.parquet as pq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.strategies.base import Unit          # noqa: E402
from updater.strategies.fetchers import eurostat, oecd  # noqa: E402
from updater.strategies.fetchers import _giant    # noqa: E402

PROD = os.path.join(ROOT, "data", "clean_full")


def _unstable(keys) -> int:
    """Count keys that contain a per-release token (must be 0 for a stable key)."""
    bad = 0
    for k in keys:
        ku = k.upper()
        if "LAST UPDATE" in ku or "LAST_UPDATE" in ku or ":OBS_FLAG=" in ku:
            bad += 1
    return bad


def _case(name, src_file, dst_file, fetcher, flow_id, meta):
    print(f"\n===== {name}: {flow_id} =====")
    tmp = tempfile.mkdtemp(prefix=f"giant_{name}_")
    try:
        src = os.path.join(PROD, src_file)
        dst = os.path.join(tmp, dst_file)
        shutil.copy2(src, dst)
        before = pq.read_metadata(dst).num_rows
        print(f"  seeded temp copy: {before:,} rows  ({dst})")

        # 1) direct per-flow fetch_flow: confirm STABLE key from live data
        import requests
        sess = requests.Session()
        since = _giant._max_obs_date(dst)
        table, status = fetcher.fetch_flow(flow_id, meta, since, sess)
        print(f"  fetch_flow(since={since}) -> status={status} "
              f"rows={0 if table is None else table.num_rows:,}")
        if table is not None and table.num_rows:
            keys = table.column("series_key").to_pylist()
            bad = _unstable(keys)
            print(f"  STABLE-KEY check: {bad} of {len(keys)} fetched keys carry a per-release token "
                  f"-> {'FAIL' if bad else 'OK'}")
            print(f"  sample key: {keys[0]}")
            assert bad == 0, f"{name}: series_key NOT stable ({bad} leaked tokens)"

        # 2) full driver run #1 against the temp dir (real merge / never-shrink / state)
        unit = Unit(source_id=name, unit_id="_all", strategy="giant_changed_units",
                    out_paths=[tmp], config={"refresh_cost": "giant"})
        # restrict the catalogue to JUST our flow so the test fetches one file, not 1,400.
        orig_cat = fetcher.fetch_catalog
        fetcher.fetch_catalog = lambda: {flow_id: meta}
        try:
            r1 = fetcher.update(unit, since=None)
        finally:
            fetcher.fetch_catalog = orig_cat
        after1 = pq.read_metadata(dst).num_rows if os.path.exists(dst) else 0
        print(f"  run#1 -> status={r1.status} obs={r1.obs} last_obs={r1.last_obs_date} "
              f"err={r1.error}")
        print(f"  rows after run#1: {after1:,}  (delta {after1 - before:+,})")
        assert after1 >= before, f"{name}: never-shrink violated ({before} -> {after1})"

        # confirm merged file's stored keys are all stable too
        merged_keys = pq.read_table(dst).column("series_key").to_pylist()
        # NOTE: the seed copy may carry OLD unstable eurostat keys (pre-re-key); the
        # NEWLY MERGED rows must be stable. Count only-new is hard, so assert the file
        # did not gain unstable keys beyond what the seed already had.
        seed_bad = _unstable(pq.read_table(src).column("series_key").to_pylist())
        now_bad = _unstable(merged_keys)
        print(f"  unstable keys: seed={seed_bad} -> after_merge={now_bad} "
              f"(new unstable keys added: {now_bad - seed_bad})")
        assert now_bad <= seed_bad, f"{name}: merge introduced NEW unstable keys"

        # 3) IDEMPOTENCY: run #2 must not grow the file (dedup proves no duplication)
        fetcher.fetch_catalog = lambda: {flow_id: meta}
        try:
            r2 = fetcher.update(unit, since=None)
        finally:
            fetcher.fetch_catalog = orig_cat
        after2 = pq.read_metadata(dst).num_rows
        print(f"  run#2 -> status={r2.status}  rows after run#2: {after2:,}  "
              f"(delta vs run#1 {after2 - after1:+,})")
        assert after2 == after1, f"{name}: NON-IDEMPOTENT — run#2 changed row count ({after1}->{after2})"

        # sidecar state recorded the flow
        st = _giant.load_state(tmp)
        print(f"  sidecar state[{flow_id}] = {st.get(flow_id)}")
        assert flow_id in st, f"{name}: flow not recorded in sidecar state"
        print(f"  RESULT: PASS")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    ok = True
    # EUROSTAT: small live flow teibs010 (exists in prod as TEIBS010.parquet)
    es_meta = {"vintage": "live", "filename": "TEIBS010.parquet"}
    ok &= _case("eurostat", "eurostat/TEIBS010.parquet", "TEIBS010.parquet",
                eurostat, "teibs010", es_meta)

    # OECD: small live flow DSD_DEO_2@DF_DEO_2 (exists as OECD.STI.DEP__DSD_DEO_2@DF_DEO_2.parquet)
    oc_stem = "OECD.STI.DEP__DSD_DEO_2@DF_DEO_2"
    oc_meta = {"vintage": "1.0", "filename": oc_stem + ".parquet",
               "agency": "OECD.STI.DEP", "id": "DSD_DEO_2@DF_DEO_2",
               "version": "1.0", "root": oecd.DEFAULT_ROOT}
    ok &= _case("oecd", "oecd/" + oc_stem + ".parquet", oc_stem + ".parquet",
                oecd, oc_stem, oc_meta)

    print("\n" + ("ALL PASS" if ok else "FAILURES"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
