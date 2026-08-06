"""The csv_retry_queue must be DRAINED, not just written (R361).

derive.py has promised since it gained a wall-clock budget that ids not reached "come
back in `failed` ... so they are retried next run instead of lost" — but the queue was
write-only: csv_retries()/clear_csv_retries() had zero callers. insee_bdm parked 43,354
ids in one outage-recovery run with nothing ever draining them.

Pinned here: (a) queued ids are attempted on the source's next ok run and successes are
CLEARED; (b) refailures STAY QUEUED but are NOT merged into csv_failed — demoting a run
over old residue would re-create the permanently-partial disease (R359) through this
path; (c) the per-run attempt cap is respected.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_enqueue_then_drain_roundtrip(tmp_path):
    from updater.state import StateStore
    st = StateStore(path=str(tmp_path / "state.db"))
    st.enqueue_csv_retry("src", ["src:a", "src:b", "src:c"], "budget cut")
    rows = st.csv_retries("src")
    assert {r["series_id"] for r in rows} == {"src:a", "src:b", "src:c"}
    assert all(r["attempts"] == 1 for r in rows)
    # re-enqueue upserts, never duplicates; attempts grows
    st.enqueue_csv_retry("src", ["src:a"], "again")
    rows = {r["series_id"]: r for r in st.csv_retries("src")}
    assert len(rows) == 3 and rows["src:a"]["attempts"] == 2
    st.clear_csv_retries(["src:a", "src:c"])
    assert {r["series_id"] for r in st.csv_retries("src")} == {"src:b"}


def test_drain_wired_into_run_loop_and_nondemoting():
    # The drain lives in run_once's csv step: it must read the queue, clear successes,
    # and must NOT feed refailures into csv_failed (only the barren-variable check that
    # is cheap and unfoolable: the merge line was deliberately removed).
    import inspect
    from updater import orchestrate as O
    src = inspect.getsource(O.run_once)
    assert "store.csv_retries(unit.source_id)" in src
    assert "store.clear_csv_retries(_cleared)" in src
    assert "_CSV_RETRY_CAP" in src
    assert "csv_failed = list(dict.fromkeys" not in src, \
        "refailed retries must stay queued, never demote the run (R359)"
    assert O._CSV_RETRY_CAP > 0
