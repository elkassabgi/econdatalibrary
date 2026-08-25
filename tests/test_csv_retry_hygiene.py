"""The csv_retry_queue must hold CATALOG ids only — raw store keys can never resolve.

Run 32816867502 (2026-08-25): ember's queue held 161,843 colon-free STORE keys
('01 Apr 2025 (Tue)|Daily (2 years)|Hard coal'), every drain attempted 20,000 of
them, and every one failed on `series_id.split(":", 1)` (ValueError: not enough
values to unpack) — cleared 0, still queued, ~1h wasted per run, forever. The
injection point was _derive_changed_csvs's crash path, which returned `changed`
(store keys) as the failed list, and the caller queued it verbatim.

Pinned here, each with the case it must BLOCK and the case it must PASS (R414):
(a) the crash path queues only ids already mapped to catalog form — never store keys;
(b) _split_retry_rows partitions queue rows into retryable vs malformed;
(c) run_once purges the malformed rows before spending drain budget on them.
"""
from __future__ import annotations

import os
import sys
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BARE_KEYS = ["01 Apr 2025 (Tue)|Daily (2 years)|Hard coal",
             "01 Apr 2022 (Fri)|Daily (all)|Gas"]


def _unit(source_id="ember"):
    u = types.SimpleNamespace()
    u.source_id = source_id
    u.key = f"{source_id}/_all"
    u.config = {}
    return u


def _res(cursors):
    r = types.SimpleNamespace()
    r.series_cursors = dict.fromkeys(cursors, "2026-08-25")
    r.obs = len(cursors)
    r.last_obs_date = None
    return r


def test_split_retry_rows_partitions_and_names_junk():
    from updater.orchestrate import _split_retry_rows
    rows = [{"series_id": "ember:yearly_electricity_data|Coal"},
            {"series_id": BARE_KEYS[0]},
            {"series_id": "ember:x"},
            {"series_id": BARE_KEYS[1]}]
    good, junk = _split_retry_rows("ember", rows)
    assert [r["series_id"] for r in good] == ["ember:yearly_electricity_data|Coal",
                                             "ember:x"]
    assert junk == BARE_KEYS
    # negative control: an all-well-formed queue purges nothing
    good2, junk2 = _split_retry_rows("ember", rows[:1])
    assert len(good2) == 1 and junk2 == []


def test_crash_path_queues_only_mapped_catalog_ids(monkeypatch):
    """derive_and_put crashes AFTER mapping: the mapped catalog ids are queued
    (legitimate retries) and the raw store keys are NOT."""
    from updater import orchestrate as O
    from updater import derive

    monkeypatch.setattr(O, "_catalog_ids_for",
                        lambda sid, changed: (["ember:a", "ember:b"], []))

    def _boom(ids, blob, **kw):
        raise RuntimeError("simulated derive crash")
    monkeypatch.setattr(derive, "derive_and_put", _boom)

    failed, note, deferred, reasons = O._derive_changed_csvs(
        _unit(), _res(BARE_KEYS), blob=object())
    assert failed == ["ember:a", "ember:b"]
    assert set(reasons) == {"ember:a", "ember:b"}
    assert deferred == []
    assert "crashed" in (note or "")
    for bare in BARE_KEYS:               # the 2026-08-25 defect: these were queued
        assert bare not in failed and bare not in reasons


def test_crash_before_mapping_queues_nothing(monkeypatch):
    """_catalog_ids_for itself crashes: nothing is queueable — the run still
    demotes on the note and the un-bumped vintage re-derives next run."""
    from updater import orchestrate as O

    def _boom(sid, changed):
        raise RuntimeError("catalog unreadable")
    monkeypatch.setattr(O, "_catalog_ids_for", _boom)

    failed, note, deferred, reasons = O._derive_changed_csvs(
        _unit(), _res(BARE_KEYS), blob=object())
    assert failed == [] and deferred == [] and reasons == {}
    assert "crashed" in (note or "")


def test_drain_purges_malformed_rows_from_queue(tmp_path):
    """The composition run_once uses: split, clear the junk, keep the rest."""
    from updater.state import StateStore
    from updater.orchestrate import _split_retry_rows
    st = StateStore(path=str(tmp_path / "state.db"))
    st.enqueue_csv_retry("ember", ["ember:ok_id"] + BARE_KEYS, "crash residue")
    rows, junk = _split_retry_rows("ember", st.csv_retries("ember"))
    assert junk == sorted(junk, key=BARE_KEYS.index) and set(junk) == set(BARE_KEYS)
    st.clear_csv_retries(junk)
    left = {r["series_id"] for r in st.csv_retries("ember")}
    assert left == {"ember:ok_id"}


def test_run_loop_wires_the_purge():
    import inspect
    from updater import orchestrate as O
    src = inspect.getsource(O.run_once)
    assert "_split_retry_rows(unit.source_id, _retry_rows)" in src
    assert "store.clear_csv_retries(_junk_ids)" in src
