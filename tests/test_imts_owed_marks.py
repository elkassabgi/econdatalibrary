"""imf_imts_direct PR B (2026-10-30, review AR-410 O9): (c) the merge-measured change signal, (b) the two-phase
owed mark _imts_owed.json, (j) Route B.

WHAT WAS WRONG ON MAIN (009b8e5a7; AR-410 matrix_head2.out 11 of 28 defects, pkc_head2.out S1n, S2):
  (c) the status came from rows ADDED, so a revision-only republish read no_change and the CSV step never ran;
  (b) a kill between merge_and_write and the Result left a store that had moved and nothing that said so: the next
      pass re-merged identical rows, read no_change, and the killed pass's keys were never derived;
  (j) a structural pass raised, so nothing an earlier pass owed was ever derived while it kept failing.

WHAT THESE TESTS HOLD (each planted; each fails on 009b8e5a7):
  * the change signal: per-call cap, revised keys only, cursors still ALL keys, a refused report = every key;
  * the mark: through blob beside the store; "merging" before the merge, fails closed; "owed" after it; "handed"
    with its token at the start of the Result's error; settled only when state.db shows the orchestrator recorded
    that token after its CSV step, never on a csv-fence note; a found mark hands over every stored key, also when
    the re-merge changes nothing (S1n), also when the kept slices are reused (revision-only kill, AR-410 P7);
  * the read fails closed: absent = nothing, raising = TransientError, corrupt = DefinitiveError whose recovery
    never says to delete it, and that recovery works;
  * Route B: a refusal, a refused merge, a raising finalize hand over what is owed or changed; with nothing, raise;
  * through the REAL orchestrate.run_once: hand-over, record, settle, then a quiet pass derives nothing.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))

import _imts_fake as fake  # noqa: E402
from test_imts_window_cap import SRC, TABLE_IDS, _slices, env, rig  # noqa: E402,F401  (fixtures)
from updater import blob, config, merge, orchestrate  # noqa: E402
from updater.errors import DefinitiveError, TransientError  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import imf_imts_direct as F  # noqa: E402

N_KEYS = len(fake.SERIES)


def _mark_path():
    return os.path.join(config.source_dir(SRC), F.OWED_FILE)


def _mark():
    p = _mark_path()
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


def _plant(obj):
    os.makedirs(config.source_dir(SRC), exist_ok=True)
    with open(_mark_path(), "w", encoding="utf-8") as fh:
        fh.write(obj if isinstance(obj, str) else json.dumps(obj))


def _kill_after_merge(env, monkeypatch, up="add"):
    """A pass killed right after merge_and_write published (the process dies; nothing returns)."""
    env.imf.up = up
    real = merge.merge_and_write

    def killed(*a, **k):
        real(*a, **k)
        raise KeyboardInterrupt("planted kill after the merge published")
    monkeypatch.setattr(merge, "merge_and_write", killed)
    with pytest.raises(KeyboardInterrupt):
        F.update(None, None)
    monkeypatch.setattr(merge, "merge_and_write", real)


def _recorded_state(path, last_error):
    st = StateStore(path)
    st.upsert_unit(SRC, "_all", strategy="bulk_snapshot_if_changed", status="ok", last_error=last_error)
    st.close()


# ---- (c) the change signal -------------------------------------------------------------------------

def test_the_cap_is_per_call_the_assembled_tables_own_size(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    seen = []
    real = merge.merge_and_write

    def spy(path, new, **k):
        seen.append((new.num_rows, k.get("report_changed_keys"), k.get("changed_keys_cap")))
        return real(path, new, **k)
    monkeypatch.setattr(merge, "merge_and_write", spy)
    r = F.update(None, None)
    assert r.status == "ok"
    assert seen == [(seen[0][0], True, seen[0][0])], seen
    assert seen[0][2] != merge.CHANGED_KEYS_CAP


def test_a_revision_hands_over_only_the_revised_keys_and_the_cursors_stay_all_keys(env):
    env.setup("base")
    env.imf.up = "rev"
    r = F.update(None, None)
    assert r.status == "ok" and "revised" in r.error
    assert sorted(r.changed_keys) == sorted(k for k in r.series_cursors
                                            if k.split(".")[2] in ("A", "Q", "M")), r.changed_keys
    assert len(r.series_cursors) == N_KEYS                   # health's frontier: every key, as before


def test_an_identical_pass_reports_an_empty_changed_set_and_leaves_no_mark(env):
    env.setup("base")
    r = F.update(None, None)
    assert (r.status, r.changed_keys) == ("no_change", {})
    assert _mark() == {}
    assert not r.error.startswith("hand-over")


def test_a_refused_report_runs_the_plain_merge_and_hands_over_every_key(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    real = merge.merge_and_write

    def refuse(path, new, **k):
        if k.get("report_changed_keys"):
            raise ValueError("report_changed_keys refused: planted")
        return real(path, new, **k)
    monkeypatch.setattr(merge, "merge_and_write", refuse)
    r = F.update(None, None)
    assert r.status == "ok" and len(r.changed_keys) == N_KEYS     # unknown = every key, never a short list


# ---- (b) the mark ------------------------------------------------------------------------------------

def test_the_mark_goes_through_blob_beside_the_store_never_beside_the_slices(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    writes = []
    real = blob.write_bytes_atomic

    def spy(path, data):
        writes.append(os.path.basename(path))
        return real(path, data)
    monkeypatch.setattr(blob, "write_bytes_atomic", spy)
    F.update(None, None)
    assert writes.count(F.OWED_FILE) == 3, writes                # merging, owed, handed
    assert os.path.dirname(_mark_path()) == config.source_dir(SRC)
    assert not os.path.exists(os.path.join(F._slice_dir(SRC), F.OWED_FILE))
    assert _mark()["form"] == "handed"


def test_s1n_a_kill_after_the_merge_leaves_merging_and_the_next_pass_hands_over_every_key(env, monkeypatch):
    env.setup("base")
    _kill_after_merge(env, monkeypatch, "add")
    assert _mark()["form"] == "merging"
    r = F.update(None, None)                                    # its own re-merge changes nothing
    assert r.status == "ok", r
    assert sorted(r.changed_keys) == sorted(r.series_cursors) and len(r.changed_keys) == N_KEYS
    assert "carried from an earlier merge's owed mark" in r.error
    assert _mark()["form"] == "handed" and r.error.startswith(_mark()["token"] + ":")


def test_a_kill_after_the_owed_save_leaves_owed_and_the_next_pass_hands_over_every_key(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    real, real_m = F.blob.read_table, merge.merge_and_write
    merged = []

    def m(*a, **k):
        r = real_m(*a, **k)
        merged.append(1)
        return r

    def die(path, *a, **k):
        if merged and path.endswith(f"{SRC}.parquet") and not k.get("columns"):
            raise KeyboardInterrupt("planted kill in the read-back, after the merge")
        return real(path, *a, **k)
    monkeypatch.setattr(merge, "merge_and_write", m)
    monkeypatch.setattr(F.blob, "read_table", die)
    with pytest.raises(KeyboardInterrupt):
        F.update(None, None)
    monkeypatch.setattr(F.blob, "read_table", real)
    assert _mark()["form"] == "owed"
    r = F.update(None, None)
    assert r.status == "ok" and len(r.changed_keys) == N_KEYS


def test_p7_revision_only_kill_before_the_slices_clear_reuses_them_and_the_mark_carries_the_debt(
        env, monkeypatch):
    env.setup("base")
    _kill_after_merge(env, monkeypatch, "rev")                  # same row count: kept slices stay valid
    env.imf.requests.clear()
    r = F.update(None, None)
    assert [u.split("?")[1] for u in env.imf.data_requests()] == ["startPeriod=2025&endPeriod=2027"]
    assert r.status == "ok" and len(r.changed_keys) == N_KEYS


def test_the_pre_merge_mark_fails_closed_nothing_is_merged_and_the_slices_stay(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    before = F.blob.row_count(env.store())
    real = F._save_owed

    def bad(d, m):
        return False if m.get("form") in ("merging", "owed") else real(d, m)
    monkeypatch.setattr(F, "_save_owed", bad)
    r = F.update(None, None)
    assert r.status == "partial" and "pre-merge owed mark not saved" in r.error
    assert r.merged_rows == 0 and F.blob.row_count(env.store()) == before
    assert len(_slices()) == 9                                   # every pulled window's file stays
    monkeypatch.setattr(F, "_save_owed", real)
    env.imf.requests.clear()
    r = F.update(None, None)
    assert r.status == "ok" and F.blob.row_count(env.store()) == before + 1
    assert [u.split("?")[1] for u in env.imf.data_requests()] == ["startPeriod=2025&endPeriod=2027"]


def test_a_handed_mark_not_recorded_is_handed_over_again(env, tmp_path, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    db = str(tmp_path / "st" / "state.db")
    os.makedirs(os.path.dirname(db))
    monkeypatch.setattr(config, "STATE_DB", db)
    _recorded_state(db, "an older attempt, before this hand-over")
    F.update(None, None)                                        # killed before the orchestrator recorded it
    tok = _mark()["token"]
    r = F.update(None, None)
    assert r.status == "ok" and len(r.changed_keys) == N_KEYS
    assert _mark()["token"] != tok


def test_an_unreadable_state_db_is_not_a_record(env, tmp_path, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    db = tmp_path / "st" / "state.db"
    os.makedirs(db.parent)
    db.write_bytes(b"not a sqlite file at all" * 100)
    monkeypatch.setattr(config, "STATE_DB", str(db))
    F.update(None, None)
    r = F.update(None, None)
    assert r.status == "ok" and len(r.changed_keys) == N_KEYS


def test_no_state_db_means_no_orchestrator_and_the_mark_settles(env):
    env.setup("base")
    env.imf.up = "add"
    assert not os.path.exists(config.STATE_DB)
    F.update(None, None)
    assert _mark()["form"] == "handed"
    r = F.update(None, None)
    assert (r.status, r.changed_keys) == ("no_change", {})
    assert _mark() == {}


def test_a_handed_mark_recorded_after_the_csv_step_is_settled(env, tmp_path, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    F.update(None, None)
    db = str(tmp_path / "st" / "state.db")
    os.makedirs(os.path.dirname(db))
    monkeypatch.setattr(config, "STATE_DB", db)
    _recorded_state(db, f"{_mark()['token']}: +1 new rows")
    r = F.update(None, None)
    assert (r.status, r.changed_keys) == ("no_change", {})
    assert _mark() == {}


def test_a_csv_fence_note_means_not_derived_and_the_mark_is_not_settled(env, tmp_path, monkeypatch):
    env.setup("base")
    env.imf.up = "add"
    F.update(None, None)
    db = str(tmp_path / "st" / "state.db")
    os.makedirs(os.path.dirname(db))
    monkeypatch.setattr(config, "STATE_DB", db)
    _recorded_state(db, f"{_mark()['token']}: +1 new rows; csv coverage note: csv phase exceeded its "
                        f"5-min fence and was abandoned for this run")
    r = F.update(None, None)
    assert r.status == "ok" and len(r.changed_keys) == N_KEYS


def test_a_stop_keeps_the_mark_and_names_the_debt(env, monkeypatch):
    env.setup("base")
    _kill_after_merge(env, monkeypatch, "add")
    env.ceiling(30.0)                                           # under the 5-min floor
    r = F.update(None, None)
    assert r.status == "partial" and "owed keys of an earlier merge" in r.error
    assert _mark()["form"] == "merging"


# ---- the read fails closed ------------------------------------------------------------------------

def test_an_absent_mark_is_nothing_owed(env):
    env.setup("base")
    assert not os.path.exists(_mark_path())
    assert F.update(None, None).status == "no_change"


def test_a_mark_whose_read_raises_is_a_transient_error_and_nothing_is_pulled(env, monkeypatch):
    env.setup("base")
    real = F.blob.read_bytes

    def boom(path):
        if path.endswith(F.OWED_FILE):
            raise OSError("planted: locked")
        return real(path)
    monkeypatch.setattr(F.blob, "read_bytes", boom)
    with pytest.raises(TransientError, match="owed mark"):
        F.update(None, None)
    assert env.imf.data_requests() == []


@pytest.mark.parametrize("body", ["{not json", "[1, 2]", "", '{"flow": "IMTS", "form": "bogus", "utc": "x"}',
                                  '{"flow": "IMTS", "form": "handed", "utc": "x"}',
                                  '{"flow": "DOTS", "form": "owed", "utc": "x"}'])
def test_a_corrupt_mark_is_definitive_names_a_recovery_that_never_deletes_and_the_recovery_works(env, body):
    env.setup("base")
    _plant(body)
    with pytest.raises(DefinitiveError) as ei:
        F.update(None, None)
    msg = str(ei.value)
    assert "RECOVERY" in msg and '"form": "owed"' in msg
    assert not any(w in msg.lower() for w in ("delete", "remove", " rm "))
    assert env.imf.data_requests() == []
    _plant({"flow": "IMTS", "form": "owed", "utc": "2026-10-30T12:00:00Z"})   # the named recovery
    r = F.update(None, None)
    assert r.status == "ok" and len(r.changed_keys) == N_KEYS


# ---- (j) Route B -------------------------------------------------------------------------------------

def test_route_b_a_structural_pass_with_a_debt_hands_it_over_with_the_refusal_text(env, monkeypatch):
    env.setup("base")
    _kill_after_merge(env, monkeypatch, "add")
    env.imf.up = "empty"
    r = F.update(None, None)
    assert r.status == "partial", r
    assert "every one of 9 slices was empty" in r.error and "handed over to the CSV step" in r.error
    assert len(r.changed_keys) == len(r.series_cursors) == N_KEYS
    assert orchestrate._should_derive_csvs(r.status, r.changed_keys)
    assert _mark()["form"] == "handed"


def test_route_b_a_window_400_with_a_debt_hands_it_over(env, monkeypatch):
    env.setup("base")
    _kill_after_merge(env, monkeypatch, "add")
    env.imf.status["1960"] = [(400, None)]
    r = F.update(None, None)
    assert r.status == "partial" and "HTTP 400" in r.error and len(r.changed_keys) == N_KEYS


def test_a_refused_merge_with_nothing_owed_raises_and_restores_the_mark(env, monkeypatch):
    env.setup("base")
    env.imf.up = "add"

    def refuse(*a, **k):
        raise DefinitiveError("planted: refusing shrink")
    monkeypatch.setattr(merge, "merge_and_write", refuse)
    with pytest.raises(DefinitiveError, match="refusing shrink"):
        F.update(None, None)
    assert _mark() == {}


def test_route_b_a_refused_merge_with_a_debt_hands_it_over(env, monkeypatch):
    env.setup("base")
    _kill_after_merge(env, monkeypatch, "add")

    def refuse(*a, **k):
        raise DefinitiveError("planted: refusing shrink")
    monkeypatch.setattr(merge, "merge_and_write", refuse)
    r = F.update(None, None)
    assert r.status == "partial" and "merge refused" in r.error and len(r.changed_keys) == N_KEYS


@pytest.mark.parametrize("up,raises", [("add", False), ("base", True)])
def test_route_b_a_finalize_that_raises_after_a_changed_merge(env, monkeypatch, up, raises):
    env.setup("base")
    env.imf.up = up
    real = F.finalize

    def fin(tally, *a, **k):
        if k.get("series_cursors"):
            raise DefinitiveError("planted: structural after the merge")
        return real(tally, *a, **k)
    monkeypatch.setattr(F, "finalize", fin)
    if raises:
        with pytest.raises(DefinitiveError, match="planted"):
            F.update(None, None)
        return
    r = F.update(None, None)
    assert r.status == "partial" and "planted: structural" in r.error
    assert len(r.series_cursors) == N_KEYS and len(r.changed_keys) == 1


# ---- through the REAL orchestrate.run_once ------------------------------------------------------------

def test_run_once_hands_over_records_settles_and_a_quiet_pass_derives_nothing(rig):
    rig.env.setup("base")
    _plant({"flow": "IMTS", "form": "merging", "utc": "2026-10-30T11:00:00Z"})   # an earlier killed merge
    rig.env.imf.up = "base"
    assert rig.run() == [(f"{SRC}/_all", "ok")]
    assert rig.put_ids() == TABLE_IDS                       # every table: the mark does not say which
    tok = _mark()["token"]
    assert (rig.state.get_unit(SRC, "_all")["last_error"] or "").startswith(tok)
    assert rig.run() == [(f"{SRC}/_all", "no_change")]       # recorded -> settled; nothing changed
    assert rig.put_ids() == []
    assert _mark() == {}


def test_run_once_killed_inside_its_csv_step_loses_nothing(rig, monkeypatch):
    rig.env.setup("base")
    rig.env.imf.up = "add"
    real = orchestrate._derive_changed_csvs

    def killed(*a, **k):
        raise KeyboardInterrupt("planted kill inside the orchestrator's CSV step")
    monkeypatch.setattr(orchestrate, "_derive_changed_csvs", killed)
    with pytest.raises(KeyboardInterrupt):
        rig.run()
    monkeypatch.setattr(orchestrate, "_derive_changed_csvs", real)
    assert _mark()["form"] == "handed"                       # returned, never recorded
    assert rig.run() == [(f"{SRC}/_all", "ok")]              # the re-merge changes nothing ...
    assert rig.put_ids() == TABLE_IDS                        # ... and every table is derived anyway
    assert rig.run() == [(f"{SRC}/_all", "no_change")]
    assert _mark() == {}
