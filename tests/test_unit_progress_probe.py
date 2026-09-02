"""Did the pass ADVANCE anything? The question three rounds of this guard got wrong.

R630: the guard counted `runs` rows, and a row records that a unit was ATTEMPTED. The
2026-09-01 pass wrote `istat partial` - whose own note says every ISTAT host was unusable and
whose last_success_utc has not moved since 2026-07-14 - and that row was read as work, so the
20-hour clock was stamped over a pass that advanced nothing. Three rounds, three mechanisms,
the same outcome, and no test ever covered the load-bearing change.

The first test below is that pass, in miniature. It is the one that would have failed in round
three, before the code shipped.
"""
import json
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PROBE = os.path.join(ROOT, "tools", "unit_progress_probe.py")

from updater.state import StateStore  # noqa: E402


def _store(path, rows, runs=()):
    """rows: (source, unit, last_success_utc, upstream_vintage[, last_obs_date])
    runs:  (source, unit, status, dur_s) - the `runs` rows a pass would have written."""
    st = StateStore(str(path))
    for r in rows:
        src, unit, last_success, vintage = r[:4]
        last_obs = r[4] if len(r) > 4 else None
        st.db.execute(
            "INSERT INTO unit_state (source_id, unit_id, last_success_utc, upstream_vintage, "
            "last_obs_date) VALUES (?,?,?,?,?)", (src, unit, last_success, vintage, last_obs))
    for src, unit, status, dur in runs:
        st.db.execute("INSERT INTO runs (source_id, unit_id, status, dur_s) VALUES (?,?,?,?)",
                      (src, unit, status, dur))
    st.db.commit()
    st.db.close()
    return str(path)


def _run(db, *args):
    env = dict(os.environ, AQUEDUCT_STATE_DB=db, PYTHONIOENCODING="utf-8")
    out = subprocess.run([sys.executable, PROBE, *args], capture_output=True, text=True, env=env)
    return out.stdout.strip().splitlines()[-1] if out.stdout.strip() else ""


def _set(db, src, unit, last_success=None, vintage=None, last_obs=None):
    import sqlite3
    con = sqlite3.connect(db)
    con.execute("UPDATE unit_state SET last_success_utc=COALESCE(?, last_success_utc), "
                "upstream_vintage=COALESCE(?, upstream_vintage), "
                "last_obs_date=COALESCE(?, last_obs_date) WHERE source_id=? AND unit_id=?",
                (last_success, vintage, last_obs, src, unit))
    con.commit()
    con.close()


def test_a_pass_that_only_attempted_things_advanced_nothing(tmp_path):
    """THE MOTIVATING PASS, with the rows it actually wrote.

    The first version of this test inserted only unit_state and left `runs` EMPTY - so the
    round-3 row-counting code would also have returned 0 and the test would have passed under
    the very code it was meant to condemn (R634). The two `runs` rows are here now: with them,
    a count of productive rows returns 1 and stamps; the probe returns 0 because nothing in
    unit_state moved."""
    db = _store(
        tmp_path / "s.db",
        [("istat", "_all", "2026-07-14T11:23:06+00:00", "v1", "2026-06-01"),
         ("unctad_tradefoodcatbyproc", "_all", None, None, None)],
        runs=[("istat", "_all", "partial", 40.7),
              ("unctad_tradefoodcatbyproc", "_all", "killed_external", 10653.0)])
    snap = str(tmp_path / "snap.json")
    assert _run(db, "--snapshot", snap) == "2"
    assert _run(db, "--diff", snap) == "0"
    # and the instrument the first three rounds used would have said otherwise
    import sqlite3
    con = sqlite3.connect(db)
    productive = con.execute(
        "SELECT COUNT(*) FROM runs WHERE status IN ('ok','no_change','partial')").fetchone()[0]
    con.close()
    assert productive == 1, "the row counter must disagree, or this test proves nothing"


def test_the_count_is_the_number_of_units_that_advanced(tmp_path):
    """R634: every earlier fixture had at most ONE advancing unit, so replacing the count with
    `1 if advanced else 0` left the whole suite green. The number is reported to the operator,
    so it is asserted."""
    db = _store(tmp_path / "s.db", [
        ("a", "_all", "2026-01-01T00:00:00+00:00", "v1", "2020-01-01"),
        ("b", "_all", "2026-01-01T00:00:00+00:00", "v1", "2020-01-01"),
        ("c", "_all", "2026-01-01T00:00:00+00:00", "v1", "2020-01-01")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "a", "_all", last_success="2026-09-02T00:00:00+00:00")
    _set(db, "b", "_all", vintage="v2")
    assert _run(db, "--diff", snap) == "2"


def test_a_backward_move_is_a_regression_not_progress(tmp_path):
    """R634: the check was `!=`, so state replaced wholesale by a pull that lost a CAS - four
    CI writers do pull-then-push (R340) - would have read as a pass having advanced."""
    db = _store(tmp_path / "s.db", [
        ("a", "_all", "2026-09-02T01:00:00+00:00", "v1", "2026-06-01")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "a", "_all", last_success="2026-07-14T11:23:06+00:00")
    assert _run(db, "--diff", snap) == "0"


def test_a_partial_that_extends_the_data_counts(tmp_path):
    """R634: last_success_utc and upstream_vintage are ONE signal - the same `ok` boolean gates
    both - so no `partial` can advance either, and `partial` is 30.3% of runs. last_obs_date is
    not gated on `ok` and never regresses, so a productive partial is visible through it."""
    db = _store(tmp_path / "s.db", [("abs", "_all", None, "v1", "2046-06-30")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "abs", "_all", last_obs="2046-12-31")
    assert _run(db, "--diff", snap) == "1"


def test_an_advanced_last_success_counts(tmp_path):
    db = _store(tmp_path / "s.db", [("bea", "_all", "2026-08-22T01:29:00+00:00", "v1")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "bea", "_all", last_success="2026-09-02T01:29:00+00:00")
    assert _run(db, "--diff", snap) == "1"


def test_a_changed_upstream_vintage_counts_even_without_a_success(tmp_path):
    """A source can hold a new vintage without reaching a state that sets last_success."""
    db = _store(tmp_path / "s.db", [("eia", "_all", None, "2026-08-01")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "eia", "_all", vintage="2026-09-02")
    assert _run(db, "--diff", snap) == "1"


def test_a_unit_that_did_not_exist_before_counts(tmp_path):
    db = _store(tmp_path / "s.db", [("bea", "_all", "2026-08-22T01:29:00+00:00", "v1")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    import sqlite3
    con = sqlite3.connect(db)
    con.execute("INSERT INTO unit_state (source_id, unit_id, last_success_utc, upstream_vintage) "
                "VALUES ('newsrc','_all','2026-09-02T00:00:00+00:00','v9')")
    con.commit()
    con.close()
    assert _run(db, "--diff", snap) == "1"


def test_last_obs_date_is_ordered_not_merely_compared(tmp_path):
    """R638: `lo != o_lo` survived all nine tests - nothing pinned directionality on the field
    this guard added. A backward last_obs_date is a regression, not progress."""
    db = _store(tmp_path / "s.db", [("abs", "_all", None, "v1", "2046-12-31")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "abs", "_all", last_obs="2046-06-30")
    assert _run(db, "--diff", snap) == "0"


def test_the_vintage_is_UNORDERED_so_any_change_counts(tmp_path):
    """R638: `uv > o_uv` also survived, because every fixture moved the vintage forward
    alphabetically. A re-issued or alphabetically earlier vintage is still a different vintage
    and still means we hold something new."""
    db = _store(tmp_path / "s.db", [("eia", "_all", None, "2026-09-02", None)])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "eia", "_all", vintage="2026-08-01")      # alphabetically EARLIER
    assert _run(db, "--diff", snap) == "1"


def test_a_brand_new_unit_with_no_state_at_all_is_not_progress(tmp_path):
    """R634/R638: `if True` for the new-unit branch survived every test. A row that appears
    with all three fields NULL records that a unit exists, not that anything happened."""
    db = _store(tmp_path / "s.db", [("bea", "_all", "2026-08-22T01:29:00+00:00", "v1")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    import sqlite3
    con = sqlite3.connect(db)
    con.execute("INSERT INTO unit_state (source_id, unit_id) VALUES ('newsrc','_all')")
    con.commit()
    con.close()
    assert _run(db, "--diff", snap) == "0"


def test_a_format_change_in_a_date_field_is_not_progress(tmp_path):
    """R638: sec_edgar holds '01mar2026-31may2026'. If that ever becomes an ISO date, a plain
    string comparison says it advanced - '2' beats '0' - and stamps the clock on a reformat."""
    db = _store(tmp_path / "s.db", [("sec_edgar", "_all", None, "v1", "01mar2026-31may2026")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "sec_edgar", "_all", last_obs="2026-05-01")
    assert _run(db, "--diff", snap) == "0"


def test_a_move_within_that_same_format_does_count(tmp_path):
    """The other half: sec_edgar is `partial` with a NULL last_success_utc, so last_obs_date is
    the ONLY field that can ever make it count. Refusing every non-ISO value would silence it
    permanently."""
    db = _store(tmp_path / "s.db", [("sec_edgar", "_all", None, "v1", "01mar2026-31may2026")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "sec_edgar", "_all", last_obs="01jun2026-31aug2026")
    assert _run(db, "--diff", snap) == "1"


def test_a_shape_valid_but_impossible_date_is_not_orderable(tmp_path):
    """R639: the two tools disagreed. The probe tested the ISO SHAPE and gen_runbook tested the
    HORIZON, so cso's '5630-12-31' - shape-valid, not a real observation date - was ordered
    happily by one and flagged by the other. One definition now, in updater/obs_date."""
    from updater.obs_date import is_orderable_obs_date
    assert is_orderable_obs_date("2026-05-01")
    assert not is_orderable_obs_date("5630-12-31")          # past the horizon
    assert not is_orderable_obs_date("1499-12-31")          # before it
    assert not is_orderable_obs_date("01mar2026-31may2026")  # not a date at all
    assert not is_orderable_obs_date(None) and not is_orderable_obs_date("")
    # and a sentinel that sorts after every real observation is not progress
    db = _store(tmp_path / "s.db", [("cso", "_all", None, "v1", "2026-01-01")])
    snap = str(tmp_path / "snap.json")
    _run(db, "--snapshot", snap)
    _set(db, "cso", "_all", last_obs="5630-12-31")
    assert _run(db, "--diff", snap) == "0"


def test_every_failure_prints_minus_one_never_zero(tmp_path):
    """0 would be read as 'nothing advanced' and is indistinguishable from a real empty pass;
    -1 is 'unknown', which the caller treats as 'do not stamp'."""
    db = _store(tmp_path / "s.db", [("bea", "_all", None, None)])
    assert _run(db, "--diff", str(tmp_path / "missing.json")) == "-1"
    assert _run(str(tmp_path / "no-such.db"), "--snapshot", str(tmp_path / "x.json")) == "-1"
    assert _run(db) == "-1"                       # neither mode requested


def test_the_snapshot_is_json_and_names_every_unit(tmp_path):
    db = _store(tmp_path / "s.db", [
        ("a", "_all", "2026-01-01T00:00:00+00:00", "v1"),
        ("b", "u2", None, "v2")])
    snap = str(tmp_path / "snap.json")
    assert _run(db, "--snapshot", snap) == "2"
    d = json.load(open(snap, encoding="utf-8"))
    assert set(d) == {"a/_all", "b/u2"}, d


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
