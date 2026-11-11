"""A declared rotator's FIRST transient after clean rotation passes is ATTENTION, not RED-SLA.

THE DEFECT (gate run 42834554441, 2026-11-11). `_stuck_transient` measured success age from
source_state.last_success_utc. A rotator's passes end `partial` by design and never set that field
(R231), so ecb - registry `rotating: true`, last_success 2026-07-16, 37 clean pure-deferral passes
since 2026-10-20 - read 117 days "since success" and went RED-SLA on its first transient (an HTTP
504 from /service/dataflow on 2026-11-10). The docstring said ecb is "partial, never
transient_fail"; that was false.

THE RULE. For a `rotating: true` source only, `_stuck_transient` is given the age of the newest
CLEAN deferral pass of each transient unit (the gate's own deferral grammar, runs.note), never
older than last_success_utc. Nothing else in the verdict reads it.

Fixtures are real ecb notes copied verbatim from the state.db runs table (2026-11-03 and
2026-11-09 rows), not hand-written approximations of them.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import health                       # noqa: E402
from updater.state import StateStore             # noqa: E402

# runs.note of ecb's 2026-11-09T18:25:02Z pass, verbatim except the id list (the real row lists 19
# ids then "+408 more"; the grammar accepts any bracketed list, the anchor test is the part outside).
CLEAN = ("113 sub-unit(s) attempted, none failed; 427 deferred by budget, not reached in this pass "
         "[ECB__CSEC__M__LV__2023.parquet: budget 35 min spent, deferred, "
         "ECB__CSEC__M__LV__2024.parquet: budget 35 min spent, deferred, +408 more]; "
         "csv coverage note: 113 changed keys are outside ecb's curated catalogue subset "
         "(catalog_scope: subset, 0 of 113 sampled keys catalogued at any prefix) — nothing "
         "served changed, served ids coherent")
# runs.note of ecb's 2026-11-03T14:58:34Z pass, verbatim: a partial WITH a failure - not an anchor.
FAILED_PARTIAL = ("1/35 sub-unit(s) transient-failed; will retry [{'agency': 'EUROSTAT', 'id': "
                  "'ICPF', 'version': '1.0', 'dsd': 'ESTAT_ESAIEA', 'dsd_agency': 'EUROSTAT', "
                  "'dsd_version': '1.0'}: format=csvdata&detail=dataonly&startPeriod=2016-Q3: HTTP "
                  "504]; csv coverage note: 34 changed keys have no catalog row for ecb (source has "
                  "35 catalog ids, UNDER the 5,000 cap — cause is NOT the cap) — served ids "
                  "coherent")
TRANSIENT = "ecb GET https://data-api.ecb.europa.eu/service/dataflow: HTTP 504"

NOW = datetime.now(timezone.utc)


def _iso(days_ago):
    return (NOW - timedelta(days=days_ago)).isoformat()


def _run_row(st, sid, uid, status, note, days_ago):
    st.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) "
                  "VALUES(?,?,?,?,?,?,?)", (_iso(days_ago), sid, uid, status, 0, 2110.0, note))
    st.db.commit()


def _store(sid, *, last_success_days=117.0, runs=(), extra_units=()):
    """A source last succeeded `last_success_days` ago, whose `_all` unit is transient_fail right now."""
    st = StateStore(path=":memory:")
    st.upsert_source(sid, status="ok", last_success_utc=_iso(last_success_days),
                     last_attempt_utc=_iso(last_success_days))
    st.upsert_unit(sid, "_all", status="transient_fail", last_error=TRANSIENT,
                   last_attempt_utc=_iso(0.01), last_success_utc=_iso(last_success_days))
    for uid in extra_units:
        st.upsert_unit(sid, uid, status="transient_fail", last_error=TRANSIENT,
                       last_attempt_utc=_iso(0.01))
    for r in runs:
        _run_row(st, sid, *r)
    return st


def _row(st, sid):
    return next(r for r in health.assess(store=st)["sources"] if r["source"] == sid)


def test_registry_preconditions():
    from updater import registry
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    assert reg["ecb"].get("rotating") is True and reg["ecb"]["cadence"] == "daily"
    assert reg["nyfed"].get("rotating") is None and reg["nyfed"]["cadence"] == "daily"


def test_real_notes_classify_as_the_gate_reads_them():
    assert health._is_clean_deferral_note(CLEAN) is True
    assert health._is_clean_deferral_note(FAILED_PARTIAL) is False
    assert health._is_clean_deferral_note(TRANSIENT) is False


def test_one_day_transient_after_clean_rotation_is_attention():
    st = _store("ecb", runs=[("_all", "partial", CLEAN, 2.0), ("_all", "partial", CLEAN, 1.0),
                             ("_all", "transient_fail", TRANSIENT, 0.01)])
    r = _row(st, "ecb")
    assert r["health"] == "ATTENTION", r
    # the anchor feeds _stuck_transient ONLY: the displayed success age is still last_success_utc
    assert r["last_success_age_d"] is not None and r["last_success_age_d"] > 100, r


def test_fourteen_days_of_transients_is_still_red_sla():
    runs = [("_all", "partial", CLEAN, 15.0)]
    runs += [("_all", "transient_fail", TRANSIENT, d) for d in range(14, 0, -1)]
    st = _store("ecb", runs=runs)
    assert _row(st, "ecb")["health"] == "RED-SLA"


def test_past_the_two_day_sla_without_a_clean_pass_is_red_sla():
    st = _store("ecb", runs=[("_all", "partial", CLEAN, 3.0)])
    assert _row(st, "ecb")["health"] == "RED-SLA"


def test_a_non_rotating_source_is_unchanged():
    """The same history on nyfed (daily, not declared rotating) still reads RED-SLA."""
    st = _store("nyfed", runs=[("_all", "partial", CLEAN, 1.0)])
    assert _row(st, "nyfed")["health"] == "RED-SLA"


def test_a_rotator_with_no_clean_deferral_falls_back_to_last_success():
    st = _store("ecb", runs=[("_all", "partial", FAILED_PARTIAL, 1.0)])
    assert _row(st, "ecb")["health"] == "RED-SLA"
    st2 = _store("ecb", runs=[])
    assert _row(st2, "ecb")["health"] == "RED-SLA"


def test_a_partial_with_a_failure_is_never_the_anchor():
    """Newest partial is the real 11-03 failed row (1 day ago); the newest CLEAN one is 10 days old."""
    st = _store("ecb", runs=[("_all", "partial", CLEAN, 10.0), ("_all", "partial", FAILED_PARTIAL, 1.0)])
    assert _row(st, "ecb")["health"] == "RED-SLA"


def test_newest_means_newest_by_time_not_by_insert_order():
    """A clean row inserted LATER (higher id) but timed 20 days ago must not mask yesterday's."""
    st = _store("ecb", runs=[("_all", "partial", CLEAN, 1.0), ("_all", "partial", CLEAN, 20.0)])
    assert _row(st, "ecb")["health"] == "ATTENTION"


def test_one_units_clean_pass_never_vouches_for_a_sibling():
    st = _store("ecb", runs=[("_all", "partial", CLEAN, 1.0)], extra_units=("_x",))
    assert _row(st, "ecb")["health"] == "RED-SLA"
    _run_row(st, "ecb", "_x", "partial", CLEAN, 0.5)
    assert _row(st, "ecb")["health"] == "ATTENTION"


def test_a_runs_read_error_falls_back_and_never_crashes_the_gate(capsys):
    st = _store("ecb", runs=[("_all", "partial", CLEAN, 1.0)])

    def boom(*a, **k):
        raise RuntimeError("runs table unreadable")
    st.partial_runs = boom
    assert _row(st, "ecb")["health"] == "RED-SLA"
    assert "could not read rotation runs" in capsys.readouterr().out


def test_a_store_without_the_method_falls_back():
    """Duck-typed stores in other tests have no partial_runs: the gate must not crash on them."""
    class _NoRuns:
        pass
    assert health._rotation_success_utc(_NoRuns(), "ecb",
                                        [{"unit_id": "_all", "status": "transient_fail"}]) is None
