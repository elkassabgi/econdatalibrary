"""AR-477 round 2 probe (reviewer's, not for merge): a pass whose STATE is not kept (refused push / kill).

Pass 1 publishes a change, the CSV phase defers K1 (booked in csv_desktop_owed of THIS run's state), and the
run's state.db is then discarded (a CAS-refused --push-state, or a killed runner). Pass 2 is the next rebuild of
the same data, on the state that WAS pushed (fresh). The question: does anything still name K1?"""
from tests.test_fhfa_ar477 import _run, _homes, IDS  # noqa: F401
from tests.test_fhfa_diff_changed_keys import D, F, _keys, _master, rig  # noqa: F401
from updater.state import StateStore


def _deferring(ids, blob, **kw):
    ids = list(dict.fromkeys(ids))
    return {"put": 0, "put_ids": [], "failed": list(ids), "deferred": len(ids), "deferred_ids": list(ids),
            "failed_reasons": {}, "skipped_identical": 0, "deferred_large": {}}


def test_r2p_a_discarded_state_loses_the_deferred_change(rig, monkeypatch, tmp_path):
    rig.rebuild({"hpi_master": _master()})
    res1 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert _keys(res1) == ["hpi_master:K1"]
    run1 = tmp_path / "run1"
    run1.mkdir()
    st1 = _run(run1, monkeypatch, res1, _deferring)
    o1, q1, d1 = _homes(st1)
    print("RUN1 (state later discarded) homes:", o1, q1, [r["series_id"] for r in d1])
    assert [r["series_id"] for r in d1] == ["fhfa:hpi_master:K1"], "precondition: run 1 booked K1"
    # run 1's state is NOT pushed. Pass 2 = the next rebuild of the same data, on the pushed (older) state.
    res2 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    print("RUN2 changed_keys:", res2.changed_keys, "cursor_cap_hit:", res2.cursor_cap_hit)
    run2 = tmp_path / "run2"
    run2.mkdir()
    asked = []

    def putting(ids, blob, **kw):
        ids = list(dict.fromkeys(ids))
        asked.extend(ids)
        return {"put": len(ids), "put_ids": ids, "failed": [], "deferred": 0, "deferred_ids": [],
                "failed_reasons": {}, "skipped_identical": 0, "deferred_large": {}}
    st2 = _run(run2, monkeypatch, res2, putting)
    o2, q2, d2 = _homes(st2)
    print("RUN2 derived:", asked, "homes:", o2, q2, d2)
    assert "fhfa:hpi_master:K1" in asked or o2 or d2, (
        "K1 changed, was never derived, and nothing names it once run 1's state was not kept")
