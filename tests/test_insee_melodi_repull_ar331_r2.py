"""AR-331 round 2 adversarial tests (the reviewer's, ported into the branch unchanged) against 6a4e310.
A FAILING test is a demonstrated defect; the C* tests are negative controls and must PASS."""
from __future__ import annotations

import json

from test_insee_melodi_repull import (env, FakeMelodi, series, _control_flows, _seed_store,  # noqa: F401
                                      run_until_quiet, P)
from updater import orchestrate
from updater.errors import DefinitiveError
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S


def _force(env, body):
    (env.store / S.FORCE_FILE).write_text(json.dumps(body), encoding="utf-8")


def _staged_pages_pulled(env):
    return [c for c in env.chunks() if "/DS_RP_TD_SEED/" in c]


def test_Y1_a_merge_refused_three_times_keeps_the_staged_pull(env, monkeypatch):
    """AR-330 C11: a DefinitiveError from merge_and_write_bounded (spill disk, shrink, column refusal) leaves
    the flow merge_pending with a loud note. 6a4e310 counts each refusal through flow_failed; the third makes
    the flow BROKEN, and condition 5's new drop then DELETES every staged chunk - a complete origin pull (weeks
    of passes on a real flow) is thrown away by a merge refusal that is not a property of the paging."""
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)

    def refuse(*a, **k):
        raise DefinitiveError("bounded merge failed before publishing (spill cap, test)")
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", refuse)
    env.run(fake)
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.MERGE_PENDING
    assert _staged_pages_pulled(env), "precondition: the complete pull is staged"
    for _ in range(3):
        env.run(fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.MERGE_PENDING and _staged_pages_pulled(env), \
        (fe["status"], fe.get("why"), _staged_pages_pulled(env))


def test_Y2_the_hard_limit_arriving_through_duckdb_is_not_booked_as_a_flow_failure(env, monkeypatch):
    """orchestrate.UNIT_TIMEOUT_FIRED exists because DuckDB turns the SIGALRM UnitTimeout into
    RuntimeError('Query interrupted'); merge.py re-raises it as such. _fatal() looks only at the class name, so
    the hard limit is booked as one more per-flow failure (and the unit runs on)."""
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)

    def interrupted(*a, **k):
        orchestrate.UNIT_TIMEOUT_FIRED = True
        raise RuntimeError("Query interrupted")
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", interrupted)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    raised = False
    try:
        env.run(fake)
    except RuntimeError:
        raised = True
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert raised or not fe.get("failures"), (fe.get("failures"), fe["status"], fe.get("why"))


def test_Y3_lifting_REPAIR_BLOCKED_does_not_re_force_every_listed_flow(env):
    """The force key without `utc` is the digest of the WHOLE file, unblock_repair_after_utc included. An
    operator who forced DS_W (the documented header format has no `utc`) and later writes
    unblock_repair_after_utc to lift REPAIR BLOCKED silently sends DS_W to a second full origin pull."""
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    _force(env, {"flows": ["DS_W"]})
    run_until_quiet(env, fake)
    assert env.state()["flows"]["DS_W"]["status"] == S.COMPLETE
    _force(env, {"flows": ["DS_W"], "unblock_repair_after_utc": "2026-10-20T00:00:00+00:00"})
    n0 = len([u for u in fake.calls if "/data/DS_W?page=1" in u])
    env.run(fake)
    n1 = len([u for u in fake.calls if "/data/DS_W?page=1" in u])
    fe = env.state()["flows"]["DS_W"]
    assert fe["status"] == S.COMPLETE and n1 == n0, (fe["status"], fe.get("why"), n1 - n0)


def test_Y4_forcing_a_second_flow_with_a_new_utc_does_not_re_pull_the_first(env):
    """Documented: 'to force a flow again, write a new utc'. The key is per FILE, so a new utc written to force
    DS_V also re-forces DS_W, which is still listed."""
    flows = _control_flows()
    flows["DS_W"] = series(13)
    flows["DS_V"] = series(14)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    env.put("DS_V", flows["DS_V"])
    _force(env, {"flows": ["DS_W"], "utc": "2026-10-17T00:00:00+00:00"})
    run_until_quiet(env, fake)
    _force(env, {"flows": ["DS_W", "DS_V"], "utc": "2026-10-18T00:00:00+00:00"})
    n0 = len([u for u in fake.calls if "/data/DS_W?page=1" in u])
    env.run(fake)
    n1 = len([u for u in fake.calls if "/data/DS_W?page=1" in u])
    assert n1 == n0, f"DS_W pulled from origin again ({n1 - n0} page-1 GETs) when only DS_V was added"


# ------------------------------------------------------------------ negative controls (must PASS)
def test_C_Y1_control_a_single_refusal_keeps_merge_pending(env, monkeypatch):
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)
    monkeypatch.setattr(M.merge, "merge_and_write_bounded",
                        lambda *a, **k: (_ for _ in ()).throw(DefinitiveError("refused (test)")))
    env.run(fake)
    assert env.state()["flows"]["DS_RP_TD_SEED"]["status"] == S.MERGE_PENDING and _staged_pages_pulled(env)


def test_C_Y3_control_an_unchanged_force_file_does_not_re_force(env):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    _force(env, {"flows": ["DS_W"]})
    run_until_quiet(env, fake)
    n0 = len([u for u in fake.calls if "/data/DS_W?page=1" in u])
    env.run(fake)
    assert len([u for u in fake.calls if "/data/DS_W?page=1" in u]) == n0
