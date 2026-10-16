"""AR-331 adversarial tests (the post-build reviewer's, ported into the branch unchanged). Each asserts what the
binding design requires; X1-X4 failed on 87799e8, C1 and C3 are its negative controls."""
from __future__ import annotations

import json
import os

from test_insee_melodi_repull import (env, FakeMelodi, series, obs, _control_flows, _seed_store,  # noqa: F401
                                      run_until_quiet, P)
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S


def _force(env, flows):
    (env.store / S.FORCE_FILE).write_text(json.dumps({"flows": flows}), encoding="utf-8")


def test_X1_operator_force_after_bootstrap_sends_a_classified_flow_to_origin(env):
    """C1 / AR-330 r2 'operator-only _force_origin.json': the operator's lever must work on a flow that is
    already classified (legacy here) - otherwise it only ever acts on the very first pass."""
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    run_until_quiet(env, fake)
    assert env.state()["flows"]["DS_W"]["status"] in S.WALKABLE      # classified and at rest
    _force(env, ["DS_W"])
    n0 = len([u for u in fake.calls if "/data/DS_W?page=1" in u])
    env.run(fake)
    fe = env.state()["flows"]["DS_W"]
    n1 = len([u for u in fake.calls if "/data/DS_W?page=1" in u])
    assert fe["status"] in (S.NEEDS_ORIGIN, S.IN_PROGRESS) or (fe.get("basis") == "origin" and n1 > n0), \
        (fe["status"], fe.get("why"))


def test_X2_a_broken_flow_has_an_operator_exit(env):
    """'BROKEN ... worked by nobody until a person looks': the person needs a lever. The only operator file is
    _force_origin.json; listing the broken flow there must take it out of BROKEN."""
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    run_until_quiet(env, fake)
    st = env.state()
    st["flows"]["DS_W"].update(status=S.BROKEN, why="repair pull failed 3 times: X")
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    _force(env, ["DS_W"])
    for _ in range(3):
        env.run(fake)
    assert env.state()["flows"]["DS_W"]["status"] != S.BROKEN


def test_X3_a_complete_flow_whose_served_object_disappears_goes_back_to_origin(env):
    """C1(b): 'the flow is zero-row or absent' -> needs_origin. F3 now returns before that rule for any flow
    with last_full_utc, so a once-complete flow whose served object vanished is classed LEGACY (probe sized
    from the OLD s_norm) and sits absent until the 30-day backstop."""
    flows = _control_flows()
    flows["DS_C2"] = series(40, extra={"AGE": "Y"})          # stored COMPLETE: 40 rows (an exact multiple)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_C2", flows["DS_C2"])
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_C2"]
    assert fe["status"] == S.COMPLETE and fe.get("last_full_utc")
    os.remove(env.store / "DS_C2.parquet")
    r = env.run(fake)
    fe = env.state()["flows"]["DS_C2"]
    assert env.rows("DS_C2") == 40 or fe["status"] in S.REPAIR_PENDING, \
        (fe["status"], fe.get("why"), fe.get("probe"), fe.get("s_norm"), r.status)


def test_X4_a_parked_tail_attempt_does_not_survive_re_classification_to_origin(env, monkeypatch):
    """A walk tail parked as needs_refresh keeps its TAIL attempt. If the served object then changes outside
    the fetcher, check_identity re-classifies the flow (needs_origin here) but leaves the attempt attached;
    the repair slot RESUMES the tail and _finalize gives it the tail's prior_status. The truncated history
    is never re-pulled, and the flow is not pending any more."""
    flows = _control_flows()
    base = series(10, year=2020) + series(10, year=2021) + series(3, year=2022)     # 23 rows, 3 years
    flows["DS_T"] = list(base)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_T", base)
    run_until_quiet(env, fake)
    assert env.state()["flows"]["DS_T"]["status"] == S.LEGACY
    # INSEE publishes 40 more 2022 observations: the tail (startPeriod=2022) is 43 rows = 5 pages > CHUNK_PAGES
    extra = [obs({"GEO": f"2026-COM-{i:05d}", "SEX": "F"}, 2022, 1.0 + i) for i in range(40)]
    fake.flows["DS_T"] = base + extra
    fake.modified["DS_T"] = "2026-10-12T00:00:00"
    real_repair = M._Pass.repair_slot
    monkeypatch.setattr(M._Pass, "repair_slot", lambda self: None)     # the repair slot is busy elsewhere
    env.run(fake)
    fe = env.state()["flows"]["DS_T"]
    assert fe["status"] == S.NEEDS_REFRESH and (fe.get("attempt") or {}).get("mode") == "tail", fe
    # an outside writer truncates the served file to its 2020 rows
    env.put("DS_T", base[:10])
    monkeypatch.setattr(M._Pass, "repair_slot", real_repair)
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_T"]
    assert env.rows("DS_T") == len(base) + len(extra), (env.rows("DS_T"), fe["status"], fe.get("basis"), fe.get("why"))


# ------------------------------------------------------------------ negative controls (must PASS)
def test_C1_control_force_on_the_first_pass_does_send_to_origin(env):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    _force(env, ["DS_W"])
    env.run(fake)
    fe = env.state()["flows"]["DS_W"]
    assert fe.get("why") == "operator force_origin" or fe.get("basis") == "origin", fe


def test_C3_control_an_absent_never_pulled_flow_goes_to_origin(env):
    flows = _control_flows()
    flows["DS_C2"] = series(40, extra={"AGE": "Y"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.run(fake)
    fe = env.state()["flows"]["DS_C2"]
    assert fe["status"] in S.REPAIR_PENDING or env.rows("DS_C2") == 40, fe
