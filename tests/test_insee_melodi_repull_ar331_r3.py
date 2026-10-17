"""AR-331 round 3 adversarial tests (the reviewer's, against a74cb6f), ported into the branch. Z1, Z3 and CZ are
unchanged. Z2 asserts the DOCUMENTED behaviour chosen under R3-1 (a force keeps a refused origin pull: a re-pull
cannot cure a merge refusal). The R3-2 and R3-3 pins follow."""
from __future__ import annotations

import json

import pytest

from test_insee_melodi_repull import (env, FakeMelodi, series, _control_flows, _seed_store,  # noqa: F401
                                      run_until_quiet, P)
from updater.errors import DefinitiveError
from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S


def _force(env, body):
    (env.store / S.FORCE_FILE).write_text(json.dumps(body), encoding="utf-8")


def _page1(fake, code):
    return len([u for u in fake.calls if f"/data/{code}?page=1" in u])


def test_Z1_a_refused_merge_never_blocks_the_rest_of_the_repair_queue(env, monkeypatch):
    """merge_pending is picked FIRST every pass; a merge refused for ever must not starve the other flows."""
    flows = _control_flows()
    flows["DS_RP_TD_B"] = series(25, extra={"AGE": "B"})
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_RP_TD_B", flows["DS_RP_TD_B"][:P])
    real = M.merge.merge_and_write_bounded

    def refuse_seed(path, *a, **k):
        if "DS_RP_TD_SEED" in str(path):
            raise DefinitiveError("spill cap (test)")
        return real(path, *a, **k)
    monkeypatch.setattr(M.merge, "merge_and_write_bounded", refuse_seed)
    for _ in range(6):
        r = env.run(fake)
    st = env.state()["flows"]
    assert st["DS_RP_TD_SEED"]["status"] == S.MERGE_PENDING and st["DS_RP_TD_SEED"]["merge_refusals"] >= 3
    assert "NEEDS THE OWNER" in r.error
    assert env.rows("DS_RP_TD_B") == 25 and st["DS_RP_TD_B"]["status"] == S.COMPLETE, st["DS_RP_TD_B"]


def test_Z2_a_force_keeps_a_refused_origin_pull_and_says_so(env, monkeypatch):
    """R3-1, chosen: keep the pull. A force on a merge_pending ORIGIN attempt whose merge is refused changes
    nothing in the attempt (a re-pull cannot cure a merge refusal): status, chunks and the refusal count stay,
    the key is recorded so it is not retried, and the note says what to do instead."""
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)
    monkeypatch.setattr(M.merge, "merge_and_write_bounded",
                        lambda *a, **k: (_ for _ in ()).throw(DefinitiveError("spill cap (test)")))
    for _ in range(3):
        env.run(fake)
    fe0 = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe0["merge_refusals"] >= 3 and fe0["attempt"]["mode"] == "origin"
    chunks0 = [c for c in env.chunks() if "/DS_RP_TD_SEED/" in c]
    _force(env, {"flows": ["DS_RP_TD_SEED"]})
    n0 = _page1(fake, "DS_RP_TD_SEED")
    r = env.run(fake)
    fe = env.state()["flows"]["DS_RP_TD_SEED"]
    assert fe["status"] == S.MERGE_PENDING and fe["attempt"]["id"] == fe0["attempt"]["id"]
    assert [c for c in env.chunks() if "/DS_RP_TD_SEED/" in c] == chunks0
    assert fe["merge_refusals"] == fe0["merge_refusals"] + 1                # still refused, still counted
    assert fe.get("forced_key") == "listed" and _page1(fake, "DS_RP_TD_SEED") == n0   # no re-pull
    assert "force_origin does not end a refused merge" in r.error
    doc = M.__doc__ or ""
    assert "THE OWNER'S ACTION" in doc and "AQUEDUCT_BOUNDED_MERGE_MEMORY" in doc
    assert "does NOT end a refused ORIGIN" in (M._Pass._merge_refused.__doc__ or "")


def test_Z3_a_present_but_shapeless_force_file_does_not_clear_every_key(env):
    """A force file that READS but has no usable `flows` (an operator typo: "flow") yields no entries, and
    every recorded forced_key is cleared as if each flow left the list. Fixing the typo then re-forces every
    listed flow into another full origin pull."""
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    _force(env, {"flows": ["DS_W"]})
    run_until_quiet(env, fake)
    _force(env, {"flow": ["DS_W"]})                         # typo
    env.run(fake)
    _force(env, {"flows": ["DS_W"]})                        # fixed
    n0 = _page1(fake, "DS_W")
    env.run(fake)
    assert _page1(fake, "DS_W") == n0, "DS_W re-pulled from origin after a typo in the force file was fixed"


# ------------------------------------------------------------------ controls (must PASS)
def test_CZ_object_form_re_forces_only_the_flow_whose_value_changed(env):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    flows["DS_V"] = series(14)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    env.put("DS_V", flows["DS_V"])
    _force(env, {"flows": {"DS_W": "2026-10-17T00:00:00", "DS_V": "2026-10-17T00:00:00"}})
    run_until_quiet(env, fake)
    _force(env, {"flows": {"DS_W": "2026-10-18T00:00:00", "DS_V": "2026-10-17T00:00:00"}})
    w0, v0 = _page1(fake, "DS_W"), _page1(fake, "DS_V")
    env.run(fake)
    assert _page1(fake, "DS_W") > w0 and _page1(fake, "DS_V") == v0


# ------------------------------------------------------------------ R3-2: every shapeless form, named
@pytest.mark.parametrize("body", ["", "   ", "[\"DS_W\"]", "{\"flows\": \"DS_W\"}", "{\"flow\": [\"DS_W\"]}",
                                  "{not json"])
def test_a_shapeless_force_file_is_ignored_loudly_and_clears_no_key(env, body):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    _force(env, {"flows": ["DS_W"]})
    run_until_quiet(env, fake)
    (env.store / S.FORCE_FILE).write_text(body, encoding="utf-8")
    r = env.run(fake)
    assert f"{S.FORCE_FILE} IGNORED this pass" in r.error
    assert env.state()["flows"]["DS_W"].get("forced_key") == "listed"


@pytest.mark.parametrize("body", [{"flows": []}, {"unblock_repair_after_utc": "2026-10-20T00:00:00+00:00"}])
def test_a_file_that_lists_no_flow_is_usable_and_takes_flows_off_the_list(env, body):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    _force(env, {"flows": ["DS_W"]})
    run_until_quiet(env, fake)
    _force(env, body)
    r = env.run(fake)
    assert "IGNORED this pass" not in (r.error or "")
    assert "forced_key" not in env.state()["flows"]["DS_W"]


# ------------------------------------------------------------------ R3-3 (R11): a force starts with no refusal count
def test_a_forced_flow_starts_with_no_merge_refusal_count(env, monkeypatch):
    flows = _control_flows()
    flows["DS_W"] = series(13)
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_W", flows["DS_W"])
    run_until_quiet(env, fake)
    st = env.state()
    st["flows"]["DS_W"]["merge_refusals"] = 2                         # left by an earlier refused TAIL merge
    (env.store / S.STATE_FILE).write_text(json.dumps(st, indent=1, sort_keys=True))
    _force(env, {"flows": ["DS_W"]})
    monkeypatch.setattr(M._Pass, "repair_slot", lambda self: None)    # no pull this pass: the force alone
    env.run(fake)
    fe = env.state()["flows"]["DS_W"]
    assert fe["status"] == S.NEEDS_ORIGIN and "merge_refusals" not in fe
