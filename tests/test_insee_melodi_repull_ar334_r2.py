"""AR-334 round 2 (968112a06), ported into the branch. R2_1 is INVERTED to the decided, disclosed behaviour
(coordinator 2026-10-17: a multi-vintage repair whose vintages share years is HELD for the owner); R2_2 as
the reviewer wrote it."""
from __future__ import annotations

import json

from updater.strategies.fetchers import _melodi_state as S
from test_insee_melodi_repull import (P, FakeMelodi, env, obs, series, _control_flows,  # noqa: F401
                                      _seed_store, run_until_quiet)
from test_insee_melodi_repull_ar334 import _years, _drift_and_hold


def test_R2_1_multi_vintage_repair_with_OVERLAPPING_years_is_held_and_named(env):
    """DS_SIDE_CREA_DEP_REG_NAT / DS_RP_MENAGES_PRINC on the mirror: both vintages span the SAME years
    (2012-2025; 2011-2022 vs 2012-2023). The commit says such a repair "whose vintage the store already
    carries is merged (ADV1)" - ADV1 used disjoint years. With overlap, rule (a) counts every new current-vintage
    row as a duplicate of the old vintage and the repair is HELD."""
    flows = _control_flows()
    cur = _years(25, [2022, 2023], geo="2026")
    flows["DS_SIDE_Y"] = cur
    fake = FakeMelodi(env.clock, flows)
    _seed_store(env, fake)
    env.put("DS_SIDE_Y", _years(25, [2022, 2023], geo="2025") + cur[:P])
    run_until_quiet(env, fake)
    fe = env.state()["flows"]["DS_SIDE_Y"]
    # the decided behaviour: the 2026 rows would duplicate the stored 2025-vintage observations of the same
    # years, so the merge is HELD, and the diagnosis says the vintage is already stored
    assert fe.get("hold") == "spelling" and env.rows("DS_SIDE_Y") == 50 + P
    d = fe["hold_detail"]["diagnosis"]
    assert "GEO vintage(s) ['2026'] already stored" in d and "rows duplicate a stored observation" in d


def test_R2_2_release_holds_list_form_works_again_after_relisting(env):
    """"release_holds" is documented as "the same two forms as flows". For flows, taking a flow off the list
    for one pass and listing it again forces it again. hold_release_key is never cleared, so a flow released
    once by the list form can never be released by the list form again (its next re-vintage stays held)."""
    fake, new = _drift_and_hold(env)
    f = env.store / S.FORCE_FILE
    f.write_text(json.dumps({"release_holds": ["DS_DRIFT"]}), encoding="utf-8")
    for _ in range(2):
        env.run(fake)
    assert not env.state()["flows"]["DS_DRIFT"].get("hold")
    assert env.rows("DS_DRIFT") == 50                                   # both spellings kept
    f.write_text(json.dumps({"flows": []}), encoding="utf-8")         # off the list for a pass
    env.run(fake)
    # a second respelling arrives
    newer = [obs({**{k: v for k, v in o["dimensions"].items() if k not in ("TIME_PERIOD", "REF")}, "REF2": "Y"},
                 2023, o["measures"]["OBS_VALUE_NIVEAU"]["value"]) for o in new]
    fake.flows["DS_DRIFT"] = newer
    fake.modified["DS_DRIFT"] = "2026-11-01T00:00:00"
    for _ in range(3):
        env.run(fake)
        if env.state()["flows"]["DS_DRIFT"].get("hold"):
            break
    assert env.state()["flows"]["DS_DRIFT"].get("hold")
    f.write_text(json.dumps({"release_holds": ["DS_DRIFT"]}), encoding="utf-8")   # listed again
    for _ in range(2):
        env.run(fake)
    assert not env.state()["flows"]["DS_DRIFT"].get("hold"), "re-listing did not release the new hold"
