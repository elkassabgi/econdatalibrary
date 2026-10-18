"""insee_melodi un-hides a WITHHELD table (api/worker/src/withheld.ts; owner decision 2026-10-17) only once it is
complete AND its served CSV holds every row, at the start of a later pass, by one conditional write.
Harness: tests/test_insee_melodi_repull.py (FakeMelodi, env)."""
from __future__ import annotations

import gzip
import hashlib
import json

from updater.strategies.fetchers import insee_melodi as M
from updater.strategies.fetchers import _melodi_state as S
from test_insee_melodi_repull import FakeMelodi, env, _control_flows, _seed_store, run_until_quiet  # noqa: F401

SEED = "DS_RP_TD_SEED"
SID = f"insee_melodi:{SEED}"
ENT = {"since": "2026-10-17", "why": "t", "until": "complete"}


class Store:
    def __init__(self):
        self.objs, self.puts = {}, []

    def get(self, key):
        return self.objs.get(key)

    def etag(self, key):
        return hashlib.md5(self.objs[key]).hexdigest() if key in self.objs else None

    def put_if(self, key, data, *, if_match=None, if_none_match=False):
        if self.etag(key) != if_match:
            return None
        self.objs[key] = data
        self.puts.append(key)
        return hashlib.md5(data).hexdigest()


def _list(ids):
    return (json.dumps({"version": 1, "ids": ids}, indent=1, sort_keys=True) + "\n").encode()


def _csv(n):
    return gzip.compress(("series_id,obs_date,value\n" + "x\n" * n).encode(), mtime=0)


def _complete(env, monkeypatch, ids, csv_lines=25):
    fake = FakeMelodi(env.clock, _control_flows())
    _seed_store(env, fake)
    run_until_quiet(env, fake)
    fe = env.state()["flows"][SEED]
    assert fe["status"] == S.COMPLETE and fe["basis"] == "origin" and fe["served"]["rows"] == 25
    st = Store()
    st.objs[M.WITHHELD_KEY] = _list(ids)
    if csv_lines is not None:
        st.objs[M._csv_object_key(SEED)] = _csv(csv_lines)
    monkeypatch.setattr(M, "_withheld_store", lambda: st)
    return fake, st


def _ids(st):
    return json.loads(st.objs[M.WITHHELD_KEY])["ids"]


def test_a_complete_table_whose_csv_holds_every_row_is_un_withheld(env, monkeypatch, capsys):
    other = {"wid:WID:x": ENT, "insee_melodi:DS_KEEP": {**ENT, "until": "operator"}}
    fake, st = _complete(env, monkeypatch, {SID: ENT, **other})
    env.run(fake)
    assert _ids(st) == other, "only the completed insee entry goes; other sources and operator entries stay"
    assert st.puts == [M.WITHHELD_KEY]
    assert f"{SEED}: UN-WITHHELD - complete per INSEE paging, CSV 25 lines == 25 rows" in capsys.readouterr().out


def test_a_csv_that_does_not_hold_every_row_keeps_it_withheld(env, monkeypatch):
    fake, st = _complete(env, monkeypatch, {SID: ENT}, csv_lines=10)
    r = env.run(fake)
    assert SID in _ids(st) and st.puts == []
    assert f"{SEED}: complete but stays WITHHELD - its served CSV has 10 data lines, the parquet 25 rows" in r.error


def test_no_csv_object_keeps_it_withheld(env, monkeypatch):
    fake, st = _complete(env, monkeypatch, {SID: ENT}, csv_lines=None)
    env.run(fake)
    assert SID in _ids(st) and st.puts == []


def test_an_owed_csv_or_an_open_attempt_keeps_it_withheld(env, monkeypatch):
    fake, st = _complete(env, monkeypatch, {SID: ENT})
    p = env.store / S.STATE_FILE
    d = json.loads(p.read_text(encoding="utf-8"))
    d["flows"][SEED]["csv_owed"] = "2023-12-31"
    p.write_text(json.dumps(d), encoding="utf-8")
    M._Pass(fake, S.Clock(now=env.clock, window_s=2700, source="t"), S.FlowState.load(), set()).unwithhold()
    assert SID in _ids(st) and st.puts == [], "the CSV of this merge has not been handed over yet"


def test_an_operator_entry_is_never_removed(env, monkeypatch):
    fake, st = _complete(env, monkeypatch, {SID: {**ENT, "until": "operator"}})
    env.run(fake)
    assert SID in _ids(st) and st.puts == []


def test_a_list_changed_meanwhile_is_not_overwritten(env, monkeypatch):
    """The operator writes the list AFTER this pass read it (here: while the pass reads the CSV object). The write
    must be conditional on the ETag READ WITH THE LIST - an ETag re-read at write time would overwrite the
    operator's change."""
    fake, st = _complete(env, monkeypatch, {SID: ENT})
    real_get = st.get

    def racing_get(key):
        if key == M._csv_object_key(SEED):
            st.objs[M.WITHHELD_KEY] = _list({SID: ENT, "insee_melodi:NEW": ENT})   # the operator, in between
        return real_get(key)
    st.get = racing_get
    r = env.run(fake)
    assert set(_ids(st)) == {SID, "insee_melodi:NEW"}, "the operator's newer list was overwritten"
    assert f"{M.WITHHELD_KEY} changed meanwhile - un-hide retried on a later pass" in r.error


def test_a_flow_that_is_not_complete_stays_withheld(env, monkeypatch):
    fake, st = _complete(env, monkeypatch, {SID: ENT})
    p = env.store / S.STATE_FILE
    d = json.loads(p.read_text(encoding="utf-8"))
    d["flows"][SEED]["status"] = S.NEEDS_ORIGIN                              # e.g. queued for a re-pull
    p.write_text(json.dumps(d), encoding="utf-8")
    M._Pass(fake, S.Clock(now=env.clock, window_s=2700, source="t"), S.FlowState.load(), set()).unwithhold()
    assert SID in _ids(st) and st.puts == []
    d["flows"][SEED]["status"] = S.COMPLETE
    d["flows"][SEED]["basis"] = "tail"                                       # complete, but only per a tail
    p.write_text(json.dumps(d), encoding="utf-8")
    M._Pass(fake, S.Clock(now=env.clock, window_s=2700, source="t"), S.FlowState.load(), set()).unwithhold()
    assert SID in _ids(st) and st.puts == []


def test_a_large_flow_waits_for_the_owner(env, monkeypatch):
    fake, st = _complete(env, monkeypatch, {SID: ENT})
    monkeypatch.setattr(M, "UNWITHHOLD_MAX_ROWS", 10)
    r = env.run(fake)
    assert SID in _ids(st) and "stays WITHHELD - 25 rows is above 10" in r.error


def test_an_invalid_list_is_left_as_it_is(env, monkeypatch):
    fake, st = _complete(env, monkeypatch, {SID: ENT})
    st.objs[M.WITHHELD_KEY] = b"{not json"
    r = env.run(fake)
    assert st.objs[M.WITHHELD_KEY] == b"{not json" and st.puts == []
    assert f"{M.WITHHELD_KEY} is not a valid list - nothing un-withheld" in r.error


def test_a_local_backend_run_before_t0_never_reads_the_list(env, monkeypatch):
    """The default _withheld_store returns None in the test harness (local-pre-t0): no R2 client is built."""
    assert S.backend_kind() == "local-pre-t0"
    assert M._withheld_store() is None


def test_an_operator_write_between_the_get_and_the_head_is_not_reverted(env, monkeypatch):
    """AR-341 finding 1 (its probe, ported): the operator withholds NEW while the pass asks for the list's ETag -
    AFTER the pass read the body. The If-Match must not carry the new object's ETag over the old body."""
    fake, st = _complete(env, monkeypatch, {SID: ENT})
    real_etag = st.etag

    def racing_etag(key):
        if key == M.WITHHELD_KEY and "insee_melodi:NEW" not in json.loads(st.objs[key])["ids"]:
            st.objs[key] = _list({SID: ENT, "insee_melodi:NEW": ENT})       # the operator, inside the HEAD
        return real_etag(key)
    st.etag = racing_etag
    r = env.run(fake)
    assert set(_ids(st)) == {SID, "insee_melodi:NEW"}, f"the operator's withhold was reverted: {_ids(st)}"
    assert st.puts == []
    assert f"{M.WITHHELD_KEY} changed between its read and its ETag" in r.error


def test_an_absent_list_is_loud(env, monkeypatch):
    """AR-341 finding 3: after T0 an absent list means the import forgot it - completed tables would stay hidden
    with no signal."""
    fake, st = _complete(env, monkeypatch, {SID: ENT})
    del st.objs[M.WITHHELD_KEY]
    r = env.run(fake)
    assert st.puts == [] and M.WITHHELD_KEY not in st.objs
    assert f"{M.WITHHELD_KEY} is ABSENT from the store - nothing un-withheld" in r.error


def test_a_held_origin_pull_released_by_release_holds_is_un_withheld_on_a_later_pass(env, monkeypatch):
    """AR-341 item 7: DS_SIDE_CREA_DEP_REG_NAT is withheld until the F1b step 2 (release_holds) merges its held
    ORIGIN pull keeping both spellings. That merge must leave the flow in the shape the un-hide accepts (complete,
    basis origin, no hold, no csv owed), so the table comes back without an operator write."""
    from test_insee_melodi_supersede import F, _force, _held_origin
    fake = _held_origin(env)
    _force(env, {"release_holds": [F]})
    run_until_quiet(env, fake)
    fe = env.state()["flows"][F]
    assert fe["status"] == S.COMPLETE and fe["basis"] == "origin", (fe.get("status"), fe.get("basis"))
    assert not fe.get("hold") and fe.get("csv_owed") is None and fe.get("hold_release_key") == "listed"
    rows = fe["served"]["rows"]
    sid = f"insee_melodi:{F}"
    st = Store()
    st.objs[M.WITHHELD_KEY] = _list({sid: ENT})
    st.objs[M._csv_object_key(F)] = _csv(rows)
    monkeypatch.setattr(M, "_withheld_store", lambda: st)
    env.run(fake)
    assert _ids(st) == {}, "the released flow stays hidden after its merge completed it"
    assert st.puts == [M.WITHHELD_KEY]
