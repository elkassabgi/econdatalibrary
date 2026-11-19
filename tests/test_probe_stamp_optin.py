"""The probe strategies store a pre-run probe only for an ADMITTED fetcher, and never seal an unfinished pass.

WHY OPT-IN. _common.finalize stamps "date-tail" on every Result, so a probe-gated unit whose fetcher names no
token is fetched at every due tick. Stamping the probe instead would skip unchanged releases, but the review of a
default-on stamp (2026-11-19) found probes that see a newer release than the fetch ingests, probes that cover
1 of 21 fetched files, and probes of another URL: for those the placeholder is the only backstop. So a fetcher
is admitted one at a time (STAMP_PROBE = True) on shadow evidence, and NONE is admitted by this change.

WHAT THESE TESTS HOLD (each has a mutant that fails it - see the PR body):
  (a) default: a fetcher that does not opt in keeps exactly the old behaviour (finalize's "date-tail" is
      stored, the next tick fetches; a None result is filled with the probe as before);
  (b) admitted: a clean pass stores the probe read BEFORE the pull with its fetch date; a release landing
      during the pull stays "changed"; the next tick skips an unchanged probe (earned no_change, no update())
      and fetches a moved one; a stamp older than STAMP_MAX_AGE_DAYS forces a real fetch; a bare byte count is
      not stamped; a fetcher's own token wins;
  (c) the flip-back seal: after a partial or transient_fail the next tick fetches although the probe equals the
      stored vintage (for any fetcher with a real token, admitted or not);
  (d) http_vintage answers None for every non-2xx (an error page's headers are not a release);
  (e) NO_STAMP: ons_uk's capped pass, through its real update() and the real run_once, stores None and the
      next tick fetches; the strategy never refills it;
  (f) the shadow probe note is on every full pass of a probe strategy, and health strips it;
  (g) manual_vintage's "first-run" semantics and the stored "force" are unchanged.
Nothing goes to the network and nothing outside tmp_path is written.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import config, health, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import base  # noqa: E402
from updater.strategies import bulk_snapshot_if_changed as S5  # noqa: E402
from updater.strategies import giant_changed_units as SG  # noqa: E402
from updater.strategies import manual_vintage as S6  # noqa: E402
from updater.strategies import overwrite_if_changed as S1  # noqa: E402
from updater.strategies.base import DATE_TAIL, NO_STAMP, STAMP_SEP, Result, Unit  # noqa: E402
from updater.strategies.fetchers import _vintage  # noqa: E402
from updater.strategies.fetchers._common import Tally, finalize  # noqa: E402

OLD = "2020-01-01T00:00:00+00:00"     # last_success far enough back that every cadence is due
OLD_TOKEN = "rel:OLD"
TODAY = dt.date(2026, 11, 19)

# (strategy module, registered live source of that strategy, strategy name) - run_once does not build gii
RUN = [(S5, "ember", "bulk_snapshot_if_changed"), (S1, "epu", "overwrite_if_changed"),
       (SG, "sec_edgar_13f", "giant_changed_units")]
RUN_IDS = [r[2] for r in RUN]
ALL = [(S5, S5.BulkSnapshotIfChanged), (S1, S1.OverwriteIfChanged), (SG, SG.GiantChangedUnits),
       (S6, S6.ManualVintage)]
ALL_IDS = ["bulk", "overwrite", "giant", "manual"]


class Fake:
    """A fetcher whose probe can MOVE when update() runs (a release landing during the pull)."""

    def __init__(self, token, result=None, after=None, admitted=False, max_age=None):
        self.token, self.after, self.calls = token, after, []
        self._result = result or (lambda: finalize(Tally(), 0, None, source="fake"))   # no_change, date-tail
        if admitted:
            self.STAMP_PROBE = True
        if max_age is not None:
            self.STAMP_MAX_AGE_DAYS = max_age

    def current_vintage(self, unit):
        return self.token

    def update(self, unit, since):
        self.calls.append(unit.key)
        if self.after is not None:
            self.token = self.after
        return self._result()


def _ok():
    t = Tally()
    t.added_unit(5, "x")
    return finalize(t, 5, "2026-10-31", source="fake", series_cursors={"k": "2026-10-31"})


def _none_ok():
    r = _ok()
    r.new_vintage = None
    return r


def _partial():
    t = Tally()
    t.added_unit(1, "a")
    t.transient_unit("b")
    return finalize(t, 1, "2026-10-31", source="fake", series_cursors={"k": "2026-10-31"})


def _stamp(tok, day=TODAY):
    return f"{tok}{STAMP_SEP}{day.isoformat()}"


@pytest.fixture(autouse=True)
def _fixed_today(monkeypatch):
    monkeypatch.setattr(base, "_today", lambda: TODAY)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_LOCATION",
              "AQUEDUCT_RUN_BUDGET_MIN", "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN",
              "ECONDL_DATA"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)   # run_once sets it
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    os.makedirs(data / "_aqueduct", exist_ok=True)
    st = StateStore(str(data / "_aqueduct" / "state.db"))
    monkeypatch.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, store: ([], None, [], {}))

    class R:
        store = st

        @staticmethod
        def seed(source, strategy, vintage, status="no_change"):
            st.upsert_unit(source, "_all", strategy=strategy, status=status, upstream_vintage=vintage,
                           last_success_utc=OLD, last_attempt_utc=OLD)

        @staticmethod
        def tick(source):
            return orchestrate.run_once(sources=[source], store=st, blob=None, dry=False)

        @staticmethod
        def stored(source):
            return (st.get_unit(source, "_all") or {}).get("upstream_vintage")

        @staticmethod
        def age(source):
            st.upsert_unit(source, "_all", last_success_utc=OLD, last_attempt_utc=OLD)

        @staticmethod
        def notes(source):
            return [r[0] for r in st.db.execute("SELECT note FROM runs WHERE source_id=? ORDER BY id",
                                                 (source,)).fetchall()]
    yield R
    st.close()


def _use(monkeypatch, mod, fetcher):
    monkeypatch.setattr(mod, "get_fetcher", lambda sid: fetcher)


# (a) default off ---------------------------------------------------------------------------------

@pytest.mark.parametrize("mod,source,strategy", RUN, ids=RUN_IDS)
def test_a_fetcher_that_does_not_opt_in_keeps_the_placeholder_and_is_fetched_again(rig, monkeypatch, mod,
                                                                                  source, strategy):
    f = Fake("rel:A")
    _use(monkeypatch, mod, f)
    rig.seed(source, strategy, DATE_TAIL)
    rig.tick(source)
    assert rig.stored(source) == DATE_TAIL, rig.stored(source)
    rig.age(source)
    rig.tick(source)
    assert f.calls == [f"{source}/_all"] * 2, "a fetcher that did not opt in must be fetched at every due tick"


@pytest.mark.parametrize("mod,cls", ALL, ids=ALL_IDS)
def test_the_old_none_fill_is_unchanged_for_a_fetcher_that_does_not_opt_in(monkeypatch, mod, cls):
    f = Fake("rel:A", result=_none_ok)
    _use(monkeypatch, mod, f)
    s = cls()
    unit = Unit("x", "_all", "s")
    s.detect_change(unit, {"upstream_vintage": OLD_TOKEN})
    assert s.run(unit, None).new_vintage == "rel:A"


def test_no_fetcher_is_admitted_by_this_change():
    import importlib
    import pathlib
    d = pathlib.Path(ROOT) / "updater" / "strategies" / "fetchers"
    admitted = [p.stem for p in sorted(d.glob("*.py"))
                if "STAMP_PROBE" in p.read_text(encoding="utf-8")]
    assert admitted == [], admitted
    assert base.stamps_probe(types.SimpleNamespace()) is False
    assert base.stamps_probe(importlib.import_module("updater.strategies.fetchers.ember")) is False


# (b) admitted ------------------------------------------------------------------------------------

@pytest.mark.parametrize("mod,source,strategy", RUN, ids=RUN_IDS)
@pytest.mark.parametrize("result", [lambda: finalize(Tally(), 0, None, source="fake"), _ok, _none_ok],
                         ids=["no_change-date-tail", "ok-date-tail", "ok-None"])
def test_admitted_clean_pass_stores_the_pre_run_probe(rig, monkeypatch, mod, source, strategy, result):
    f = Fake("rel:A", result=result, after="rel:B", admitted=True)    # a release lands DURING the pull
    _use(monkeypatch, mod, f)
    rig.seed(source, strategy, DATE_TAIL)
    rig.tick(source)
    assert f.calls == [f"{source}/_all"], rig.notes(source)
    assert rig.stored(source) == _stamp("rel:A"), ("the stamp must be the probe read BEFORE the pull",
                                                   rig.stored(source))


@pytest.mark.parametrize("mod,source,strategy", RUN, ids=RUN_IDS)
def test_admitted_second_tick_skips_an_unchanged_probe_and_fetches_a_moved_one(rig, monkeypatch, mod, source,
                                                                               strategy):
    f = Fake("rel:A", admitted=True)
    _use(monkeypatch, mod, f)
    rig.seed(source, strategy, DATE_TAIL)
    rig.tick(source)
    assert rig.stored(source) == _stamp("rel:A")
    rig.age(source)
    out = rig.tick(source)
    assert f.calls == [f"{source}/_all"], "update() ran although the probe had not moved"
    assert out == [(f"{source}/_all", "no_change")], out
    assert rig.notes(source)[-1] == "vintage probe matched stored (rel:A)", rig.notes(source)
    assert rig.stored(source) == _stamp("rel:A"), "an earned no_change must not move the fetch date"
    rig.age(source)
    f.token = "rel:B"
    rig.tick(source)
    assert f.calls == [f"{source}/_all"] * 2, "a moved probe must fetch"
    assert rig.stored(source) == _stamp("rel:B")


@pytest.mark.parametrize("mod,cls", ALL, ids=ALL_IDS)
@pytest.mark.parametrize("age,skips", [(29, True), (30, False), (400, False)])
def test_admitted_stamp_older_than_max_age_forces_a_fetch(monkeypatch, mod, cls, age, skips):
    f = Fake("rel:A", admitted=True, max_age=30)
    _use(monkeypatch, mod, f)
    us = {"upstream_vintage": _stamp("rel:A", TODAY - dt.timedelta(days=age)), "status": "no_change"}
    v = cls().detect_change(Unit("x", "_all", "s"), us)
    assert (v is None) is skips, (age, v)


@pytest.mark.parametrize("mod,cls", ALL, ids=ALL_IDS)
def test_admitted_stamp_without_a_date_does_not_gate(monkeypatch, mod, cls):
    """A token stored before admission (no fetch date) is not trusted: the first admitted pass fetches."""
    _use(monkeypatch, mod, Fake("rel:A", admitted=True))
    assert cls().detect_change(Unit("x", "_all", "s"),
                               {"upstream_vintage": "rel:A", "status": "ok"}) is not None


@pytest.mark.parametrize("mod,cls", ALL, ids=ALL_IDS)
def test_admitted_bare_byte_count_and_own_token(monkeypatch, mod, cls):
    s = cls()
    unit = Unit("x", "_all", "s")
    _use(monkeypatch, mod, Fake("1048576", admitted=True))          # http_vintage's Content-Length fallback
    s.detect_change(unit, {"upstream_vintage": OLD_TOKEN, "status": "ok"})
    assert s.run(unit, None).new_vintage == DATE_TAIL, "a byte count is not a release token"

    def own():
        r = _ok()
        r.new_vintage = "manifest:fa8d64841bbceb80"
        return r
    _use(monkeypatch, mod, Fake("rel:A", result=own, admitted=True))
    s.detect_change(unit, {"upstream_vintage": OLD_TOKEN, "status": "ok"})
    assert s.run(unit, None).new_vintage == "manifest:fa8d64841bbceb80", "the fetcher's own token wins"


# (c) the flip-back seal ------------------------------------------------------------------------------

@pytest.mark.parametrize("mod,cls", ALL, ids=ALL_IDS)
@pytest.mark.parametrize("status", ["partial", "transient_fail", "definitive_fail", "killed_external"])
@pytest.mark.parametrize("admitted", [False, True], ids=["legacy", "admitted"])
def test_after_an_unclean_pass_an_equal_probe_still_fetches(monkeypatch, mod, cls, status, admitted):
    _use(monkeypatch, mod, Fake("rel:A", admitted=admitted))
    stored = _stamp("rel:A") if admitted else "rel:A"
    s = cls()
    assert s.detect_change(Unit("x", "_all", "s"), {"upstream_vintage": stored, "status": status}) is not None
    # control: the same pair after a clean pass skips
    assert s.detect_change(Unit("x", "_all", "s"), {"upstream_vintage": stored, "status": "ok"}) is None


@pytest.mark.parametrize("mod,source,strategy", RUN, ids=RUN_IDS)
def test_a_returning_probe_does_not_seal_a_partial_through_run_once(rig, monkeypatch, mod, source, strategy):
    """ok stores rel:A (the old None-fill); a partial keeps it; the probe flaps away and back to rel:A. The
    tick after the partial must FETCH, not book an earned no_change over the unfinished pass."""
    seq = {"r": _none_ok}
    f = Fake("rel:A", result=lambda: seq["r"]())
    _use(monkeypatch, mod, f)
    rig.seed(source, strategy, DATE_TAIL)
    rig.tick(source)
    assert rig.stored(source) == "rel:A"
    rig.age(source)
    seq["r"] = _partial
    f.token = "rel:A2"                 # a replica's ETag
    rig.tick(source)
    assert rig.stored(source) == "rel:A", "a partial keeps the old vintage"
    f.token = "rel:A"                  # ... and back
    seq["r"] = _none_ok
    out = rig.tick(source)
    assert len(f.calls) == 3, ("the tick after a partial must fetch", out, rig.notes(source))
    assert out == [(f"{source}/_all", "ok")], out


# (d) http_vintage --------------------------------------------------------------------------------

@pytest.mark.parametrize("code,want", [(200, '"etag-1"'), (204, '"etag-1"'), (304, None), (403, None),
                                       (404, None), (410, None), (500, None), (503, None)])
def test_http_vintage_names_a_file_only_on_a_2xx(monkeypatch, code, want):
    monkeypatch.setattr(_vintage.time, "sleep", lambda s: None)
    calls = []

    class S:
        @staticmethod
        def head(url, **k):
            calls.append(url)
            return types.SimpleNamespace(status_code=code, headers={"ETag": '"etag-1"', "Content-Length": "9"})
    assert _vintage.http_vintage("https://example.invalid/f.xlsx", session=S) == want
    if code >= 500:
        assert len(calls) == 3, "a 5xx is still retried"


# (e) NO_STAMP ------------------------------------------------------------------------------------

@pytest.fixture
def ons(tmp_path, monkeypatch):
    """ons_uk's real update() over 13 fake catalogue datasets: MAX_PER_RUN (12) caps the pass."""
    from updater.strategies.fetchers import ons_uk as O
    monkeypatch.setattr(O.config, "source_dir", lambda sid: str(tmp_path / "ons"))
    items = [{"id": f"ds{i:02d}", "last_updated": "2026-11-01",
              "links": {"latest_version": {"id": "1", "href": f"https://example.invalid/ds{i:02d}/1"}}}
             for i in range(13)]
    monkeypatch.setattr(O.ig, "get_all_datasets", lambda: [dict(it) for it in items])
    fetched = []

    def one(ds_id, version_href=None):
        fetched.append(ds_id)
        return ds_id, [f"{ds_id}:s"], [dt.date(2026, 10, 1)], [1.0], None
    monkeypatch.setattr(O, "_fetch_one", one)
    return O, fetched


def test_ons_uk_capped_pass_returns_no_stamp_and_the_strategy_keeps_it(ons, monkeypatch):
    O, fetched = ons
    _use(monkeypatch, S5, O)
    s = S5.BulkSnapshotIfChanged()
    unit = Unit("ons_uk", "_all", "bulk_snapshot_if_changed")
    assert s.detect_change(unit, {"upstream_vintage": OLD_TOKEN, "status": "ok"}) is not None
    assert s._cur and s._cur.startswith("ons_uk:")
    res = s.run(unit, None)
    assert len(fetched) == 12 and res.status == "ok", (len(fetched), res.status, res.error)
    assert res.new_vintage == NO_STAMP, res.new_vintage


def test_ons_uk_capped_pass_stores_none_and_the_next_tick_fetches(rig, ons):
    O, fetched = ons
    rig.seed("ons_uk", "bulk_snapshot_if_changed", OLD_TOKEN)
    out = rig.tick("ons_uk")
    assert out == [("ons_uk/_all", "ok")], (out, rig.notes("ons_uk"))
    assert rig.stored("ons_uk") is None, ("a capped rotation still owes a dataset; nothing may be stamped",
                                          rig.stored("ons_uk"))
    rig.age("ons_uk")
    out = rig.tick("ons_uk")                               # the catalogue has not moved
    assert len(fetched) == 13, ("the owed dataset is fetched at the next tick", fetched, out)


@pytest.mark.parametrize("mod,source,strategy", RUN, ids=RUN_IDS)
@pytest.mark.parametrize("admitted", [False, True], ids=["legacy", "admitted"])
def test_no_stamp_stores_none_through_run_once(rig, monkeypatch, mod, source, strategy, admitted):
    def res():
        r = _ok()
        r.new_vintage = NO_STAMP
        return r
    f = Fake("rel:A", result=res, admitted=admitted)
    _use(monkeypatch, mod, f)
    rig.seed(source, strategy, OLD_TOKEN)
    out = rig.tick(source)
    assert out == [(f"{source}/_all", "ok")], (out, rig.notes(source))
    assert rig.stored(source) is None, rig.stored(source)


# (f) the shadow probe note -------------------------------------------------------------------------

@pytest.mark.parametrize("mod,source,strategy", RUN, ids=RUN_IDS)
@pytest.mark.parametrize("token,shown", [("rel:A", "rel:A"), (None, "none")])
def test_every_full_pass_logs_the_pre_run_probe(rig, monkeypatch, mod, source, strategy, token, shown):
    f = Fake(token, result=_ok, after="rel:B")
    _use(monkeypatch, mod, f)
    rig.seed(source, strategy, DATE_TAIL)
    rig.tick(source)
    note = rig.notes(source)[-1]
    assert note.endswith(f"; probe note: {shown}"), note
    assert note.startswith("+5 new rows"), note
    # health reads the note without it - its gates, the stuck alarm and the digest see the old text
    rows = [r for r in health.read_runs(rig.store.db) if r["source_id"] == source]
    assert rows[-1]["note"] == "+5 new rows", rows[-1]


def test_a_strategy_without_a_probe_gets_no_probe_note():
    assert orchestrate._with_probe_note("+1 new rows", object()) == "+1 new rows"
    s = S5.BulkSnapshotIfChanged()
    s._cur = "a; b"
    assert orchestrate._with_probe_note(None, s) == "probe note: a;b"


@pytest.mark.parametrize("note,want", [(None, None), ("probe note: x", None), ("+1 new rows", "+1 new rows"),
                                       ("+1 new rows; probe note: x", "+1 new rows"),
                                       ("a; csv coverage note: y; probe note: z", "a; csv coverage note: y")])
def test_strip_probe_note(note, want):
    assert health.strip_probe_note(note) == want


def test_a_deferral_note_with_a_probe_note_is_still_a_clean_deferral():
    t = Tally()
    t.deferred_unit("b")
    t.attempted = 3
    note = finalize(t, 1, None, source="x").error
    assert health._is_clean_deferral_note(note)
    assert health._is_clean_deferral_note(note + "; probe note: rel:A")


def test_admitted_fetcher_under_manual_vintage_is_stamped(monkeypatch):
    """manual_vintage passes its fetcher to _carry_probe like the other three (gii is not built by run_once,
    so this is the strategy directly)."""
    f = Fake("ed:2", admitted=True)
    _use(monkeypatch, S6, f)
    s = S6.ManualVintage()
    unit = Unit("gii", "_all", "manual_vintage")
    assert s.detect_change(unit, {"upstream_vintage": _stamp("ed:1"), "status": "ok"}) == "ed:2"
    assert s.run(unit, None).new_vintage == _stamp("ed:2")
    assert f.calls == ["gii/_all"]


@pytest.mark.parametrize("mod,cls", ALL, ids=ALL_IDS)
def test_an_unparseable_fetch_date_does_not_gate(monkeypatch, mod, cls):
    _use(monkeypatch, mod, Fake("v1", admitted=True))
    assert base.split_stamp("v1#fetched=garbage") == ("v1#fetched=garbage", None)
    s = cls()
    assert s._probe_matches(Fake("v1", admitted=True), "v1",
                            {"upstream_vintage": "v1#fetched=garbage", "status": "ok"}) is False
    assert s.detect_change(Unit("x", "_all", "s"),
                           {"upstream_vintage": "v1#fetched=garbage", "status": "ok"}) is not None


# (g) manual_vintage + "force" unchanged ---------------------------------------------------------

def test_manual_vintage_first_run_semantics_are_unchanged(monkeypatch):
    s = S6.ManualVintage()
    unit = Unit("gii", "_all", "manual_vintage")
    _use(monkeypatch, S6, Fake(None))
    assert s.detect_change(unit, None) == "first-run"
    assert s.detect_change(unit, {"upstream_vintage": "first-run", "status": "ok"}) is None
    assert s.detect_change(unit, {"upstream_vintage": "ed:1", "status": "ok"}) is None
    _use(monkeypatch, S6, Fake("ed:2"))
    assert s.detect_change(unit, {"upstream_vintage": "ed:1", "status": "ok"}) == "ed:2"
    _use(monkeypatch, S6, Fake("ed:1"))
    assert s.detect_change(unit, {"upstream_vintage": "ed:1", "status": "ok"}) is None


@pytest.mark.parametrize("mod,source,strategy", RUN, ids=RUN_IDS)
def test_an_undeterminable_probe_still_stores_force_when_the_fetcher_names_nothing(rig, monkeypatch, mod,
                                                                                  source, strategy):
    f = Fake(None, result=lambda: Result(status="no_change", obs=0, new_vintage=None, error="no new rows"))
    _use(monkeypatch, mod, f)
    rig.seed(source, strategy, OLD_TOKEN)
    rig.tick(source)
    assert rig.stored(source) == "force"
    rig.age(source)
    rig.tick(source)
    assert len(f.calls) == 2
