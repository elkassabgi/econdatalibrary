"""A probe that returns finalize's "date-tail" placeholder must never seal a unit as no_change.

WHAT WAS WRONG. _common.finalize stamps new_vintage="date-tail" on every Result, so a unit whose fetcher
names no release token stores that literal as upstream_vintage. _who_base.current_vintage returned the
same literal, meaning "always fetch". The probe strategies compared it with the stored literal, found
them equal, and answered "unchanged"; the orchestrator then booked an EARNED no_change with no fetch and
advanced last_success. who_hwf/who_rs were last fetched 2026-08-02 and who_sdg 2026-08-11, while each
due tick wrote "vintage probe matched stored (date-tail)" with dur 0.0 and health read them fresh.

WHAT THESE TESTS HOLD:
  (a) the three WHO probes return None (undeterminable -> always fetch);
  (b) bulk_snapshot_if_changed / overwrite_if_changed / giant_changed_units, given probe "date-tail" and
      stored "date-tail", answer "fetch" and expose _cur None; a real equal token still answers None;
  (c) extend_by_date and sdmx_delta still return "date-tail" (their always-fetch token by design);
  (d) through the REAL run_once: a WHO unit storing "date-tail" calls update(); ANY bulk fetcher whose probe
      returns "date-tail" calls update(); the orchestrator itself writes no earned no_change when the
      probe and the stored vintage are both "date-tail"; a real equal token still books the earned
      no_change with no update() (no regression);
  (e) no fetcher's current_vintage returns the placeholder literal.
Nothing goes to the network and nothing outside tmp_path is written.
"""
from __future__ import annotations

import ast
import os
import pathlib
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import config, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import bulk_snapshot_if_changed as S5  # noqa: E402
from updater.strategies import giant_changed_units as SG  # noqa: E402
from updater.strategies import overwrite_if_changed as S1  # noqa: E402
from updater.strategies.base import DATE_TAIL, Result, Unit  # noqa: E402
from updater.strategies.extend_by_date import ExtendByDate  # noqa: E402
from updater.strategies.fetchers import _who_base, who_hwf, who_rs, who_sdg  # noqa: E402
from updater.strategies.sdmx_delta import SdmxDelta  # noqa: E402

OLD = "2020-01-01T00:00:00+00:00"     # last_success far enough back that every cadence is due


def _fake_fetcher(token):
    calls = []

    def update(unit, since):
        calls.append(unit.key)
        return Result(status="no_change", obs=0, new_vintage=DATE_TAIL, error="no new rows")
    return types.SimpleNamespace(current_vintage=lambda unit: token, update=update, calls=calls)


# ---- (a) the WHO probes ---------------------------------------------------------------------------

@pytest.mark.parametrize("mod", [who_hwf, who_rs, who_sdg])
def test_the_who_probe_is_undeterminable_not_the_placeholder(mod):
    assert mod.current_vintage(Unit(mod.SOURCE, "_all", "bulk_snapshot_if_changed")) is None
    assert _who_base.current_vintage(None, mod.SOURCE) is None


# ---- (b) the three fetcher-probe strategies ---------------------------------------------------------

@pytest.mark.parametrize("mod,cls", [(S5, S5.BulkSnapshotIfChanged), (S1, S1.OverwriteIfChanged),
                                     (SG, SG.GiantChangedUnits)])
def test_a_placeholder_probe_equal_to_the_stored_placeholder_fetches(monkeypatch, mod, cls):
    monkeypatch.setattr(mod, "get_fetcher", lambda sid: _fake_fetcher(DATE_TAIL))
    s = cls()
    v = s.detect_change(Unit("x", "_all", "s"), {"upstream_vintage": DATE_TAIL})
    assert v is not None, "the placeholder sealed the unit as unchanged"
    assert s._cur is None, "the orchestrator must not see the placeholder as a probe token"


@pytest.mark.parametrize("mod,cls", [(S5, S5.BulkSnapshotIfChanged), (S1, S1.OverwriteIfChanged),
                                     (SG, SG.GiantChangedUnits)])
def test_a_real_equal_token_still_skips(monkeypatch, mod, cls):
    monkeypatch.setattr(mod, "get_fetcher", lambda sid: _fake_fetcher("etag:abc"))
    s = cls()
    assert s.detect_change(Unit("x", "_all", "s"), {"upstream_vintage": "etag:abc"}) is None
    assert s._cur == "etag:abc"


# ---- (c) the S2 strategies keep their always-fetch token ---------------------------------------------

@pytest.mark.parametrize("cls", [ExtendByDate, SdmxDelta])
def test_the_date_tail_strategies_still_return_their_token(cls):
    assert cls().detect_change(Unit("x", "_all", "s"), {"upstream_vintage": DATE_TAIL}) == DATE_TAIL


# ---- (d) through the real run_once ----------------------------------------------------------------

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

    def run(source, vintage):
        st.upsert_unit(source, "_all", strategy="bulk_snapshot_if_changed", status="no_change",
                       upstream_vintage=vintage, last_success_utc=OLD, last_attempt_utc=OLD)
        results = orchestrate.run_once(sources=[source], store=st, blob=None, dry=False)
        notes = [r[0] for r in st.db.execute("SELECT note FROM runs WHERE source_id=? ORDER BY id",
                                             (source,)).fetchall()]
        return results, notes
    yield run
    st.close()


@pytest.mark.parametrize("source", ["who_hwf", "who_rs", "who_sdg"])
def test_a_who_unit_storing_the_placeholder_is_fetched(rig, monkeypatch, source):
    calls = []

    def fake_run(source_id, prefix, budget_env="WHO_BASE_BUDGET_MIN"):
        calls.append(source_id)
        return Result(status="no_change", obs=0, new_vintage=DATE_TAIL, error="no new rows")
    monkeypatch.setattr(_who_base, "run", fake_run)
    results, notes = rig(source, DATE_TAIL)
    assert calls == [source], (results, notes)
    assert not any(n and n.startswith("vintage probe matched stored") for n in notes), notes


def test_any_fetcher_whose_probe_returns_the_placeholder_is_fetched(rig, monkeypatch):
    f = _fake_fetcher(DATE_TAIL)
    monkeypatch.setattr(S5, "get_fetcher", lambda sid: f)
    results, notes = rig("ember", DATE_TAIL)
    assert f.calls == ["ember/_all"], (results, notes)
    assert not any(n and n.startswith("vintage probe matched stored") for n in notes), notes


def test_the_orchestrator_books_no_earned_no_change_on_the_placeholder(rig, monkeypatch):
    """Second line of defence: a strategy that answers "unchanged" while exposing the placeholder."""
    class _Sealing:
        def is_due(self, unit, us, **k):
            return True

        def detect_change(self, unit, us):
            self._cur = DATE_TAIL
            return None

        def run(self, unit, since=None):                  # pragma: no cover
            raise AssertionError("no update() expected")
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _Sealing())
    results, notes = rig("ember", DATE_TAIL)
    assert ("ember/_all", "no_change") not in results, results
    assert notes == [], notes


def test_a_real_equal_token_still_books_the_earned_no_change(rig, monkeypatch):
    f = _fake_fetcher("etag:abc")
    monkeypatch.setattr(S5, "get_fetcher", lambda sid: f)
    results, notes = rig("ember", "etag:abc")
    assert results == [("ember/_all", "no_change")], results
    assert f.calls == [], "no update() on an unchanged real token"
    assert notes == ["vintage probe matched stored (etag:abc)"], notes


# ---- (e) the sweep ----------------------------------------------------------------------------

def test_no_fetcher_probe_returns_the_placeholder_literal():
    d = pathlib.Path(ROOT) / "updater" / "strategies" / "fetchers"
    bad, seen = [], 0
    for p in sorted(d.glob("*.py")):
        for n in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
            if isinstance(n, ast.FunctionDef) and n.name == "current_vintage":
                seen += 1
                for r in ast.walk(n):
                    if (isinstance(r, ast.Return) and isinstance(r.value, ast.Constant)
                            and r.value.value == DATE_TAIL):
                        bad.append(f"{p.name}:{r.lineno}")
    assert seen > 50, f"the sweep saw only {seen} probes - it is not reading the fetchers"
    assert bad == [], bad
