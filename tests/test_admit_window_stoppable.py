"""Admission for WINDOW-STOPPABLE units (starvation plan R10, "THE BINDING CONSTRAINT NOW", 2026-11-02).

The start gate in run_once reserved min(2 x unit timeout, max(3 x estimate, 10)) - 90 min for every giant - and
run_local_heavy.ps1 clamps a pass to untilBlock - 25, so windows are at most ~90 min and no giant was ever
admitted (the 2026-11-02 01:07Z pass: 20 BUDGET SKIP lines, "reserves 90 ... and 90 min remain"). Since #205,
#206, #208/#209 and #211, four fetchers read orchestrate._RUN_DEADLINE_TS and stop at the run ceiling on their
own, keeping their work. They now DECLARE it (registry `window_stop: true`) and each exposes
window_admission_reserve_min(): margin floor + ONE durable unit of progress + the merge/assembly reserve kept
before the ceiling (review AR-415 B). The gate admits them on that reserve. Every other unit keeps today's
reserve exactly.

oecd is deliberately NOT declared: oecd.py never reads the run ceiling ("no unit clock exists on Windows"); it
resumes after a hard-stop kill, it does not stop at the window end.

The run tests drive the REAL run_once over the REAL registry with REAL strategies and fetcher modules; the cost
estimates are planted as runs rows, and `_has_adapter` - the first call after the gate - is a recorder, so
nothing past the gate runs (the test_budget_skip_due_label pattern). The turn, order and multi-pass tests are in
test_admit_window_stop_turns.py.
"""
from __future__ import annotations

import ast
import glob
import inspect
import os
import re

import pytest

from updater import orchestrate, registry
from updater.state import StateStore

DECLARED = {"census": 17.0, "wid": 20.0, "imf_imts_direct": 55.0,
            "unctad_nonplasticsubststradebypartner": 75.0}
UNDECLARED_GIANT = "oecd"                      # resumable, but does not stop at the window end
FETCHER_DIR = os.path.join(os.path.dirname(orchestrate.__file__), "strategies", "fetchers")
ENV = ("AQUEDUCT_CENSUS_MARGIN_MIN", "AQUEDUCT_WID_MARGIN_MIN", "AQUEDUCT_IMTS_MARGIN_MIN",
       "AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "AQUEDUCT_UNCTAD_MARGIN_MIN", "AQUEDUCT_UNCTAD_ASSEMBLY_RESERVE_MIN",
       "CENSUS_TIMEOUT_LARGE")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in ENV + ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)


def _run(tmp_path, monkeypatch, capsys, budget, sources, cost=True):
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    os.makedirs(tmp_path, exist_ok=True)
    store = StateStore(str(tmp_path / "state.db"))
    if cost:
        for sid in sources:
            # A giant's cost on record (3 h): today's rule reserves the 90-min cap for every one of them.
            store.log_run(sid, "_all", "ok", dur_s=3 * 3600.0)
    admitted = []
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda unit: admitted.append(unit.source_id) or False)
    try:
        orchestrate.run_once(sources=list(sources), dry=True, store=store)
    except SystemExit:
        pass
    out = capsys.readouterr().out
    skips = {m.group(1): (float(m.group(2)), m.group(3)) for m in re.finditer(
        r"BUDGET SKIP (\w+)/_all \([^)]*\): the gate reserves (\d+) min \((.+?)\) and", out)}
    return admitted, skips, out


ALL = sorted(DECLARED) + [UNDECLARED_GIANT]


def test_declared_units_pass_a_90_min_window_on_their_own_reserve(tmp_path, monkeypatch, capsys):
    admitted, skips, out = _run(tmp_path, monkeypatch, capsys, budget=90, sources=ALL)
    assert sorted(admitted) == sorted(DECLARED), out[-3000:]
    for sid, need in DECLARED.items():
        assert re.search(rf"WINDOW STOP {sid}/_all: passes the start gate on its declared {need:g}-min reserve", out)


def test_an_undeclared_giant_with_90_min_left_is_still_skipped_on_todays_line(tmp_path, monkeypatch, capsys):
    admitted, skips, out = _run(tmp_path, monkeypatch, capsys, budget=90, sources=ALL)
    assert UNDECLARED_GIANT not in admitted
    assert skips[UNDECLARED_GIANT] == (90.0, "worst case, not an estimate"), out[-3000:]


def test_a_declared_unit_whose_own_reserve_does_not_fit_is_skipped_and_the_line_names_the_basis(
        tmp_path, monkeypatch, capsys):
    admitted, skips, out = _run(tmp_path, monkeypatch, capsys, budget=60, sources=ALL)
    assert "unctad_nonplasticsubststradebypartner" not in admitted          # 75 > 60
    need, basis = skips["unctad_nonplasticsubststradebypartner"]
    assert need == 75.0 and basis.startswith("declared by its window-stopping fetcher"), out[-3000:]
    assert sorted(admitted) == ["census", "imf_imts_direct", "wid"]
    assert skips[UNDECLARED_GIANT][1] == "worst case, not an estimate"


def test_the_gate_reads_the_fetchers_env(tmp_path, monkeypatch, capsys):
    """Raise imts' merge reserve: the gate's reserve moves with it (5 + 30 + 45 = 80) and imts is skipped in an
    80-min pass that admits it on the default (55)."""
    admitted, _, _ = _run(tmp_path, monkeypatch, capsys, budget=80, sources=ALL)
    assert "imf_imts_direct" in admitted
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "45")
    admitted, skips, out = _run(tmp_path / "b", monkeypatch, capsys, budget=80, sources=ALL)
    assert "imf_imts_direct" not in admitted and skips["imf_imts_direct"][0] == 80.0, out[-3000:]


def test_a_short_unit_timeout_does_not_shrink_a_declared_reserve(tmp_path, monkeypatch, capsys):
    """AQUEDUCT_UNIT_TIMEOUT_MIN=10 makes the worst case 20; the declared figure still wins (review AR-415), so
    nonplastics is NOT admitted below its 75 min, and the undeclared giant reserves today's min(20, 3 x est)."""
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", "10")
    admitted, skips, out = _run(tmp_path, monkeypatch, capsys, budget=70, sources=ALL)
    assert "unctad_nonplasticsubststradebypartner" not in admitted, out[-3000:]
    assert skips["unctad_nonplasticsubststradebypartner"][0] == 75.0
    assert UNDECLARED_GIANT in admitted                                    # 20 < 70, exactly as today


def test_a_disabled_unit_timeout_keeps_reserving_nothing_for_everyone(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", "0")
    admitted, skips, out = _run(tmp_path, monkeypatch, capsys, budget=30, sources=ALL)
    assert skips == {} and sorted(admitted) == sorted(ALL), out[-3000:]


def test_never_run_declared_unit_uses_its_declared_reserve(tmp_path, monkeypatch, capsys):
    """est None: today's rule reserves the 90-min worst case; a declared unit's own figure wins."""
    admitted, skips, out = _run(tmp_path, monkeypatch, capsys, budget=30, sources=ALL, cost=False)
    assert sorted(admitted) == ["census", "wid"], out[-3000:]
    assert skips[UNDECLARED_GIANT] == (90.0, "worst case, not an estimate")
    assert skips["imf_imts_direct"][0] == 55.0


def test_a_declaration_whose_reserve_cannot_be_read_fails_closed(tmp_path, monkeypatch, capsys):
    from updater.strategies import fetchers
    mod = fetchers.get("wid")
    for bad in (float("nan"), -1.0, "5", True):
        monkeypatch.setattr(mod, "window_admission_reserve_min", lambda b=bad: b)
        d = tmp_path / f"b{len(str(bad))}{type(bad).__name__}"
        admitted, skips, out = _run(d, monkeypatch, capsys, budget=90, sources=["wid", "census"])
        assert "wid" not in admitted and skips["wid"] == (90.0, "worst case, not an estimate"), out[-3000:]
        assert "WINDOW STOP wid/_all: window_admission_reserve_min() returned" in out
        assert admitted == ["census"]
    monkeypatch.delattr(mod, "window_admission_reserve_min")
    admitted, skips, out = _run(tmp_path / "c", monkeypatch, capsys, budget=90, sources=["wid", "census"])
    assert "wid" not in admitted and "has no window_admission_reserve_min()" in out


def test_an_import_that_raises_at_the_gate_falls_back_and_the_pass_goes_on(tmp_path, monkeypatch, capsys):
    """The BROKEN-adapter handler sits AFTER the gate; the gate's own import must not end run_once."""
    from updater.strategies import fetchers
    real = fetchers.get

    def get(sid):
        if sid == "wid":
            raise ImportError("planted: wid will not import")
        return real(sid)
    monkeypatch.setattr(fetchers, "get", get)
    admitted, skips, out = _run(tmp_path, monkeypatch, capsys, budget=90, sources=["wid", "census"])
    assert skips["wid"] == (90.0, "worst case, not an estimate"), out[-3000:]
    assert "window_admission_reserve_min() failed: ImportError" in out
    assert admitted == ["census"]


def test_window_stop_reserve_is_none_for_undeclared_units_and_imports_nothing(monkeypatch):
    from updater.strategies import fetchers
    called = []
    monkeypatch.setattr(fetchers, "get", lambda sid: called.append(sid))
    units = {u.source_id: u for u in registry.all_units()}
    assert orchestrate._window_stop_reserve(units[UNDECLARED_GIANT]) == (None, None)
    assert orchestrate._window_stop_reserve(units["eia"]) == (None, None)
    assert called == []


# ---- the declared set, pinned both ways ----

def _module_binds(path: str, name: str) -> bool:
    """Does this module bind `name` at top level (def, assignment, or import)? AST, never a text search: a
    string mention or a comment must not count, and `x: T = ...` / `from m import name` must."""
    tree = ast.parse(open(path, encoding="utf-8").read(), filename=path)
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return True
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in n.targets):
            return True
        if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name) and n.target.id == name:
            return True
        if isinstance(n, (ast.Import, ast.ImportFrom)) and any((a.asname or a.name) == name for a in n.names):
            return True
    return False


def _exposing(paths) -> set:
    return {os.path.basename(p)[:-3] for p in paths
            if not os.path.basename(p).startswith("_") and _module_binds(p, "window_admission_reserve_min")}


def _declared() -> set:
    return {e["source_id"] for e in registry.load()["sources"] if e.get("window_stop") is True}


def _fetcher_paths():
    return sorted(glob.glob(os.path.join(FETCHER_DIR, "*.py")))


def test_the_declared_set_is_exactly_the_four():
    assert _declared() == set(DECLARED)
    assert UNDECLARED_GIANT not in _declared()


def test_exactly_the_declared_fetchers_expose_the_reserve():
    paths = _fetcher_paths()
    assert len(paths) > 100                                        # the scan saw the real directory
    assert _exposing(paths) == _declared()


def test_a_fifth_module_exposing_the_reserve_fails_the_pin(tmp_path):
    planted = tmp_path / "unctad_biotrademerch.py"
    planted.write_text("from ._unctad import make\n\n\ndef window_admission_reserve_min() -> float:\n"
                       "    return 1.0\n", encoding="utf-8")
    assert _exposing(_fetcher_paths() + [str(planted)]) != _declared()
    alt = tmp_path / "ann.py"
    alt.write_text("window_admission_reserve_min: object = lambda: 1.0\n", encoding="utf-8")
    assert _exposing([str(alt)]) == {"ann"}
    mention = tmp_path / "mention.py"
    mention.write_text("# window_admission_reserve_min\nX = 'window_admission_reserve_min'\n", encoding="utf-8")
    assert _exposing([str(mention)]) == set()


def test_every_declared_module_imports_and_returns_its_default():
    from updater.strategies import fetchers
    for sid, need in DECLARED.items():
        assert fetchers.get(sid).window_admission_reserve_min() == need, sid


def test_no_other_unctad_module_exposes_it():
    from updater.strategies import fetchers
    others = [os.path.basename(p)[:-3] for p in _fetcher_paths()
              if os.path.basename(p).startswith("unctad_")
              and os.path.basename(p) != "unctad_nonplasticsubststradebypartner.py"]
    assert len(others) > 10
    for sid in others[:5] + others[-5:]:
        assert not hasattr(fetchers.get(sid), "window_admission_reserve_min"), sid


def test_registry_refuses_a_non_true_window_stop():
    reg = registry.load()
    e = next(x for x in reg["sources"] if x["source_id"] == "wid")
    e["window_stop"] = "yes"
    assert any("window_stop must be true" in p for p in registry.validate(reg))
    e["window_stop"] = True
    assert not any("window_stop" in p for p in registry.validate(reg))


def test_the_declaration_rides_in_unit_config_as_a_strict_boolean():
    units = {u.source_id: u for u in registry.all_units()}
    assert all(units[s].config["window_stop"] is True for s in DECLARED)
    assert units[UNDECLARED_GIANT].config["window_stop"] is False
    # an entry that bypassed validate (a hand-built registry) still declares only on boolean true
    for v in ("yes", 1, "true"):
        (u,) = registry.to_units({"source_id": "x", "strategy": "extend_by_date", "cadence": "monthly",
                                  "window_stop": v})
        assert u.config["window_stop"] is False, v


# ---- the gate and each fetcher's stop check read the SAME env names: change one, both move ----

def test_census_and_wid_reserve_is_the_helper_their_stop_check_reads(monkeypatch):
    from updater.strategies.fetchers import census, wid
    for mod, var in ((census, "AQUEDUCT_CENSUS_MARGIN_MIN"), (wid, "AQUEDUCT_WID_MARGIN_MIN")):
        assert "floor_s = _margin_floor_s()" in inspect.getsource(mod.update if mod is wid else mod)
        extra = (mod.TIMEOUT_LARGE / 60 + mod.ADMISSION_MERGE_MIN) if mod is census else mod.ADMISSION_COUNTRY_MIN
        monkeypatch.setenv(var, "12.5")
        assert mod._margin_floor_s() == 750.0 and mod.window_admission_reserve_min() == 12.5 + extra
        for bad in ("bad", "nan", "-1"):
            monkeypatch.setenv(var, bad)
            assert mod.window_admission_reserve_min() == 5.0 + extra == mod._margin_floor_s() / 60 + extra
        monkeypatch.delenv(var)


class _Sentinel(Exception):
    pass


def test_imts_stop_check_and_reserve_move_together(tmp_path, monkeypatch):
    from updater.strategies.fetchers import imf_imts_direct as m
    seen = {}
    real = m._env_num
    monkeypatch.setattr(m, "_env_num", lambda name, d: seen.setdefault(name, real(name, d)))
    monkeypatch.setattr(m.config, "source_dir", lambda sid: str(tmp_path))

    def stop():
        raise _Sentinel
    monkeypatch.setattr(m, "_plan", stop)
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "7")
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "31")
    with pytest.raises(_Sentinel):
        m.run_sliced(m.FLOW, m.AGENCY, m.SOURCE)
    stop_check = seen["AQUEDUCT_IMTS_MARGIN_MIN"] + seen["AQUEDUCT_IMTS_MERGE_RESERVE_MIN"]
    seen.clear()
    assert m.window_admission_reserve_min() == stop_check + m.ADMISSION_SLICE_MIN == 68.0


def test_unctad_stop_check_and_reserve_move_together(tmp_path, monkeypatch):
    from updater.strategies.fetchers import _unctad
    from updater.strategies.fetchers import unctad_nonplasticsubststradebypartner as np_mod
    got = {}

    class _J:
        def Window(self, end_ts, floor_s, say, reserve_s):
            got.update(floor_s=floor_s, reserve_s=reserve_s)
            raise _Sentinel
    monkeypatch.setattr(_unctad, "_job", lambda: _J())
    monkeypatch.setattr(_unctad.config, "source_dir", lambda sid: str(tmp_path))
    monkeypatch.setenv("AQUEDUCT_UNCTAD_MARGIN_MIN", "3")
    monkeypatch.setenv("AQUEDUCT_UNCTAD_ASSEMBLY_RESERVE_MIN", "41")
    with pytest.raises(_Sentinel):
        np_mod.update(None, None)
    assert (np_mod.window_admission_reserve_min()
            == (got["floor_s"] + got["reserve_s"]) / 60 + _unctad.ADMISSION_NETWORK_MIN == 54.0)
