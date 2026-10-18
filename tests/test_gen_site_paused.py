"""Every refresh sentence on a source page is TRUE for every source it reaches (PR #169, review AR-345).

2026-10-18: regenerating insee_melodi.html (paused since 2026-10-17) printed "not yet wired - the data here is the
verified initial load"; the committed page said "live - on the daily update run". Both untrue. AR-345 then showed
that "last automated update <last_success>" is false too (a partial run that merged rows never sets last_success:
bls merged rows on 10-08 and 10-17 after its 10-02 no_change), that census and oecd ran without a last_success,
and that a run_location: local source IS run - by the workstation update pass. catalog/gen_site.py refresh_rows
now states only what the registry and the updater state prove; updater/registry.py refuses `paused` on a source
any run still picks up; status.html shows a paused source as paused. These tests use the REAL state shape
(source_state + unit_state), a partial-only history, run_location: local and served_by: lane.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
sys.path.insert(0, ROOT)
from updater import config, registry  # noqa: E402

REASON = "updates are on hold until its incomplete tables are repaired"
BOARD = ' See the <a href="status.html">Source Status board</a>.'


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_paused_under_test", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _state_db(path, sources, units):
    """The real shapes (updater/state.py): source_state and unit_state, as the updater writes them."""
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, owner TEXT, enabled INTEGER DEFAULT 1, note TEXT)")
    con.execute("CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, "
                "obs_count INTEGER DEFAULT 0, attempt_count INTEGER DEFAULT 0, last_error TEXT, "
                "PRIMARY KEY(source_id, unit_id))")
    con.executemany("INSERT INTO source_state (source_id, status, last_success_utc, last_attempt_utc) "
                    "VALUES (?, ?, ?, ?)", sources)
    con.executemany("INSERT INTO unit_state (source_id, unit_id, status, last_success_utc, last_attempt_utc, "
                    "attempt_count) VALUES (?, '_all', ?, ?, ?, ?)", units)
    con.commit()
    con.close()


@pytest.fixture
def state(G, tmp_path):
    db = tmp_path / "state.db"
    _state_db(db, sources=[
        # bls's real shape: a no_change on 10-02 is the last FULL completion; later runs were partial and merged rows
        ("loc_partial_after", "partial", "2026-10-02T11:28:13+00:00", "2026-10-17T03:00:00+00:00"),
        # census / oecd: partial-only history - rows merged, never a full completion
        ("loc_partial_only", "partial", None, "2026-10-10T02:00:00+00:00"),
        ("lane_src", "ok", "2026-09-04T01:02:02+00:00", "2026-09-04T01:00:00+00:00"),
        ("any_ran", "ok", "2026-08-01T00:00:00+00:00", "2026-08-01T00:00:00+00:00"),
    ], units=[
        ("loc_partial_after", "partial", "2026-10-02T11:28:13+00:00", "2026-10-17T03:00:00+00:00", 6),
        ("loc_partial_only", "partial", None, "2026-10-10T02:00:00+00:00", 5),
        ("lane_src", "ok", "2026-09-04T01:02:02+00:00", "2026-09-04T01:00:00+00:00", 1),
        ("any_ran", "ok", "2026-08-01T00:00:00+00:00", "2026-08-01T00:00:00+00:00", 1),
        ("any_only_attempt", "transient_fail", None, "2026-07-01T00:00:00+00:00", 1),
    ])
    return G.read_state_facts(str(db))


META = {
    "paused_src": {"paused": REASON, "run_location": "any", "served_by": None},
    "loc_partial_after": {"paused": None, "run_location": "local", "served_by": None},
    "loc_partial_only": {"paused": None, "run_location": "local", "served_by": None},
    "loc_never": {"paused": None, "run_location": "local", "served_by": None},
    "lane_src": {"paused": None, "run_location": "local", "served_by": "lane"},
    "any_ran": {"paused": None, "run_location": "any", "served_by": None},
    "any_only_attempt": {"paused": None, "run_location": "any", "served_by": None},
    "any_never": {"paused": None, "run_location": "any", "served_by": None},
}
UNWIRED = {k: False for k in META} | {"wired_src": True}


def _r(G, sid, state, meta=META, wiring=UNWIRED):
    return dict(G.refresh_rows(sid, "weekly", wiring=wiring, meta=meta, state=state))


def test_the_state_is_read_with_its_real_shape(state):
    assert state["last_success"] == {"loc_partial_after": "2026-10-02T11:28:13+00:00",
                                     "lane_src": "2026-09-04T01:02:02+00:00", "any_ran": "2026-08-01T00:00:00+00:00"}
    assert state["attempted"] == {"loc_partial_after", "loc_partial_only", "lane_src", "any_ran", "any_only_attempt"}


def test_a_paused_source_says_paused_with_its_escaped_reason_and_the_board(G, state):
    r = _r(G, "paused_src", state)
    assert r == {"Update cadence": "weekly <em>(paused)</em>",
                 "Automated refresh": "<strong>paused</strong> &mdash; " + REASON + "." + BOARD}
    nasty = {"paused_src": {**META["paused_src"], "paused": "a <b>bold</b> & reason"}}
    assert "a &lt;b&gt;bold&lt;/b&gt; &amp; reason." in _r(G, "paused_src", state, meta=nasty)["Automated refresh"]
    wired = _r(G, "paused_src", state, wiring={"paused_src": True})
    assert "<strong>paused</strong>" in wired["Automated refresh"], "paused wins over wired"


def test_a_local_source_whose_last_full_run_predates_later_partial_runs(G, state):
    """bls: the date is the last FULLY COMPLETED run, never 'the last update' (partials merged rows after it)."""
    r = _r(G, "loc_partial_after", state)
    assert r["Update cadence"] == "weekly <em>(workstation update pass)</em>"
    assert r["Automated refresh"] == ("on the workstation update pass, not the cloud daily run &mdash; last fully "
                                      "completed automated run 2026-10-02." + BOARD)


def test_a_partial_only_history_is_never_called_not_yet_wired(G, state):
    """census / oecd: rows merged on partial runs, no last_success."""
    r = _r(G, "loc_partial_only", state)
    assert r["Automated refresh"] == ("on the workstation update pass, not the cloud daily run &mdash; no automated "
                                      "run has completed without errors yet." + BOARD)
    r2 = _r(G, "any_only_attempt", state)
    assert r2["Automated refresh"].startswith("not on the cloud daily run or the workstation update pass &mdash; no "
                                              "automated run has completed without errors yet.")


def test_a_lane_source_names_its_lane_and_no_date(G, state):
    r = _r(G, "lane_src", state)
    assert r == {"Update cadence": "weekly <em>(its own update lane)</em>",
                 "Automated refresh": "refreshed by its own update lane, not the cloud daily run." + BOARD}
    assert _r(G, "lane_src", None) == r, "the same with no state"


def test_never_attempted_and_the_other_controls(G, state):
    for sid in ("loc_never", "any_never"):
        r = _r(G, sid, state)
        assert r["Update cadence"] == "weekly <em>(target &mdash; not yet automated)</em>"
        assert r["Automated refresh"] == "not yet wired &mdash; the data here is the verified initial load." + BOARD
    r = _r(G, "any_ran", state)
    assert r["Automated refresh"] == ("not on the cloud daily run or the workstation update pass &mdash; last fully "
                                      "completed automated run 2026-08-01." + BOARD)
    assert _r(G, "wired_src", state) == {
        "Update cadence": "weekly",
        "Automated refresh": "<strong>live</strong> &mdash; this database is on the daily update run"}
    assert G.refresh_rows("y", "weekly", wiring={}, meta=META, state=state) == [("Update cadence", "weekly")]


def test_an_unreadable_state_names_no_date_and_never_says_never_wired(G):
    for sid, start in (("loc_partial_after", "on the workstation update pass, not the cloud daily run."),
                       ("loc_never", "on the workstation update pass, not the cloud daily run."),
                       ("any_never", "not on the cloud daily run or the workstation update pass.")):
        r = _r(G, sid, None)
        assert r["Automated refresh"] == start + BOARD, sid


def test_no_sentence_says_not_on_the_daily_update_run_alone(G, state):
    """AR-345: that phrase reads as 'not updated' while the workstation pass updates the source."""
    for sid in META:
        for st in (state, None):
            assert "not on the daily update run" not in "".join(v for _k, v in G.refresh_rows(
                sid, "weekly", wiring=UNWIRED, meta=META, state=st))


def test_the_state_is_the_updaters_own_and_is_read_lazily_not_at_import(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("gen_site_lazy_probe", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod._STATE_CACHE == [], "the state was read at import"
    assert mod._state_db_path() == config.STATE_DB
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "elsewhere" / "state.db"))
    assert mod._state_db_path() == str(tmp_path / "elsewhere" / "state.db"), "updater.config.STATE_DB, not a copy"
    assert mod.read_state_facts(str(tmp_path / "absent.db")) is None


def test_the_committed_registry_pauses_insee_melodi_and_no_run_picks_it_up(G):
    assert G.PAUSED.get("insee_melodi") == REASON
    entry = next(e for e in registry.load()["sources"] if e["source_id"] == "insee_melodi")
    assert entry["live"] is False and entry.get("run_location", "any") != "local"
    for wf, run in registry._paused_run_members(None).items():
        assert "insee_melodi" not in run, wf
    assert registry.validate(registry.load(), expected_count=config.EXPECTED_SOURCE_COUNT) == []


def _wf(root, heavy_ids=(), sec=""):
    d = root / ".github" / "workflows"
    d.mkdir(parents=True, exist_ok=True)
    (d / "updater-heavy.yml").write_text("env:\n  ALL='" + json.dumps(list(heavy_ids)) + "'\n", encoding="utf-8")
    (d / "sec-edgar-daily.yml").write_text(sec, encoding="utf-8")
    return str(root)


@pytest.mark.parametrize("extra,why", [({"paused": ""}, "non-empty"), ({"paused": 3}, "non-empty"),
                                       ({"paused": "x", "live": True}, "live: true"),
                                       ({"paused": "x", "run_location": "local"}, "run_location: local")])
def test_the_registry_refuses_a_paused_source_a_run_still_picks_up(tmp_path, extra, why):
    root = _wf(tmp_path)
    reg = {"sources": [{"source_id": "s", "strategy": "sdmx_delta", "cadence": "monthly", **extra}]}
    assert any(why in p for p in registry.validate(reg, workflows_root=root)), registry.validate(reg, workflows_root=root)


def test_the_registry_refuses_paused_in_the_heavy_matrix_or_sec_edgar_daily(tmp_path):
    ok = {"source_id": "s", "strategy": "sdmx_delta", "cadence": "monthly", "live": False, "paused": "a reason"}
    assert registry.validate({"sources": [ok]}, workflows_root=_wf(tmp_path / "a")) == [], "control"
    p = registry.validate({"sources": [ok]}, workflows_root=_wf(tmp_path / "b", heavy_ids=["s", "t"]))
    assert any("updater-heavy.yml still runs it" in x for x in p), p
    sec = {**ok, "source_id": "sec_edgar"}
    p = registry.validate({"sources": [sec]}, workflows_root=_wf(tmp_path / "c", sec="run: sec_edgar refresh"))
    assert any("sec-edgar-daily.yml still runs it" in x for x in p), p


def test_only_updates_this_sources_paused_key_in_sources_meta(G, tmp_path, monkeypatch):
    out = tmp_path / "site"
    out.mkdir()
    meta = {"aaa": {"desc": "d", "name": "A"}, "insee_melodi": {"desc": "x", "name": "INSEE"}}
    (out / "sources_meta.json").write_text(json.dumps(meta, ensure_ascii=False, separators=(",", ":"),
                                                      sort_keys=True), encoding="utf-8")
    monkeypatch.setattr(G, "OUT_DIR", str(out))
    monkeypatch.setattr(G, "load_registry", lambda: ({}, {"aaa": {}, "insee_melodi": {}},
                                                     {"aaa": {"n_series": 1}, "insee_melodi": {"n_series": 94}}, {}))
    monkeypatch.setattr(G, "load_sidecar", lambda: ({}, "2026-10-18"))
    monkeypatch.setattr(G, "build_record", lambda sid, *a: {"id": sid, "reservable": 1, "license_id": "x"})
    monkeypatch.setattr(G, "build_page_seo", lambda records: {})
    monkeypatch.setattr(G, "jsonld_license", lambda r: {"x": 1})
    monkeypatch.setattr(G, "render_dataset_page", lambda rec: f"<html><body>{rec['id']}</body></html>")
    monkeypatch.setattr(G, "_load_lastmod_state", lambda: {"x": {}})
    G.main(["--only", "insee_melodi"])
    got = json.loads((out / "sources_meta.json").read_text(encoding="utf-8"))
    assert got == {"aaa": {"desc": "d", "name": "A"}, "insee_melodi": {"desc": "x", "name": "INSEE", "paused": REASON}}


def test_a_full_build_puts_paused_into_sources_meta(G):
    src = open(GEN, encoding="utf-8").read()
    body = src[src.index('meta_path = os.path.join(OUT_DIR, "sources_meta.json")'):]
    body = body[:body.index("json.dump(meta, f")]
    assert 'meta[sid]["paused"] = why' in body


def test_the_status_board_shows_paused_not_retrying():
    s = open(os.path.join(ROOT, "catalog", "site", "status.html"), encoding="utf-8").read()
    assert ".paused{background:" in s and '<span class="dot paused"></span> paused' in s, "style and legend"
    assert "if(m.paused)cls='paused';" in s
    assert s.index("if(m.paused)cls='paused';") < s.index("if(cls==='ok')ok++;"), "decided before it is counted"
    assert "(cls==='paused'?'paused':esc(st))" in s, "the status cell says paused"


def test_the_page_renders_through_refresh_rows(G):
    src = open(GEN, encoding="utf-8").read()
    body = src[src.index("def render_dataset_page("):]
    body = body[:body.index("\ndef ")]
    assert "acc_rows.extend(refresh_rows(rec[\"id\"], rec[\"cadence\"]))" in body
    assert "not yet wired" not in body, "the wording lives in refresh_rows only"
