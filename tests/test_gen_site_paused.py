"""A source page never says "not yet wired" of a source that has run before, nor "live" of one that is paused.

2026-10-18: regenerating insee_melodi.html (paused since 2026-10-17 for the repair of its incomplete tables)
printed "Update cadence: monthly (target - not yet automated)" and "Automated refresh: not yet wired - the data
here is the verified initial load". The source was wired and updated many times. The committed page said "live -
this database is on the daily update run", which was untrue too. catalog/gen_site.py refresh_rows now reads a
registry `paused: "<reason>"` and the updater's own last_success, and updater/registry.py validates the field.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
sys.path.insert(0, ROOT)
from updater import registry  # noqa: E402


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_paused_under_test", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _rows(G, sid, **kw):
    return dict(G.refresh_rows(sid, "monthly", **kw))


def test_a_paused_source_says_paused_with_its_reason(G):
    r = _rows(G, "insee_melodi", wiring={"insee_melodi": False},
              paused={"insee_melodi": "incomplete tables are being re-downloaded from INSEE; updates resume after "
                                      "the repair"}, last_success={})
    assert r["Update cadence"] == "monthly <em>(paused)</em>"
    assert r["Automated refresh"].startswith("<strong>paused</strong> &mdash; incomplete tables are being "
                                             "re-downloaded from INSEE; updates resume after the repair.")
    assert "not yet" not in "".join(r.values()) and "live" not in "".join(r.values())


def test_paused_wins_over_wired(G):
    r = _rows(G, "x", wiring={"x": True}, paused={"x": "a reason"}, last_success={})
    assert "<strong>paused</strong>" in r["Automated refresh"] and "daily update run" not in r["Automated refresh"]


def test_a_source_that_has_run_before_is_never_called_not_yet_wired(G):
    r = _rows(G, "bis", wiring={"bis": False}, paused={}, last_success={"bis": "2026-09-25T01:12:33+00:00"})
    assert r["Update cadence"] == "monthly <em>(not on the daily update run)</em>"
    assert r["Automated refresh"].startswith("not on the daily update run &mdash; last automated "
                                             "update 2026-09-25.")
    assert "not yet wired" not in r["Automated refresh"] and "initial load" not in r["Automated refresh"]


def test_unknown_state_never_claims_never_wired(G):
    r = _rows(G, "bis", wiring={"bis": False}, paused={}, last_success=None)
    assert r["Automated refresh"].startswith("not on the daily update run.")
    assert "not yet wired" not in "".join(r.values())


def test_controls_live_and_never_run(G):
    live = _rows(G, "ecb", wiring={"ecb": True}, paused={}, last_success={})
    assert live == {"Update cadence": "monthly",
                    "Automated refresh": "<strong>live</strong> &mdash; this database is on the daily update run"}
    never = _rows(G, "new_src", wiring={"new_src": False}, paused={}, last_success={"other": "2026-01-01"})
    assert never["Update cadence"] == "monthly <em>(target &mdash; not yet automated)</em>"
    assert never["Automated refresh"].startswith("not yet wired &mdash; the data here is the verified initial load.")
    assert G.refresh_rows("y", "monthly", wiring={}, paused={}, last_success={}) == [("Update cadence", "monthly")], \
        "no wiring known at all: say nothing, as before"


def test_the_committed_registry_pauses_insee_melodi_and_the_generator_reads_it(G):
    assert G.PAUSED.get("insee_melodi") == ("incomplete tables are being re-downloaded from INSEE; updates resume "
                                            "after the repair")
    entry = next(e for e in registry.load()["sources"] if e["source_id"] == "insee_melodi")
    assert entry["live"] is False


@pytest.mark.parametrize("bad,why", [({"paused": ""}, "non-empty"), ({"paused": 3}, "non-empty"),
                                     ({"paused": "x", "live": True}, "live: true")])
def test_the_registry_validates_paused(bad, why):
    reg = {"sources": [{"source_id": "s", "strategy": "sdmx_delta", "cadence": "monthly", **bad}]}
    probs = registry.validate(reg)
    assert any(why in p for p in probs), probs
    ok = {"sources": [{"source_id": "s", "strategy": "sdmx_delta", "cadence": "monthly", "live": False,
                       "paused": "a reason"}]}
    assert registry.validate(ok) == []


def test_last_success_is_read_from_the_state_db(G, tmp_path, monkeypatch):
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, last_success_utc TEXT)")
    con.executemany("INSERT INTO source_state VALUES (?, ?)", [("bis", "2026-09-25T01:12:33+00:00"), ("n", None)])
    con.commit()
    con.close()
    monkeypatch.setenv("AQUEDUCT_STATE_DIR", str(tmp_path))
    assert G.load_last_success() == {"bis": "2026-09-25T01:12:33+00:00"}
    monkeypatch.setenv("AQUEDUCT_STATE_DIR", str(tmp_path / "absent"))
    assert G.load_last_success() is None


def test_the_page_renders_through_refresh_rows(G):
    src = open(GEN, encoding="utf-8").read()
    body = src[src.index("def render_dataset_page("):]
    body = body[:body.index("\ndef ")]
    assert "acc_rows.extend(refresh_rows(rec[\"id\"], rec[\"cadence\"]))" in body
    assert "not yet wired" not in body, "the wording lives in refresh_rows only"
