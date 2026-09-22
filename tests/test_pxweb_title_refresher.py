"""The PxWeb title refresher must be additive, must reach the right hosts, and must refuse blind.

Context. `tools/catalog_pxweb_flowgrain.py` titles each flow-grain row from the source's cached
`_catalog.json` and falls back to the bare table id when the cache has no entry. Those caches were
frozen three different ways: `bfs` and `dst` have `_catalog.json` files that NO code writes, and
`statfin`'s is rebuilt only if it is deleted, because `crawl_catalog()` returns the cached file
whenever it exists. 85 tables across the three would have been catalogued under a bare id.

These tests are offline by design - CI has neither the parquet store nor the publishers - so they
pin the parts that can be wrong without a network: the merge invariant, the URL each source is
asked on, and the refusal when the control fails.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

spec = importlib.util.spec_from_file_location(
    "refresh_pxweb_titles", os.path.join(ROOT, "tools", "refresh_pxweb_titles.py"))
rt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rt)


def test_every_source_has_a_fetcher():
    """A source added to the cataloguer without one here goes on writing bare ids in silence."""
    assert set(rt.FETCHERS) == {"statfin", "bfs", "dst"}


def test_the_merge_appends_and_never_drops():
    old = [{"id": "A", "text": "alpha"}, {"id": "B", "text": "beta"}]
    got = [{"id": "C", "text": "gamma"}]
    new = rt.merge_additive(old, got)
    assert [t["id"] for t in new] == ["A", "B", "C"]
    assert new[0] == {"id": "A", "text": "alpha"}          # untouched, not rewritten


def test_an_incoming_duplicate_never_overwrites_an_existing_title():
    """A bad or partial response must not be able to degrade a good file."""
    old = [{"id": "A", "text": "the real title"}]
    new = rt.merge_additive(old, [{"id": "A", "text": ""}])
    assert new == old


def test_the_merge_refuses_rather_than_losing_an_id():
    """Planted positive: if the merge is ever rewritten to filter, it must raise, not return."""
    class Hostile(list):
        def __add__(self, other):                           # simulate a merge that drops A
            return [t for t in other]
    with pytest.raises(SystemExit):
        rt.merge_additive(Hostile([{"id": "A", "text": "alpha"}]), [{"id": "B", "text": "b"}])


def test_each_source_is_asked_on_its_documented_url(monkeypatch):
    seen = {}

    def fake_http(url):
        seen["url"] = url
        return {"title": "T", "text": "T"}

    monkeypatch.setattr(rt, "_http", fake_http)

    rt.FETCHERS["statfin"]("15iq.px", "ashi/15iq.px")
    assert seen["url"].startswith("https://pxdata.stat.fi/PxWeb/api/v1/en/StatFin/")
    assert seen["url"].endswith("/ashi/15iq.px"), "StatFin needs the folder path, not the bare id"

    rt.FETCHERS["bfs"]("px-x-01_101", "")
    assert seen["url"] == "https://www.pxweb.bfs.admin.ch/api/v1/en/px-x-01_101/px-x-01_101.px"

    rt.FETCHERS["dst"]("FOLK1A", "")
    assert "api.statbank.dk" in seen["url"] and "id=FOLK1A" in seen["url"]
    assert "lang=en" in seen["url"], "without lang=en Statbank answers in Danish"


def test_it_refuses_a_source_whose_control_does_not_resolve(monkeypatch, capsys):
    """If already-titled tables cannot be fetched either, the URL is wrong and every '404' would be
    reported as the publisher withdrawing a table. That must refuse, not publish a false verdict."""
    monkeypatch.setattr(rt, "missing_tables",
                        lambda cat, src: ([("X", "f/X")], ["KNOWN1", "KNOWN2", "KNOWN3"]))
    monkeypatch.setattr(rt, "_cataloguer", lambda: None)
    monkeypatch.setitem(rt.FETCHERS, "dst",
                        lambda tid, path: (_ for _ in ()).throw(OSError("unreachable")))
    monkeypatch.setattr(rt.time, "sleep", lambda *_: None)
    assert rt.refresh("dst", apply=False) == 2
    assert "REFUSING" in capsys.readouterr().err


def test_a_resolving_control_lets_the_run_proceed(monkeypatch, capsys):
    """Negative control for the test above - otherwise 'it refused' proves nothing."""
    monkeypatch.setattr(rt, "missing_tables",
                        lambda cat, src: ([("X", "f/X")], ["KNOWN1", "KNOWN2", "KNOWN3"]))
    monkeypatch.setattr(rt, "_cataloguer", lambda: None)
    monkeypatch.setitem(rt.FETCHERS, "dst", lambda tid, path: "a real title")
    monkeypatch.setattr(rt.time, "sleep", lambda *_: None)
    assert rt.refresh("dst", apply=False) == 0
    assert "REFUSING" not in capsys.readouterr().err
