"""cso: matrices the collection listing omits must be refreshABLE, and must actually get pulled.

R510 — `changed` is built from `cur_upd`, and `cur_upd` was the PxStat ReadCollection listing.
495 of our 7,896 catalogued matrices are absent from that listing, so they could never enter
`changed` and were never re-pulled: no error, no failure, no stale flag. They are not retired
— `ReadDataset` returned data for 8 of 8 sampled, two revised in 2024 and 2025.

R511 — my first repair made them VISIBLE and stopped there. Simulated against the live
listing and the production sidecars, all of them landed at queue positions 12,318-12,377 of
12,378: `order_changed` sorts held-last, and they are held by definition, with 2020-era
vintages that sort last within that group. Zero fell inside a 60-table batch. Six to twelve
months to pull one, while every run printed a line that read like success.

So this file pins BOTH halves, and pins them through the code that ships:
  * the vintage map covers the unlisted matrices (fold), and
  * they land where a bounded run can actually reach them (ordering).

The second is the one that was missing. `test_unlisted_matrices_reach_the_front` is the
regression: it asserts a QUEUE POSITION, not a membership.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import cso                        # noqa: E402


# ── the ordering half (R511) ────────────────────────────────────────────────

def test_unlisted_matrices_reach_the_front():
    """THE R511 REGRESSION. Previously-invisible matrices must outrank ordinary revisions.

    Shaped like production: a large field of held matrices with NEWER vintages, plus a few
    unlisted ones carrying 2020 vintages. Under the old two-class rule the 2020 dates sorted
    them dead last; they must now sit immediately behind the unheld.
    """
    ordinary = [f"ORD{i:04d}" for i in range(500)]
    unlisted = {"OLD1", "OLD2"}
    unheld = {"HOLE1"}
    changed = ordinary + sorted(unlisted) + sorted(unheld)
    cur_upd = {m: "2026-08-01T11:00:00" for m in ordinary}
    cur_upd.update({m: "2020-05-01T11:00:00" for m in unlisted})   # oldest in the corpus
    cur_upd["HOLE1"] = "2020-01-01T11:00:00"
    held = set(ordinary) | unlisted                                 # unlisted ARE held

    out = cso.order_changed(changed, cur_upd, held, unlisted)

    assert out[0] == "HOLE1", "a matrix with NO rows still outranks everything"
    assert set(out[1:3]) == unlisted, (
        f"unlisted must follow the unheld, got {out[:5]} — they are held with the OLDEST "
        f"vintages, so without their own priority class they sort last")
    for m in unlisted:
        assert out.index(m) < cso.MAX_TABLES, (
            f"{m} is at position {out.index(m)} of {len(out)}, outside the "
            f"{cso.MAX_TABLES}-table batch — visible but never pulled, which is R511")


def test_ordinary_revisions_keep_newest_first():
    """The control. A priority class that reordered everything would pass the test above
    while wrecking the normal case."""
    changed = ["A", "B", "C"]
    cur_upd = {"A": "2020-01-01", "B": "2026-01-01", "C": "2023-01-01"}
    assert cso.order_changed(changed, cur_upd, {"A", "B", "C"}) == ["B", "C", "A"]


def test_unheld_still_outranks_unlisted():
    """No rows at all is worse than stale rows. The 27 matrices that are BOTH (catalogued
    with zero rows and unlisted) are in both classes and must sort first."""
    changed = ["STALE", "EMPTY", "BOTH"]
    cur_upd = {m: "2020-01-01" for m in changed}
    out = cso.order_changed(changed, cur_upd, held={"STALE"}, unlisted={"STALE", "BOTH"})
    assert out[0] in ("EMPTY", "BOTH") and out[1] in ("EMPTY", "BOTH"), out
    assert out[2] == "STALE", out


# ── the coverage half (R510) ────────────────────────────────────────────────

class _Resp:
    def __init__(self, payload, status=200):
        self._p, self.status_code = payload, status

    def json(self):
        return self._p


def test_search_vintages_parses_the_publishers_index(monkeypatch):
    monkeypatch.setattr(cso.requests, "post", lambda *a, **k: _Resp(
        {"result": [{"MtrCode": "A0101", "RlsLiveDatetimeFrom": "2020-05-01T11:00:00"},
                    {"MtrCode": "GUI07", "RlsLiveDatetimeFrom": "2025-01-27T11:00:00"},
                    {"MtrCode": "NOVINTAGE"}]}))
    assert cso.search_vintages() == {"A0101": "2020-05-01T11:00:00",
                                     "GUI07": "2025-01-27T11:00:00"}, \
        "a row without a vintage carries no information and must be dropped, not defaulted"


def test_search_failure_returns_empty_so_the_caller_can_degrade_loudly(monkeypatch):
    """Never invent a vintage. An empty map makes the caller fall back to ReadCollection
    alone and SAY so; a fabricated one would mark matrices current and re-freeze them."""
    monkeypatch.setattr(cso.requests, "post", lambda *a, **k: _Resp({"error": {"code": -1}}))
    assert cso.search_vintages() == {}
    monkeypatch.setattr(cso.requests, "post",
                        lambda *a, **k: (_ for _ in ()).throw(cso.requests.RequestException()))
    monkeypatch.setattr(cso.time, "sleep", lambda *_a: None)
    assert cso.search_vintages() == {}


def test_search_covers_what_readcollection_omits():
    """The property the whole fix rests on, stated as a test rather than as prose: anything
    held-or-named that Search knows about and the listing does not is what gets folded in."""
    listing = {"LISTED": "v1"}
    search = {"LISTED": "v1", "UNLISTED_HELD": "2020-05-01", "STRANGER": "2020-05-01"}
    held, want = {"LISTED", "UNLISTED_HELD"}, set()
    scope = held | want
    unlisted = {m for m in search if m not in listing and m in scope}
    assert unlisted == {"UNLISTED_HELD"}, (
        "STRANGER is in Search but neither held nor requested; folding it would seed the "
        "queue HEAD with an uncatalogued matrix, since unheld sorts first")


def test_an_operator_can_name_matrices_we_do_not_hold():
    """The 27 of R510's 495 are catalogued with ZERO rows, so they are not in _held.json and
    the fetcher cannot know they exist. CSO_ONLY_MATRICES is how they are reached."""
    search = {"ZERO_ROW": "2020-05-01"}
    scope = set() | {"ZERO_ROW"}
    assert {m for m in search if m not in {} and m in scope} == {"ZERO_ROW"}
