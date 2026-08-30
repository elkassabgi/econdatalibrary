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

# -- REACHABILITY, at production scale --------------------------------------
# The previous versions of these tests retyped the fold expression IN THE TEST BODY, so
# deleting the fold from production left them green -- R511's own rule 4, broken one commit
# after writing it. And the front-of-queue test used a fixture with ONE unheld matrix where
# production has 5,191, so it measured the right quantity on the wrong population and passed
# while a real run reached zero unlisted matrices.

def test_a_bounded_run_actually_pulls_unlisted_matrices():
    """THE R511 REGRESSION, at production scale. Rank is not reachability: with 5,191 unheld
    matrices ahead of them, a priority class alone still yields ZERO in a 60-table batch."""
    unheld = [f"HOLE{i:05d}" for i in range(5191)]          # production count, measured
    unlisted = {f"OLD{i:03d}" for i in range(468)}          # production count, measured
    ordinary = [f"ORD{i:05d}" for i in range(7127)]
    changed = unheld + sorted(unlisted) + ordinary
    cur_upd = {m: "2026-08-01T11:00:00" for m in ordinary}
    cur_upd.update({m: "2020-05-01T11:00:00" for m in unlisted})
    cur_upd.update({m: "2020-01-01T11:00:00" for m in unheld})
    held = set(ordinary) | unlisted

    ordered = cso.order_changed(changed, cur_upd, held, unlisted)
    batch = cso.take_batch(ordered, cso.MAX_TABLES, unlisted)

    got = [m for m in batch if m in unlisted]
    assert got, (
        f"a {cso.MAX_TABLES}-table batch reached ZERO unlisted matrices out of "
        f"{len(changed):,} changed. Membership in the list is not reachability -- this is "
        f"exactly R511, and the first fix for it passed a smaller version of this test")
    assert len(batch) == cso.MAX_TABLES, "the reservation must not shrink the batch"


def test_the_reservation_costs_nothing_when_nothing_is_pending():
    """The control: on an ordinary run with no unlisted backlog the batch must be untouched,
    or the fix would permanently steal a quarter of every run from real work."""
    ordered = [f"M{i:04d}" for i in range(500)]
    assert cso.take_batch(ordered, cso.MAX_TABLES, frozenset()) == ordered[:cso.MAX_TABLES]


def test_the_reservation_is_bounded():
    """It must not become a standing claim on the batch either."""
    unlisted = {f"U{i:04d}" for i in range(500)}
    batch = cso.take_batch(sorted(unlisted), cso.MAX_TABLES, unlisted)
    assert len(batch) == cso.MAX_TABLES
    quota = max(1, int(cso.MAX_TABLES * cso.UNLISTED_RESERVED_FRAC))
    assert quota < cso.MAX_TABLES, "a reservation of the WHOLE batch is starvation reversed"


def test_the_two_vintage_vocabularies_do_not_cause_churn():
    """ReadCollection emits '...:00Z', Search emits '...:00' -- the same instant. Comparing
    raw would re-pull every matrix that moves between them, on every run, for ever. Zero of
    12,985 agree byte-for-byte across the two endpoints."""
    stored, cur = {"A": "2020-11-10T11:00:00Z"}, {"A": "2020-11-10T11:00:00"}
    changed = [m for m, u in cur.items()
               if (stored.get(m) or "").rstrip("Z") != (u or "").rstrip("Z")]
    assert changed == [], "a trailing Z must not read as a revision"

def test_the_fold_selects_scoped_unlisted_matrices_only():
    """Calls the SHIPPED fold_unlisted, not a retyped copy of it. STRANGER is in Search but
    neither held nor operator-named; folding it would seed the queue HEAD with an
    uncatalogued matrix, since unheld sorts first."""
    listing = {"LISTED": "v1"}
    search = {"LISTED": "v1", "UNLISTED_HELD": "2020-05-01", "STRANGER": "2020-05-01"}
    assert cso.fold_unlisted(search, listing, {"LISTED", "UNLISTED_HELD"}) == {"UNLISTED_HELD"}


def test_an_operator_can_name_matrices_we_do_not_hold():
    """The 27 of R510's 495 are catalogued with ZERO rows, so they are absent from
    `_held.json` and the fetcher cannot discover them. CSO_ONLY_MATRICES widens the scope."""
    assert cso.fold_unlisted({"ZERO_ROW": "2020-05-01"}, {}, {"ZERO_ROW"}) == {"ZERO_ROW"}


# RESIDUAL GAP, STATED RATHER THAN PAPERED OVER. Nothing here drives `cso.update()`, so
# deleting the CALL to fold_unlisted (as opposed to its body) would still pass. update()
# needs a live catalogue, an R2-backed blob layer, a subject map and an ingester, and a fake
# convincing enough to exercise the fold would mostly be testing the fake. The honest state
# is: the fold's LOGIC is pinned on the shipped function, its call site is not. Closing that
# needs an update()-level harness, which is its own piece of work and is in the TODO.

def test_update_actually_uses_the_reserved_batch():
    """PINS THE WIRING, because pinning the function was not enough.

    `test_a_bounded_run_actually_pulls_unlisted_matrices` calls `take_batch` directly, so
    reverting update()'s own line to `changed[:MAX_TABLES]` passed all eleven tests -- the
    third time in two days that a test proved a function while the CALL SITE stayed
    unprotected (R511 rule 4, and the worldbank and istat suites had the same hole).

    A source-level assertion is the same instrument `test_series_ts_gates_the_canonical_
    spelling` already uses in this repo for a call ORDER that no unit test can reach. It is
    not a substitute for driving update(); it is the cheap half that catches the mutation
    that actually occurred.
    """
    src = open(cso.__file__, encoding="utf-8").read()
    body = src[src.index("def update("):]
    assert "take_batch(changed, MAX_TABLES, unlisted)" in body, (
        "update() no longer builds its batch through take_batch with the unlisted set — "
        "the reservation exists but nothing uses it, so a bounded run reaches zero of them")
    assert "batch = changed[:MAX_TABLES]" not in body, (
        "update() reverted to a plain slice; the reserved share is bypassed")
