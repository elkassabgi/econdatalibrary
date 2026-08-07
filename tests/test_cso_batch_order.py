"""cso picks 60 matrices per run; WHICH 60 decides whether real holes ever get filled.

Measured 2026-08-07: the store held 7,608 matrices while the revision cursor held 61, so
12,788 of the publisher's 12,908 looked "changed" and newest-revision-first spent every
bounded run re-pulling matrices we already had — while 290 CATALOGUED matrices with zero
rows in the store waited roughly 213 runs for their turn. A run already takes ~34 minutes
for 60 matrices, so MAX_TABLES cannot buy the difference; only ordering can.
"""
from __future__ import annotations

from updater.strategies.fetchers.cso import order_changed

CUR = {
    "OLD_UNHELD": "2026-01-01T00:00:00Z",
    "NEW_UNHELD": "2026-08-06T00:00:00Z",
    "OLD_HELD":   "2026-02-01T00:00:00Z",
    "NEW_HELD":   "2026-08-07T00:00:00Z",
}
HELD = {"OLD_HELD", "NEW_HELD"}


def test_unheld_matrices_come_before_held_ones():
    """The point of the change: a matrix with NO rows in the store outranks every matrix
    we can already serve, even a more recently revised one."""
    order = order_changed(list(CUR), CUR, HELD)
    assert order[:2] == ["NEW_UNHELD", "OLD_UNHELD"], order
    assert set(order[2:]) == HELD, order
    # explicitly: the freshest HELD matrix still loses to the STALEST unheld one
    assert order.index("OLD_UNHELD") < order.index("NEW_HELD"), order


def test_newest_first_is_preserved_inside_each_group():
    order = order_changed(list(CUR), CUR, HELD)
    assert order.index("NEW_UNHELD") < order.index("OLD_UNHELD"), order
    assert order.index("NEW_HELD") < order.index("OLD_HELD"), order


def test_empty_held_set_is_exactly_the_old_behaviour():
    """No seed yet (or a brand-new source) must degrade to plain newest-revision-first,
    not to some arbitrary order."""
    assert order_changed(list(CUR), CUR, set()) == \
        sorted(CUR, key=lambda m: CUR[m], reverse=True)


def test_every_input_survives_exactly_once():
    """An ordering must never drop or duplicate work — that would silently shrink the
    batch a bounded run believes it is covering."""
    order = order_changed(list(CUR), CUR, HELD)
    assert sorted(order) == sorted(CUR)


def test_missing_revision_dates_do_not_crash_or_jump_the_queue():
    cur = dict(CUR, NO_DATE_UNHELD=None)
    order = order_changed(list(cur), cur, HELD)
    assert sorted(order) == sorted(cur)
    # a dateless matrix is still unheld, so it precedes the held ones ...
    assert order.index("NO_DATE_UNHELD") < order.index("NEW_HELD"), order
    # ... but sorts last among unheld, since it carries no freshness evidence
    assert order.index("NO_DATE_UNHELD") > order.index("OLD_UNHELD"), order
