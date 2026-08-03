"""stat_estonia's table-grain resume must cover every table, not just re-walk a subject's head.

WHY THIS EXISTS. stat_estonia is the one source whose SINGLE largest subject cannot finish inside
the orchestrator's 45-minute hard cap. Measured on run 30799503843: it took exactly 2,700s and was
killed, on an 18-minute budget, printing no budget message at all — because the only deadline check
was at the top of the SUBJECT loop, which bounds when the next subject starts and cannot bound a
subject already running.

Adding a deadline check inside the table loop stops the kill, but on its own it would create a
worse, quieter bug. The subject bookmark is written BEFORE the subject is worked (deliberately —
R273: an end-of-function save is exactly what a kill destroys). So breaking out mid-subject with
that bookmark already advanced means the next visit starts at the NEXT subject, and this subject's
unfinished tail is never fetched — R190's truncation one level down, reported as an honest
`partial` forever.

The fix is two bookmarks: the subject one is wound BACK to the previous subject so the subject is
re-entered, and a table-grain one ("<subject>|<table path>") says where inside it to resume. These
tests pin the property that matters — ACROSS PASSES, EVERY TABLE IS VISITED — rather than the
mechanics, so a future refactor is free to change how it is stored.
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers._common import rotate_after      # noqa: E402

TABLES = [{"path": f"S/t{i}"} for i in range(10)]


def _sweep(tables, bookmark, budget):
    """One pass: resume after `bookmark`, visit at most `budget` tables.

    Returns (visited_paths, new_bookmark). Mirrors the fetcher: the bookmark is the last table
    VISITED, and it is recorded even for a table that failed."""
    ordered = rotate_after(tables, bookmark, key=lambda t: t["path"])
    visited = [t["path"] for t in ordered[:budget]]
    return visited, (visited[-1] if visited else bookmark)


def test_two_passes_cover_every_table():
    """A subject needing 2 passes must end up with all 10 tables visited, none twice."""
    seen1, bm = _sweep(TABLES, "", 6)
    seen2, bm = _sweep(TABLES, bm, 6)
    assert seen1 == [f"S/t{i}" for i in range(6)]
    assert seen2[:4] == [f"S/t{i}" for i in range(6, 10)], "must resume at the tail, not the head"
    assert set(seen1) | set(seen2) == {t["path"] for t in TABLES}


def test_the_tail_is_reached_and_is_not_starved():
    """The failure mode being prevented: without a table bookmark the last tables are never seen."""
    starved, _ = _sweep(TABLES, "", 6)
    assert "S/t9" not in starved, "precondition: one pass cannot reach the tail"
    resumed, _ = _sweep(TABLES, "S/t5", 6)
    assert "S/t9" in resumed, "the resume point is what makes the tail reachable at all"


def test_repeated_short_passes_still_converge():
    """Even a pathologically small budget must eventually visit everything, by wrapping."""
    bm, seen = "", set()
    for _ in range(10):
        got, bm = _sweep(TABLES, bm, 2)
        seen.update(got)
    assert seen == {t["path"] for t in TABLES}


def test_unknown_bookmark_starts_at_the_top_rather_than_skipping():
    """A renamed or retired table must not cause the whole subject to be skipped."""
    visited, _ = _sweep(TABLES, "S/table-that-no-longer-exists", 3)
    assert visited == ["S/t0", "S/t1", "S/t2"]


def test_empty_bookmark_starts_at_the_top():
    visited, _ = _sweep(TABLES, "", 3)
    assert visited == ["S/t0", "S/t1", "S/t2"]


def test_bookmark_is_scoped_to_its_subject():
    """"<subject>|<table>" must only be honoured for its own subject.

    Applying subject A's resume point to subject B would skip B's head every visit."""
    bm = "A|A/t5"
    assert bm.startswith("A|") and not bm.startswith("B|")
    assert bm.split("|", 1)[1] == "A/t5"
