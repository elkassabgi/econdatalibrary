"""WU-2b: truncated cursor evidence books the full_rederive_owed debt.

fhfa's rebuild changes all ~89,706 catalogued series against CURSOR_CAP=50,000, so
39,706 catalogued CSVs never re-derived via §5.7 — silently: no note, no demotion
(the spec's measured I4 hole). The fix is a disclosure chain: fetcher sets
Result.cursor_cap_hit -> orchestrator books full_rederive_owed -> health lists it
until a wholesale campaign's success stamp clears it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate  # noqa: E402
from updater.strategies.base import Result  # noqa: E402

_FETCHERS = os.path.join(os.path.dirname(orchestrate.__file__),
                         "strategies", "fetchers")


def test_default_is_false():
    """Un-migrated fetchers change nothing: the flag defaults False."""
    assert Result(status="ok").cursor_cap_hit is False


def test_fhfa_sets_the_flag_from_the_cap_check():
    """Call-site pin (R511 style): fhfa computes cap_hit from CURSOR_CAP and puts
    it on the Result — not a re-implementation, the one shipped check."""
    src = open(os.path.join(_FETCHERS, "fhfa.py"), encoding="utf-8").read()
    assert "cap_hit = len(cursors) >= CURSOR_CAP" in src
    assert "res.cursor_cap_hit = cap_hit" in src


def test_orchestrator_books_the_debt_on_the_flag():
    """Wiring pin: the orchestrator's success path answers cursor_cap_hit with
    note_full_rederive_owed (the durable debt — same lifecycle as the no-cursors
    branch; cleared only by a wholesale campaign's success stamp)."""
    src = open(os.path.join(os.path.dirname(orchestrate.__file__), "orchestrate.py"),
               encoding="utf-8").read()
    i = src.find('getattr(res, "cursor_cap_hit", False)')
    assert i != -1, "orchestrate no longer reads cursor_cap_hit"
    block = src[i:i + 900]
    assert "note_full_rederive_owed" in block, (
        "cursor_cap_hit no longer books full_rederive_owed next to where it is read")
    assert "changed-set evidence truncated" in block


def test_stale_fhfa_docstring_is_gone():
    """The '~5k' fhfa figure survived 18x growth (annual_tract alone is 63,930);
    pin the corrected text so it cannot quietly regress."""
    src = open(os.path.join(_FETCHERS, "_common.py"), encoding="utf-8").read()
    assert "fhfa ~5k" not in src
    assert "63,930" in src
