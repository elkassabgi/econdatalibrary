"""A failing census sub-unit must say WHY it failed.

Measured 2026-09-17: census's 2026-09-14 run recorded `8/45 sub-unit(s) transient-failed; will
retry [intltrade/imports/sitc time=2026-03 for=None, ...]` and nothing more. Both except branches
discarded the exception, so three days of failures carried no cause and the only way to learn one
was to replay a request by hand. A read timeout, a 429 and an upstream HTML error page are three
different problems, and they were all wearing that same message.

The note is deliberately short: every failing sub-unit's note is concatenated into one
`unit_state.last_error`, so a traceback per flow would push the useful part out of view.
"""
from __future__ import annotations

import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.errors import DefinitiveError, TransientError  # noqa: E402
from updater.strategies.fetchers import census  # noqa: E402


def test_the_reason_carries_the_type_and_the_message():
    e = TransientError("census intltrade/imports/sitc: HTTPSConnectionPool(host='api.census.gov', "
                       "port=443): Read timed out. (read timeout=420) after 3 attempts")
    got = census._why(e)
    assert got.startswith("TransientError: "), got
    assert "Read timed out" in got, "the cause must survive into the note"
    assert "\n" not in got and "  " not in got, "the note must stay on one line"


def test_a_long_message_is_trimmed_but_not_emptied():
    got = census._why(TransientError("x" * 5000))
    assert 0 < len(got) <= 200, len(got)
    assert got.startswith("TransientError: ")


def test_an_exception_with_no_message_still_names_itself():
    """The failure mode this whole change is about: a note that says nothing."""
    assert census._why(DefinitiveError("")) == "DefinitiveError"
    assert census._why(DefinitiveError("   ")) == "DefinitiveError"


def test_whitespace_is_collapsed():
    got = census._why(TransientError("line one\n\tline two"))
    assert got == "TransientError: line one line two", got


def test_both_failure_branches_record_the_reason():
    """The source-level pin. Catching the exception is pointless if the note drops it again."""
    src = inspect.getsource(census)
    assert "except TransientError as e:" in src, "the transient branch must bind the exception"
    assert "except DefinitiveError as e:" in src, "the definitive branch must bind the exception"
    # Anchor on the except BRANCH, not on the first tally call in the file: census reports
    # sub-unit failures from several places, and only these two handle a caught exception.
    for branch in ("except TransientError as e:", "except DefinitiveError as e:"):
        i = src.index(branch)
        block = src[i:i + 700]
        end = block.index("break")
        assert "_why(e)" in block[:end], (
            f"the block under `{branch}` must pass the reason to the tally; got:\n"
            f"{block[:end]}")
