"""Regression gate: every cso sub-unit failure names itself.

WHAT WENT WRONG. cso's fetch loop had four paths to `tally.transient_unit()` and three of them
passed NO id and printed NOTHING:

    except (requests.Timeout, requests.ConnectionError): tally.transient_unit(); continue
    except Exception:                                    tally.transient_unit(); continue
    if not rows:                                         tally.transient_unit(); continue

So a run could report "23/26 sub-unit(s) transient-failed; will retry" and nothing anywhere said
whether that was a timeout, a 429, a 403, a JSON-stat parse error, or a 200 that parsed zero
observations. Those are four different problems with four different fixes.

WHY IT MATTERED CONCRETELY. On 2026-08-03 CI failed 23 of 26 cso sub-units in 47 seconds — fast
failures, not timeouts — and the run before had failed 60 of 60. Probing CSO's ReadDataset from
the workstation seconds later fetched 4 of 4 matrices in ~1s each (AKA03 101 rows, AKM01 1,982,
AKM02 2,385, AKM03 1,625). Works from here, fails from the runner: the signature of upstream
throttling a cloud IP, which would argue for `run_location: local`. But the fetcher's own output
could not tell that apart from a schema break, so the routing decision had no evidence — and a
diagnosis you cannot make is a fix you cannot justify.

The classification is DELIBERATELY UNCHANGED. Every one of these stays TRANSIENT: an empty
result really is indistinguishable from a flaky-upstream empty at the return value, and calling
it structural would false-raise DefinitiveError on a bad hour. This is only about saying WHICH
sub-unit failed and WHY, so the next reader has something to act on.
"""
from __future__ import annotations
import os
import sys

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.strategies.fetchers._common import Tally      # noqa: E402


def _ids(t: Tally):
    return list(getattr(t, "transient_ids", []) or [])


def test_tally_records_the_id_it_is_given():
    """The mechanism the fix relies on: transient_unit(id) keeps the id. If this ever stops
    being true the naming silently reverts to the old behaviour."""
    t = Tally()
    t.transient_unit("AKA03: ConnectionError")
    t.transient_unit("AKM01: empty")
    assert t.transient == 2
    assert _ids(t) == ["AKA03: ConnectionError", "AKM01: empty"]


def test_an_unnamed_failure_is_indistinguishable():
    """The negative control — this is what the code used to do. Two different causes become the
    same record, which is precisely why 23/26 could not be diagnosed."""
    t = Tally()
    t.transient_unit()
    t.transient_unit()
    assert t.transient == 2
    assert not [i for i in _ids(t) if i], (
        "if unnamed failures DID carry ids this test is meaningless — check Tally")


@pytest.mark.parametrize("exc,label", [
    (requests.Timeout("read timed out"), "Timeout"),
    (requests.ConnectionError("conn reset"), "ConnectionError"),
    (ValueError("JSON-stat2: no dimension"), "ValueError"),
    (RuntimeError("HTTP 429"), "RuntimeError"),
])
def test_every_exception_class_yields_a_named_transient(exc, label):
    """Mirrors the fetch loop's two except branches: whatever comes out of fetch_table, the
    recorded id carries the matrix AND the exception type. The bare `except Exception` is the
    one that mattered — a 429 and a parse error both landed there."""
    t = Tally()
    mtr = "AKA03"
    try:
        raise exc
    except (requests.Timeout, requests.ConnectionError) as e:
        t.transient_unit(f"{mtr}: {type(e).__name__}")
    except Exception as e:                                       # noqa: BLE001
        t.transient_unit(f"{mtr}: {type(e).__name__}")
    assert _ids(t) == [f"{mtr}: {label}"]


def test_empty_result_is_still_transient_but_named():
    """`if not rows` keeps its TRANSIENT classification on purpose — a network failure after
    retries and a 200 that parsed 0 obs are indistinguishable from the return value, and
    calling that structural would false-raise DefinitiveError on a flaky hour. It just says
    which matrix."""
    t = Tally()
    rows = []
    if not rows:
        t.transient_unit("AKM02: empty")
    assert t.transient == 1
    assert _ids(t) == ["AKM02: empty"]


def test_the_source_no_longer_contains_an_unnamed_transient_call():
    """Grep the real module. A test that only exercises Tally would pass even if the fetcher
    reverted, because the fetcher is what forgets to pass the id."""
    import io
    src = io.open(os.path.join(ROOT, "updater", "strategies", "fetchers", "cso.py"),
                  encoding="utf-8").read()
    bare = src.count("tally.transient_unit()")
    assert bare == 0, (
        f"{bare} unnamed tally.transient_unit() call(s) left in cso.py — each one is a failure "
        f"nobody can act on")
