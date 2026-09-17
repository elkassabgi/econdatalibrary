"""statcan must enumerate changed cubes by RELEASE TIME, not from the single-day change feed.

`getChangedCubeList/<date>` means "changed ON <date>", not "on or after". The fetcher read it as
a since-feed for its entire life, so `update()` inspected ONE calendar day per run and then
advanced the watermark past the others. Measured 2026-09-17 against www150: the feed's counts
run 3, 52, 30, 0, 15, 0, 16, 14, 15, 60 as the date advances — five increases and two zero days
sitting between larger ones, which a since-feed cannot produce. Independently, 505 cubes had
been released since 2026-07-29 and 456 of them are ones we hold.

These tests pin the contract that replaced it, and the two ways it must refuse rather than
report an empty change set — because "nothing changed" advances the watermark, and an
enumeration that silently returns [] would skip a window for ever.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.errors import TransientError  # noqa: E402
from updater.strategies.fetchers import statcan  # noqa: E402


def _lite(n_recent=3, n_old=1200, recent="2026-09-16", old="2026-01-02"):
    """A plausible getAllCubesListLite payload: mostly old cubes, a few recent ones."""
    out = [{"productId": 10000000 + i, "releaseTime": f"{old}T08:30"} for i in range(n_old)]
    out += [{"productId": 20000000 + i, "releaseTime": f"{recent}T08:30"} for i in range(n_recent)]
    return out


def test_it_selects_by_release_time_not_by_a_single_day(monkeypatch):
    """The whole point: a cutoff must return everything released ON OR AFTER it, across days."""
    payload = (_lite(n_recent=0)
               + [{"productId": 31, "releaseTime": "2026-09-10T08:30"},
                  {"productId": 32, "releaseTime": "2026-09-12T08:30"},
                  {"productId": 33, "releaseTime": "2026-09-16T08:30"}])
    monkeypatch.setattr(statcan, "_get", lambda ep, **kw: payload)

    got = statcan._changed_pids(dt.date(2026, 9, 11))
    assert got == {32, 33}, (
        "the enumeration must span every day at or after the cutoff; a single-day reading would "
        f"return one day's cubes or none. got {sorted(got)}")

    # And the boundary is inclusive, matching the FEED_SLACK_DAYS intent of absorbing skew.
    assert 31 in statcan._changed_pids(dt.date(2026, 9, 10))


def test_an_empty_enumeration_refuses_instead_of_reporting_no_change(monkeypatch):
    """The silent empty result is the dangerous one: `update()` advances the watermark on a
    clean pass, so [] would jump the window permanently."""
    monkeypatch.setattr(statcan, "_get", lambda ep, **kw: [])
    with pytest.raises(TransientError) as e:
        statcan._changed_pids(dt.date(2026, 9, 11))
    assert "floor" in str(e.value).lower() or "nothing changed" in str(e.value).lower()


def test_a_short_list_refuses_too(monkeypatch):
    """StatCan publishes thousands of cubes. A handful means a truncated or broken response,
    which must not read as a quiet day."""
    monkeypatch.setattr(statcan, "_get", lambda ep, **kw: _lite(n_recent=1, n_old=5))
    with pytest.raises(TransientError):
        statcan._changed_pids(dt.date(2026, 9, 11))


def test_cubes_without_a_release_time_cannot_silently_empty_the_result(monkeypatch):
    """If the field the filter depends on disappears, that is a structural break — not a day
    on which nothing was released. Refusing is what keeps the watermark still."""
    payload = [{"productId": 10000000 + i} for i in range(1200)]     # no releaseTime at all
    monkeypatch.setattr(statcan, "_get", lambda ep, **kw: payload)
    with pytest.raises(TransientError) as e:
        statcan._changed_pids(dt.date(2026, 9, 11))
    assert "releasetime" in str(e.value).lower()


def test_the_envelope_form_is_accepted_too(monkeypatch):
    """WDS returns a bare list here, but the sibling endpoints wrap in {"object": [...]}.
    Accept both rather than depend on which one this endpoint uses today."""
    monkeypatch.setattr(statcan, "_get",
                        lambda ep, **kw: {"status": "SUCCESS", "object": _lite(n_recent=2)})
    got = statcan._changed_pids(dt.date(2026, 9, 1))
    assert len(got) == 2, got


def test_the_old_single_day_endpoint_is_no_longer_called(monkeypatch):
    """A regression guard with teeth: if anyone restores getChangedCubeList, this fails.
    The control is that the NEW endpoint is the one asked for."""
    seen = []

    def _spy(endpoint, **kw):
        seen.append(endpoint)
        return _lite(n_recent=1)

    monkeypatch.setattr(statcan, "_get", _spy)
    statcan._changed_pids(dt.date(2026, 9, 1))
    assert seen == ["getAllCubesListLite"], seen
    assert not any("getChangedCubeList" in s for s in seen), (
        "getChangedCubeList is a SINGLE-DAY feed; selecting from it re-opens the defect that "
        "left 456 held cubes unreachable")
