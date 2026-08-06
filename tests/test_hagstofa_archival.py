"""hagstofa: a stored table whose time dim is gone AND whose data ended years ago is a
frozen ARCHIVE (quiet), not a structural break re-fired every sweep.

Pinned 2026-08-05: KOS03190 probed live — 'Participation by sex, age and municipality
2018', variables Municipality/Age/Sex, NO time dimension. Seven such archival event
tables (2018 elections, 2011 census) kept hagstofa permanently partial. A RECENT stored
max still classifies structural: a live table losing its time dimension is a real break.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _run(monkeypatch, since_date):
    from updater.strategies.fetchers import hagstofa as H
    meta = {"title": "Participation by sex, age and municipality 2018",
            "variables": [{"code": "Municipality", "values": ["a"], "time": False},
                          {"code": "Age", "values": ["b"], "time": False}]}
    monkeypatch.setattr(H, "_get_meta", lambda sess, url: meta)
    monkeypatch.setattr(H.time, "sleep", lambda s: None)
    return H._fetch_table(object(), "Ibuar", "kosningar/x/KOS03190.px",
                          "IS:x", since_date)


def test_old_stored_max_is_frozen_archive(monkeypatch):
    rows, outcome = _run(monkeypatch, dt.date(2018, 12, 31))
    assert (rows, outcome) == ([], "quiet")


def test_recent_stored_max_is_still_structural(monkeypatch):
    rows, outcome = _run(monkeypatch, dt.date.today() - dt.timedelta(days=30))
    assert (rows, outcome) == ([], "structural")


def test_never_stored_timeless_table_stays_empty(monkeypatch):
    rows, outcome = _run(monkeypatch, None)
    assert (rows, outcome) == ([], "empty")
