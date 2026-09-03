"""The first-pass crawlers had no monitoring at all, and the gap cost two hours on 2026-09-03.

`FIRSTPASS_DIRS = {"cbs_nl", "gus_dbw", "dbnomics"}` (orchestrate.py:50) are excluded from the
normal run and have no `unit_state` row, so nothing in the daily email mentioned them. The project
memory had said so outright — "nothing would report them if the crawlers stopped" — and that day
gus_dbw's refresh sat stalled on upstream connection resets from 10:11 UTC and was found only
because someone happened to be reading process memory for an unrelated reason.

THE ONE THING THESE TESTS REALLY PROTECT is that the signal is the newest file of ANY type.
Measured that day:

    cbs_nl    newest _.ckpt.json        0.01 h      newest PARQUET   0.02 h
    gus_dbw   newest _checkpoint.json   1.99 h      newest PARQUET  10 DAYS

gus_dbw writes a checkpoint continuously and parquet about every ten days. A parquet-only check
calls it ten days dead while it is working normally — the exact false alarm that teaches a reader
to skip the section, and a mistake this project has made before in the other direction.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.send_digest import (FIRSTPASS_STALE_HOURS,  # noqa: E402
                                 firstpass_ages)

HOUR = 3600.0


def _touch(path, age_hours, now):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    t = now - age_hours * HOUR
    os.utime(path, (t, t))


def test_a_checkpoint_counts_even_when_the_parquet_is_ancient(tmp_path):
    """gus_dbw's real shape: parquet ten days old, checkpoint minutes old, crawler healthy."""
    now = time.time()
    _touch(tmp_path / "gus_dbw" / "data.parquet", 240.0, now)
    _touch(tmp_path / "gus_dbw" / "_checkpoint.json", 1.99, now)

    got = firstpass_ages(str(tmp_path), ["gus_dbw"], now)
    assert len(got) == 1
    name, age, newest = got[0]
    assert name == "gus_dbw"
    assert newest == "_checkpoint.json", "a parquet-only signal would report ten days dead"
    assert abs(age - 1.99) < 0.05
    assert age < FIRSTPASS_STALE_HOURS, "1.99h is normal for this crawler and must not alarm"


def test_a_missing_directory_is_omitted_not_alarmed(tmp_path):
    """dbnomics has no directory — the domain is banned (R251). Reporting it daily is noise."""
    now = time.time()
    _touch(tmp_path / "cbs_nl" / "a.parquet", 0.01, now)
    got = firstpass_ages(str(tmp_path), ["cbs_nl", "dbnomics"], now)
    assert [g[0] for g in got] == ["cbs_nl"]


def test_an_empty_directory_is_omitted(tmp_path):
    (tmp_path / "cbs_nl").mkdir()
    assert firstpass_ages(str(tmp_path), ["cbs_nl"], time.time()) == []


def test_a_genuinely_dead_crawler_crosses_the_threshold(tmp_path):
    """The whole point: if it stops writing anything at all, the section must say so."""
    now = time.time()
    _touch(tmp_path / "gus_dbw" / "_checkpoint.json", FIRSTPASS_STALE_HOURS + 3.0, now)
    (name, age, _newest), = firstpass_ages(str(tmp_path), ["gus_dbw"], now)
    assert age > FIRSTPASS_STALE_HOURS


def test_oldest_first_so_the_worst_reads_first(tmp_path):
    now = time.time()
    _touch(tmp_path / "cbs_nl" / "a", 0.5, now)
    _touch(tmp_path / "gus_dbw" / "b", 4.0, now)
    assert [g[0] for g in firstpass_ages(str(tmp_path), ["cbs_nl", "gus_dbw"], now)] == \
        ["gus_dbw", "cbs_nl"]


def test_subdirectories_are_not_mistaken_for_writes(tmp_path):
    """A stale crawler whose directory holds a fresh SUBDIR must still read as stale."""
    now = time.time()
    _touch(tmp_path / "cbs_nl" / "old.parquet", 20.0, now)
    (tmp_path / "cbs_nl" / "_meta").mkdir()
    (name, age, newest), = firstpass_ages(str(tmp_path), ["cbs_nl"], now)
    assert newest == "old.parquet" and age > FIRSTPASS_STALE_HOURS


def test_the_dirs_come_from_orchestrate_not_a_copy():
    """A second copy of FIRSTPASS_DIRS is R676's shape and drifts the first time one is added."""
    from updater.orchestrate import FIRSTPASS_DIRS
    assert "gus_dbw" in FIRSTPASS_DIRS and "cbs_nl" in FIRSTPASS_DIRS
    import updater.send_digest as sd
    text = open(sd.__file__, encoding="utf-8").read()
    assert "from updater.orchestrate import FIRSTPASS_DIRS" in text, (
        "send_digest must IMPORT the list, never restate it")
    assert '"cbs_nl"' not in text and "'cbs_nl'" not in text, (
        "send_digest names a first-pass dir literally — that is the copy this forbids")
