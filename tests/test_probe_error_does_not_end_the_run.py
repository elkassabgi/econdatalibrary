"""An unexpected error in one unit's change probe is that unit's failure, never the end of the run.

WHY. From 2026-10-08 UNCTAD answered HTTP 400 ("report instance not found") for the metadata of one report.
`requests` raised HTTPError inside the fetcher's current_vintage, run_once caught only UnitTimeout and
TransientError around the probe, and the exception left the loop: four daily runs in a row ended with exit 1,
the last three on their first unit, so no other cloud source was attempted (runs 37780844667, 37856392728,
37932152673, 37998288087). The fetch further down in the same loop has always had the missing branch.

These tests drive the REAL run_once over two real registry sources with the strategy replaced by a fake. The
two fetcher modules are imported (the loop checks that they exist); none of their functions is called and
nothing goes to the network. The first probe that is called raises; the tests pin what the loop does then.
"""
from __future__ import annotations

import pytest

from updater import orchestrate
from updater.state import StateStore

SOURCES = ["cnb", "frankfurter"]          # two cloud sources with a fetcher module; no function of theirs is called


class _Strategy:
    """is_due: always. detect_change: the FIRST call raises `error`; later calls answer None (unchanged)."""

    def __init__(self, error):
        self.error = error
        self.probed = []

    def is_due(self, unit, us):
        return True

    def detect_change(self, unit, us):
        self.probed.append(unit.source_id)
        if len(self.probed) == 1:
            raise self.error
        return None

    def run(self, unit, since=None):                       # pragma: no cover - a probe that says "unchanged"
        raise AssertionError("no fetch is expected in these tests")


def _run(tmp_path, monkeypatch, error, dry=False):
    monkeypatch.delenv("AQUEDUCT_LIVE_ONLY", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    # the run budget and the unit timeout decide whether a unit is admitted at all: with a budget of
    # 90 minutes or less in the environment (0 means no budget) both units are BUDGET-SKIPPED and 7 of this
    # file's 8 tests fail (reviews AR-265, AR-265b, AR-271: 60, 89 and 90 fail; 91, 120 and 0 pass)
    monkeypatch.delenv("AQUEDUCT_RUN_BUDGET_MIN", raising=False)
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    # run_once sets this module global; put it back when the test ends
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    strat = _Strategy(error)
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: strat)
    store = StateStore(str(tmp_path / "state.db"))
    return strat, store, lambda: orchestrate.run_once(sources=list(SOURCES), dry=dry, store=store)


def test_the_second_unit_is_probed_after_the_first_probe_raised(tmp_path, monkeypatch, capsys):
    error = RuntimeError("400 Client Error: Bad Request for url: https://publisher.example/meta")
    strat, store, run = _run(tmp_path, monkeypatch, error)
    results = run()
    assert sorted(strat.probed) == sorted(SOURCES), "both units were probed: the run went on after the error"
    first = strat.probed[0]
    assert (f"{first}/_all", "error") in results
    row = store.get_unit(first, "_all")
    assert row["status"] == "transient_fail"
    assert row["last_error"].startswith("detect:UNEXPECTED:RuntimeError(") and "400 Client Error" in row["last_error"]
    assert row["last_success_utc"] is None, "a failed probe is never a success"
    assert row["last_attempt_utc"], "the attempt is recorded, so the source does not look never-tried"
    runs = store.db.execute("SELECT status, note FROM runs WHERE source_id=?", (first,)).fetchall()
    assert len(runs) == 1 and runs[0][0] == "transient_fail" and runs[0][1].startswith("detect:UNEXPECTED:")
    assert f"PROBE ERROR {first}/_all" in capsys.readouterr().out
    # the other unit answered "unchanged" with no probe token: nothing is recorded for it, and no failure either
    other = strat.probed[1]
    assert store.get_unit(other, "_all") is None
    assert not any(k == f"{other}/_all" and s in ("error", "transient_fail") for k, s in results)


def test_a_dry_run_reports_the_probe_error_and_writes_no_state(tmp_path, monkeypatch):
    strat, store, run = _run(tmp_path, monkeypatch, ValueError("unreadable answer"), dry=True)
    results = run()
    assert sorted(strat.probed) == sorted(SOURCES)
    first = strat.probed[0]
    assert (f"{first}/_all", "error") in results
    assert store.get_unit(first, "_all") is None
    assert store.db.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0


@pytest.mark.parametrize("stop", [KeyboardInterrupt(), SystemExit("credentials missing")])
def test_an_interrupt_or_an_exit_in_the_probe_still_ends_the_run(tmp_path, monkeypatch, stop):
    """Not Exceptions: Ctrl-C and a deliberate SystemExit (ingest_unctad_ds raises one when its keys are
    missing) are not turned into one unit's failure."""
    strat, store, run = _run(tmp_path, monkeypatch, stop)
    with pytest.raises(type(stop)):
        run()
    assert len(strat.probed) == 1, "the run ended at the first probe"
    assert store.get_unit(strat.probed[0], "_all") is None


# ---- added after review AR-265 ----------------------------------------------------------------------------

def test_the_class_of_the_outage_and_the_size_and_time_of_the_record(tmp_path, monkeypatch):
    """requests' HTTPError (an OSError, the class the outage raised), a text far over the clip limit, and a
    probe that took time: the record is clipped and carries the probe's duration."""
    import time

    import requests

    error = requests.exceptions.HTTPError("400 Client Error: Bad Request for url: https://publisher.example/"
                                          + "x" * 5000)
    strat, store, run = _run(tmp_path, monkeypatch, error)
    plain = strat.detect_change

    def detect_change(unit, us):
        if not strat.probed:
            time.sleep(0.3)
        return plain(unit, us)
    strat.detect_change = detect_change
    results = run()
    first = strat.probed[0]
    assert (f"{first}/_all", "error") in results and len(strat.probed) == 2
    row = store.get_unit(first, "_all")
    assert row["last_error"].startswith("detect:UNEXPECTED:HTTPError(")
    assert len(row["last_error"]) < 1600 and "truncated" in row["last_error"]
    assert store.db.execute("SELECT dur_s FROM runs WHERE source_id=?", (first,)).fetchone()[0] >= 0.2


def test_a_text_the_log_cannot_encode_does_not_end_the_run(tmp_path, monkeypatch):
    """The workstation job redirects stdout to a file: cp1252 on Windows. The handler's own print raised
    UnicodeEncodeError for a text outside it, so the error left run_once and nothing was recorded."""
    import io
    import sys

    strat, store, run = _run(tmp_path, monkeypatch, RuntimeError("column 'wskaźnik 数据' missing"))
    raw = io.BytesIO()
    monkeypatch.setattr(sys, "stdout", io.TextIOWrapper(raw, encoding="cp1252", errors="strict"))
    results = run()
    sys.stdout.flush()
    first = strat.probed[0]
    assert (f"{first}/_all", "error") in results and len(strat.probed) == 2
    assert store.get_unit(first, "_all")["status"] == "transient_fail"
    assert b"PROBE ERROR" in raw.getvalue()


def test_a_dry_run_does_not_say_recorded(tmp_path, monkeypatch, capsys):
    strat, store, run = _run(tmp_path, monkeypatch, ValueError("unreadable answer"), dry=True)
    run()
    line = [ln for ln in capsys.readouterr().out.splitlines() if "PROBE ERROR" in ln]
    assert len(line) == 1 and "recorded" not in line[0] and "dry run" in line[0]


def test_the_heavy_workflow_fails_a_dedicated_run_on_every_failure_label():
    """updater-heavy.yml greps the CLI's per-unit summary for the labels that mean "this source failed". A
    label run_once can give a failed unit and that list does not name turns a failed dedicated run GREEN."""
    import os
    import re

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    text = open(os.path.join(root, ".github", "workflows", "updater-heavy.yml"), encoding="utf-8").read()
    marker = 'bad=$(grep -E "^\\s+('
    assert text.count(marker) == 1, "the guard's grep was not found, or there are two: this test no longer reads it"
    tail = text.split(marker, 1)[1]
    labels = set(tail.split(")", 1)[0].split("|"))
    assert tail.split(")", 1)[1].startswith("\\s+${{ matrix.source }}/")
    assert {"transient_fail", "broken_adapter", "timeout", "error"} <= labels
    assert "partial" not in labels                       # deliberate: see the workflow's own note
    rx = re.compile(r"^\s+(" + "|".join(sorted(labels)) + r")\s+cnb/")
    assert rx.search(f"  {'error':16} cnb/_all")         # the line updater/run.py prints
    assert not rx.search(f"  {'no_change':16} cnb/_all")
