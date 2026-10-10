"""An unexpected error in one unit's change probe is that unit's failure, never the end of the run.

WHY. From 2026-10-08 UNCTAD answered HTTP 400 ("report instance not found") for the metadata of one report.
`requests` raised HTTPError inside the fetcher's current_vintage, run_once caught only UnitTimeout and
TransientError around the probe, and the exception left the loop: four daily runs in a row ended with exit 1,
the last three on their first unit, so no cloud source was attempted (runs 37780844667, 37856392728,
37932152673, 37998288087). The fetch a few lines below has always had the missing branch.

These tests drive the REAL run_once over two real registry sources with the strategy replaced by a fake, so
no network and no fetcher runs. The first probe that is called raises; the tests pin what the loop does then.
"""
from __future__ import annotations

import pytest

from updater import orchestrate
from updater.state import StateStore

SOURCES = ["cnb", "frankfurter"]          # two cloud sources with a fetcher module; nothing of theirs is run


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
