"""statfin: `ok` means a complete ROTATION CYCLE, never a pass the budget cut short (R303, 2026-09-23).

Measured: the 2026-09-05 run (33988619740) reached 28 of 134 subjects in its 30-minute budget and
reported `ok`, so the next pass waited the monthly cadence (25.2 days) and a full rotation took ~5
such passes, ~125 days, against an 84-day data clock. StatFin updated kbar to 2026M08 on 2026-08-25;
kbar was among the 106 subjects skipped, and R2 still ends it at 2026-07-01.

Now a pass that stops before every subject has been visited SINCE THE LAST `ok` books the unvisited
ones as deferred (`partial`). The scheduling that follows is base.is_due's, pinned below: a
`partial` never advances last_success, so a due source stays due on every run until the cycle
completes, and the completing pass is `ok`.

The real update() runs; only the network, the store's parquet side and the clock are faked. The
rotation bookmark and the cycle file are written for real, under tmp_path (local backend).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import statfin as sf  # noqa: E402

SUBJECTS = ("aaa", "kbar", "ton")
TABLES = [{"path": f"{s}/{t}.px"} for s in SUBJECTS for t in ("11a", "11b")]


class _Deadline:
    """Lets `allow` subjects start, then reports the budget spent."""
    def __init__(self, allow):
        self.allow, self.asked = allow, 0

    def spent(self):
        self.asked += 1
        return self.asked > self.allow

    def elapsed_min(self):
        return 30.0


def _wire(monkeypatch, tmp_path, allow, flaky=()):
    monkeypatch.setattr(sf.config, "source_dir", lambda source: str(tmp_path))
    monkeypatch.setattr(sf, "_session", lambda: None)
    monkeypatch.setattr(sf, "_crawl_catalog", lambda sess: list(TABLES))
    monkeypatch.setattr(sf, "_table_frontier", lambda out_dir, subj: {})
    monkeypatch.setattr(sf, "Deadline", lambda minutes=None: _Deadline(allow))
    visited = []

    def _query(sess, path, since_date):
        visited.append(path)
        if path in flaky:
            raise sf.TransientError("pretend StatFin timed out")
        return [(path.replace("/", ":") + ":x=1", dt.date(2026, 8, 1), 1.0)], "data"
    monkeypatch.setattr(sf, "_query_table", _query)
    monkeypatch.setattr(sf.blob, "row_count", lambda path: 0)
    monkeypatch.setattr(sf.merge, "merge_and_write", lambda path, tbl, **k: (tbl.num_rows, None))
    return visited


def _subjects(visited):
    return {p.split("/")[0] for p in visited}


def test_a_budget_stop_reads_partial_and_names_what_was_deferred(monkeypatch, tmp_path):
    visited = _wire(monkeypatch, tmp_path, allow=1)
    res = sf.update(None, None)
    assert _subjects(visited) == {"aaa"}, visited
    assert res.status == "partial", f"a pass that covered 1 of 3 subjects read {res.status}"
    assert "kbar" in (res.error or "") and "ton" in (res.error or ""), res.error
    assert "none failed" in (res.error or ""), "a deferral is not a failure (R303)"


def test_the_cycle_completes_across_passes_and_only_then_reads_ok(monkeypatch, tmp_path):
    """Pass 1 visits aaa, pass 2 kbar, pass 3 ton. Only pass 3 completes the cycle."""
    statuses = []
    for _ in range(3):
        visited = _wire(monkeypatch, tmp_path, allow=1)
        statuses.append((sf.update(None, None).status, sorted(_subjects(visited))))
    assert statuses == [("partial", ["aaa"]), ("partial", ["kbar"]), ("ok", ["ton"])], statuses
    cyc = json.load(open(os.path.join(str(tmp_path), sf.CYCLE_FILE), encoding="utf-8"))
    assert cyc["visited"] == [] and cyc.get("completed_utc"), "a completed cycle must reset"


def test_a_failed_table_in_the_completing_pass_does_not_restart_the_cycle(monkeypatch, tmp_path):
    """Review AR-119 (a): resetting on a pass with a transient table started a fresh ~5-run cycle
    where one clean pass would have closed this one."""
    _wire(monkeypatch, tmp_path, allow=2)
    sf.update(None, None)                                              # aaa, kbar
    _wire(monkeypatch, tmp_path, allow=1, flaky={"ton/11a.px"})
    assert sf.update(None, None).status == "partial"                   # ton, one table failed
    cyc = json.load(open(os.path.join(str(tmp_path), sf.CYCLE_FILE), encoding="utf-8"))
    assert cyc["visited"] == ["aaa", "kbar"], \
        "ton's table failed, so ton stays OWED - neither reset nor counted as done (review R1103)"
    visited = _wire(monkeypatch, tmp_path, allow=99)       # the rotation resumes AFTER ton, so
    res = sf.update(None, None)                              # the pass must wrap round to reach it
    assert "ton" in {p.split("/")[0] for p in visited} and res.status == "ok", \
        "the next clean pass refetches ton and closes the cycle"


def test_a_subject_whose_tables_failed_is_refetched_before_the_cycle_closes(monkeypatch, tmp_path):
    """Review R1103, P2 (found on stat_latvia, same code shape here): marked visited at the START,
    a subject whose tables failed counted as done and the next pass closed the cycle without it."""
    _wire(monkeypatch, tmp_path, allow=1, flaky={"aaa/11a.px", "aaa/11b.px"})
    sf.update(None, None)                                             # aaa: every table failed
    _wire(monkeypatch, tmp_path, allow=2)
    res = sf.update(None, None)                                       # kbar, ton - budget ends
    assert res.status == "partial" and "aaa (" in (res.error or ""), \
        f"aaa was never fetched successfully, so the cycle cannot close: {res.status} {res.error}"


def test_the_deferral_names_only_subjects_unvisited_this_cycle(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=1)
    sf.update(None, None)                                  # visits aaa
    _wire(monkeypatch, tmp_path, allow=1)
    res = sf.update(None, None)                            # visits kbar
    assert res.status == "partial"
    assert "ton" in res.error and "aaa (" not in res.error, \
        f"aaa was visited earlier in this cycle and is not owed: {res.error}"


def test_a_pass_skips_subjects_already_visited_this_cycle(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=1)
    sf.update(None, None)                                              # aaa
    visited = _wire(monkeypatch, tmp_path, allow=99)
    res = sf.update(None, None)
    assert {p.split("/")[0] for p in visited} == {"kbar", "ton"} and res.status == "ok", \
        "the pass does only the owed subjects and closes the cycle"


def test_skipped_subjects_still_count_toward_the_reported_total(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=1)
    sf.update(None, None)                                              # aaa
    monkeypatch.setattr(sf.blob, "row_count", lambda path: 7 if path.endswith("aaa.parquet") else 0)
    _wire_keep_rowcount = sf.blob.row_count
    visited = _wire(monkeypatch, tmp_path, allow=99)
    monkeypatch.setattr(sf.blob, "row_count", _wire_keep_rowcount)
    res = sf.update(None, None)                                        # skips aaa
    assert "aaa" not in {p.split("/")[0] for p in visited}
    assert res.obs >= 7, f"aaa's 7 stored rows must be in the total: {res.obs}"


def test_negative_control_a_complete_pass_is_ok(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    res = sf.update(None, None)
    assert res.status == "ok", (res.status, res.error)


def test_scheduling_a_partial_stays_due_every_run_and_an_ok_waits_the_cadence():
    """What the comment in statfin.update() says about scheduling, measured through base.is_due."""
    from updater.strategies.base import Unit
    from updater.strategies.sdmx_delta import SdmxDelta
    s = SdmxDelta()
    u = Unit("statfin", "_all", "sdmx_delta", cadence="monthly")
    old_ok = "2026-09-05T22:26:17+00:00"
    partial = {"status": "partial", "last_success_utc": old_ok,
               "last_attempt_utc": "2026-10-01T06:40:00+00:00"}
    for t in ("2026-10-01T18:00:00+00:00", "2026-10-02T06:00:00+00:00"):
        assert s.is_due(u, partial, dt.datetime.fromisoformat(t)), \
            f"a partial cycle must be due again at {t}"
    done = {"status": "ok", "last_success_utc": "2026-10-02T07:10:00+00:00",
            "last_attempt_utc": "2026-10-02T07:10:00+00:00"}
    assert not s.is_due(u, done, dt.datetime.fromisoformat("2026-10-02T18:00:00+00:00")), \
        "a completed cycle must wait the cadence"
    assert s.is_due(u, done, dt.datetime.fromisoformat("2026-10-28T06:00:00+00:00"))
