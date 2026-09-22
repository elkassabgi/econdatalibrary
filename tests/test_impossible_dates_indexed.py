# -*- coding: utf-8 -*-
"""The indexed sweep must answer EXACTLY what the full scan answers.

`tools/audit_catalogue_impossible_dates.py --indexed` exists so the census can run against the
live 11.9 GB catalogue, whose full scan the tool's own header records as starving for hours on
the crawlers' disk - which is why the census was never routine. A cheaper access path is only
worth having if it gives the same answer, so this runs BOTH modes over the same fixture and
requires identical per-source numbers.

Every case carries a positive control: the fixture deliberately contains bad dates, so a run
that reports nothing has failed rather than passed. That matters here because "no impossible
dates" is exactly what a broken predicate, an empty table or a mistyped column name all print.
"""
from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TOOL = os.path.join(_REPO, "tools", "audit_catalogue_impossible_dates.py")

# source_id, series_id, start_date, end_date
ROWS = [
    # --- sane, must never be reported -------------------------------------------------
    ("bls", "bls:A", "1913-01-01", "2026-07-01"),
    ("bls", "bls:B", "1950-01-01", "2020-01-01"),
    ("worldbank", "worldbank:A", "1961-12-31", "2024-12-31"),
    # --- impossible START (the stat_slovenia year-0001 shape) --------------------------
    ("cbs_nl", "cbs_nl:X", "0002-07-31", "2009-12-31"),
    ("cbs_nl", "cbs_nl:Y", "0002-07-31", "2000-12-31"),
    # --- impossible END (the eurostat 9999 sentinel) -----------------------------------
    ("eurostat", "eurostat:A", "9999-12-31", "9999-12-31"),
    # --- BOTH ends bad, so early_start and late_end must each count it -----------------
    ("ggdc", "ggdc:A", "0001-01-01", "9999-12-31"),
]


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    p = str(tmp_path_factory.mktemp("impdates") / "catalog.db")
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, "
                "start_date TEXT, end_date TEXT)")
    # the index the --indexed path relies on; its absence would make the sweep a scan
    con.execute("CREATE INDEX ix_series_source_id ON series(source_id)")
    con.executemany("INSERT INTO series (source_id, series_id, start_date, end_date) "
                    "VALUES (?,?,?,?)", ROWS)
    con.commit()
    con.close()
    return p


def _run(db_path, *extra):
    r = subprocess.run([sys.executable, _TOOL, db_path, *extra], capture_output=True,
                       text=True, encoding="utf-8", errors="replace", cwd=_REPO)
    assert r.returncode == 0, f"tool exited {r.returncode}: {r.stderr[-400:]}"
    return r.stdout


def _parse(out):
    """{source: (early_start, late_end, any_bad, n_rows)} from the table the tool prints."""
    found = {}
    for line in out.splitlines():
        m = re.match(r"^(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\b", line)
        if m and m.group(1) not in ("source", "rows:", "sources:"):
            found[m.group(1)] = tuple(int(m.group(i)) for i in (2, 3, 4, 5))
    return found


def test_fixture_actually_contains_defects(db):
    """Positive control. If this fails, every 'agrees' below is agreement about nothing."""
    parsed = _parse(_run(db))
    assert parsed, "the full scan reported NO sources - the fixture or the predicate is broken"
    assert set(parsed) == {"cbs_nl", "eurostat", "ggdc"}, parsed
    # and the sane sources must NOT appear
    assert "bls" not in parsed and "worldbank" not in parsed, parsed


def test_indexed_matches_full_scan_exactly(db):
    full = _parse(_run(db))
    indexed = _parse(_run(db, "--indexed"))
    assert indexed == full, f"indexed={indexed} full={full}"


def test_counts_are_the_expected_ones(db):
    """Pin the numbers themselves, not just that the two modes agree with each other.

    Two modes can agree and both be wrong; this is what catches a predicate that counts a
    both-ends-bad row once instead of in each column.
    """
    parsed = _parse(_run(db, "--indexed"))
    assert parsed["cbs_nl"] == (2, 0, 2, 2), parsed["cbs_nl"]
    # eurostat:A is 9999-12-31 -> 9999-12-31, so BOTH ends read as the sentinel - but the
    # early-start column asks `start_date < '1500-01-01'` and these are string comparisons,
    # where '9999-12-31' sorts ABOVE '1500-01-01'. So it is late_end only. Written here as
    # (1,1,1,1) first and corrected by the run: the tool was right and the expectation was not.
    assert parsed["eurostat"] == (0, 1, 1, 1), parsed["eurostat"]
    # ggdc's single row is bad at BOTH ends: counted in early_start AND late_end, once in any
    assert parsed["ggdc"] == (1, 1, 1, 1), parsed["ggdc"]


def test_indexed_reports_the_whole_table_total(db):
    """`rows:` is the table total, not just the offending rows - in both modes."""
    for extra in ((), ("--indexed",)):
        out = _run(db, *extra)
        m = re.search(r"^rows:\s+(\d+)", out, re.M)
        assert m, out[:300]
        assert int(m.group(1)) == len(ROWS), f"{extra}: said {m.group(1)}, fixture has {len(ROWS)}"


def test_indexed_does_not_warn_about_the_live_catalogue(db, tmp_path):
    """The live-file warning is for the scanning path; --indexed is the remedy it points at."""
    live = tmp_path / "data"
    live.mkdir()
    target = str(live / "catalog.db")
    con = sqlite3.connect(target)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, "
                "start_date TEXT, end_date TEXT)")
    con.execute("CREATE INDEX ix_series_source_id ON series(source_id)")
    con.executemany("INSERT INTO series (source_id, series_id, start_date, end_date) "
                    "VALUES (?,?,?,?)", ROWS)
    con.commit()
    con.close()

    scan = subprocess.run([sys.executable, _TOOL, target], capture_output=True, text=True,
                          encoding="utf-8", errors="replace", cwd=_REPO)
    idx = subprocess.run([sys.executable, _TOOL, target, "--indexed"], capture_output=True,
                         text=True, encoding="utf-8", errors="replace", cwd=_REPO)
    assert "WARNING" in scan.stderr, "the scanning path stopped warning about the live file"
    assert "WARNING" not in idx.stderr, idx.stderr
