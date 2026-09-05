"""source_data_through: an observed-only source can never be stamped past today (R730).

WHY. core/sync_state_d1.py stamps each source's data_through as MAX(end_date) < 2900 over the
catalogue copy the CI job pulls from R2. That copy lags the curated catalogue until
tools/refresh_r2_catalog.py runs (R250, a manual action), so sec_edgar's filer typos
(2215-09-30 on CIK0001647705, 6016-06-30 on VICR) kept returning as /v1/sources data_through
twice a day after they had been corrected locally. A filed period cannot end after today: for
the sources in DATA_THROUGH_OBSERVED_ONLY the stamp is the newest period that has already
ended. Every other source keeps its plain MAX - boc's 2095 projection horizon included.

Negative control (R346): a source NOT in the set with a future MAX must still stamp that MAX,
so a regression that caps everything fails here too.
"""
from __future__ import annotations
import datetime as dt
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_state_d1 as d1sync  # noqa: E402


def _state_db(path):
    from updater.state import DDL
    con = sqlite3.connect(path)
    con.executescript(DDL)
    con.execute(
        "INSERT INTO source_state(source_id,strategy,cadence,status,last_success_utc,"
        "last_attempt_utc,owner,enabled,note) VALUES ('src0','extend_by_date','daily',"
        "'ok','2026-07-01T00:00:00+00:00','2026-07-02T00:00:00+00:00',NULL,1,NULL)")
    con.commit()
    con.close()


def _catalog(path, rows):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?)", rows)
    con.commit()
    con.close()


def _stamps(tmp_path, monkeypatch, rows):
    db = str(tmp_path / "state.db")
    _state_db(db)
    cat = str(tmp_path / "catalog.db")
    _catalog(cat, rows)
    monkeypatch.setenv("ECONDL_CATALOG", cat)
    out = str(tmp_path / "sql")
    os.makedirs(out, exist_ok=True)
    files, counts = d1sync.emit_sql(db, out)
    mem = sqlite3.connect(":memory:")
    for p in files:
        mem.executescript(open(p, encoding="utf-8").read())
    got = dict(mem.execute("SELECT source_id, data_through FROM source_data_through").fetchall())
    mem.close()
    return got, counts


def test_observed_only_source_is_stamped_with_the_newest_ended_period(tmp_path, monkeypatch):
    assert "sec_edgar" in d1sync.DATA_THROUGH_OBSERVED_ONLY
    rows = [
        ("sec_edgar:A", "sec_edgar", "2026-08-01"),
        ("sec_edgar:B", "sec_edgar", "2215-09-30"),      # the CIK0001647705 typo class
        ("sec_edgar:C", "sec_edgar", "2026-02-25"),
        ("boc:X", "boc", "2095-12-31"),                   # genuine projection horizon: untouched
        ("eurostat:Y", "eurostat", "9999-12-31"),         # the 2900 sentinel still applies
        ("eurostat:Z", "eurostat", "2026-01-01"),
    ]
    got, counts = _stamps(tmp_path, monkeypatch, rows)
    assert got["sec_edgar"] == "2026-08-01"
    assert got["boc"] == "2095-12-31"
    assert got["eurostat"] == "2026-01-01"
    assert counts["source_data_through"] == 3


def test_observed_only_source_without_a_future_row_keeps_its_plain_max(tmp_path, monkeypatch):
    got, _ = _stamps(tmp_path, monkeypatch, [("sec_edgar:A", "sec_edgar", "2026-08-01"),
                                             ("sec_edgar:B", "sec_edgar", "2025-12-31")])
    assert got["sec_edgar"] == "2026-08-01"


def test_negative_control_other_sources_are_not_capped(tmp_path, monkeypatch):
    far = (dt.date.today() + dt.timedelta(days=3650)).isoformat()
    got, _ = _stamps(tmp_path, monkeypatch, [("boc:X", "boc", far), ("boc:Y", "boc", "2020-01-01")])
    assert got["boc"] == far


def test_observed_only_source_with_only_future_rows_stamps_null(tmp_path, monkeypatch):
    # nothing has ended yet: NULL is the honest answer, not a future date
    got, _ = _stamps(tmp_path, monkeypatch, [("sec_edgar:B", "sec_edgar", "2215-09-30")])
    assert got["sec_edgar"] is None
