"""source_data_through: a source stamped from D1 is never stamped from the catalogue copy (R730, R737).

WHY. core/sync_state_d1.py stamps each source's data_through as MAX(end_date) < 2900 over the
catalogue copy the CI job pulls from R2. sec_edgar's rows in that copy are not that source's truth
(its refresher catalogues on D1 only, R726): the copy carried filer typos (2215-09-30) and old-rule
forward rows, so the sync overwrote a correct hand stamp twice on 2026-09-05, and the first fix - a
"MAX of periods already ended" over the same copy - stamped 2026-09-01, a forward row itself, one
that would have crept forward with the calendar (R737). The rule now: sources in
DATA_THROUGH_FROM_D1 are left out of the sync's stamp entirely; tools/stamp_source_data_through.py
stamps them from D1's own rows after every refresher run. Every other source keeps its plain MAX -
boc's 2095 projection horizon included.

Negative control (R346): a source NOT in the set with a future MAX must still stamp that MAX, so a
regression that drops or caps everything fails here too; and a fixture with an old-rule sec_edgar
row dated TOMORROW must produce no sec_edgar stamp at all.
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


def test_a_d1_stamped_source_is_never_stamped_from_the_copy(tmp_path, monkeypatch):
    assert "sec_edgar" in d1sync.DATA_THROUGH_FROM_D1
    tomorrow = (dt.date.today() + dt.timedelta(days=1)).isoformat()
    rows = [
        ("sec_edgar:A", "sec_edgar", "2026-08-01"),
        ("sec_edgar:B", "sec_edgar", "2215-09-30"),      # the CIK0001647705 typo class
        ("sec_edgar:OZSC", "sec_edgar", tomorrow),        # an old-rule forward row: the creep of R737
        ("boc:X", "boc", "2095-12-31"),                   # genuine projection horizon: untouched
        ("eurostat:Y", "eurostat", "9999-12-31"),         # the 2900 sentinel still applies
        ("eurostat:Z", "eurostat", "2026-01-01"),
    ]
    got, counts = _stamps(tmp_path, monkeypatch, rows)
    assert "sec_edgar" not in got                        # stamped from D1 by the refresher, never from here
    assert got["boc"] == "2095-12-31"
    assert got["eurostat"] == "2026-01-01"
    assert counts["source_data_through"] == 2


def test_negative_control_other_sources_are_not_capped_or_dropped(tmp_path, monkeypatch):
    far = (dt.date.today() + dt.timedelta(days=3650)).isoformat()
    got, counts = _stamps(tmp_path, monkeypatch, [("boc:X", "boc", far), ("boc:Y", "boc", "2020-01-01")])
    assert got["boc"] == far
    assert counts["source_data_through"] == 1


def test_a_copy_holding_only_d1_stamped_sources_stamps_nothing(tmp_path, monkeypatch):
    got, counts = _stamps(tmp_path, monkeypatch, [("sec_edgar:B", "sec_edgar", "2215-09-30")])
    assert got == {}
    assert counts["source_data_through"] == 0
