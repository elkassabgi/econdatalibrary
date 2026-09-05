"""sec_edgar: the daily catalogue update must never predicate on series_fts.series_id.

WHY. `series_fts` is fts5(series_id UNINDEXED, title, geography). A WHERE on its series_id is a
full scan of the FTS index (~23.8M rows on D1, R492). `tools/refresh_sec_edgar.py` emitted
`DELETE FROM series_fts WHERE series_id='<id>'` once per changed company per day (commit
daf0b5b0a, 2026-08-24) and the path never executed on CI until 2026-09-05 (R726/R730). The
one-statement measurement CLAUDE.md requires was made that day at 11:28Z:
`SELECT count(*) FROM series_fts WHERE series_id = 'sec_edgar:AAPL'` did not finish inside
D1's storage timeout (error 7429). So the rule is enforced in code: `d1_catalog_statements`
touches the FTS index by INSERT alone (new companies only, decided by a primary-key pre-read)
and `assert_no_fts_predicate` refuses any statement list that predicates on it.

The negative control (R346): the OLD per-company shape must be REFUSED by the guard, so a
regression back to it fails this file rather than the next CI run.
"""
from __future__ import annotations
import importlib.util
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _load():
    p = os.path.join(ROOT, "tools", "refresh_sec_edgar.py")
    spec = importlib.util.spec_from_file_location("_refresh_sec_edgar_d1", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


M = _load()

SPANS = [
    ("AAPL", "1994-09-30", "2026-06-27", "Apple Inc. (AAPL)", 320193),          # existing, same title
    ("BF-B", "2009-04-30", "2026-07-31", "Brown-Forman Corp (BF-B, BF-A)", 14693),  # new registrant
    ("CIK0002124122", "2025-12-31", "2026-06-30", "O'Neil Holdings", 2124122),  # new, apostrophe
    ("MSFT", "1993-06-30", "2026-06-30", "Microsoft Corp (MSFT)", 789019),    # existing, title changed
]
EXISTING = {"sec_edgar:AAPL": "Apple Inc. (AAPL)", "sec_edgar:MSFT": "MICROSOFT CORP (MSFT)"}


def test_no_statement_predicates_on_series_fts():
    stmts, _n, _t = M.d1_catalog_statements(SPANS, EXISTING)
    M.assert_no_fts_predicate(stmts)          # must not raise
    for s in stmts:
        low = " ".join(s.lower().split())
        assert not ("series_fts" in low and "where" in low), s
        assert not low.startswith("delete"), s


def test_existing_id_gets_one_pk_update_and_no_fts_statement():
    stmts, n_new, n_title = M.d1_catalog_statements(SPANS[:1], EXISTING)
    assert stmts == ["UPDATE series SET start_date='1994-09-30', end_date='2026-06-27' "
                     "WHERE series_id='sec_edgar:AAPL';"]
    assert (n_new, n_title) == (0, 0)


def test_new_id_gets_series_insert_and_fts_insert_only():
    stmts, n_new, n_title = M.d1_catalog_statements(SPANS[1:2], EXISTING)
    assert n_new == 1 and n_title == 0
    assert len(stmts) == 2
    assert stmts[0].startswith("INSERT OR IGNORE INTO series (")
    assert "'sec_edgar:BF-B'" in stmts[0] and "'2009-04-30','2026-07-31'" in stmts[0]
    assert stmts[1] == ("INSERT INTO series_fts (series_id, title, geography) VALUES "
                        "('sec_edgar:BF-B','Brown-Forman Corp (BF-B, BF-A)','US');")


def test_title_change_rides_the_pk_update_and_is_counted():
    stmts, n_new, n_title = M.d1_catalog_statements(SPANS[3:], EXISTING)
    assert (n_new, n_title) == (0, 1)
    assert stmts == ["UPDATE series SET start_date='1993-06-30', end_date='2026-06-30', "
                     "title='Microsoft Corp (MSFT)' WHERE series_id='sec_edgar:MSFT';"]


def test_quotes_are_escaped():
    stmts, _n, _t = M.d1_catalog_statements(SPANS[2:3], EXISTING)
    assert "O''Neil Holdings" in stmts[0] and "O''Neil Holdings" in stmts[1]
    assert "O'Neil" not in stmts[0].replace("O''Neil", "")


def test_counts_over_the_whole_batch():
    stmts, n_new, n_title = M.d1_catalog_statements(SPANS, EXISTING)
    assert (n_new, n_title) == (2, 1)
    assert len(stmts) == 1 + 2 + 2 + 1


def test_negative_control_old_delete_shape_is_refused():
    old_shape = [
        "INSERT OR IGNORE INTO series (series_id, source_id) VALUES ('sec_edgar:X','sec_edgar');",
        "DELETE FROM series_fts WHERE series_id = 'sec_edgar:X';",
        "INSERT INTO series_fts (series_id, title, geography) VALUES ('sec_edgar:X','X','US');",
    ]
    with pytest.raises(RuntimeError, match="series_fts"):
        M.assert_no_fts_predicate(old_shape)


def test_guard_also_refuses_a_match_and_a_lowercase_where():
    with pytest.raises(RuntimeError):
        M.assert_no_fts_predicate(["select count(*) from series_fts where series_id='a'"])
    with pytest.raises(RuntimeError):
        M.assert_no_fts_predicate(["SELECT series_id FROM series_fts WHERE series_fts MATCH 'apple'"])
    M.assert_no_fts_predicate(["INSERT INTO series_fts (series_id, title, geography) VALUES ('a','b','c');"])
