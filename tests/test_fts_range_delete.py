"""The whole-source FTS reconcile costs ONE full scan, and only when it is safe to.

WHY. `series_fts` is fts5(series_id UNINDEXED, ...), so EVERY `WHERE series_id IN (...)`
DELETE full-scans the table — 23,843,482 rows measured on live D1 2026-08-26 — regardless
of how many ids the list holds. Cost is therefore per STATEMENT, and R492's rule is to
raise ARITY, never add statements. A whole-source reconcile can take that to its limit:
one range predicate `series_id >= 'src:' AND series_id < 'src;'` covers every id of the
source in a single statement. Measured stake: idb Option B catalogues 957,011 ids, which
at 500/stmt is 1,915 statements x 23.8M rows = 4.56e10 rows ~ $45.60, against ~$0.024 for
the range form.

The danger is the mirror image, and it is why this is opt-in: a range delete removes the
index rows of the WHOLE source, including series that are not in `rows`. Used on the
incremental pending-queue path — whose rows are a partial slice and can mix sources — it
would silently unlist everything the slice omits. R487 already records that an FTS delete
whose matching insert never runs leaves series unfindable.

Pinned here as discriminating pairs: the range form appears ONLY with an explicit source,
it replaces (never accompanies) the id-list deletes, every row still gets its INSERT, and
a partial or mixed set falls back to the per-block form.
"""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sync_catalog_d1 import emit_sql, FTS_DELETE_PER_STMT  # noqa: E402

COLS = ["series_id", "source_id", "title"]


def _rows(source: str, n: int, start: int = 0):
    return [{"series_id": f"{source}:K{i:05d}", "source_id": source,
             "title": f"title {i}", "geography": None} for i in range(start, start + n)]


def _stmts(rows, fts_range_source=None):
    with tempfile.TemporaryDirectory() as d:
        emit_sql(COLS, rows, d, None, fts_range_source=fts_range_source)
        out = []
        for fn in sorted(os.listdir(d)):
            out.append(open(os.path.join(d, fn), encoding="utf-8").read())
        return "\n".join(out)


def test_range_form_is_one_delete_and_replaces_the_id_lists():
    rows = _rows("idb", 1200)          # 3 blocks at 500/stmt
    sql = _stmts(rows, fts_range_source="idb")
    assert sql.count("DELETE FROM series_fts") == 1, "exactly one scan, not one per block"
    assert "series_id >= 'idb:' AND series_id < 'idb;'" in sql
    assert "DELETE FROM series_fts WHERE series_id IN" not in sql, \
        "the range delete must REPLACE the id-list deletes, never accompany them"


def test_every_row_still_gets_an_fts_insert():
    rows = _rows("idb", 1200)
    sql = _stmts(rows, fts_range_source="idb")
    for r in rows:
        assert f"'{r['series_id']}'" in sql, f"{r['series_id']} lost its index row"


def test_without_the_flag_the_old_per_block_form_is_unchanged():
    rows = _rows("idb", 1200)
    sql = _stmts(rows)                  # no fts_range_source
    expected_blocks = -(-len(rows) // FTS_DELETE_PER_STMT)
    assert sql.count("DELETE FROM series_fts WHERE series_id IN") == expected_blocks
    assert "series_id >= 'idb:'" not in sql


def test_the_range_bound_stops_at_the_source_boundary():
    # ':' is 0x3A and ';' is 0x3B, so the half-open range covers exactly `idb:*` and
    # cannot reach a neighbouring source id such as `idb_extra:...`.
    sql = _stmts(_rows("idb", 10), fts_range_source="idb")
    assert "'idb:'" in sql and "'idb;'" in sql
    assert ord(";") == ord(":") + 1


def test_quotes_in_a_source_name_cannot_break_out():
    sql = _stmts(_rows("o'x", 3), fts_range_source="o'x")
    assert "'o''x:'" in sql and "'o''x;'" in sql


# ---------------------------------------------------------------------------------------
# THE DECISION, not the emitter. Every test above hands `fts_range_source` straight to
# emit_sql, so all five prove what emit_sql does when told the set is whole - and NOTHING
# tested who decides that. The decision lived in main() as
# `all(r["source_id"] == a.source for r in grp)`, which is true of any SUBSET, and by the
# time it ran the diff had already reduced `rows` to the changed ones. Measured on the real
# state: 105 new cbs_nl ids to send, 5,154 unchanged and dropped by the diff, one range
# DELETE emitted, 105 re-inserted - 5,049 series silently unlisted (R658).
from core.sync_catalog_d1 import whole_source_reconcile  # noqa: E402


def test_the_range_form_is_REFUSED_when_the_diff_dropped_rows():
    """The bug, in one line. A homogeneous slice is not a whole source."""
    rows = _rows("cbs_nl", 105)
    assert whole_source_reconcile("cbs_nl", rows, skipped_by_diff=5154) is None
    assert whole_source_reconcile("cbs_nl", rows, skipped_by_diff=1) is None


def test_the_range_form_is_offered_when_nothing_was_dropped():
    rows = _rows("cbs_nl", 105)
    assert whole_source_reconcile("cbs_nl", rows, skipped_by_diff=0) == "cbs_nl"


def test_no_source_no_range_form():
    """The pending-queue path passes no --source and can mix sources."""
    assert whole_source_reconcile(None, _rows("cbs_nl", 10), 0) is None
    assert whole_source_reconcile("", _rows("cbs_nl", 10), 0) is None


def test_a_mixed_group_is_refused_even_with_nothing_dropped():
    rows = _rows("cbs_nl", 5) + _rows("gus_dbw", 5)
    assert whole_source_reconcile("cbs_nl", rows, 0) is None


def test_a_sharded_source_is_refused():
    """A source split across two D1 databases has only part of itself in each group, and a
    range predicate inside one database is still a claim about the whole source."""
    rows = _rows("cbs_nl", 105)
    assert whole_source_reconcile("cbs_nl", rows, 0, n_groups=2) is None
    assert whole_source_reconcile("cbs_nl", rows, 0, n_groups=1) == "cbs_nl"


def test_an_empty_group_is_refused():
    assert whole_source_reconcile("cbs_nl", [], 0) is None


def test_main_ASKS_the_function_rather_than_re_deciding():
    """A guard that main() does not call is not a guard. The previous decision was inline,
    so extracting it is only an improvement if the call site actually uses it."""
    import inspect
    from core import sync_catalog_d1 as m
    src = inspect.getsource(m.main)
    assert "whole_source_reconcile(" in src, "main() no longer asks the guard"
    assert 'all(r.get("source_id") == a.source for r in grp)' not in src, \
        "the old inline homogeneity test is back in main()"
