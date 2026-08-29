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
