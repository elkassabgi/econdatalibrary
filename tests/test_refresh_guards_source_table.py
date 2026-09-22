"""The refresh superset guard must cover BOTH tables a source lives in, not just `series`.

`tools/refresh_r2_catalog.py` has guarded per-source `series` counts since 2026-08-02. It did not
guard the `source` METADATA table, so an upload could delete rows carrying a source's name,
attribution, homepage and licence id and print nothing at all.

One did. The 2026-09-22 refresh took `source` from 349 rows to 328 - a loss of 21 - and the tool's
output never mentioned it; it was found afterwards by an independent per-table diff of the old and
new R2 objects. That upload was benign, because all 21 were ids we withhold, but the guard could
not have told the difference: a hosted source would have lost its metadata just as quietly, while
its series stayed put and every count the tool printed stayed green.

This is the R709 / R1061 shape again - a check that stops one table short of the set, and reads as
full cover because the half it does check is the half that usually moves.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

refresh = pytest.importorskip("tools.refresh_r2_catalog")


def _db(tmp_path, name, rows):
    """A minimal catalogue: just the `source` table the guard reads."""
    p = os.path.join(str(tmp_path), name)
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE source (source_id, name)")
    con.executemany("INSERT INTO source VALUES (?, ?)", rows)
    con.commit()
    con.close()
    return p


def test_source_row_ids_reads_the_source_table(tmp_path):
    p = _db(tmp_path, "a.db", [("worldbank", "World Bank"), ("imf_commodity", "IMF")])
    assert refresh.source_row_ids(p) == {"worldbank", "imf_commodity"}


def test_source_row_ids_drops_torn_page_phantoms(tmp_path):
    """The series side already excludes non-text ids; this side must agree, or a corrupt R2 copy
    makes every future upload look like it deletes a source."""
    p = _db(tmp_path, "b.db", [("worldbank", "World Bank"), (b"\x00\x11rawpage", "phantom")])
    assert refresh.source_row_ids(p) == {"worldbank"}


def test_an_undeclared_loss_blocks_and_a_declared_one_does_not():
    old, new = {"worldbank", "imf_commodity", "oecd"}, {"worldbank"}
    # planted positive: with nothing declared, BOTH losses must be reported
    assert refresh.blocking_losses(old, new, set()) == ["imf_commodity", "oecd"]
    # declaring one leaves the other blocking - a partial declaration must not wave the rest through
    assert refresh.blocking_losses(old, new, {"oecd"}) == ["imf_commodity"]
    # declaring both clears it
    assert refresh.blocking_losses(old, new, {"oecd", "imf_commodity"}) == []


def test_negative_control_a_clean_superset_blocks_nothing():
    """If this ever returns something, the test above proves nothing."""
    old, new = {"worldbank"}, {"worldbank", "imf_commodity"}
    assert refresh.blocking_losses(old, new, set()) == []


def test_the_guard_is_actually_wired_into_the_upload_path():
    """The functions above can be perfect while main() never calls them - which is precisely the
    state this file was written to end. Pin the call site, not just the helper."""
    src = open(os.path.join(ROOT, "tools", "refresh_r2_catalog.py"), encoding="utf-8").read()
    assert "source_row_ids(cur_db)" in src, "main() no longer compares the R2 copy's source rows"
    assert "blocking_losses(" in src, "main() no longer computes which losses block"
    # and it must still be able to REFUSE, not merely narrate
    guard = src[src.index("old_src, new_src"):]
    assert "return 2" in guard, "the source-row guard no longer aborts the upload"
