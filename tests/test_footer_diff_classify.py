"""footer_diff must compare max observation date, not row count alone.

Its docstring has always said "row count plus max obs date is the only comparison that answers
the question asked". Its `_meta` returned `m.num_rows` and nothing else, so every verdict it
ever produced was a row-count verdict — including the `--all` run on 2026-09-01 that reported
0 files behind across 322 sources, after which a content fingerprint found fed_board differing
on 11 of 36 objects and fhfa on 2 of 18, every one at an IDENTICAL row count (R555).

`classify` is the whole decision, so it is tested directly: a file gaining observations without
gaining rows must read BEHIND, and a file diverging on the two axes must land in the merge
queue rather than the sync-down list.
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.footer_diff import classify, file_meta  # noqa: E402


def test_equal_rows_but_a_later_date_is_BEHIND():
    """The fed_board shape: same count, newer coverage upstream."""
    assert classify((264, "2026-08-05"), (264, "2026-08-28")) == "behind"


def test_equal_rows_and_equal_date_is_SAME():
    assert classify((264, "2026-08-28"), (264, "2026-08-28")) == "same"


def test_fewer_rows_is_still_BEHIND():
    assert classify((100, "2026-01-01"), (120, "2026-01-01")) == "behind"


def test_more_rows_locally_is_AHEAD():
    assert classify((120, "2026-01-01"), (100, "2026-01-01")) == "ahead"


def test_equal_rows_but_an_EARLIER_remote_date_is_AHEAD():
    assert classify((264, "2026-08-28"), (264, "2026-08-05")) == "ahead"


def test_DIVERGED_goes_to_the_merge_queue_not_the_sync_list():
    """More rows locally but a later date on R2: copying either way loses something, so it
    must be filed as 'ahead', which mirror_sync never overwrites."""
    assert classify((300, "2026-01-01"), (200, "2026-06-01")) == "ahead"


def test_a_dateless_schema_falls_back_to_rows_only():
    assert classify((264, None), (264, None)) == "same"
    assert classify((264, None), (300, None)) == "behind"


def test_file_meta_reads_rows_AND_max_date_from_the_footer(tmp_path):
    t = pa.table({"series_key": pa.array(["a", "b", "c"]),
                  "obs_date": pa.array(["2025-01-01", "2026-08-28", "2024-01-01"]),
                  "obs_value": pa.array([1.0, 2.0, 3.0])})
    p = str(tmp_path / "m.parquet")
    pq.write_table(t, p)
    rows, mx = file_meta(p)
    assert rows == 3
    assert mx == "2026-08-28", f"max obs date not read from the footer statistics: {mx!r}"


def test_file_meta_returns_None_for_a_schema_with_no_date_column(tmp_path):
    t = pa.table({"series_key": pa.array(["a"]), "LegalName": pa.array(["x"])})
    p = str(tmp_path / "n.parquet")
    pq.write_table(t, p)
    rows, mx = file_meta(p)
    assert (rows, mx) == (1, None)
