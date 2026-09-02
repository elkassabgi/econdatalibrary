"""derive_one may now refresh a served series, and may not shrink one silently.

Two defects, both found while sizing the gus_dbw repair.

CREATE-ONLY. `head_object` succeeding short-circuited to `SKIP ... (already present)`
unconditionally, so the tool that exists to derive one series to R2 could not REFRESH one -
and the 16 gus_dbw areas serving a nine-day-old vintage could not be repaired with it at all.

NO MIRROR PREFLIGHT. It then PUT whatever it had just built over whatever was there. A
reviewer measured `gus_dbw:area_8` as 20,015 bytes SMALLER locally than its served copy, so a
refresh would have published a regression and printed OK - R383's shape, and precisely what
mirror_sync's never-shrink rule exists to stop in the other direction.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.derive_one import SHRINK_TOLERANCE, _existing_rows, shrink_verdict  # noqa: E402


def head(size=None, rows=None):
    h = {}
    if size is not None:
        h["ContentLength"] = size
    if rows is not None:
        h["Metadata"] = {"rows": str(rows)}
    return h


def test_a_first_upload_cannot_shrink_anything():
    ok, why = shrink_verdict(100, 1000, None)
    assert ok and "no existing object" in why


def test_fewer_rows_than_the_served_copy_is_REFUSED():
    ok, why = shrink_verdict(90, 900, head(size=1000, rows=100))
    assert not ok
    assert "10 fewer" in why, why


def test_equal_or_more_rows_is_allowed():
    assert shrink_verdict(100, 900, head(size=1000, rows=100))[0]
    assert shrink_verdict(101, 900, head(size=1000, rows=100))[0]


def test_rows_beat_bytes_when_both_are_known():
    """A gzip that compresses better is not a loss. With row counts on both sides the byte
    comparison must not be consulted at all, or a better-compressed refresh gets refused."""
    ok, why = shrink_verdict(200, 10, head(size=1_000_000, rows=100))
    assert ok, why
    assert "rows" in why and "%" not in why


def test_a_served_copy_with_no_row_count_falls_back_to_bytes():
    """Every object uploaded before today. gus_dbw:area_8's real numbers."""
    ok, why = shrink_verdict(None, 1_000_000 - 20_015, head(size=1_000_000))
    assert not ok
    assert "2.00% smaller" in why, why


def test_a_byte_drop_inside_the_tolerance_passes():
    """gzip over nearly-identical CSV is stable to well under a percent, so a hair's
    difference must not block an otherwise good refresh."""
    ok, _why = shrink_verdict(None, int(1_000_000 * (1 - SHRINK_TOLERANCE / 2)),
                              head(size=1_000_000))
    assert ok


def test_nothing_comparable_means_no_opinion():
    assert shrink_verdict(None, None, head(size=1000))[0]
    assert shrink_verdict(None, 500, head())[0]


def test_the_row_metadata_is_read_from_either_header_form():
    assert _existing_rows({"Metadata": {"rows": "7"}}) == 7
    assert _existing_rows({"Metadata": {"x-amz-meta-rows": "7"}}) == 7
    assert _existing_rows({"Metadata": {"rows": "not a number"}}) is None
    assert _existing_rows({}) is None
    assert _existing_rows(None) is None


def test_the_refresh_flag_exists_and_the_default_still_skips():
    """A --force that main() does not read is not a flag, and changing the DEFAULT would
    silently make every existing caller re-derive its whole queue."""
    import inspect

    import tools.derive_one as m
    src = inspect.getsource(m.main)
    assert '"--force" in sys.argv' in src, "main() does not read --force"
    assert "if head and not force:" in src, "the skip is no longer conditional on --force"


def test_the_put_helper_carries_metadata_to_boto3():
    """The row count is only exact for FUTURE comparisons if it actually reaches the object."""
    import inspect

    from core import derive_csv as d
    sig = inspect.signature(d._put_gzip_file_with_backoff)
    assert "metadata" in sig.parameters, sig
    src = inspect.getsource(d._put_gzip_file_with_backoff)
    assert "Metadata=(metadata or {})" in src, "metadata never reaches put_object"
