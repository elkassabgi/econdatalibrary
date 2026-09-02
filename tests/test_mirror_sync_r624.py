"""Round-5 additions: the cases R624 measured, pinned so they cannot come back.

Every schema here is one the reviewer took off the live mirror: 409 files / 103,679,078 rows
carry a timestamp whose TIME is the grain (edgar_insider 405, insee_sirene 4), and the keyed
files that a retired column refused for ever are the ordinary (series_key, obs_date) shape.
"""
import datetime as dt
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.mirror_sync import IdentityCheckFailed, lost_identities  # noqa: E402


def _w(path, table):
    pq.write_table(table, path)
    return str(path)


def test_whole_row_keeps_the_time_of_day(tmp_path):
    """R624 MAJOR 1: every temporal column was collapsed to a DATE, so two filings at 09:30 and
    16:00 replaced by two at 02:00 and 03:00 the same day compared EQUAL.

    R628: the accessions are held FIXED and only the TIME moves. The first version of this test
    moved both, so the rows differed on a non-temporal column and the old ::DATE normaliser
    returned 2 as well - it asserted the right number for the wrong reason."""
    ts = lambda h: dt.datetime(2026, 3, 4, h, 30)          # noqa: E731
    a = _w(tmp_path / "a.parquet", pa.table({
        "accession": ["x1", "x2"],
        "filed_at": pa.array([ts(9), ts(16)], pa.timestamp("us"))}))
    b = _w(tmp_path / "b.parquet", pa.table({
        "accession": ["x1", "x2"],
        "filed_at": pa.array([ts(2), ts(3)], pa.timestamp("us"))}))
    assert lost_identities(a, b)[0] == 2
    # the same filings at the same times are not a loss
    c = _w(tmp_path / "c.parquet", pa.table({
        "accession": ["x1", "x2"],
        "filed_at": pa.array([ts(9), ts(16)], pa.timestamp("us"))}))
    assert lost_identities(a, c)[0] == 0


def test_the_temporal_comparison_does_not_depend_on_which_side_is_which(tmp_path):
    """R628 MEDIUM 4: _norm consulted the other side's type for numbers and not for temporals,
    so a local VARCHAR date against an incoming TIMESTAMP counted 1 lost and the same pair with
    the roles swapped counted 0. Zero observations are lost in either direction."""
    txt = _w(tmp_path / "txt.parquet", pa.table({"k": ["r1"], "when": ["2026-03-04"]}))
    stamp = _w(tmp_path / "stamp.parquet", pa.table({
        "k": ["r1"], "when": pa.array([dt.datetime(2026, 3, 4, 0, 0)], pa.timestamp("us"))}))
    assert lost_identities(txt, stamp)[0] == lost_identities(stamp, txt)[0] == 0


def test_a_TIME_column_compares_instead_of_failing_the_check(tmp_path):
    """R628 MINOR: TIME matched the TIMESTAMP branch and DuckDB has no TIME->TIMESTAMP cast, so
    any such file was a permanent CHECK FAILED."""
    a = _w(tmp_path / "a.parquet", pa.table({
        "k": ["r1"], "at": pa.array([dt.time(9, 30)], pa.time64("us"))}))
    b = _w(tmp_path / "b.parquet", pa.table({
        "k": ["r1"], "at": pa.array([dt.time(16, 0)], pa.time64("us"))}))
    assert lost_identities(a, b)[0] == 1
    assert lost_identities(a, a)[0] == 0


def test_a_date_still_reconciles_against_a_timestamp(tmp_path):
    """The DATE cast existed to reconcile a publisher writing DATE where we hold TIMESTAMP.
    TIMESTAMP does that too, without discarding the time."""
    a = _w(tmp_path / "a.parquet", pa.table({
        "k": ["r1"], "when": pa.array([dt.date(2026, 3, 4)], pa.date32())}))
    b = _w(tmp_path / "b.parquet", pa.table({
        "k": ["r1"], "when": pa.array([dt.datetime(2026, 3, 4, 0, 0)], pa.timestamp("us"))}))
    assert lost_identities(a, b)[0] == 0


def test_a_retired_unrelated_column_does_not_refuse_a_keyed_file_for_ever(tmp_path):
    """R624 MAJOR 2: the requirement named EVERY column, so a publisher retiring or renaming an
    unrelated one refused the file permanently - nothing clears it and the same schema arrives
    every pass. Zero observations are lost in any of these three."""
    base = {"series_key": ["k1"], "obs_date": pa.array([dt.date(2020, 1, 1)], pa.date32()),
            "value": [1.0]}
    a = _w(tmp_path / "a.parquet", pa.table({**base, "status_flag": ["P"]}))
    b = _w(tmp_path / "b.parquet", pa.table(base))                       # retired
    c = _w(tmp_path / "c.parquet", pa.table({**base, "status": ["P"]}))  # renamed
    d = _w(tmp_path / "d.parquet", pa.table({**base, "STATUS_FLAG": ["P"]}))  # recased
    for other in (b, c, d):
        assert lost_identities(a, other)[0] == 0


def test_a_missing_IDENTITY_column_still_refuses(tmp_path):
    a = _w(tmp_path / "a.parquet", pa.table({
        "series_key": ["k1"], "obs_date": pa.array([dt.date(2020, 1, 1)], pa.date32()),
        "value": [1.0]}))
    b = _w(tmp_path / "b.parquet", pa.table({"series_key": ["k1"], "value": [1.0]}))
    with pytest.raises(IdentityCheckFailed):
        lost_identities(a, b)


def test_large_integers_compare_exactly_when_both_sides_are_integers(tmp_path):
    """R624 MEDIUM 4: routing integers through DOUBLE lost everything past 2^53."""
    a = _w(tmp_path / "a.parquet", pa.table({
        "id": pa.array([9007199254740993], pa.int64()), "t": ["x"]}))
    b = _w(tmp_path / "b.parquet", pa.table({
        "id": pa.array([9007199254740992], pa.int64()), "t": ["x"]}))
    assert lost_identities(a, b)[0] == 1


def test_an_integer_that_became_a_float_still_reconciles(tmp_path):
    """The other half of the same trade: 1 and 1.0 must not read as a lost row."""
    a = _w(tmp_path / "a.parquet", pa.table({"n": pa.array([1], pa.int64()), "t": ["x"]}))
    b = _w(tmp_path / "b.parquet", pa.table({"n": pa.array([1.0], pa.float64()), "t": ["x"]}))
    assert lost_identities(a, b)[0] == 0


def test_a_list_column_is_compared_as_text_not_cast_to_a_number(tmp_path):
    """R624 MEDIUM 5: the type test matched a SUBSTRING of the rendered type, so INTEGER[] took
    the numeric branch and raised, and STRUCT(REAL_GDP VARCHAR) took it on a FIELD NAME."""
    a = _w(tmp_path / "a.parquet", pa.table({
        "xs": pa.array([[1, 2]], pa.list_(pa.int64())), "t": ["x"]}))
    b = _w(tmp_path / "b.parquet", pa.table({
        "xs": pa.array([[1, 2]], pa.list_(pa.int64())), "t": ["x"]}))
    assert lost_identities(a, b)[0] == 0
    c = _w(tmp_path / "c.parquet", pa.table({
        "xs": pa.array([[3, 4]], pa.list_(pa.int64())), "t": ["x"]}))
    assert lost_identities(a, c)[0] == 1


def test_a_struct_whose_field_is_named_like_a_number_is_not_cast(tmp_path):
    fields = pa.struct([("REAL_GDP", pa.string())])
    a = _w(tmp_path / "a.parquet", pa.table({
        "s": pa.array([{"REAL_GDP": "a"}], fields), "t": ["x"]}))
    b = _w(tmp_path / "b.parquet", pa.table({
        "s": pa.array([{"REAL_GDP": "b"}], fields), "t": ["x"]}))
    assert lost_identities(a, b)[0] == 1


def test_blobs_compare_by_bytes_not_by_their_rendering(tmp_path):
    a = _w(tmp_path / "a.parquet", pa.table({
        "b": pa.array([b"\x00"], pa.binary()), "t": ["x"]}))
    b = _w(tmp_path / "b.parquet", pa.table({
        "b": pa.array([b"\\x00"], pa.binary()), "t": ["x"]}))
    assert lost_identities(a, b)[0] == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
