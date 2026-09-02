"""The mirror-sync containment check must be right in BOTH directions.

Both failures are recorded, not hypothetical:

  * it must SEE a genuinely dropped observation — never-shrink on a row COUNT does not, because
    a merge that adds rows to one family and drops another passes a count test (R549 F5);
  * it must NOT invent one. Guessing `cols[1]` as the date column made gleif's `LegalName` and
    defillama's `name` into a time axis, so a RENAME read as 6,817 lost observations and three
    files were refused with zero identities actually lost (R551).
"""
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.mirror_sync import lost_identities  # noqa: E402


def _dated(path, pairs):
    t = pa.table({"series_key": pa.array([k for k, _d, _v in pairs]),
                  "obs_date": pa.array([d for _k, d, _v in pairs]),
                  "obs_value": pa.array([v for _k, _d, v in pairs])})
    pq.write_table(t, path)
    return path


def _named(path, rows):
    """The gleif/defillama shape: an identity column and a NAME, with no time axis at all."""
    t = pa.table({"series_key": pa.array([k for k, _n in rows]),
                  "LegalName": pa.array([n for _k, n in rows])})
    pq.write_table(t, path)
    return path


def test_a_dropped_observation_is_detected(tmp_path):
    a = _dated(str(tmp_path / "a.parquet"),
               [("K1", "2025-01-01", 1.0), ("K2", "2025-01-01", 2.0)])
    b = _dated(str(tmp_path / "b.parquet"),
               [("K1", "2025-01-01", 1.0)])
    n, mode = lost_identities(a, b)
    assert n == 1, f"a dropped (key,date) pair was not detected: {n}"
    assert "obs_date" in mode


def test_added_rows_are_not_counted_as_losses(tmp_path):
    a = _dated(str(tmp_path / "c.parquet"), [("K1", "2025-01-01", 1.0)])
    b = _dated(str(tmp_path / "d.parquet"),
               [("K1", "2025-01-01", 1.0), ("K2", "2026-01-01", 2.0)])
    n, _ = lost_identities(a, b)
    assert n == 0, "growth was miscounted as loss"


def test_a_REVISED_VALUE_is_not_a_loss(tmp_path):
    """Values change on revision; the identity survives. Containment is about identities."""
    a = _dated(str(tmp_path / "e.parquet"), [("K1", "2025-01-01", 2.9)])
    b = _dated(str(tmp_path / "f.parquet"), [("K1", "2025-01-01", 3.5)])
    n, _ = lost_identities(a, b)
    assert n == 0, "a revised value was counted as a lost observation"


def test_a_RENAME_in_a_dateless_schema_is_NOT_a_loss(tmp_path):
    """The gleif case. Comparing on (key, LegalName) reported 6,817 losses with 0 LEIs gone."""
    a = _named(str(tmp_path / "g.parquet"), [("LEI1", "OLD NAME"), ("LEI2", "STABLE")])
    b = _named(str(tmp_path / "h.parquet"), [("LEI1", "NEW NAME"), ("LEI2", "STABLE")])
    n, mode = lost_identities(a, b)
    assert n == 0, f"a rename was counted as {n} lost identities — the R551 false refusal"
    assert "key-only" in mode, f"expected key-only comparison for a dateless schema: {mode}"


def test_a_dateless_schema_still_detects_a_REAL_disappearance(tmp_path):
    a = _named(str(tmp_path / "i.parquet"), [("LEI1", "X"), ("LEI2", "Y")])
    b = _named(str(tmp_path / "j.parquet"), [("LEI1", "X")])
    n, _ = lost_identities(a, b)
    assert n == 1, "a genuinely removed entity was missed by the key-only comparison"


def test_identical_files_report_nothing(tmp_path):
    pairs = [("K1", "2025-01-01", 1.0), ("K2", "2025-06-30", 2.0)]
    a = _dated(str(tmp_path / "k.parquet"), pairs)
    b = _dated(str(tmp_path / "l.parquet"), pairs)
    assert lost_identities(a, b)[0] == 0


def test_a_lost_COPY_of_a_duplicated_pair_is_detected(tmp_path):
    """R571: cbs_nl keys repeat (R573). A set test saw 0 when the incoming file dropped copies;
    the copy-aware count is max(local copies - incoming copies, 0) per identity."""
    a = _dated(str(tmp_path / "a.parquet"),
               [("K1", "2025-07-01", 10.0), ("K1", "2025-07-01", 15.0), ("K2", "2025-01-01", 2.0)])
    b = _dated(str(tmp_path / "b.parquet"),
               [("K1", "2025-07-01", 10.0), ("K2", "2025-01-01", 2.0)])
    n, mode = lost_identities(a, b)
    assert n == 1, f"a dropped copy of a duplicated (key,date) pair was not detected: {n}"
    assert "copy-aware" in mode
    # more copies incoming is not a loss
    assert lost_identities(b, a)[0] == 0


def test_copy_aware_equals_the_set_count_when_keys_are_unique(tmp_path):
    a = _dated(str(tmp_path / "u.parquet"),
               [("K1", "2025-01-01", 1.0), ("K2", "2025-01-01", 2.0), ("K3", "2025-01-01", 3.0)])
    b = _dated(str(tmp_path / "v.parquet"), [("K3", "2025-01-01", 3.0)])
    assert lost_identities(a, b)[0] == 2


import datetime as dt  # noqa: E402


def test_NULL_identities_are_not_losses_when_both_sides_hold_them(tmp_path):
    """R595: a USING join never matches NULL, so three real fdic files compared unequal to
    themselves (history.parquet: 14,058 'lost'). IS NOT DISTINCT FROM makes NULL = NULL."""
    import pyarrow as pa, pyarrow.parquet as pq
    a = tmp_path / "a.parquet"; b = tmp_path / "b.parquet"
    t = pa.table({"series_key": pa.array([None, None, "k1"], pa.string()),
                  "obs_date": pa.array([None, None, dt.date(2020, 1, 1)], pa.date32()),
                  "value": pa.array([1.0, 2.0, 3.0])})
    pq.write_table(t, a); pq.write_table(t, b)
    assert lost_identities(str(a), str(b))[0] == 0
    # a lost NULL copy is still a loss
    t2 = pa.table({"series_key": pa.array([None, "k1"], pa.string()),
                   "obs_date": pa.array([None, dt.date(2020, 1, 1)], pa.date32()),
                   "value": pa.array([1.0, 3.0])})
    pq.write_table(t2, b)
    assert lost_identities(str(a), str(b))[0] == 1


def test_DATE_vs_TIMESTAMP_drift_is_not_a_loss(tmp_path):
    """R595: both sides compared as DATE when the column is temporal."""
    import pyarrow as pa, pyarrow.parquet as pq
    a = tmp_path / "a.parquet"; b = tmp_path / "b.parquet"
    pq.write_table(pa.table({"series_key": ["k1", "k2"], "obs_date": pa.array([dt.date(2020, 1, 1), dt.date(2020, 2, 1)], pa.date32()),
                             "value": [1.0, 2.0]}), a)
    pq.write_table(pa.table({"series_key": ["k1", "k2"],
                             "obs_date": pa.array([dt.datetime(2020, 1, 1), dt.datetime(2020, 2, 1)], pa.timestamp("us")),
                             "value": [1.0, 2.0]}), b)
    assert lost_identities(str(a), str(b))[0] == 0


def test_unique_identities_take_the_set_path_and_agree_with_the_copy_aware_count(tmp_path):
    import pyarrow as pa, pyarrow.parquet as pq
    a = tmp_path / "a.parquet"; b = tmp_path / "b.parquet"
    pq.write_table(pa.table({"series_key": ["k1", "k2", "k3"], "obs_date": pa.array([dt.date(2020, 1, 1)] * 3, pa.date32()), "value": [1.0, 2.0, 3.0]}), a)
    pq.write_table(pa.table({"series_key": ["k1", "k3"], "obs_date": pa.array([dt.date(2020, 1, 1)] * 2, pa.date32()), "value": [1.0, 3.0]}), b)
    n, mode = lost_identities(str(a), str(b))
    assert n == 1 and "set path" in mode


def test_tz_aware_TIMESTAMP_vs_DATE_is_not_a_loss_whatever_the_host_zone(tmp_path):
    """R605: the cast to DATE runs in UTC on the connection, so a UTC-midnight timestamp and its
    date agree on every host; and the incoming file's type is inspected, not only the local one."""
    import pyarrow as pa, pyarrow.parquet as pq
    a = tmp_path / "a.parquet"; b = tmp_path / "b.parquet"
    pq.write_table(pa.table({"series_key": ["k1", "k2"], "obs_date": pa.array([dt.date(2020, 1, 1), dt.date(2020, 2, 1)], pa.date32()),
                             "value": [1.0, 2.0]}), a)
    ts = pa.array([dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc), dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc)],
                  pa.timestamp("us", tz="UTC"))
    pq.write_table(pa.table({"series_key": ["k1", "k2"], "obs_date": ts, "value": [1.0, 2.0]}), b)
    assert lost_identities(str(a), str(b))[0] == 0
    # the drift the other way round: local VARCHAR-ISO dates vs an incoming TIMESTAMP
    pq.write_table(pa.table({"series_key": ["k1", "k2"], "obs_date": ["2020-01-01", "2020-02-01"], "value": [1.0, 2.0]}), a)
    assert lost_identities(str(a), str(b))[0] == 0


def test_a_failing_identity_check_raises_instead_of_pretending(tmp_path):
    """R605: a missing column in the incoming file must surface as a CHECK failure (the caller
    keeps the local copy and records it), never as 0 lost."""
    import pyarrow as pa, pyarrow.parquet as pq
    a = tmp_path / "a.parquet"; b = tmp_path / "b.parquet"
    pq.write_table(pa.table({"series_key": ["k1"], "obs_date": pa.array([dt.date(2020, 1, 1)], pa.date32()), "value": [1.0]}), a)
    pq.write_table(pa.table({"series_key": ["k1"], "value": [1.0]}), b)
    import pytest
    with pytest.raises(Exception):
        lost_identities(str(a), str(b))


def test_a_schema_with_no_key_column_is_compared_on_EVERY_column(tmp_path):
    """R617. The identity used to fall back to `cols[0]`, untested. On the live mirror that
    meant 1,598 files and 827,032,326 rows across 12 sources compared on whatever column
    happened to be first: cepii_baci on `year` (89,207,221 rows, ~25 distinct values), cftc on
    the float measure ' Total Reportable Positions-Long (All)', edgar_pointers on `cik` at 28.3
    rows per value. Every row of a file could be replaced and the check returned 0 lost, after
    which the local copy was gone and the ledger was deleted for having nothing to report.
    With no key column the identity is the WHOLE ROW."""
    import pyarrow as pa, pyarrow.parquet as pq
    a = tmp_path / "a.parquet"
    pq.write_table(pa.table({"CERT": [1, 2], "value": [1.0, 2.0]}), a)
    n, mode = lost_identities(str(a), str(a))
    assert n == 0 and "WHOLE ROW" in mode and "POSITIONAL" not in mode


def test_every_trade_flow_replaced_is_counted_not_reported_as_zero(tmp_path):
    """cepii_baci/baci_hs17.parquet: `year` first, no recognised date column."""
    import pyarrow as pa, pyarrow.parquet as pq
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(pa.table({"year": pa.array([2020] * 6, pa.int32()),
                             "exporter": ["FRA", "DEU", "ITA", "ESP", "USA", "CHN"],
                             "importer": ["USA"] * 6, "product": ["010121"] * 6,
                             "value": [1.0] * 6, "quantity": [1.0] * 6}), a)
    pq.write_table(pa.table({"year": pa.array([2020] * 6, pa.int32()),
                             "exporter": ["BRA", "IND", "JPN", "KOR", "MEX", "CAN"],
                             "importer": ["USA"] * 6, "product": ["999999"] * 6,
                             "value": [9.0] * 6, "quantity": [9.0] * 6}), b)
    n, _ = lost_identities(str(a), str(b))
    assert n == 6


def test_a_measure_column_is_never_the_identity(tmp_path):
    """cftc/cot_all.parquet: the first column is a float measurement, 625,856 rows over
    193,376 distinct values. Every weekly report here moves to a different market and date."""
    import pyarrow as pa, pyarrow.parquet as pq
    c0 = " Total Reportable Positions-Long (All)"
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(pa.table({c0: [100.0, 200.0, 300.0],
                             "Report_Date_as_YYYY-MM-DD": ["2025-01-01", "2025-01-08", "2025-01-15"],
                             "Market_and_Exchange_Names": ["WHEAT"] * 3}), a)
    pq.write_table(pa.table({c0: [100.0, 200.0, 300.0],
                             "Report_Date_as_YYYY-MM-DD": ["2099-01-01", "2099-01-08", "2099-01-15"],
                             "Market_and_Exchange_Names": ["CORN"] * 3}), b)
    n, _ = lost_identities(str(a), str(b))
    assert n == 3


def test_replacing_every_accession_under_an_unchanged_cik_is_counted(tmp_path):
    """edgar_pointers: 106,754 rows over 3,777 distinct cik in one real shard."""
    import pyarrow as pa, pyarrow.parquet as pq
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(pa.table({"cik": ["1"] * 5 + ["2"] * 5,
                             "accession": [f"acc{i}" for i in range(10)]}), a)
    pq.write_table(pa.table({"cik": ["1"] * 5 + ["2"] * 5, "accession": ["zzz"] * 10}), b)
    n, _ = lost_identities(str(a), str(b))
    assert n == 10


def test_whole_row_mode_does_not_invent_losses_from_number_formatting(tmp_path):
    """-0.0 against 0.0, and INT 1 against DOUBLE 1.0, each reported one lost row before."""
    import pyarrow as pa, pyarrow.parquet as pq
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(pa.table({"cert": [1, 2], "v": pa.array([-0.0, 1.0], pa.float64())}), a)
    pq.write_table(pa.table({"cert": [1, 2], "v": pa.array([0.0, 1.0], pa.float64())}), b)
    assert lost_identities(str(a), str(b))[0] == 0
    c, d = tmp_path / "c.parquet", tmp_path / "d.parquet"
    pq.write_table(pa.table({"cert": [1], "v": pa.array([1], pa.int64())}), c)
    pq.write_table(pa.table({"cert": [1], "v": pa.array([1.0], pa.float64())}), d)
    assert lost_identities(str(c), str(d))[0] == 0


def test_whole_row_mode_is_copy_aware_like_the_keyed_path(tmp_path):
    import pyarrow as pa, pyarrow.parquet as pq
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(pa.table({"cik": ["1", "1", "1"], "accession": ["x", "x", "y"]}), a)
    pq.write_table(pa.table({"cik": ["1", "1"], "accession": ["x", "y"]}), b)
    assert lost_identities(str(a), str(b))[0] == 1


def test_the_key_and_date_columns_are_found_whatever_their_case(tmp_path):
    """The lookup was case-sensitive, so a file naming its columns Series_Key / Obs_Date fell
    to the positional path even though it HAS an identity."""
    import datetime as _dt
    import pyarrow as pa, pyarrow.parquet as pq
    a = tmp_path / "a.parquet"
    pq.write_table(pa.table({"Series_Key": ["k1"],
                             "Obs_Date": pa.array([_dt.date(2020, 1, 1)], pa.date32()),
                             "Value": [1.0]}), a)
    _, mode = lost_identities(str(a), str(a))
    assert "Series_Key" in mode and "WHOLE ROW" not in mode


# What the operator actually SEES is asserted in tests/test_mirror_sync_output.py, which runs
# sync_source with only the network boundary stubbed and reads its stdout. The version that
# lived here grepped inspect.getsource() for two substrings and would have passed with the
# print unreachable - the reviewer's point, and it was right.


def test_a_stale_classification_is_refused(tmp_path, monkeypatch, capsys):
    """R617: the docstring promised this refusal and no such code existed; the JSON on disk
    was 30 hours old and would have been consumed silently."""
    import json as _json
    import sys as _sys
    from tools import mirror_sync
    p = tmp_path / "diff.json"
    p.write_text(_json.dumps({"sources": []}), encoding="utf-8")
    old = time.time() - 40 * 3600
    os.utime(p, (old, old))     # Windows rejects an atime of 0
    monkeypatch.setattr(_sys, "argv", ["mirror_sync.py", "--from-json", str(p)])
    assert mirror_sync.main() == 2
    out = capsys.readouterr().out
    assert "REFUSED" in out and "footer_diff" in out
    os.utime(p, None)
    monkeypatch.setattr(_sys, "argv", ["mirror_sync.py", "--from-json", str(p)])
    assert mirror_sync.main() == 0
