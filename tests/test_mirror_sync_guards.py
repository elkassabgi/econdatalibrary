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
