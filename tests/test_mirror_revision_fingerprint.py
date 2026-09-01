"""The mirror guard must detect a publisher REVISION and ignore a re-encode.

R549: three eurostat flows were served at a superseded vintage — TEC00115 (real GDP growth)
had 11 revised values with identical byte size, identical row count and identical max
observation date. Every shape-based test cleared it. Meanwhile the desktop writes parquet with
pyarrow 23.0.0 and CI with 25.0.1, so byte/md5 comparison flags a pure re-encode of identical
data as a difference (59 such false positives in that same population).

So the instrument has to satisfy BOTH properties at once, which is what these tests pin:
  * a changed VALUE changes the fingerprint, even when rows and max date are unchanged;
  * a different ENCODING of the same values does not.

These call the shipped `content_fingerprint_sql` rather than restating the rule, because a
test that re-types the logic passes while production is broken (R547).
"""
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.derive_csv import content_fingerprint_sql  # noqa: E402

KEYS = [f"freq=A:geo=DK:idx={i}" for i in range(6)]
DATES = ["2025-12-31"] * 6
VALUES = [2.9, 3.1, 1.4, 1.1, 5.0, 7.25]


def _write(path, values, **kw):
    t = pa.table({"series_key": pa.array(KEYS), "obs_date": pa.array(DATES),
                  "obs_value": pa.array(values)})
    pq.write_table(t, path, **kw)
    return path


def _fp(path):
    q = duckdb.connect()
    p = str(path).replace(os.sep, "/")
    cols = [r[0] for r in q.execute(f"describe select * from read_parquet('{p}')").fetchall()]
    return q.execute(content_fingerprint_sql(cols, p)).fetchone()[0]


def _shape(path):
    q = duckdb.connect()
    p = str(path).replace(os.sep, "/")
    return q.execute(f"select count(*), max(obs_date)::VARCHAR from read_parquet('{p}')"
                     ).fetchone()


def test_a_revised_value_changes_the_fingerprint_though_shape_is_identical(tmp_path):
    """The TEC00115 case: same rows, same max date, one number different."""
    a = _write(str(tmp_path / "a.parquet"), VALUES)
    revised = list(VALUES)
    revised[0] = 3.5                      # DK 2025 real GDP growth, 2.9 -> 3.5
    b = _write(str(tmp_path / "b.parquet"), revised)

    assert _shape(a) == _shape(b), "fixture is wrong: the shapes must be identical"
    assert _fp(a) != _fp(b), "a revised value did not change the fingerprint — the guard is " \
                             "still blind to exactly the R549 case"


def test_a_pure_re_encode_does_NOT_change_the_fingerprint(tmp_path):
    """The pyarrow-version confound: different bytes, identical data, must NOT be flagged."""
    a = _write(str(tmp_path / "z1.parquet"), VALUES, compression="zstd")
    b = _write(str(tmp_path / "z2.parquet"), VALUES, compression="snappy",
               row_group_size=2)
    assert open(a, "rb").read() != open(b, "rb").read(), \
        "fixture is wrong: the two files must differ on disk"
    assert _fp(a) == _fp(b), "a re-encode of identical data changed the fingerprint — this " \
                             "would raise the 59 false positives R549 measured"


def test_row_ORDER_does_not_change_the_fingerprint(tmp_path):
    a = _write(str(tmp_path / "o1.parquet"), VALUES)
    t = pa.table({"series_key": pa.array(list(reversed(KEYS))),
                  "obs_date": pa.array(DATES),
                  "obs_value": pa.array(list(reversed(VALUES)))})
    b = str(tmp_path / "o2.parquet")
    pq.write_table(t, b)
    assert _fp(a) == _fp(b), "the fingerprint is order-dependent; a re-sorted store copy " \
                             "would read as a revision"


def test_duplicate_rows_do_not_cancel(tmp_path):
    """`bit_xor` would make an even number of identical rows vanish; `sum` must not."""
    one = _write(str(tmp_path / "d1.parquet"), VALUES)
    t = pa.table({"series_key": pa.array(KEYS + KEYS[:1]),
                  "obs_date": pa.array(DATES + DATES[:1]),
                  "obs_value": pa.array(VALUES + VALUES[:1])})
    two = str(tmp_path / "d2.parquet")
    pq.write_table(t, two)
    assert _fp(one) != _fp(two), "a duplicated row cancelled out of the fingerprint"


@pytest.mark.parametrize("bad", ['we"ird', "sp ace"])
def test_odd_column_names_do_not_break_the_query(tmp_path, bad):
    t = pa.table({bad: pa.array(["x", "y"]), "obs_value": pa.array([1.0, 2.0])})
    p = str(tmp_path / "c.parquet")
    pq.write_table(t, p)
    assert _fp(p) is not None
