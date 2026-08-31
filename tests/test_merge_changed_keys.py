"""The merge-measured changed-key report (cursor-contract step 3, option-1 API).

The discriminating pair §5 demands, both legs:
  * DEFAULT PATH BYTE-IDENTICAL — no kwarg, 2-tuple return, published bytes equal to
    a pre-change merge of the same inputs;
  * OPT-IN returns exactly the keys whose SERVED value changed, with per-key maxima —
    and an idempotent boundary re-fetch reports {} (the false-positive class the
    fetched-key channel institutionalises, ecb 25/25 changed==attempted).

Plus the value-identity edges the step-2 decision fixed: bitwise floats, null==null
same, NaN==NaN same; refusals raise with NOTHING escaping; overwrite and over-cap
refuse at entry BEFORE any I/O.
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.errors import DefinitiveError  # noqa: E402
from updater.merge import merge_and_write  # noqa: E402


def _tbl(rows):
    """rows: list of (series_key, obs_date, value)"""
    return pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date":   pa.array([r[1] for r in rows], pa.string()),
        "value":      pa.array([r[2] for r in rows], pa.float64()),
    })


def _publish(path, rows):
    n, _ = merge_and_write(str(path), _tbl(rows))
    return n


def test_default_path_is_byte_identical_and_two_tuple(tmp_path):
    p1, p2 = tmp_path / "a.parquet", tmp_path / "b.parquet"
    _publish(p1, [("A", "2024-01-01", 1.0)])
    _publish(p2, [("A", "2024-01-01", 1.0)])
    r1 = merge_and_write(str(p1), _tbl([("B", "2024-02-01", 2.0)]))
    assert isinstance(r1, tuple) and len(r1) == 2          # nobody's unpack breaks
    r2 = merge_and_write(str(p2), _tbl([("B", "2024-02-01", 2.0)]),
                         report_changed_keys=True)
    assert len(r2) == 3
    assert open(p1, "rb").read() == open(p2, "rb").read()  # opt-in changed NO bytes


def test_extension_revision_and_idempotent_refetch(tmp_path):
    p = tmp_path / "s.parquet"
    _publish(p, [("A", "2024-01-01", 1.0), ("A", "2024-02-01", 2.0),
                 ("C", "2024-01-01", 9.0)])

    # idempotent boundary re-fetch: same key, same date, same value -> NOT changed
    n, last, ch = merge_and_write(str(p), _tbl([("A", "2024-02-01", 2.0)]),
                                  report_changed_keys=True)
    assert ch == {}

    # revision: same key+date, DIFFERENT value -> changed, max = the revised date
    _, _, ch = merge_and_write(str(p), _tbl([("A", "2024-02-01", 3.0)]),
                               report_changed_keys=True)
    assert ch == {"A": "2024-02-01"}

    # extension: brand-new observation key -> changed; untouched series C absent
    _, _, ch = merge_and_write(str(p), _tbl([("A", "2024-03-01", 4.0)]),
                               report_changed_keys=True)
    assert ch == {"A": "2024-03-01"}

    # mixed batch: one idempotent, one revision, one extension, one new series
    _, _, ch = merge_and_write(
        str(p), _tbl([("A", "2024-03-01", 4.0),        # idempotent
                      ("A", "2024-01-01", 1.5),        # revision (older period!)
                      ("C", "2024-02-01", 7.0),        # extension
                      ("D", "2024-01-01", 5.0)]),      # new series
        report_changed_keys=True)
    assert ch == {"A": "2024-01-01", "C": "2024-02-01", "D": "2024-01-01"}
    # the revision's max is the REVISED period, not the series' frontier — the
    # channel reports what changed, the cursor keeps reporting the frontier


def test_no_existing_file_reports_every_key(tmp_path):
    p = tmp_path / "new.parquet"
    _, _, ch = merge_and_write(str(p), _tbl([("A", "2024-01-01", 1.0),
                                             ("B", "2024-02-01", 2.0)]),
                               report_changed_keys=True)
    assert ch == {"A": "2024-01-01", "B": "2024-02-01"}


def test_internal_duplicate_chain_compares_the_winner(tmp_path):
    p = tmp_path / "c.parquet"
    _publish(p, [("A", "2024-01-01", 1.0)])
    # new table carries TWO rows for the same key; the last wins (2.0 -> 1.0 -> same
    # as existing? no: winner is the LAST new row = 1.0 == existing -> NOT changed)
    _, _, ch = merge_and_write(str(p), _tbl([("A", "2024-01-01", 2.0),
                                             ("A", "2024-01-01", 1.0)]),
                               report_changed_keys=True)
    assert ch == {}
    # ...and the reverse order: winner 2.0 != existing 1.0 -> changed
    _, _, ch = merge_and_write(str(p), _tbl([("A", "2024-01-01", 1.0),
                                             ("A", "2024-01-01", 2.0)]),
                               report_changed_keys=True)
    assert ch == {"A": "2024-01-01"}


def test_null_and_nan_value_identity(tmp_path):
    p = tmp_path / "n.parquet"
    base = pa.table({
        "series_key": pa.array(["N", "F"], pa.string()),
        "obs_date":   pa.array(["2024-01-01", "2024-01-01"], pa.string()),
        "value":      pa.array([None, float("nan")], pa.float64()),
    })
    merge_and_write(str(p), base)

    same = merge_and_write(str(p), base, report_changed_keys=True)[2]
    assert same == {}                       # null==null and NaN==NaN are SAME

    repl = pa.table({
        "series_key": pa.array(["N"], pa.string()),
        "obs_date":   pa.array(["2024-01-01"], pa.string()),
        "value":      pa.array([5.0], pa.float64()),
    })
    assert merge_and_write(str(p), repl, report_changed_keys=True)[2] == \
        {"N": "2024-01-01"}                 # null -> value IS a change


def test_refusals_raise_with_nothing_escaping(tmp_path):
    p = tmp_path / "r.parquet"
    # ONE series, 100 dated rows — re-merging with dedup on series_key ALONE
    # collapses 100 -> 1, far below min_ratio, so never-shrink refuses.
    _publish(p, [("K", f"2024-01-{i+1:02d}", float(i)) for i in range(28)])
    before = open(p, "rb").read()
    with pytest.raises(DefinitiveError):    # never-shrink refusal, opt-in active
        merge_and_write(str(p), _tbl([("K", "2024-02-01", 99.0)]),
                        min_ratio=0.97, report_changed_keys=True,
                        dedup_keys=("series_key",))
    assert open(p, "rb").read() == before   # file untouched, no partial report


def test_per_series_max_over_multiple_changed_runs(tmp_path):
    """The reviewer's C1 mutant (per-key MIN instead of MAX) survives every other
    test — kill it: one series with TWO changed runs must report the LATER date."""
    p = tmp_path / "m.parquet"
    _publish(p, [("A", "2024-01-01", 1.0), ("A", "2024-03-01", 3.0)])
    _, _, ch = merge_and_write(
        str(p), _tbl([("A", "2024-01-01", 9.0),      # revision (older period)
                      ("A", "2024-02-01", 5.0)]),    # extension (middle period)
        report_changed_keys=True)
    assert ch == {"A": "2024-02-01"}, ch             # MAX of the changed dates, not MIN


def test_first_publish_with_missing_dedup_key_refuses(tmp_path):
    """The reviewer's C2: on first publish the merge-branch key guard never runs, so
    a table missing series_key reported keyed by the WRONG column and one missing
    all keys reported a silent {}. The entry guard must refuse both, before I/O."""
    p = tmp_path / "k.parquet"
    nokey = pa.table({"obs_date": pa.array(["2024-01-01"], pa.string()),
                      "value": pa.array([1.0], pa.float64())})
    with pytest.raises(ValueError):
        merge_and_write(str(p), nokey, report_changed_keys=True)
    assert not os.path.exists(p)


def test_entry_guards(tmp_path):
    p = tmp_path / "g.parquet"
    with pytest.raises(ValueError):
        merge_and_write(str(p), _tbl([("A", "2024-01-01", 1.0)]),
                        mode="overwrite", report_changed_keys=True)
    with pytest.raises(ValueError):
        merge_and_write(str(p), _tbl([("A", "2024-01-01", 1.0)] * 3),
                        report_changed_keys=True, changed_keys_cap=2)
    assert not os.path.exists(p)            # refused BEFORE any I/O


def test_multi_key_dedup_with_period_column(tmp_path):
    """The eia shape: dedup on (series_key, obs_date, period) — the report still
    groups by series_key and an added period cell is an extension."""
    p = tmp_path / "e.parquet"
    def tbl(rows):
        return pa.table({
            "series_key": pa.array([r[0] for r in rows], pa.string()),
            "obs_date":   pa.array([r[1] for r in rows], pa.string()),
            "value":      pa.array([r[2] for r in rows], pa.float64()),
            "period":     pa.array([r[3] for r in rows], pa.string()),
        })
    keys = ("series_key", "obs_date", "period")
    merge_and_write(str(p), tbl([("E", "2024-01-01", 1.0, "H1")]), dedup_keys=keys)
    _, _, ch = merge_and_write(str(p), tbl([("E", "2024-01-01", 1.0, "H1")]),
                               dedup_keys=keys, report_changed_keys=True)
    assert ch == {}
    _, _, ch = merge_and_write(str(p), tbl([("E", "2024-01-01", 2.0, "H2")]),
                               dedup_keys=keys, report_changed_keys=True)
    assert ch == {"E": "2024-01-01"}        # new period cell = extension


def test_report_does_not_perturb_published_rows(tmp_path):
    """The dedup RESULT with the report active equals the result without it —
    the report is a read-only observer of the same pass."""
    pa_, pb = tmp_path / "x.parquet", tmp_path / "y.parquet"
    rows = [("A", "2024-01-01", 1.0), ("A", "2024-01-01", 2.0),
            ("B", "2024-01-01", None), ("B", "2024-01-01", None)]
    merge_and_write(str(pa_), _tbl(rows))
    merge_and_write(str(pb), _tbl(rows), report_changed_keys=True)
    assert pq.read_table(str(pa_)).equals(pq.read_table(str(pb)))
