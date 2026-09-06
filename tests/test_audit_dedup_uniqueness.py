"""`audit_file` streams now, and its verdict must not change (R806) - nor go quiet (R815).

The old implementation read the whole table and called
`t.group_by(list(key_cols)).aggregate([]).num_rows`. pyarrow's hash aggregate FAST-FAILS on a
large table: exit 0xC0000409, no exception, no traceback, and with stdout buffered, no output at
all. It survives imf's largest file at 6,300,194 rows and dies on cso's at 29,760,740 and vdem's
at 77,371,121, so 17 of 379 stores were unmeasurable and a crash was indistinguishable from a
tool that printed nothing.

REVIEW FAILED THE FIRST VERSION OF THIS FILE (R815) and every finding is pinned below:

  * the cap counter never left the loop, so a fully-capped store printed "0 under-keyed file(s)"
    and exited 0 - a LOUDER failure replaced by a SILENT pass, on 16 of the 18 stores concerned;
  * a fully-capped store printed "all N file(s) lack series_key/obs_date", which is false and
    sends the reader to declare a DEDUP that already exists;
  * `test_a_duplicate_SPLIT_ACROSS_ROW_GROUPS_is_still_found` did not discriminate: `iter_batches`
    coalesces at batch_size=1,000,000, so the two-row-group fixture arrived as ONE batch and a
    per-batch mutant scored 10/10. The fixtures now force the batch boundary;
  * nine of ten tests failed the old code only with `AttributeError` on the namedtuple - existence,
    not behaviour - and `_PACK_BASE`, the cap comparison, the post-cap row count and per-batch
    counting all SURVIVED mutation because no fixture had more than three distinct values.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_spec = importlib.util.spec_from_file_location(
    "audit_dedup_uniqueness", os.path.join(ROOT, "tools", "audit_dedup_uniqueness.py"))
aud = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(aud)

KEY = ("series_key", "obs_date")
D1, D2 = dt.date(2026, 9, 5), dt.date(2026, 9, 6)


def _write(path, keys, dates, row_group_size=None):
    t = pa.table({"series_key": pa.array(keys, pa.string()),
                  "obs_date": pa.array(dates, pa.date32()),
                  "value": pa.array([float(i) for i in range(len(keys))], pa.float64())})
    pq.write_table(t, path, row_group_size=row_group_size or max(len(keys), 1))
    return path


# ------------------------------------------------------------------ verdicts
def test_a_uniquely_keyed_file_reports_pairs_equal_to_rows(tmp_path):
    p = _write(str(tmp_path / "clean.parquet"), ["a", "a", "b"], [D1, D2, D1])
    r = aud.audit_file(p, KEY)
    assert (r.rows, r.keys, r.pairs, r.capped) == (3, 2, 3, False)


def test_an_under_keyed_file_reports_fewer_pairs_than_rows(tmp_path):
    p = _write(str(tmp_path / "bad.parquet"), ["a", "a", "a"], [D1, D1, D2])
    r = aud.audit_file(p, KEY)
    assert (r.rows, r.keys, r.pairs) == (3, 1, 2) and r.pairs < r.rows


def test_a_file_without_the_key_columns_returns_None(tmp_path):
    p = str(tmp_path / "other.parquet")
    pq.write_table(pa.table({"a": pa.array([1]), "b": pa.array([2])}), p)
    assert aud.audit_file(p, KEY) is None


def test_an_empty_file_is_zero_not_a_crash(tmp_path):
    r = aud.audit_file(_write(str(tmp_path / "e.parquet"), [], []), KEY)
    assert (r.rows, r.keys, r.pairs, r.capped) == (0, 0, 0, False)


def test_a_three_column_key_is_honoured(tmp_path):
    p = str(tmp_path / "three.parquet")
    pq.write_table(pa.table({"series_key": pa.array(["a", "a"]),
                             "obs_date": pa.array([D1, D1], pa.date32()),
                             "period": pa.array(["Q1", "Q2"])}), p)
    assert aud.audit_file(p, ("series_key", "obs_date", "period")).pairs == 2
    assert aud.audit_file(p, KEY).pairs == 1


def test_the_key_count_EXCLUDES_nulls_as_the_old_implementation_did(tmp_path):
    """R815: `pc.count_distinct` excluded nulls; `len(ordinals[0])` does not. Left alone this
    silently changed `keys` by one for any store with a null first key column."""
    p = str(tmp_path / "nulls.parquet")
    pq.write_table(pa.table({"series_key": pa.array(["a", None, None]),
                             "obs_date": pa.array([D1, D1, D2], pa.date32())}), p)
    r = aud.audit_file(p, KEY)
    assert (r.rows, r.keys, r.pairs) == (3, 1, 3), (r.rows, r.keys, r.pairs)


# ------------------------------------------------------------------ streaming, for real
def test_a_duplicate_SPLIT_ACROSS_BATCHES_is_still_found(tmp_path):
    """THE test for a streaming rewrite, and the first version of it did not work: it wrote two
    ROW GROUPS, but `iter_batches` coalesces at batch_size=1,000,000, so one batch arrived and a
    per-batch-aggregate mutant passed. Forcing batch_size=2 makes the boundary real."""
    p = _write(str(tmp_path / "split.parquet"), ["a", "b", "a", "c"], [D1] * 4, row_group_size=2)
    r = aud.audit_file(p, KEY, batch_size=2)
    assert (r.rows, r.pairs) == (4, 3), (
        f"rows={r.rows} pairs={r.pairs}: a cross-BATCH duplicate was missed")


def test_the_row_count_spans_batches(tmp_path):
    p = _write(str(tmp_path / "many.parquet"),
               [f"k{i}" for i in range(50)], [D1] * 50, row_group_size=5)
    assert aud.audit_file(p, KEY, batch_size=5).rows == 50


def test_the_PACKING_stays_injective_when_BOTH_columns_are_wide(tmp_path):
    """R815: `_PACK_BASE = 1 << 8` survived mutation. My first fixture here used 300 distinct
    keys and ONE date, which cannot collide at any base: with `packed = o0 * BASE + o1` and
    `o1` always 0, every product is distinct. A collision needs BOTH ordinals populated —
    (o0=1, o1=0) meets (o0=0, o1=256) at 256 once the second column exceeds the base.

    So: 2 keys x 300 dates, every combination present, all 600 pairs distinct."""
    dates = [dt.date(2000, 1, 1) + dt.timedelta(days=i) for i in range(300)]
    keys, ds = [], []
    for k in ("a", "b"):
        for d in dates:
            keys.append(k)
            ds.append(d)
    p = _write(str(tmp_path / "wide.parquet"), keys, ds)
    r = aud.audit_file(p, KEY, batch_size=37)
    assert (r.rows, r.keys, r.pairs) == (600, 2, 600), (r.rows, r.keys, r.pairs)


# ------------------------------------------------------------------ the cap
def test_past_the_cap_the_file_is_UNMEASURED_with_no_counts_invented(tmp_path, monkeypatch):
    monkeypatch.setattr(aud, "MAX_EXACT_PAIRS", 2)
    p = _write(str(tmp_path / "cap.parquet"),
               [f"k{i}" for i in range(10)], [D1] * 10, row_group_size=2)
    r = aud.audit_file(p, KEY, batch_size=2)
    assert r.capped is True
    assert r.rows == 10, "the row count must still be complete AFTER capping"
    assert r.pairs == -1 and r.keys == -1, "neither count survived; both must say so"


def test_the_cap_fires_only_ABOVE_the_threshold(tmp_path, monkeypatch):
    """R815: `>` vs `>=` survived. A file with exactly MAX_EXACT_PAIRS pairs is measurable."""
    monkeypatch.setattr(aud, "MAX_EXACT_PAIRS", 5)
    p = _write(str(tmp_path / "exact.parquet"), [f"k{i}" for i in range(5)], [D1] * 5)
    r = aud.audit_file(p, KEY, batch_size=2)
    assert r.capped is False and r.pairs == 5


# ------------------------------------------------------------------ what main() reports
def _run_main(monkeypatch, capsys, tmp_path, files, cap=None):
    src = tmp_path / "clean_full" / "src"
    src.mkdir(parents=True)
    for name, keys, dates in files:
        _write(str(src / name), keys, dates)
    monkeypatch.setattr(aud.config, "source_dir", lambda s: str(src))
    monkeypatch.setattr(aud, "dedup_key_for", lambda s: (KEY, "declared"))
    if cap is not None:
        monkeypatch.setattr(aud, "MAX_EXACT_PAIRS", cap)
    monkeypatch.setattr(sys, "argv", ["audit", "src", "--quiet-ok"])
    rc = aud.main()
    return rc, capsys.readouterr().out


def test_main_EXITS_NONZERO_when_every_file_is_capped(monkeypatch, capsys, tmp_path):
    """R815 finding 1, the one that mattered: `unmeasured` was loop-local, so a fully-capped
    store printed "0 under-keyed file(s)" and exited 0. Live, vdem did exactly that - R806's
    loud crash replaced by a silent pass, on 16 of the 18 stores concerned."""
    rc, out = _run_main(monkeypatch, capsys, tmp_path,
                        [("a.parquet", [f"k{i}" for i in range(10)], [D1] * 10)], cap=2)
    assert "UNMEASURED" in out
    assert rc != 0, "a store with no verdict must not exit 0"
    assert "UNMEASURED" in out.strip().splitlines()[-1], "the FINAL line must carry it too"


def test_main_does_not_claim_the_columns_are_missing_when_they_are_capped(
        monkeypatch, capsys, tmp_path):
    """R815 finding 2: the R330 gate keyed on `checked == 0`, so a fully-capped store was told
    to "Declare DEDUP in the fetcher" for columns it already has."""
    _rc, out = _run_main(monkeypatch, capsys, tmp_path,
                         [("a.parquet", [f"k{i}" for i in range(10)], [D1] * 10)], cap=2)
    assert "lack series_key/obs_date" not in out, out
    assert "NO VERDICT" in out


def test_main_still_says_MEASURED_NOTHING_when_the_columns_really_are_absent(
        monkeypatch, capsys, tmp_path):
    src = tmp_path / "clean_full" / "src"
    src.mkdir(parents=True)
    pq.write_table(pa.table({"a": pa.array([1])}), str(src / "x.parquet"))
    monkeypatch.setattr(aud.config, "source_dir", lambda s: str(src))
    monkeypatch.setattr(aud, "dedup_key_for", lambda s: (KEY, "declared"))
    monkeypatch.setattr(sys, "argv", ["audit", "src", "--quiet-ok"])
    rc = aud.main()
    out = capsys.readouterr().out
    assert "MEASURED NOTHING" in out and rc != 0


def test_main_exits_zero_on_a_genuinely_clean_store(monkeypatch, capsys, tmp_path):
    """The control: if this ever fails, every assertion above is meaningless because the tool
    reports a problem no matter what it is given."""
    rc, out = _run_main(monkeypatch, capsys, tmp_path,
                        [("a.parquet", ["a", "b"], [D1, D1])])
    assert rc == 0, out
    assert "UNDER-KEYED" not in out and "UNMEASURED" not in out
