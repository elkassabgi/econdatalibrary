"""jobs/ingest_defillama.py must not write one date twice (R773).

`/protocol/<slug>` ends its `tvl` array with an intraday "now" point on the current day, and the
settled 00:00 UTC close for that day is already in the array. This ingester wrote both, nothing
downstream dedups it (merge_and_write, which does, never touches these files), and the served
CSV hands a user that date twice with two different values: 33 of 107 store objects carry 21,759
such pairs, seven of the eight served protocol CSVs among them.

Measured from the publisher on 2026-09-06, which is what fixes the rule:

    /protocol/aave   2,302 points, exactly ONE duplicated date - today
                     ts=1788652800  00:00:00Z  18309869039   <- settled close, FIRST
                     ts=1788665243  03:27:23Z  18397673946   <- intraday "now", appended after
    /protocol/lido   2,088 points, same shape, and 146 points NOT at midnight of which only
                     one is part of a duplicated pair

That second number is why `test_a_lone_non_midnight_point_SURVIVES` exists: the obvious rule -
"drop anything not at 00:00 UTC" - would have deleted 145 legitimate lido observations. A test
that only checked the duplicate would have passed for both rules and caught nothing.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from jobs import ingest_defillama as _mod                      # noqa: E402

D1 = dt.date(2026, 9, 5)
D2 = dt.date(2026, 9, 6)


def test_the_settled_close_is_kept_and_the_intraday_point_is_dropped():
    cols = {"series_key": ["aave|__total__", "aave|__total__"],
            "obs_date": [D2, D2],
            "value": [18309869039.0, 18397673946.0]}
    out, dropped = _mod._dedup_first(cols)
    assert dropped == 1
    assert out["value"] == [18309869039.0], out["value"]
    assert out["obs_date"] == [D2]


def test_a_lone_non_midnight_point_SURVIVES():
    """The 145 lido observations a timestamp-based rule would have deleted. Each is the ONLY
    point for its date, so it is the observation, not a duplicate."""
    cols = {"series_key": ["lido|__total__"] * 2,
            "obs_date": [D1, D2],
            "value": [1.0, 2.0]}
    out, dropped = _mod._dedup_first(cols)
    assert dropped == 0 and out["value"] == [1.0, 2.0]


def test_two_series_may_share_a_date():
    """The key is the PAIR. Deduping on obs_date alone would delete every other series."""
    cols = {"series_key": ["a|__total__", "b|__total__"],
            "obs_date": [D2, D2], "value": [1.0, 2.0]}
    out, dropped = _mod._dedup_first(cols)
    assert dropped == 0 and len(out["series_key"]) == 2


def test_every_parallel_column_is_filtered_together():
    """A dedup that trimmed series_key and not value would silently mis-pair every later row."""
    cols = {"series_key": ["a", "a", "b"], "obs_date": [D2, D2, D2],
            "value": [1.0, 2.0, 3.0], "tvl_usd": [10.0, 20.0, 30.0]}
    out, dropped = _mod._dedup_first(cols)
    assert dropped == 1
    assert out["series_key"] == ["a", "b"]
    assert out["value"] == [1.0, 3.0]
    assert out["tvl_usd"] == [10.0, 30.0]


def test_write_parquet_writes_no_duplicate_pair(tmp_path):
    p = str(tmp_path / "tvl_protocol_shard00.parquet")
    n = _mod.write_parquet(p, {"series_key": ["aave|__total__"] * 2 + ["lido|__total__"],
                               "obs_date": [D2, D2, D2],
                               "value": [1.0, 2.0, 3.0]})
    t = pq.read_table(p)
    assert n == 2, f"the returned row count must be what was WRITTEN, got {n}"
    assert t.num_rows == 2
    pairs = list(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist()))
    assert len(set(pairs)) == len(pairs) == 2
    assert t.column("value").to_pylist() == [1.0, 3.0]


def test_a_catalogue_file_without_obs_date_is_untouched(tmp_path):
    """The catalogue phases write name/tvl/mcap rows with no obs_date and legitimately repeat a
    name. Deduping them would be a different defect."""
    p = str(tmp_path / "_catalog_chains.parquet")
    n = _mod.write_parquet(p, {"name": ["Ethereum", "Ethereum"], "tvl": [1.0, 2.0]})
    assert n == 2 and pq.read_table(p).num_rows == 2


def test_an_empty_column_set_still_writes_nothing(tmp_path):
    assert _mod.write_parquet(str(tmp_path / "x.parquet"),
                              {"series_key": [], "obs_date": [], "value": []}) == 0


# ------------------------------------------------------------------ DISCRIMINATION CONTROL
def test_the_OLD_behaviour_wrote_both_rows(tmp_path):
    """Without the dedup, these exact fixtures produce the defect: one key, one date, two values.
    If this ever stops holding, the tests above are no longer measuring anything."""
    import pyarrow as pa
    keys = ["aave|__total__", "aave|__total__"]
    dates = [D2, D2]
    vals = [18309869039.0, 18397673946.0]
    p = str(tmp_path / "old.parquet")
    pq.write_table(pa.table({"series_key": pa.array(keys), "obs_date": pa.array(dates, pa.date32()),
                             "value": pa.array(vals, pa.float64())}), p)
    t = pq.read_table(p)
    pairs = list(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist()))
    assert len(pairs) == 2 and len(set(pairs)) == 1, "the control no longer reproduces R773"
    assert len(set(t.column("value").to_pylist())) == 2
