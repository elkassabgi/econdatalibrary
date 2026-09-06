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

That second number is why `test_a_lone_point_for_its_date_SURVIVES` exists: the obvious rule -
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


def test_a_lone_point_for_its_date_SURVIVES():
    """The 145 lido observations a timestamp-based rule would have deleted are non-midnight
    points that are the ONLY point for their date.

    HONEST LIMIT, and it is the reason this seam was chosen (R805 finding 4): no timestamp
    reaches `_dedup_first` — by here a row is (key, date, value) — so the rival "drop anything
    not at 00:00 UTC" rule is not merely wrong, it is UNIMPLEMENTABLE at this seam. This test
    therefore pins the invariant (a lone point for a date is kept) and cannot discriminate
    between the two rules; the measurement that ruled the other one out is in the commit message
    the measurement that ruled the other rule out is quoted in full in `_dedup_first`'s own
    docstring, where it travels with the code — an earlier version of this line cited a session
    scratch path no reader could open, which is a citation to nowhere."""
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
def test_NEUTRALISING_the_dedup_brings_the_defect_straight_back(tmp_path, monkeypatch):
    """A control has to exercise the MODULE. My first version of this test built a duplicated
    parquet with pyarrow directly and asserted it was duplicated — it never called
    `write_parquet`, so it could not fail unless pyarrow itself changed. It was theatre.

    This replaces `_dedup_first` with a pass-through and asserts the defect returns through the
    real writer, which is what proves the seam is load-bearing rather than decorative."""
    monkeypatch.setattr(_mod, "_dedup_first", lambda cols: (cols, 0))
    p = str(tmp_path / "neutralised.parquet")
    n = _mod.write_parquet(p, {"series_key": ["aave|__total__"] * 2,
                               "obs_date": [D2, D2],
                               "value": [18309869039.0, 18397673946.0]})
    t = pq.read_table(p)
    pairs = list(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist()))
    assert n == 2 and len(pairs) == 2 and len(set(pairs)) == 1, (
        "with the dedup neutralised the writer must reproduce R773 — if it does not, "
        "`write_parquet` is not the seam these tests think it is")
    assert len(set(t.column("value").to_pylist())) == 2


def test_the_SECOND_producer_ACTUALLY_writes_no_duplicate_pair(tmp_path, monkeypatch):
    """R807 finding 2: the only test covering the second producer parsed it with `ast`, so
    deleting the ONE line that consumes `_dedup_first`'s return left the log still printing
    "dropped 12 row(s)" while the duplicates went back into the file — and all nine tests passed.
    A static assertion cannot see a discarded return value. This drives the tool's real `main()`.
    """
    from jobs import ingest_defillama                              # noqa: F401
    import tools.defillama_parent_protocols as tool

    out = tmp_path / "tvl_protocol_shard_parents.parquet"
    monkeypatch.setattr(tool, "OUT", str(out))
    monkeypatch.setattr(tool, "SLUGS", ["aave"])

    SETTLED, INTRADAY = 1788652800, 1788665243        # 00:00:00Z and 03:27:23Z, same UTC day
    payload = {"tvl": [{"date": 1788566400, "totalLiquidityUSD": 1.0},
                       {"date": SETTLED, "totalLiquidityUSD": 18309869039.0},
                       {"date": INTRADAY, "totalLiquidityUSD": 18397673946.0}],
               "chainTvls": {"Ethereum": {"tvl": [
                   {"date": SETTLED, "totalLiquidityUSD": 5.0},
                   {"date": INTRADAY, "totalLiquidityUSD": 6.0}]}}}

    class _R:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return payload

    class _S:
        headers = {}
        def get(self, *a, **k): return _R()

    monkeypatch.setattr(tool.requests, "Session", lambda: _S())
    assert tool.main() == 0

    t = pq.read_table(str(out))
    pairs = list(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist()))
    assert len(pairs) == len(set(pairs)), (
        f"the second producer still wrote a duplicated (series_key, obs_date): {pairs}")
    vals = t.column("value").to_pylist()
    dup_day = max(d for _k, d in pairs)
    total = [v for (k, d), v in zip(pairs, vals) if k == "aave|__total__" and d == dup_day]
    chain = [v for (k, d), v in zip(pairs, vals) if k == "aave|Ethereum" and d == dup_day]
    assert total == [18309869039.0], (
        f"the survivor must be the 00:00 settled close, got {total}")
    assert chain == [5.0], f"chainTvls must be deduped the same way, got {chain}"


def test_the_SECOND_producer_shares_the_one_rule(tmp_path):
    """R805 finding 1: `tools/defillama_parent_protocols.py` is a SECOND raw writer, and it held
    147 duplicate pairs (141 contradictory) reaching six of the seven corrupted served CSVs. Its
    own docstring said its rows were built "with the ingester's own construction" — copied, not
    imported, which is why fixing one producer left the other writing the defect. A rule that
    lives in two places gets fixed in one, so this pins that it is IMPORTED."""
    import ast
    src = open(os.path.join(ROOT, "tools", "defillama_parent_protocols.py"),
               encoding="utf-8").read()
    tree = ast.parse(src)
    imported = any(isinstance(n, ast.ImportFrom)
                   and n.module == "jobs.ingest_defillama"
                   and any(a.name == "_dedup_first" for a in n.names)
                   for n in ast.walk(tree))
    assert imported, ("the parent-protocols tool must IMPORT _dedup_first, not re-implement it")
    assert "_dedup_first(" in src, "imported but never called"
