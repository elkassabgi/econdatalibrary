"""A flow-grain source's BARE table key must resolve, not fall through to zero rows.

WHAT THIS PINS (2026-08-07). `_FLOW_GRAIN` sources map one catalogue id to every store row whose
key begins `"<flow>:"`. That trailing colon is deliberate — without it `AG_LND_DGRD` would also
match `AG_LND_DGRD2` — but it silently excluded the case where a PxWeb table has ALL its
dimensions eliminated. Such a table holds exactly one series and the ingester stores it under the
table id with no suffix at all:

    store key   ICE:Samfelag:launogtekjur:2_lvt:4_adrar:LAU04801.px          (224 rows)
    predicate   starts_with(series_key, "ICE:...:LAU04801.px:")              -> 0 rows

So the resolver raised "zero rows matched", `core/derive_csv.py` counted it under
"N unresolvable (store-coverage gaps)", and hagstofa's derive skipped those ids on every run
while their data sat in the store. The count was printed, never chased.

Measured across all eleven _FLOW_GRAIN sources before the predicate was widened, because it
serves every one of them: 1,860 bare-keyed rows across 7 catalogued ids, ALL in hagstofa —
stat_latvia, stat_estonia, ssb, bfs, dst, statfin, stat_slovenia, scb, unsdg and cso have none.
The fix adds an exact-equality arm, which is additive and cannot reintroduce the prefix
collision the colon exists to prevent.
"""
from __future__ import annotations

import inspect
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))


def test_the_predicate_matches_the_bare_key_and_the_prefix():
    """Both arms, on a synthetic table so the test does not depend on the live store."""
    import pyarrow as pa
    import pyarrow.dataset as ds
    from econdl import _resolve

    tbl = pa.table({
        "series_key": ["FLOW.px", "FLOW.px:A=1", "FLOW.px:A=2", "FLOW2.px", "FLOW2.px:A=1"],
        "obs_date": ["2020-01-01"] * 5,
        "value": [1.0, 2.0, 3.0, 4.0, 5.0],
    })
    import pyarrow.compute as pc
    native = "FLOW.px"
    pred = (pc.equal(ds.field("series_key"), native)
            | pc.starts_with(ds.field("series_key"), native + ":"))
    got = sorted(ds.dataset(tbl).to_table(filter=pred).column("series_key").to_pylist())
    assert got == ["FLOW.px", "FLOW.px:A=1", "FLOW.px:A=2"], got
    # FLOW2.px must NOT be swept in — that is what the trailing colon protects, and the new
    # equality arm must not undo it.
    assert "FLOW2.px" not in got and "FLOW2.px:A=1" not in got


def test_the_resolver_still_carries_both_arms():
    """A unit test on the expression passes even if the resolver drops one arm."""
    from econdl import _resolve
    src = inspect.getsource(_resolve._resolve_generic_long)
    assert "pc.equal(ds.field(key_col), native)" in src, (
        "the bare-key arm is gone; flow-grain tables whose dimensions are all eliminated will "
        "resolve to zero rows again and be silently counted as store-coverage gaps")
    assert 'pc.starts_with(ds.field(key_col), native + ":")' in src, (
        "the prefix arm is gone")


def test_pc_or_is_not_used_for_expression_binding():
    """`pc.or_(...)` builds but raises ArrowKeyError only when the dataset is SCANNED — i.e. at
    derive time, long after any import-time check would have passed."""
    from econdl import _resolve
    src = inspect.getsource(_resolve._resolve_generic_long)
    assert "pc.or_(" not in src, (
        "pc.or_ is not registered for expression binding; use `|` on the Expressions")
