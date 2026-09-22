"""Retiring or delisting a source must clear the FRESHNESS projection too, or it keeps publishing.

`/v1/last-updates` is built from `LAST_UPDATES` (api/worker/src/sql.ts), which selects from
`unit_state` with NO join to `source` and no denylist filter, and `lastUpdates.ts` applies none
either. So a source whose `series`, `source` and `source_counts` rows are deleted vanishes from
`/v1/sources` and goes on serving its id, unit id, status, `last_updated`, `source_date_accessed`,
`last_obs_date`, `next_update_expected` and `obs_count` from `/v1/last-updates` indefinitely.

Measured against the live worker on 2026-09-21: 282 ids on `/v1/last-updates` against 321 on
`/v1/sources`; 15 appeared on the first and not the second, and 11 of those were in the committed
gate. One id was cleared by hand that day, which is what showed the tools were the cause rather
than a one-off.

This is the same class as R709, which added `source_counts` to these tools after a retired source
kept contributing its count to the fleet total. The lesson did not reach one table further, so this
test pins the whole set rather than the one table that bit last.
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS = ("retire_source.py", "delist_source_rows.py")
REQUIRED = ("series", "source", "source_counts",
            "unit_state", "source_state", "source_data_through")


def _deleted_tables(name: str) -> set:
    src = open(os.path.join(ROOT, "tools", name), encoding="utf-8").read()
    return set(re.findall(r"DELETE FROM (\w+) WHERE source_id=", src))


def test_both_tools_clear_every_table_that_keeps_a_source_visible():
    for name in TOOLS:
        got = _deleted_tables(name)
        missing = [t for t in REQUIRED if t not in got]
        assert not missing, (
            f"{name} does not delete from {missing}. A table left behind keeps the retired source "
            f"reachable on some surface - that is how /v1/last-updates went on publishing 15 ids, "
            f"11 of them gated.")


def test_the_two_tools_do_not_drift_apart():
    """They are the same operation with different entry points; R1061's lesson is that a fix
    applied to one call site and not its twin is half a fix."""
    a, b = (_deleted_tables(n) for n in TOOLS)
    assert a == b, f"retire_source.py deletes {sorted(a)} but delist_source_rows.py deletes {sorted(b)}"


def test_the_freshness_tables_are_named_explicitly():
    """A regression guard with teeth: dropping any one of the three re-opens the disclosure."""
    for name in TOOLS:
        got = _deleted_tables(name)
        for t in ("unit_state", "source_state", "source_data_through"):
            assert t in got, f"{name} stopped clearing {t}"
