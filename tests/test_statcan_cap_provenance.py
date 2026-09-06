"""The cataloguer must take its cap from what the derive RECORDED, and refuse a disagreement.

WHY (R832 / R833). `--max-rows` has to match the value the derive ran with, or the completeness
guard compares against the wrong set of oversized tables. That parameter used to be persisted
NOWHERE - not in `_split_map.json`, whose entries are `{dim, parts, rows}`, and not in
`logs/statcan_tables_summary.json`. Both tools merely shared a default constant, which guarantees
agreement only while NEITHER side is overridden - precisely the case that never holds on a giant.

The cost of that: a cataloguer run at 500,000 against a derive run near 3,000,000 reported "965
tables exceed 500,000 rows but 372 have no split-map entry". 367 of those 372 were tables the
derive had correctly written whole. I read the refusal as a frozen pipeline and escalated a
multi-day re-derive that was never needed - while the catalogue had in fact been applied five days
earlier, 1:1 coherent with R2.

Three behaviours, pinned from both sides:

  recorded, no flag        -> ADOPT it, and say so
  recorded, flag disagrees -> REFUSE (return 1), naming both numbers
  NOT recorded             -> fall back, and WARN that the cap is unknown; never silent
"""
from __future__ import annotations

import importlib.util
import io
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "catalog_statcan_tables.py")


def _load():
    spec = importlib.util.spec_from_file_location("_catalog_statcan_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _run(tmp_path, summary: dict | None, argv_extra: list):
    """Run main() with ROOT patched. `--expect-kept 1` without `--r2-keys` is an existing
    fail-closed early exit, so the run stops right after the cap is resolved and never touches
    the store - the cap logic is what is under test, nothing else."""
    root = str(tmp_path)
    os.makedirs(os.path.join(root, "logs"), exist_ok=True)
    if summary is not None:
        with io.open(os.path.join(root, "logs", "statcan_tables_summary.json"),
                     "w", encoding="utf-8") as fh:
            json.dump(summary, fh)
    m = _load()
    m.ROOT = root
    argv = sys.argv
    sys.argv = ["catalog_statcan_tables", "--expect-kept", "1"] + argv_extra
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        rc = m.main()
    finally:
        sys.stdout = real
        sys.argv = argv
    return rc, buf.getvalue(), m


BASE = {"considered": 8207, "units": 466341, "put": 252425, "refused": [], "dry_run": False}


def test_recorded_cap_is_adopted(tmp_path):
    """The whole point: stop guessing."""
    rc, out, m = _run(tmp_path, dict(BASE, max_rows=3_000_000), [])
    assert "cap 3,000,000 adopted from the derive's recorded max_rows" in out, out
    # and it really did stop at the pre-existing fail-closed exit, not somewhere else
    assert rc == 1 and "need --r2-keys" in out
    assert m.MAX_ROWS_DEFAULT == 500_000, "the derive's default must not have been edited"


def test_disagreeing_flag_is_refused(tmp_path):
    """FAIL CLOSED. A silent mismatch changes which tables are checked for a split entry, which
    is the entire completeness guard - so it must stop the run, not adjust it."""
    rc, out, _ = _run(tmp_path, dict(BASE, max_rows=3_000_000), ["--max-rows", "500000"])
    assert rc == 1, out
    assert "REFUSING: --max-rows 500,000 disagrees with the cap the derive recorded" in out, out
    assert "3,000,000" in out, out
    # it must refuse BEFORE the --expect-kept exit, i.e. this is the reason it stopped
    assert "need --r2-keys" not in out, out


def test_agreeing_flag_is_accepted(tmp_path):
    """The guard must not fire on agreement - a refusal that always fires is not a guard."""
    rc, out, _ = _run(tmp_path, dict(BASE, max_rows=3_000_000), ["--max-rows", "3000000"])
    assert "REFUSING: --max-rows" not in out, out
    assert rc == 1 and "need --r2-keys" in out


def test_unrecorded_cap_warns_and_is_never_silent(tmp_path):
    """A legacy summary cannot confirm the cap. Falling back is fine; falling back QUIETLY is
    how the original failure was possible."""
    rc, out, _ = _run(tmp_path, dict(BASE), [])          # no max_rows key
    assert "does NOT record max_rows" in out, out
    assert "500,000" in out and "artifact" in out, out
    assert rc == 1 and "need --r2-keys" in out


def test_missing_summary_says_so(tmp_path):
    """No summary at all is a third case and must not masquerade as 'recorded'."""
    rc, out, _ = _run(tmp_path, None, [])
    assert "derive summary unreadable" in out, out
    assert "does NOT record max_rows" in out, out
    assert rc == 1
