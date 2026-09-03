"""A fetcher that rebuilds a Result must carry every field, or it drops one silently.

Three fetchers take the Result `finalize()` returned and rebuild it to change the status and the
message - `res = Result(status="partial", obs=res.obs, ...)` in bls.py, census.py and dst.py.
Every field NOT named in such a call reverts to its dataclass default with no error and no log
line.

The optional fields are not decoration:

  - `changed_keys` is what section 5.7 derives the CSVs from when it is present. Dropping it
    falls back to `series_cursors.keys()`, which answers a DIFFERENT question - the audit of
    2026-08-31 found series_cursors serving three contradictory contracts at once, over-reporting
    the changed set (ecb changed == attempted, 25/25).
  - `series_cursors` is the per-series freshness the orchestrator persists so a frozen series
    cannot hide behind a unit-level max.
  - `cursor_cap_hit` and `merged_rows` feed coverage and honesty checks downstream.

All three call sites currently pass everything. This test exists so the fourth one cannot quietly
not: the failure mode is invisible at runtime, which is exactly the kind that needs a mechanical
check rather than a convention (R676's lesson, and R501/R503 before it).
"""
from __future__ import annotations

import ast
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")


def _result_fields() -> list[str]:
    src = open(os.path.join(ROOT, "updater", "strategies", "base.py"), encoding="utf-8").read()
    for node in ast.parse(src).body:
        if isinstance(node, ast.ClassDef) and node.name == "Result":
            return [st.target.id for st in node.body
                    if isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name)]
    raise AssertionError("Result dataclass not found in updater/strategies/base.py")


def _rebuild_sites() -> list[tuple[str, int, set[str]]]:
    """Every `Result(...)` call whose arguments read attributes off an existing Result."""
    out = []
    for dirpath, _, filenames in os.walk(FETCHERS):
        for name in sorted(filenames):
            if not name.endswith(".py"):
                continue
            path = os.path.join(dirpath, name)
            try:
                tree = ast.parse(open(path, encoding="utf-8").read())
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                        and node.func.id == "Result" and "res." in ast.unparse(node)):
                    out.append((os.path.relpath(path, ROOT), node.lineno,
                                {k.arg for k in node.keywords if k.arg}))
    return out


def test_the_detector_finds_the_known_sites() -> None:
    """A guard that matches nothing passes vacuously. Anchor it to the sites that exist."""
    files = {f for f, _, _ in _rebuild_sites()}
    assert len(files) >= 3, f"expected at least the three known rebuild sites, found {files}"
    for expected in ("bls.py", "census.py", "dst.py"):
        assert any(expected in f for f in files), f"no Result rebuild detected in {expected}"


@pytest.mark.parametrize("site", _rebuild_sites(),
                         ids=lambda s: f"{os.path.basename(s[0])}:{s[1]}")
def test_rebuild_carries_every_field(site) -> None:
    path, lineno, passed = site
    fields = _result_fields()
    missing = [f for f in fields if f not in passed and f != "status"]
    assert not missing, (
        f"{path}:{lineno} rebuilds a Result without passing: {', '.join(missing)}.\n"
        "Those fields revert to their dataclass defaults silently. `changed_keys` decides which "
        "CSVs section 5.7 derives, and `series_cursors` is the per-series freshness the "
        "orchestrator persists — losing either produces no error, just wrong output."
    )
