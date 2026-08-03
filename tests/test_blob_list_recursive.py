"""Regression gate: a nested store must not read as an empty one.

WHY. `blob.list_parquets` returns basenames and drops nested keys — deliberately, so a nested
key cannot masquerade as a top-level flow. But not every store is flat. bea's 591 files live
at `clean_full/bea/<Dataset>/<Table>.parquet`, and for that store the non-recursive listing
returns [] — the SAME answer as a missing or empty store, which is how a real bug hid:
`_tree_frontier` walked the tree with a raw `glob.glob(out_dir/**)` (R36), found nothing under
AQUEDUCT_BACKEND=r2, and silently fell back to a grouped file holding under 2% of the series.
Measured 2026-08-03 against the live store: non-recursive 0 files, recursive 591, and the
frontier went from None to 2026-04-01.

So the two modes are pinned together here. The default must stay basenames-only — relaxing it
would let a nested key reach callers that do `os.path.join(out_dir, fn)` expecting a flow —
and recursive must return dir-relative names with FORWARD slashes, so a Windows walk produces
the same spelling an R2 listing does.
"""
from __future__ import annotations
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater import blob   # noqa: E402


def _tree(base):
    os.makedirs(os.path.join(base, "Regional"), exist_ok=True)
    os.makedirs(os.path.join(base, "NIUnderlyingDetail", "deep"), exist_ok=True)
    for rel in ("flat.parquet",
                "Regional/CAINC5S.parquet",
                "Regional/CAGDP2.parquet",
                "NIUnderlyingDetail/U20403.parquet",
                "NIUnderlyingDetail/deep/X.parquet",
                "notes.txt",                       # non-parquet, must be ignored
                "Regional/readme.md"):
        p = os.path.join(base, *rel.split("/"))
        with open(p, "wb") as f:
            f.write(b"")


def test_default_is_basenames_only(tmp_path):
    base = str(tmp_path / "bea")
    _tree(base)
    got = blob.list_parquets(base)
    assert got == ["flat.parquet"], (
        f"default listing must stay basenames-only, got {got} — a nested key reaching a "
        f"caller that does os.path.join(out_dir, fn) would be treated as a top-level flow")


def test_recursive_finds_the_nested_store(tmp_path):
    base = str(tmp_path / "bea")
    _tree(base)
    got = blob.list_parquets(base, recursive=True)
    assert got == sorted([
        "NIUnderlyingDetail/U20403.parquet",
        "NIUnderlyingDetail/deep/X.parquet",
        "Regional/CAGDP2.parquet",
        "Regional/CAINC5S.parquet",
        "flat.parquet",
    ]), got
    assert all("\\" not in n for n in got), (
        "names must use forward slashes so a Windows walk spells keys the way R2 does")


def test_recursive_result_joins_back_to_a_real_file(tmp_path):
    # The caller's contract: os.path.join(out_dir, rel) must address the file. This is what
    # _tree_frontier relies on, and it is the step where a separator mismatch would bite.
    base = str(tmp_path / "bea")
    _tree(base)
    for rel in blob.list_parquets(base, recursive=True):
        assert os.path.isfile(os.path.join(base, rel)), rel


def test_missing_dir_is_empty_not_an_error(tmp_path):
    missing = str(tmp_path / "nope")
    assert blob.list_parquets(missing) == []
    assert blob.list_parquets(missing, recursive=True) == []
