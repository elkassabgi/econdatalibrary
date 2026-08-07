"""A budget without a way to resume is a TRUNCATION, not a budget — enforced here.

Deadline's own docstring has said this for weeks: sub-unit lists in this package are
overwhelmingly stable in order, so a bound over a fixed order re-walks the same PREFIX every
run and the tail is never fetched at all — "a silent outage wearing a reassuring `partial`".
Saying it in a docstring did not stop it happening three more times.

Found 2026-08-07 by auditing every Deadline-using fetcher, and each one verified against the
real store and run history rather than taken on the auditor's word:

    ecb           540 sorted files, 35-min budget. Four consecutive runs deferred 280/349/
                  338/307, always a SUFFIX; best prefix ever reached 260 of 540. Indices
                  260-539 — 280 files over 107 agency__flow groups, including euro reference
                  rates, HICP, yield curves and 64 ESTAT__QSA files — had NEVER been fetched.
    ssb           186 sorted grp_* files, 40-min budget. A 2,401 s run (exactly the budget)
                  deferred 135 sub-units from grp_Fb (sorted index 53) onward; ~71% of groups
                  never reached.
    insee_melodi  145 flows, 25-min budget. Six flows holding ZERO rows sat at positions
                  123-143 and could never be pulled.

So a fetcher that bounds a sweep MUST do one of:
  (a) rotate its starting point — load_rotation / save_rotation / rotate_after; or
  (b) skip already-fresh sub-units cheaply via a per-sub-unit sidecar, so every run starts
      somewhere new by construction (eia, zillow, bis, fed_board work this way).

This test is a RATCHET, not a full verdict. It cannot tell (b) from "no mechanism at all" by
static inspection — that needs reading the loop, which is what the audit did. What it CAN do
is refuse to let the unreviewed population grow: every name below was audited on 2026-08-07,
so a NEW entry means someone added a budgeted sweep without resumption and must either wire
rotation or justify a sidecar here, with the reason written down.
"""
from __future__ import annotations

import os
import re

FETCHER_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "updater", "strategies", "fetchers")

# Audited 2026-08-07. Each of these bounds a sweep but does NOT call rotate_after; the audit
# read every one and found a per-sub-unit freshness sidecar, a single-unit loop, or a list
# that always fits the budget. Removing a name is fine (it means rotation was added);
# ADDING one requires reading the loop and recording why it is safe.
NO_ROTATION_REVIEWED = {
    "_dbnomics", "bea", "bis", "boc", "comtrade", "cso", "dst", "eia", "ember",
    "fed_board", "idb", "ilostat", "ksh_stadat", "snb", "stat_slovenia", "stats_nz",
    "zillow",
}


def _budgeted_without_rotation() -> set[str]:
    out = set()
    for fn in sorted(os.listdir(FETCHER_DIR)):
        if not fn.endswith(".py") or fn in ("__init__.py", "_common.py"):
            continue
        src = open(os.path.join(FETCHER_DIR, fn), encoding="utf-8").read()
        if "Deadline(" not in src:
            continue
        if not re.search(r"\brotate_after\s*\(", src):
            out.add(fn[: -len(".py")])
    return out


def test_no_new_budgeted_sweep_without_resumption():
    """The ratchet: the unreviewed set may shrink, never grow."""
    new = _budgeted_without_rotation() - NO_ROTATION_REVIEWED
    assert not new, (
        f"{sorted(new)} bound a sweep with Deadline but never call rotate_after. A budget "
        f"over a fixed-order list re-walks the same prefix forever and the tail is NEVER "
        f"fetched, while the run reports `partial` with nothing failing — that is how ecb "
        f"left 280 of 540 files and ssb ~71% of its groups unfetched. Either wire "
        f"load_rotation/save_rotation/rotate_after, or confirm by READING THE LOOP that a "
        f"per-sub-unit freshness sidecar makes every run start somewhere new, and add the "
        f"name to NO_ROTATION_REVIEWED with the reason.")


def test_the_three_repaired_fetchers_still_rotate():
    """Guards the actual 2026-08-07 fixes against a silent revert."""
    missing = {f for f in ("ecb", "ssb", "insee_melodi")
               if f in _budgeted_without_rotation()}
    assert not missing, f"{sorted(missing)} lost its rotation bookmark — see ledger R190/R377"


def test_rotation_is_saved_after_the_deferral_branch():
    """A bookmark stamped BEFORE the deferral check names a sub-unit that was deferred, so
    the next run skips exactly what the deferral promised to return to."""
    for name in ("ecb", "ssb", "insee_melodi"):
        src = open(os.path.join(FETCHER_DIR, f"{name}.py"), encoding="utf-8").read()
        i_save = src.index("save_rotation(")
        i_defer = src.index("deferred_unit(")
        assert i_save > i_defer, (
            f"{name}: save_rotation appears before the deferred_unit branch, which would "
            f"bookmark a sub-unit that was never worked on")
