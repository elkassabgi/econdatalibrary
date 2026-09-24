"""THE machine-wide CUTOVER flag of the econ self-hosting move (docs/ECON_SELF_HOSTING_PLAN.md, section 3,
changes 4b and 5).

After T0 the cloud copy (the econ R2 bucket and the two econ D1 catalogue databases) is frozen: nothing may
write to it. Every write chokepoint (the R2 client hook, d1_remote(), the updater's local mode) asks ONE
question - is_cut_over() - and this module is the only place that answers it.

WHY IT IS BUILT THIS WAY (review R1167 B and the plan):
  * ONE file for the whole machine, not one per checkout: there are dozens of worktrees, and a flag in
    one of them would leave every other one writing.
  * The path is a MODULE CONSTANT with no environment, argument or config override. An override lets any
    process point the check at a missing file, and a missing file means "not cut over" - so an override
    is a way to write to the frozen copy. Tests monkeypatch FLAG_PATH; tests/test_cutover.py fails on any
    environment read in this module.
  * os.stat, never os.path.exists: exists() turns a PermissionError into "absent", i.e. into "not cut
    over". Here only FileNotFoundError and NotADirectoryError mean "not cut over" (so the ubuntu CI
    runners, where the path never exists, keep writing until T0); ANY other failure means "cut over"
    (fail closed).
  * Ahmed creates C:\\ProgramData\\econ elevated, with an ACL that lets Users only read it, and creates the
    flag file at T0. No code creates or deletes it.
"""
from __future__ import annotations

import os

FLAG_PATH = r"C:\ProgramData\econ\CUTOVER"


class CutoverRefused(RuntimeError):
    """A write to the retired cloud copy was attempted after T0."""


def is_cut_over() -> bool:
    """True once the flag file exists, and whenever its state cannot be read (fail closed)."""
    try:
        os.stat(FLAG_PATH)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except Exception:  # noqa: BLE001 - PermissionError, OSError, anything: cannot tell = cut over
        return True
    return True


def refuse_if_cut_over(what: str) -> None:
    """Raise CutoverRefused naming `what` once the cloud copy is retired."""
    if is_cut_over():
        raise CutoverRefused(
            f"refused: {what} - econ is self-hosted since T0 ({FLAG_PATH} exists or cannot be read); "
            "the cloud copy is frozen and nothing may write to it")
