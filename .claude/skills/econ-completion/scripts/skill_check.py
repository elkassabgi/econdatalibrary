#!/usr/bin/env python3
"""Session preflight for the econ-completion skill.

Deterministic, stdlib-only. Run at the start of every working session:
    python skill_check.py [--econ E:/research/econfindatalibrary] [--hf D:/research/hfdatalibrary]
                          [--plan "D:/research/deepseek econ plan/ECONLIB_COMPLETION_PLAN.md"]

Exit 0  = preflight passed.
Exit 1  = a HARD requirement is missing or unverifiable: STOP and fix what is named.
Exit 2  = a SOFT requirement is missing: create the named file, then continue.

Never modifies anything. Never touches the network or Cloudflare.

--------------------------------------------------------------------------------
FIXED 2026-08-30 after the skill's own C5 was turned on this file. Three defects,
each proven by a case in tests/test_skill_check.py:

  1. FAIL-OPEN ON A REDIRECTED ROOT. `--hf`/`--econ` were advertised but did not
     redirect the HARD list: those paths carry no `{}` placeholders, so the
     `.format()` applied to them was dead code, while the ledger_check block DID
     use `args.hf`. Measured: `--hf D:/nonexistent_repo_xyz` printed
     "[OK] hf repo -> D:\\research\\hfdatalibrary", SKIPPED the ledger check
     (it was gated on `os.path.exists(args.hf)`), and exited 0. A wrong flag
     silently disabled the most important check and still passed.

  2. "CANNOT MEASURE" PASSED. An exception while running ledger_check.py was
     downgraded to a warning, so a crashed guard let the session proceed. The
     skill's own C5 says the except branch IS the guard: it must refuse.

  3. THE DOC AND THE CODE DISAGREED ON EXIT 2. SKILL.md said "non-zero means
     STOP"; this file said 2 = "allowed to continue". SKILL.md now states the
     tiers explicitly and this docstring is the contract.

Paths are resolved from the arguments throughout, so this file no longer hard-codes
a machine layout it cannot verify (R330: a hardcoded path is the most common way to
get "0 defects in 0 files examined").
"""
import argparse
import datetime as _dt
import os
import subprocess
import sys

ECON_DEFAULT = r"E:\research\econfindatalibrary"
HF_DEFAULT = r"D:\research\hfdatalibrary"
PLAN_DEFAULT = r"D:\research\deepseek econ plan\ECONLIB_COMPLETION_PLAN.md"

# Where the plan may live, in order. The plan started life outside version control;
# a repo copy is preferred because an unversioned scratch directory is one tidy-up
# away from hard-stopping every session.
PLAN_CANDIDATES = (
    os.path.join("{econ}", "docs", "ECONLIB_COMPLETION_PLAN.md"),
    os.path.join("{hf}", "docs", "ECONLIB_COMPLETION_PLAN.md"),
)


def _resolve_plan(explicit, econ, hf):
    """Return (path, ok). Prefer a repo copy; fall back to the explicit/default path."""
    for cand in PLAN_CANDIDATES:
        p = cand.format(econ=econ, hf=hf)
        if os.path.exists(p):
            return p, True
    return explicit, os.path.exists(explicit)


def run(econ, hf, plan, out=print):
    """Returns (exit_code, failures, warns). Pure enough to test."""
    plan_path, plan_ok = _resolve_plan(plan, econ, hf)

    hard = [
        ("econ repo", econ),
        ("hf repo", hf),
        ("plan", plan_path),
        ("ledger", os.path.join(hf, ".claude", "MISTAKES.md")),
        ("numbers", os.path.join(hf, ".claude", "NUMBERS.md")),
        ("ledger_check.py", os.path.join(
            hf, ".claude", "skills", "adversarial-review", "tools", "ledger_check.py")),
    ]
    soft = [
        ("WORKLOG.md (econ repo root)", os.path.join(econ, "WORKLOG.md")),
        ("this skill's SKILL.md", os.path.join(
            econ, ".claude", "skills", "econ-completion", "SKILL.md")),
    ]

    out("=" * 72)
    out("econ-completion preflight  " + _dt.datetime.now().isoformat(timespec="seconds"))
    out("=" * 72)

    failures, warns = [], []
    for name, path in hard:
        ok = os.path.exists(path)
        out(("  [OK]   " if ok else "  [MISS] ") + name + "  ->  " + path)
        if not ok:
            failures.append(name)
    for name, path in soft:
        ok = os.path.exists(path)
        out(("  [OK]   " if ok else "  [WARN] ") + name + "  ->  " + path)
        if not ok:
            warns.append(name)

    # The ledger check is the load-bearing one, and any inability to run it is a
    # FAILURE, never a warning. There is deliberately NO `else` here: an absent
    # ledger_check.py is already a HARD failure above, and a mutation test proved a
    # defensive else-branch to be unreachable dead code whose "coverage" came from the
    # hard list instead (R488 - a test that gives the right answer for the wrong reason
    # is not validated).
    ledger_check = os.path.join(
        hf, ".claude", "skills", "adversarial-review", "tools", "ledger_check.py")
    if os.path.exists(ledger_check):
        out("-" * 72)
        out("ledger_check.py --digest:")
        try:
            r = subprocess.run([sys.executable, ledger_check, "--digest"],
                               cwd=hf, capture_output=True, text=True, timeout=600)
            for line in (r.stdout + r.stderr).splitlines()[-6:]:
                out("    " + line)
            if r.returncode != 0:
                out("  [FAIL] ledger_check.py --digest exited %d - STOP and fix the ledger"
                    % r.returncode)
                failures.append("ledger_check --digest non-zero")
        except Exception as e:  # noqa: BLE001
            # C5: "cannot measure" must REFUSE. This branch used to warn.
            out("  [FAIL] could not run ledger_check.py: %s" % e)
            failures.append("ledger_check not runnable (cannot measure = refuse)")

    out("-" * 72)
    out("FIVE NON-NEGOTIABLES (re-read before work):")
    out(" 1. Every number carries its instrument and date.")
    out(" 2. A claim about the running system is settled only by the running system.")
    out(" 3. Prose rules do not hold - ship the enforcement with the rule.")
    out(" 4. Adversarial review runs in parallel with everything consequential.")
    out(" 5. RESERVED decisions stop the work until the owner releases them.")
    out("-" * 72)

    if failures:
        out("HARD FAILURES (%d): %s" % (len(failures), "; ".join(failures)))
        out("STOP. Fix the named items before doing any work.")
        return 1, failures, warns
    if warns:
        out("WARNINGS (%d): %s" % (len(warns), "; ".join(warns)))
        out("Create the named file, then continue.")
        return 2, failures, warns
    out("PASS - session may begin. Append today's intent to WORKLOG.md first.")
    return 0, failures, warns


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--econ", default=ECON_DEFAULT)
    ap.add_argument("--hf", default=HF_DEFAULT)
    ap.add_argument("--plan", default=PLAN_DEFAULT)
    a = ap.parse_args()
    code, _, _ = run(a.econ, a.hf, a.plan)
    return code


if __name__ == "__main__":
    sys.exit(main())
