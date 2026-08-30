#!/usr/bin/env python3
"""Session preflight for the econ-completion skill.

Deterministic, stdlib-only. Run at the start of every working session:
    python skill_check.py [--econ E:/research/econfindatalibrary] [--hf D:/research/hfdatalibrary]

Exit 0  = preflight passed.
Exit 1  = a HARD requirement is missing: stop and fix what is named.
Exit 2  = a SOFT requirement is missing (warn, but allowed to continue).

Never modifies anything. Never touches the network or Cloudflare.
"""
import argparse
import os
import subprocess
import sys
import datetime as _dt

ECON_DEFAULT = r"E:\research\econfindatalibrary"
HF_DEFAULT = r"D:\research\hfdatalibrary"
PLAN_PATH = r"D:\research\deepseek econ plan\ECONLIB_COMPLETION_PLAN.md"

HARD = [
    ("econ repo", ECON_DEFAULT),
    ("hf repo", HF_DEFAULT),
    ("plan", PLAN_PATH),
    ("ledger", os.path.join(HF_DEFAULT, ".claude", "MISTAKES.md")),
    ("numbers", os.path.join(HF_DEFAULT, ".claude", "NUMBERS.md")),
    ("ledger_check.py", os.path.join(HF_DEFAULT, ".claude", "skills", "adversarial-review", "tools", "ledger_check.py")),
]
SOFT = [
    ("WORKLOG.md (econ repo root)", os.path.join(ECON_DEFAULT, "WORKLOG.md")),
    ("this skill's SKILL.md", os.path.join(ECON_DEFAULT, ".claude", "skills", "econ-completion", "SKILL.md")),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--econ", default=ECON_DEFAULT)
    ap.add_argument("--hf", default=HF_DEFAULT)
    args = ap.parse_args()

    hard = [(n, p if "{" not in p else p.format(econ=args.econ, hf=args.hf)) for n, p in HARD]
    soft = [(n, p.format(econ=args.econ, hf=args.hf)) for n, p in SOFT]

    print("=" * 72)
    print("econ-completion preflight  " + _dt.datetime.now().isoformat(timespec="seconds"))
    print("=" * 72)

    failures = []
    for name, path in hard:
        ok = os.path.exists(path)
        print(("  [OK]   " if ok else "  [MISS] ") + name + "  ->  " + path)
        if not ok:
            failures.append(name)

    warns = []
    for name, path in soft:
        ok = os.path.exists(path)
        print(("  [OK]   " if ok else "  [WARN] ") + name + "  ->  " + path)
        if not ok:
            warns.append(name)

    ledger_check = os.path.join(args.hf, ".claude", "skills", "adversarial-review", "tools", "ledger_check.py")
    if os.path.exists(ledger_check) and os.path.exists(args.hf):
        print("-" * 72)
        print("ledger_check.py --digest:")
        try:
            r = subprocess.run(
                [sys.executable, ledger_check, "--digest"],
                cwd=args.hf, capture_output=True, text=True, timeout=600,
            )
            for line in (r.stdout + r.stderr).splitlines()[-6:]:
                print("    " + line)
            if r.returncode != 0:
                print("  [FAIL] ledger_check.py --digest exited %d — STOP and fix the ledger first" % r.returncode)
                failures.append("ledger_check --digest non-zero")
        except Exception as e:  # noqa: BLE001
            print("  [ERR ] could not run ledger_check.py: %s" % e)
            warns.append("ledger_check not runnable")

    print("-" * 72)
    print("FIVE NON-NEGOTIABLES (re-read before work):")
    print(" 1. Every number carries its instrument and date.")
    print(" 2. A claim about the running system is settled only by the running system.")
    print(" 3. Prose rules do not hold - ship the enforcement with the rule.")
    print(" 4. Adversarial review runs in parallel with everything consequential.")
    print(" 5. RESERVED decisions stop the work until the owner releases them.")
    print("-" * 72)

    if failures:
        print("HARD FAILURES (%d): %s" % (len(failures), "; ".join(failures)))
        print("STOP. Fix the named items before doing any work.")
        return 1
    if warns:
        print("WARNINGS (%d): %s" % (len(warns), "; ".join(warns)))
        print("Create the missing WORKLOG.md / install the skill if they apply to this session.")
        return 2
    print("PASS — session may begin. Append today's intent to WORKLOG.md first.")
    return 0


if __name__ == "__main__":
    sys.exit(main())