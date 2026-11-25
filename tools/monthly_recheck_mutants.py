"""Mutation check for the data-clock re-check (design monthly_recheck_20261124/DESIGN.md section 9: each bound of the
predicate needs a mutant that a test kills, review C9 / R1154; R2-C1: each guard names its mutant).

RUN IT ON A COPY, NEVER ON A CHECKOUT. It edits tracked files on disk, one mutant at a time, so it refuses a root that
holds a .git directory. Make the copy with `git archive`:

    git archive --format=tar HEAD | tar -x -C F:/somewhere/copy
    python tools/monthly_recheck_mutants.py --root F:/somewhere/copy --basetemp F:/somewhere/bt
    python tools/monthly_recheck_mutants.py --root F:/somewhere/copy --basetemp F:/somewhere/bt --only no-lead

The procedure (the tools/release_watch_mutants.py pattern, with a byte-exact restore instead of git):
  1. control: every mutant's test selection passes on the unmutated copy;
  2. per mutant: each `old` must occur EXACTLY ONCE (a "\\n" in a mutant matches the file's own line ending, so a
     CRLF copy and an LF copy are both matched - R2208); run its selection; it is KILLED only when pytest reports a
     failed test (an import or collection error is not a kill);
  3. restore the original bytes and require their sha256 to match before the next mutant.
Exit 0 only when the control passes and every mutant is killed.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import time

BASE, ORCH, HEALTH, LAG = ("updater/strategies/base.py", "updater/orchestrate.py", "updater/health.py",
                           "tools/measure_ci_lag.py")
T = "tests/test_monthly_recheck.py"
TR = "tests/test_monthly_recheck_replay.py"
TP = "tests/test_replay_parity_declared_units.py"
TL = "tests/test_budget_skip_due_label.py"


def _guard(gid):
    return f"{T}::test_the_guards_through_the_label[{gid}]"


def _held(hid):
    return f"{T}::test_a_held_unit_whose_date_test_holds_says_why[{hid}]"


# (name, file, [(old, new), ...], pytest selection)
MUTANTS = [
    # ---- the date test (base.clock_recheck_due)
    ("regular-after-clock-removed", BASE, [("    return (regular > fire\n", "    return (True\n")],
     [_guard("G1-regular-check-first")]),
    ("regular-after-clock-inverted", BASE, [("    return (regular > fire\n", "    return (regular <= fire\n")],
     [_guard("G1-regular-check-first")]),
    ("no-lead", BASE, [("and now >= fire - timedelta(days=RECHECK_LEAD_DAYS)", "and True")],
     [_guard("G2-before-the-lead")]),
    ("no-cap", BASE, [("and now <= fire + timedelta(days=RECHECK_CAP_DAYS)", "and True")],
     [_guard("G3-past-the-cap")]),
    ("no-spacing", BASE, [("and _due_after(RECHECK_RETRY_DAYS, last_attempt_utc, now))", "and True)")],
     [_guard("G4-spacing")]),
    ("spacing-one-day", BASE, [("RECHECK_RETRY_DAYS = 0.5\n", "RECHECK_RETRY_DAYS = 1\n")],
     [f"{TR}::test_r4c3_a_pass_that_comes_6_h_earlier_on_the_clock_day_still_rechecks"]),
    # ---- who is covered (orchestrate.recheck_verdict)
    ("no-scope", ORCH, [("    if lat != RECHECK_CADENCE:\n", "    if False:\n")],
     [_guard("G8-scope")]),
    ("no-units-rule", ORCH, [("    elif n_units != 1:\n", "    elif False:\n")],
     [_held("units")]),
    ("no-window-stop-rule", ORCH, [("    elif (unit.config or {}).get(\"window_stop\") is True:\n", "    elif False:\n")],
     [_guard("G13-window-stop")]),
    ("no-cost-gate", ORCH, [("    elif cost_s > RECHECK_MAX_COST_S:\n", "    elif False:\n")],
     [_guard("G5-cost")]),
    ("no-history-gate", ORCH, [("        if bad:\n", "        if False:\n")],
     [_guard("G6-history-partial")]),
    ("strict-history-residual-py", ORCH,
     [("bad = [s for s in statuses_of() if s in RECHECK_HOLD_STATUSES]",
       "bad = [s for s in statuses_of() if s not in (\"ok\", \"no_change\")]")],
     [_guard("G6b-transient-does-not-hold")]),
    ("no-uv-exclusion", ORCH, [("        elif health.upstream_claim_covers(entry, n, now):\n", "        elif False:\n")],
     [_guard("G7-uv-live")]),
    ("uv-exclusion-ignores-expiry", HEALTH,
     [("    return upstream_claim(entry, newest_obs, now)[0] == \"live\"",
       "    return upstream_claim(entry, newest_obs, now)[0] is not None")],
     [_guard("G7b-uv-expired")]),
    ("held-said-when-the-date-test-fails", ORCH,
     [("now):\n        return False, None, fire, regular\n",
       "now) and cost_s is not None and cost_s <= RECHECK_MAX_COST_S:\n        return False, None, fire, regular\n")],
     [f"{T}::test_no_held_line_when_the_date_test_does_not_hold"]),
    # ---- the call sites (orchestrate._unit_due, the label)
    ("no-try-around-the-recheck", ORCH,
     [("    except Exception as e:  # noqa: BLE001 - the re-check adds passes; it must never take the run down\n",
       "    except ZeroDivisionError as e:  # mutant\n")],
     [f"{TR}::test_g12_an_exception_in_the_recheck_never_ends_the_run"]),
    ("regular-is-due-not-asked", ORCH,
     [("    if strat.is_due(unit, us, now=now):\n        return DueVerdict(True)\n", "")],
     [f"{TL}::test_the_due_check_still_follows_the_budget_gate_and_the_adapter_check"]),
    ("label-ignores-the-recheck", ORCH,
     [("        return \"due\" if _unit_due(get_strategy(unit.strategy), unit, us, store, entry=entry, costs=costs,\n"
       "                                  now=now) else \"not due\"\n",
       "        return \"due\" if get_strategy(unit.strategy).is_due(unit, us) else \"not due\"\n")],
     [f"{T}::test_f2_the_budget_skip_label_says_due_when_the_clock_fires_before_the_regular_check"]),
    # ---- the gate's clock, extracted (health) - G9 / G10
    ("data-clock-slack-widened", HEALTH,
     [("                 * (SLA_TOLERANCE + DATA_SLACK_PERIODS))\n",
       "                 * (SLA_TOLERANCE + DATA_SLACK_PERIODS + 0.1))\n")],
     [f"{T}::test_g9_g10_the_gate_reads_as_before[G9-red]",
      f"{T}::test_g10_the_extracted_clock_is_the_old_inline_formula_for_every_entry"]),
    ("publication-lag-unclamped", HEALTH,
     [("data_days += min(max(0.0, float(_lag)), publication_lag_cap(lat_cadence, period))",
       "data_days += max(0.0, float(_lag))")],
     [f"{T}::test_g9_g10_the_gate_reads_as_before[publication-lag-clamped]"]),
    ("red-data-branch-skipped", HEALTH,
     [("                health = \"RED-DATA\"       # job 'succeeds' but our NEWEST data is stale\n",
       "                health = \"OK\"\n")],
     [f"{T}::test_g9_g10_the_gate_reads_as_before[G9-red]"]),
    # ---- the replay (tools/measure_ci_lag.py UnitBook) - G11
    ("book-without-the-recheck", LAG,
     [("return self._regular_due(sid, now) or self._recheck_due(sid, now, costs)",
       "return self._regular_due(sid, now)")],
     [f"{TP}::test_the_replay_and_run_once_take_the_same_decisions_and_write_the_same_records[R-recheck]"]),
]


def _sha(p):
    with open(p, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _pytest(root, sel, basetemp):
    cmd = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", f"--basetemp={basetemp}", *sel]
    r = subprocess.run(cmd, cwd=root, capture_output=True, text=True)
    tail = (r.stdout + r.stderr).strip().splitlines()[-1:] or [""]
    return r.returncode, tail[0], r.stdout + r.stderr


def _apply(root, m) -> bytes:
    """Apply mutant m on disk; return the original bytes. Every edit must match exactly once."""
    name, f, edits, _sel = m
    p = os.path.join(root, f)
    with open(p, "rb") as fh:
        orig = fh.read()
    src = orig.decode("utf-8")
    nl = "\r\n" if "\r\n" in src else "\n"
    for old, new in edits:
        old, new = old.replace("\n", nl), new.replace("\n", nl)
        n = src.count(old)
        if n != 1:
            raise SystemExit(f"mutant {name}: expected 1 occurrence in {f}, found {n}: {old[:80]!r}")
        src = src.replace(old, new)
    with open(p, "wb") as fh:
        fh.write(src.encode("utf-8"))
    return orig


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="a git-archive COPY (no .git) - never a checkout")
    ap.add_argument("--basetemp", required=True)
    ap.add_argument("--only", action="append")
    a = ap.parse_args(argv)
    root = os.path.abspath(a.root)
    if os.path.exists(os.path.join(root, ".git")):
        print(f"refused: {root} holds .git - run on a `git archive` copy, never on a checkout")
        return 2
    todo = [m for m in MUTANTS if not a.only or m[0] in a.only]
    hashes = {m[1]: _sha(os.path.join(root, m[1])) for m in todo}
    started = time.time()
    for m in todo:                                       # 1. control
        rc, tail, _out = _pytest(root, m[3], a.basetemp)
        print(f"control  {m[0]:40} rc={rc}  {tail}", flush=True)
        if rc != 0:
            print("refused: a control selection fails on the unmutated copy")
            return 3
    killed = 0
    for m in todo:                                       # 2. one mutant at a time
        orig = _apply(root, m)
        try:
            rc, tail, _out = _pytest(root, m[3], a.basetemp)
        finally:
            with open(os.path.join(root, m[1]), "wb") as fh:          # 3. restore, byte-exact
                fh.write(orig)
        if _sha(os.path.join(root, m[1])) != hashes[m[1]]:
            print(f"STOP: {m[1]} does not match its original sha256 after restoring mutant {m[0]}")
            return 4
        ok = rc != 0 and " failed" in tail
        killed += ok
        print(f"mutant   {m[0]:40} {'KILLED' if ok else 'SURVIVED'} rc={rc}  {tail}", flush=True)
    print(f"{killed}/{len(todo)} mutants killed; every file restored to its sha256; {time.time() - started:.0f} s")
    return 0 if killed == len(todo) else 1


if __name__ == "__main__":
    sys.exit(main())
