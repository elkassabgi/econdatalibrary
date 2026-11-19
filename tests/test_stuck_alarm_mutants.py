"""Mutation tests for the stuck-run alarm: each named mutant of updater/health.py must make tests/test_stuck_alarm.py
FAIL. A check that cannot fail is not a check (econ-updater rule 10; "receipts must be able to fail").

How: a passing CONTROL run of tests/test_stuck_alarm.py first (no mutant), then each mutant ONE AT A TIME in its
own pytest subprocess. tests/test_stuck_alarm.py reads STUCK_ALARM_MUTANT at import, applies the textual
replacement below to the source of updater/health.py and execs it into the same module object, so every importer
(send_digest included) runs the mutant. Nothing is written to the checkout. A mutant whose target text is not
found EXACTLY ONCE fails here, so a refactor cannot silently turn a mutant into a no-op.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HEALTH = os.path.join(ROOT, "updater", "health.py")
SUITE = os.path.join(ROOT, "tests", "test_stuck_alarm.py")

# name -> (exact text in updater/health.py, replacement)
MUTANTS = {
    "1_killed_not_a_failure": (
        '_STUCK_FAIL_STATUSES = ("transient_fail", "definitive_fail", "killed_external")',
        '_STUCK_FAIL_STATUSES = ("transient_fail", "definitive_fail")'),
    "2_strict_threshold": (
        "if k >= STUCK_RUNS and", "if k > STUCK_RUNS and"),
    "3_truncated_breaks": (
        "            unknown += 1\n", "            break\n"),
    "4_ignore_expiry": (
        'if today > a["expires"]:', "if False:"),
    "5_no_all_failed_partial": (
        'if status == "partial" and _stuck_all_failed(note):', "if False:"),
    "6_note_first_200_chars": (
        "m = _STUCK_ALL_FAILED.search(note)", "m = _STUCK_ALL_FAILED.search(note[:200])"),
    "7_no_span_check": (
        "and span_h >= STUCK_MIN_SPAN_HOURS:", "and True:"),
    "8_neutral_breaks": (
        '        if kind == "neutral":\n            continue\n', '        if kind == "neutral":\n            break\n'),
    "9_ack_hides": (
        "acknowledged.append(variant)", "pass"),
}


# A NO-OP mutant (a comment changes): it MUST survive. Without it, a harness that reported every run as "killed"
# (an import error, a wrong path) would pass this file.
NOOP = ("NOOP", ("# PHASE 1 CHANGES NO VERDICT AND NO GATE.", "# PHASE 1 CHANGES NO VERDICT AND NO GATE (noop)."))


def mutated_health_source(name: str) -> str:
    src = open(HEALTH, encoding="utf-8").read()
    old, new = dict(MUTANTS, **dict([NOOP]))[name]
    n = src.count(old)
    if n != 1:
        raise AssertionError(f"mutant {name}: target found {n} times in updater/health.py (must be exactly 1)")
    return src.replace(old, new)


@pytest.mark.parametrize("name", sorted(MUTANTS) + [NOOP[0]])
def test_each_mutant_target_exists_exactly_once(name):
    mutated_health_source(name)


def _run_suite(tmp_path, mutant):
    env = dict(os.environ)
    env.pop("STUCK_ALARM_MUTANT", None)
    if mutant:
        env["STUCK_ALARM_MUTANT"] = mutant
    base = tmp_path / (mutant or "control")
    return subprocess.run(
        [sys.executable, "-m", "pytest", SUITE, "-q", "-x", "-p", "no:cacheprovider", "--basetemp", str(base)],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=900)


def test_control_passes_then_every_mutant_is_killed(tmp_path):
    control = _run_suite(tmp_path, None)
    assert control.returncode == 0, "the CONTROL run failed, so a killed mutant would prove nothing:\n" + \
        control.stdout[-3000:] + control.stderr[-2000:]
    noop = _run_suite(tmp_path, NOOP[0])
    assert noop.returncode == 0, "the NO-OP mutant was 'killed', so the harness cannot tell a survivor:\n" + \
        noop.stdout[-3000:] + noop.stderr[-2000:]
    survivors, killed_by = [], {}
    for name in sorted(MUTANTS):                       # one at a time, each in its own process
        r = _run_suite(tmp_path, name)
        # KILLED means a TEST FAILED. A collection or import error is a broken mutant, not a kill.
        if r.returncode == 0 or not re.search(r"\b\d+ failed\b", r.stdout) or re.search(r"\b\d+ errors?\b", r.stdout):
            survivors.append(f"{name} (exit {r.returncode}): {r.stdout[-400:]}")
            continue
        m = re.search(r"FAILED \S+::(\S+)", r.stdout)
        killed_by[name] = m.group(1) if m else "?"
    print("killed by:", killed_by)
    assert not survivors, f"mutant(s) the suite did not kill: {survivors}"
