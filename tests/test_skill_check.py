"""Discriminating cases for the econ-completion preflight.

The skill's own rule C5: "a guard ships with a discriminating pair - one case it MUST
block, one it MUST let through - in the same commit, and the guard's `except` branch IS
the guard ('cannot measure' must refuse, never pass)."

`skill_check.py` shipped with no test at all, and turning C5 on it found three real
defects. Every case below is one of those defects, or the pass case that proves the
guard is not simply refusing everything (R414: a guard that blocks a legitimate seed
passes a "refuses a tiny state" test just as well as a correct one).
"""
import importlib.util
import os
import stat
import subprocess
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", ".claude", "skills", "econ-completion",
                      "scripts", "skill_check.py")


def _load():
    spec = importlib.util.spec_from_file_location("skill_check", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fake_world(tmp, *, worklog=True, skillmd=True, ledger_check="pass",
                ledger=True, numbers=True, plan=True):
    """Build a minimal econ/hf pair on disk. ledger_check: pass|fail|absent|raise."""
    econ = os.path.join(tmp, "econ")
    hf = os.path.join(tmp, "hf")
    os.makedirs(os.path.join(econ, ".claude", "skills", "econ-completion"), exist_ok=True)
    os.makedirs(os.path.join(hf, ".claude", "skills", "adversarial-review", "tools"),
                exist_ok=True)
    if worklog:
        open(os.path.join(econ, "WORKLOG.md"), "w").close()
    if skillmd:
        open(os.path.join(econ, ".claude", "skills", "econ-completion", "SKILL.md"),
             "w").close()
    if ledger:
        open(os.path.join(hf, ".claude", "MISTAKES.md"), "w").close()
    if numbers:
        open(os.path.join(hf, ".claude", "NUMBERS.md"), "w").close()

    plan_path = os.path.join(tmp, "plan.md")
    if plan:
        open(plan_path, "w").close()

    lc = os.path.join(hf, ".claude", "skills", "adversarial-review", "tools",
                      "ledger_check.py")
    if ledger_check != "absent":
        # Only the two states a fixture can actually create. A "raise" variant was
        # removed: it wrote a script that exits 0 and raises nothing, so it was dead
        # code inside the very suite that exists to prove cases are real. The
        # unrunnable case is produced by monkeypatching subprocess.run instead.
        body = {
            "pass": "import sys\nprint('ledger: ok')\nsys.exit(0)\n",
            "fail": "import sys\nprint('ledger: BAD')\nsys.exit(1)\n",
        }[ledger_check]
        with open(lc, "w") as fh:
            fh.write(body)
    return econ, hf, plan_path


def test_pass_case_everything_present(tmp_path):
    """MUST LET THROUGH. Without this, a guard that refuses everything looks correct."""
    econ, hf, plan = _fake_world(str(tmp_path))
    code, failures, warns = _load().run(econ, hf, plan, out=lambda *a: None)
    assert code == 0, "a complete world must pass, got %d (%s)" % (code, failures)
    assert failures == [] and warns == []


def test_redirected_root_is_not_ignored(tmp_path):
    """MUST BLOCK. The measured fail-open: --hf pointed at a nonexistent directory

    used to print "[OK] hf repo -> <the hardcoded default>", skip the ledger check,
    and exit 0."""
    econ, hf, plan = _fake_world(str(tmp_path))
    bad_hf = os.path.join(str(tmp_path), "nonexistent_hf")
    code, failures, _ = _load().run(econ, bad_hf, plan, out=lambda *a: None)
    assert code == 1, "a bad --hf must FAIL, got %d" % code
    assert any("hf repo" in f for f in failures), failures
    # and it must not silently skip the ledger verification
    assert any("ledger" in f for f in failures), \
        "a missing hf root must also surface the unverifiable ledger, got %s" % failures


def test_missing_ledger_check_refuses(tmp_path):
    """MUST BLOCK. 'cannot measure' refuses - the guard's absence is not a pass.

    Note precisely what this proves: the refusal comes from the HARD path list, not
    from a branch beside the subprocess call. A mutation test showed a defensive
    `else` there was unreachable and that this assertion was passing via the hard
    list instead - the right answer for the wrong reason (R488). The dead branch was
    removed rather than left to look like coverage.
    """
    econ, hf, plan = _fake_world(str(tmp_path), ledger_check="absent")
    code, failures, _ = _load().run(econ, hf, plan, out=lambda *a: None)
    assert code == 1, "an absent ledger_check must FAIL, got %d" % code
    assert any("ledger_check.py" == f for f in failures), \
        "the hard-list entry is what must fire, got %s" % failures


def test_failing_ledger_check_fails(tmp_path):
    """MUST BLOCK. A non-zero ledger_check is the condition the preflight exists for."""
    econ, hf, plan = _fake_world(str(tmp_path), ledger_check="fail")
    code, failures, _ = _load().run(econ, hf, plan, out=lambda *a: None)
    assert code == 1, "a failing ledger_check must FAIL, got %d" % code
    assert any("non-zero" in f for f in failures), failures


def test_unrunnable_ledger_check_fails_not_warns(tmp_path):
    """MUST BLOCK. The except branch IS the guard (C5); it used to warn and continue."""
    econ, hf, plan = _fake_world(str(tmp_path))
    mod = _load()
    real = subprocess.run

    def boom(*a, **k):
        raise OSError("simulated: interpreter unavailable")

    mod.subprocess.run = boom
    try:
        code, failures, _ = mod.run(econ, hf, plan, out=lambda *a: None)
    finally:
        mod.subprocess.run = real
    assert code == 1, "an unrunnable ledger_check must FAIL, got %d" % code
    assert any("not runnable" in f for f in failures), failures


def test_missing_worklog_is_soft(tmp_path):
    """MUST NOT BLOCK, but must not be silent either: exit 2, named."""
    econ, hf, plan = _fake_world(str(tmp_path), worklog=False)
    code, failures, warns = _load().run(econ, hf, plan, out=lambda *a: None)
    assert code == 2, "a missing WORKLOG is soft, got %d" % code
    assert failures == []
    assert any("WORKLOG" in w for w in warns), warns


def test_missing_plan_blocks(tmp_path):
    """MUST BLOCK. The plan is a hard dependency; losing it must not be silent."""
    econ, hf, plan = _fake_world(str(tmp_path), plan=False)
    code, failures, _ = _load().run(econ, hf, plan, out=lambda *a: None)
    assert code == 1, "an absent plan must FAIL, got %d" % code
    assert any("plan" in f for f in failures), failures


def test_plan_is_found_in_a_repo_copy_when_present(tmp_path):
    """A repo copy is preferred over the unversioned scratch path, and satisfies the check."""
    econ, hf, plan = _fake_world(str(tmp_path), plan=False)   # scratch copy absent
    os.makedirs(os.path.join(econ, "docs"), exist_ok=True)
    open(os.path.join(econ, "docs", "ECONLIB_COMPLETION_PLAN.md"), "w").close()
    code, failures, _ = _load().run(econ, hf, plan, out=lambda *a: None)
    assert code == 0, "a repo copy of the plan must satisfy the check, got %d (%s)" % (
        code, failures)


# --------------------------------------------------------------------------- sync
# The plan mandates installing this skill into BOTH repos. Nothing enforced that the
# two copies agree, and on 2026-08-30 they actually DIVERGED for about twenty minutes:
# the econ copy had the fail-open fixed while the hf copy still printed "PASS ... EXIT=0"
# for a nonexistent --hf root. CI only ever exercises the econ copy, so the divergence
# was invisible. These cases make a drift between the two installs a test failure.

HF_SKILL = r"D:\research\hfdatalibrary\.claude\skills\econ-completion"
ECON_SKILL = os.path.join(HERE, "..", ".claude", "skills", "econ-completion")

_SYNCED = ["SKILL.md",
           os.path.join("scripts", "skill_check.py"),
           os.path.join("references", "protocols.md"),
           os.path.join("references", "failure-classes.md"),
           os.path.join("references", "state-baseline.md"),
           os.path.join("references", "phase-playbooks.md")]


@pytest.mark.parametrize("rel", _SYNCED)
def test_both_installed_copies_are_identical(rel):
    econ_f = os.path.normpath(os.path.join(ECON_SKILL, rel))
    hf_f = os.path.normpath(os.path.join(HF_SKILL, rel))
    if not os.path.exists(hf_f):
        pytest.skip("hf copy not present on this machine: %s" % hf_f)
    a = open(econ_f, "rb").read().replace(b"\r\n", b"\n")
    b = open(hf_f, "rb").read().replace(b"\r\n", b"\n")
    assert a == b, (
        "the two installed skill copies have DIVERGED for %s.\n"
        "  econ: %s (%d bytes)\n  hf  : %s (%d bytes)\n"
        "Re-sync them; a divergence on the fail-open case went undetected for 20 minutes "
        "on 2026-08-30 because CI only tests the econ copy." % (rel, econ_f, len(a), hf_f, len(b)))
