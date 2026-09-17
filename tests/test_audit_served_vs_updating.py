"""Served vs updating vs not served — the standing question, asked with one command.

WHY. This was answered by hand on 2026-09-06 by composing four helpers, and a question answered by
hand is answered differently each time. The populations must come from the existing sources of
truth, not from a fresh definition (R262).

Two things this pins that a hand answer got wrong once each:

  * SERVED must subtract the GATE. `SUPPORTED_SOURCES` alone only means the worker can resolve an
    id; any gated id in it answers 451. Forgetting the subtraction overstates what is
    served, and the first hand answer did exactly that - it reported `denylisted: 0` because a
    regex silently matched nothing, and no warning fired.
  * "no source_state row" IS NOT "not updating". That table belongs to the CLOUD updater; the big
    local crawlers never write it, so it reports "never scheduled" about processes that are
    running (R838). It must be printed as explicitly not a verdict.
"""
from __future__ import annotations

import importlib.util
import io
import os
import pytest
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "audit_served_vs_updating.py")


_REPO = os.path.dirname(_HERE) if "_HERE" in dir() else os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))
_STATE = os.path.join(_REPO, "data", "_aqueduct", "state.db")
# NEEDS THE REAL STATE DATABASE (R857). `tools/audit_served_vs_updating.py:84` opens
# data/_aqueduct/state.db read-only; a runner has no such file, the subprocess exits non-zero,
# and this test asserts returncode == 0. Desktop only, and it says which file it wanted.
needs_state = pytest.mark.skipif(
    not os.path.exists(_STATE),
    reason=f"needs the real state database at {_STATE} - desktop only")

def _load():
    spec = importlib.util.spec_from_file_location("_svu_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_the_denylist_parses_against_the_real_file():
    """Calls the REAL function against the REAL artifact. Four mutants once survived in a sibling
    tool because every test monkeypatched this and the suite ran under a stub (R840)."""
    ids = _load().denylisted()
    assert len(ids) >= 40, len(ids)
    assert all(isinstance(i, str) and i.islower() for i in ids), sorted(ids)[:5]


def test_a_short_denylist_is_REFUSED(tmp_path):
    """FAIL CLOSED, and something must TRIP the guard or it is untested. A half-matching regex
    yields a SHORT list, not an empty one, and a short list OVERSTATES what is served — the one
    direction that matters."""
    m = _load()
    d = tmp_path / "api" / "worker" / "src"
    d.mkdir(parents=True)
    io.open(str(d / "denylist.ts"), "w", encoding="utf-8").write(
        "export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([\n"
        '  "one",\n  "two",\n]);\n')
    m.ROOT = str(tmp_path)
    try:
        got = m.denylisted()
    except RuntimeError as e:
        assert "implausibly few" in str(e), str(e)
    else:
        raise AssertionError(f"a 2-id denylist was accepted: {got}")


def test_an_unparseable_denylist_is_REFUSED(tmp_path):
    """The other failure shape: the declaration renamed or reformatted away entirely."""
    m = _load()
    d = tmp_path / "api" / "worker" / "src"
    d.mkdir(parents=True)
    io.open(str(d / "denylist.ts"), "w", encoding="utf-8").write(
        "export const SOMETHING_ELSE = new Set([]);\n")
    m.ROOT = str(tmp_path)
    try:
        m.denylisted()
    except RuntimeError as e:
        assert "could not parse" in str(e), str(e)
    else:
        raise AssertionError("an unparseable denylist was accepted")


@needs_state
def test_it_runs_and_the_gate_is_still_applied_to_served():
    """End to end against the real repo: the gate must still stand between SUPPORTED_SOURCES and
    SERVED, and the printed arithmetic must hold.

    THE ASSERTION CHANGED 2026-09-17, BECAUSE ITS PREMISE DID (and it had been failing here since
    2026-09-16 without anyone seeing it). This used to read `served < supported`, on the reasoning
    the docstring at the top of this file still explains: a gated id was IN the allowlist and
    answered 451, so subtracting the gate had to make the number smaller. The owner-ordered removal
    took those ids out of `SUPPORTED_SOURCES` altogether, so the subtraction now removes nothing
    and the two counts are equal. `served < supported` therefore asserts the OLD arrangement, and
    on this machine it failed.

    What must still be true is the property, not the inequality: no gated id may sit in the
    allowlist, and `served` may never EXCEED `supported` (which would mean the gate was skipped).
    Stated that way the guard survives either arrangement — if a gated id is ever re-added to the
    allowlist the inequality returns on its own, and the membership check fails immediately either
    way.

    Note this test is `@needs_state`, so it SKIPS on a CI runner and only ever runs here. That is
    why the stale assertion sat red for a day with the workflow green: the copy that can see real
    numbers is the copy nobody watches.
    """
    import subprocess

    repo = os.path.dirname(_HERE)
    r = subprocess.run([sys.executable, os.path.join("tools", "audit_served_vs_updating.py")],
                       cwd=repo, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-600:]
    out = r.stdout

    def num(label):
        for ln in out.splitlines():
            if ln.strip().startswith(label):
                return int(ln.rsplit(":", 1)[1].split()[0].replace(",", ""))
        raise AssertionError(f"line not found: {label!r}\n{out}")

    supported = num("SUPPORTED_SOURCES")
    served = num("SERVED = supported minus gated")
    assert served <= supported, (
        f"SERVED ({served}) exceeds SUPPORTED_SOURCES ({supported}) — the gate cannot ADD "
        f"sources, so the arithmetic in the tool is wrong")

    # The property the inequality used to stand in for. Read from the TOOL's own output rather
    # than re-parsed here: the tool already computes this intersection, its denylist parser
    # fails closed on an unparseable or implausibly short list, and this file's own docstring
    # records what a second hand-rolled regex cost last time ("it reported `denylisted: 0`
    # because a regex silently matched nothing, and no warning fired"). R249: the run and the
    # test call the same code.
    import re
    m = re.search(r"\((\d+) of them in SUPPORTED_SOURCES\)", out)
    assert m, ("the tool no longer prints the gated/SUPPORTED_SOURCES intersection, so this "
               "property cannot be checked — treat that as a failure, not a pass:\n" + out)
    assert supported > 0, f"SUPPORTED_SOURCES parsed as {supported}; the check would be vacuous"
    assert int(m.group(1)) == 0, (
        "a gated id is present in SUPPORTED_SOURCES. That is the arrangement the removal undid "
        "and it must not come back by accident. The ids are deliberately not reproduced here; "
        "run tools/audit_served_vs_updating.py to see the tool's own output")

    assert "NOT A VERDICT" in out, out
    assert "LOCAL route never" in out, out       # the local-route caveat must be printed


def test_the_no_state_bucket_is_never_called_a_failure():
    """R838: judging the local-route crawlers by the cloud updater's table reports 'never
    scheduled' about processes that are running. The wording is the guard."""
    src = io.open(_TOOL, encoding="utf-8").read()
    assert "NOT A VERDICT" in src, "the no-state bucket must not read as a failure count"
    assert "LOCAL route" in src or "LOCAL crawlers" in src or "local route" in src.lower(), src[:0]
