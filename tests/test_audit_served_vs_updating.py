"""Served vs updating vs not served — the standing question, asked with one command.

WHY. This was answered by hand on 2026-09-06 by composing four helpers, and a question answered by
hand is answered differently each time. The populations must come from the existing sources of
truth, not from a fresh definition (R262).

Two things this pins that a hand answer got wrong once each:

  * SERVED must subtract the GATE. `SUPPORTED_SOURCES` alone only means the worker can resolve an
    id; a gated source is in it and answers 451. Forgetting the subtraction overstates what is
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
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "audit_served_vs_updating.py")


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


def test_it_runs_and_subtracts_the_gate_from_served():
    """End to end against the real repo: SERVED must be strictly smaller than SUPPORTED_SOURCES,
    and the printed arithmetic must hold. If the gate were dropped the two would be equal."""
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
    assert served < supported, (served, supported)
    assert "NOT A VERDICT" in out, out
    assert "LOCAL route never" in out, out       # the local-route caveat must be printed


def test_the_no_state_bucket_is_never_called_a_failure():
    """R838: judging the local-route crawlers by the cloud updater's table reports 'never
    scheduled' about processes that are running. The wording is the guard."""
    src = io.open(_TOOL, encoding="utf-8").read()
    assert "NOT A VERDICT" in src, "the no-state bucket must not read as a failure count"
    assert "LOCAL route" in src or "LOCAL crawlers" in src or "local route" in src.lower(), src[:0]
