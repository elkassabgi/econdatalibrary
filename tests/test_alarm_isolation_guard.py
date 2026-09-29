"""The alarm-isolation guard in tests/conftest.py can FAIL: each kind of leaked process-wide alarm state fails the
leaking test by name, and a test that restores what it changes passes. Runs the REAL conftest in an isolated
pytest (pytester, subprocess) so the leak cannot reach this process."""
import os
import textwrap

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFTEST = os.path.join(ROOT, "tests", "conftest.py")


def _run(pytester, body):
    src = open(CONFTEST, encoding="utf-8").read()
    pytester.makeconftest(f"import sys\nsys.path.insert(0, {ROOT!r})\n" + src)
    pytester.makepyfile(test_leak=textwrap.dedent(body))
    return pytester.runpytest_subprocess("-q", "-p", "no:cacheprovider", "-p", "pytester")


LEAKS = {
    "defer": ("from updater import orchestrate\norchestrate._DEFER_ALARM = True", "_DEFER_ALARM=True"),
    "pending": ("from updater import orchestrate\norchestrate._ALARM_PENDING = RuntimeError('x')", "_ALARM_PENDING"),
    "module": ("import sys, types\nsys.modules['updater.orchestrate'] = types.SimpleNamespace()",
               "sys.modules['updater.orchestrate'] replaced"),
    "fired": ("from updater import orchestrate\norchestrate.UNIT_TIMEOUT_FIRED = True", "UNIT_TIMEOUT_FIRED=True"),
    "sigint": ("import signal\nsignal.signal(signal.SIGINT, lambda s, f: None)", "the SIGINT handler"),
    # the 2026-09-28 class: a re-import rebinds sys.modules AND the package attribute derive._wait_slice reads
    "reimport": ("import importlib, sys\nsys.modules.pop('updater.orchestrate')\n"
                 "importlib.import_module('updater.orchestrate')", "the updater package's 'orchestrate' attribute"),
    "package_attr": ("import types, updater\nupdater.orchestrate = types.SimpleNamespace()",
                     "the updater package's 'orchestrate' attribute replaced"),
    "reload": ("import importlib\nfrom updater import orchestrate\nimportlib.reload(orchestrate)",
               "orchestrate.UnitTimeout is a different class"),
}


@pytest.mark.parametrize("kind", sorted(LEAKS))
def test_a_leaked_alarm_state_fails_the_leaking_test_by_name(pytester, kind):
    code, marker = LEAKS[kind]
    body = "def test_leaks():\n" + textwrap.indent(code, "    ") + "\n\ndef test_next():\n    pass\n"
    res = _run(pytester, body)
    out = res.stdout.str()
    assert "left process-wide alarm state changed" in out and marker in out, out[-1500:]
    res.assert_outcomes(passed=2, errors=1)          # the leaker errors at teardown; the next test is clean


def test_a_test_that_restores_what_it_changes_passes(pytester):
    body = """
    import signal
    from updater import orchestrate

    def test_well_behaved(monkeypatch):
        monkeypatch.setattr(orchestrate, "_DEFER_ALARM", True)
        prev = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, lambda s, f: None)
        signal.signal(signal.SIGINT, prev)
    """
    res = _run(pytester, body)
    res.assert_outcomes(passed=1)
