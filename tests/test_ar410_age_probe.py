"""AR-410 probe, copied in at AR-410's request (PR #208 fix): do the kept-slice age bounds work on the head? (mutant h08, which
removes them, survives the PR's own suite.)"""
from tests.test_imts_window_cap import F, env, _slices  # noqa: F401


def _keep_all_closed(env, monkeypatch):
    env.setup("base")
    monkeypatch.setenv("AQUEDUCT_IMTS_MARGIN_MIN", "1")
    monkeypatch.setenv("AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "1")
    env.imf.per_request, env.imf.chunks = 0.0, 1
    env.ceiling(100.0)                     # closed+open slices need 60 s; newest needs 120 s: stop
    assert F.update(None, None).status == "partial"
    assert len(_slices()) == 8, _slices()
    env.imf.requests.clear()


def _windows(env):
    return sorted(u.split("startPeriod=")[1].split("&")[0] for u in env.imf.data_requests())


def test_probe_within_age_only_newest(env, monkeypatch):
    _keep_all_closed(env, monkeypatch)
    env.clock.t += 5 * 86400
    env.mp.setattr(F.orchestrate if hasattr(F, "orchestrate") else __import__("updater.orchestrate",
                   fromlist=["x"]), "_RUN_DEADLINE_TS", None)
    F.update(None, None)
    print("PROBE 5d", _windows(env))
    assert _windows(env) == ["2025"]


def test_probe_open_window_ages_out_after_31d(env, monkeypatch):
    _keep_all_closed(env, monkeypatch)
    env.clock.t += 32 * 86400
    env.mp.setattr(__import__("updater.orchestrate", fromlist=["x"]), "_RUN_DEADLINE_TS", None)
    F.update(None, None)
    print("PROBE 32d", _windows(env))
    assert _windows(env) == ["2020", "2025"]


def test_probe_closed_windows_age_out_after_365d(env, monkeypatch):
    _keep_all_closed(env, monkeypatch)
    env.clock.t += 366 * 86400
    env.mp.setattr(__import__("updater.orchestrate", fromlist=["x"]), "_RUN_DEADLINE_TS", None)
    F.update(None, None)
    print("PROBE 366d", _windows(env))
    assert len(_windows(env)) >= 9
