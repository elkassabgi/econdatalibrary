"""updater/run.py after T0 (plan change 4): the updater runs self-hosted only, from the fixed machine-wide
places, under the single-writer lock. Before T0 nothing changes. No real E: or ProgramData path is used."""
import sys
import types

import pytest

from core import catalog_path, cutover
from updater import config, run


@pytest.fixture
def live(tmp_path, monkeypatch):
    """A cut-over machine whose fixed places are temporary folders, and a correct configuration."""
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    state, store = tmp_path / "live" / "state", tmp_path / "store"
    state.mkdir(parents=True)
    store.mkdir()
    monkeypatch.setattr(catalog_path, "LIVE_STATE_DIR", str(state))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(store))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(state / "writer.lock"))
    monkeypatch.setattr(config, "STATE_DIR", str(state))
    monkeypatch.setattr(config, "ROOT", str(store))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "selfhost")
    return tmp_path


def _args(**kw):
    base = dict(pull_state=False, push_state=False, source=None, strategy=None, cadence=None, force=False, dry=True)
    return types.SimpleNamespace(**{**base, **kw})


def test_before_t0_nothing_changes(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    assert run._selfhost_preflight(_args(pull_state=True)) is False


def test_the_correct_configuration_passes(live):
    assert run._selfhost_preflight(_args()) is True


@pytest.mark.parametrize("change,needle", [
    (lambda mp, t: None, None),
    (lambda mp, t: mp.setenv("AQUEDUCT_BACKEND", "r2"), "AQUEDUCT_BACKEND"),
    (lambda mp, t: mp.setenv("AQUEDUCT_BACKEND", "local"), "AQUEDUCT_BACKEND"),
    (lambda mp, t: mp.delenv("AQUEDUCT_BACKEND"), "AQUEDUCT_BACKEND"),
    (lambda mp, t: mp.setattr(config, "STATE_DIR", str(t / "worktree" / "data" / "_aqueduct")), "state dir"),
    (lambda mp, t: mp.setattr(config, "ROOT", str(t / "worktree")), "store root"),
])
def test_anything_else_is_refused(live, monkeypatch, capsys, change, needle):
    change(monkeypatch, live)
    if needle is None:
        assert run._selfhost_preflight(_args()) is True
        return
    with pytest.raises(SystemExit) as e:
        run._selfhost_preflight(_args())
    assert e.value.code == 2 and needle in capsys.readouterr().err


@pytest.mark.parametrize("flag", ["pull_state", "push_state"])
def test_the_cloud_state_steps_are_refused(live, capsys, flag):
    with pytest.raises(SystemExit) as e:
        run._selfhost_preflight(_args(**{flag: True}))
    assert e.value.code == 2 and flag.replace("_", "-") in capsys.readouterr().err


def test_the_state_functions_refuse_on_their_own(live, monkeypatch):
    monkeypatch.setattr(run, "_zstd", lambda: pytest.fail("reached the cloud step"))
    assert run.pull_state() == 2
    assert run.push_state() == 2


def test_the_run_holds_the_writer_lock_throughout(live, monkeypatch):
    seen = {}

    def run_once(**kw):
        seen["held"] = catalog_path._held is not None
        return []
    monkeypatch.setitem(sys.modules, "updater.orchestrate", types.SimpleNamespace(run_once=run_once))
    import updater
    monkeypatch.setattr(updater, "orchestrate", sys.modules["updater.orchestrate"], raising=False)
    monkeypatch.setattr(sys, "argv", ["updater.run", "--dry"])
    run.main()
    assert seen == {"held": True}
    assert catalog_path._held is None, "released after the run"
