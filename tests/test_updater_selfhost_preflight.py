"""updater/run.py after T0 (plan change 4): the updater runs self-hosted only, from the fixed machine-wide
places, under the single-writer lock. Before T0 nothing changes. No real E: or ProgramData path is used."""
import os
import sys
import types

import pytest

from core import catalog_path, cutover
from updater import config, run


REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def live(tmp_path, monkeypatch):
    """A cut-over machine whose production checkout is THIS checkout (the preflight also checks where the
    code runs from), with a correct configuration and the lock in a temporary folder."""
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", REPO)
    monkeypatch.setattr(catalog_path, "LIVE_STATE_DIR", os.path.join(REPO, "data", "_aqueduct"))
    # the catalogue is a temporary path, never this checkout's (R1230: in the production checkout, where
    # tools/pretest.py runs this suite, REPO/data/catalog.db IS the live build)
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "live" / "data" / "catalog.db"))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "writer.lock"))
    monkeypatch.setattr(config, "ROOT", REPO)
    monkeypatch.setattr(config, "STATE_DIR", os.path.join(REPO, "data", "_aqueduct"))
    monkeypatch.setattr(config, "DATA_ROOT", os.path.join(REPO, "data", "clean_full"))
    monkeypatch.setattr(config, "REGISTRY", os.path.join(REPO, "updater", "registry.yaml"))
    for v in ("ECONDL_CATALOG", "ECONDL_DATA"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "selfhost")
    # econdl carries its OWN copies of the flag and build paths (it cannot import core): a real T0 sets both
    # (R1199 - no fixture modelled that, so econdl's post-T0 answer was never under test)
    monkeypatch.setattr(_econdl_catalog(), "_CUTOVER_FLAG", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(_econdl_catalog(), "_BUILD_DB", str(tmp_path / "live" / "data" / "catalog.db"))
    return tmp_path


def _econdl_catalog():
    import core.derive_csv  # noqa: F401 - the same sys.path the preflight and the run use
    from econdl import _catalog
    return _catalog


def _econdl_resolve():
    import core.derive_csv  # noqa: F401
    from econdl import _resolve
    return _resolve


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
    (lambda mp, t: mp.setattr(config, "STATE_DIR", str(t / "worktree" / "data" / "_aqueduct")), "STATE_DIR"),
    (lambda mp, t: mp.setattr(config, "ROOT", str(t / "worktree")), "config.ROOT"),
    # R1176: every other override the updater reads
    (lambda mp, t: mp.setattr(config, "DATA_ROOT", str(t / "elsewhere" / "clean_full")), "DATA_ROOT"),
    (lambda mp, t: mp.setattr(config, "REGISTRY", str(t / "registry.yaml")), "REGISTRY"),
    (lambda mp, t: mp.setenv("ECONDL_CATALOG", str(t / "catalog.db")), "ECONDL_CATALOG"),
    (lambda mp, t: mp.setenv("ECONDL_DATA", str(t / "clean_full")), "ECONDL_DATA"),
    # AR-153: what econdl itself resolves - an econdl built for another layout names its own build
    (lambda mp, t: mp.setattr(_econdl_catalog(), "_BUILD_DB", str(t / "site" / "data" / "catalog.db")),
     "econdl's catalogue"),
    # R1199: econdl's own refusal of an $ECONDL_CATALOG override is LISTED, not an uncaught RuntimeError
    (lambda mp, t: mp.setenv("ECONDL_CATALOG", str(t / "x" / "catalog.db")), "refused by econdl"),
    (lambda mp, t: mp.setattr(_econdl_resolve(), "_DEFAULT_DATA", str(t / "site" / "data" / "clean_full")),
     "econdl's data root"),
    # the code runs from a worktree while every setting names production (econdl follows the code)
    (lambda mp, t: (mp.setattr(catalog_path, "LIVE_STORE_ROOT", str(t / "prod")),
                    mp.setattr(catalog_path, "LIVE_STATE_DIR", str(t / "prod" / "data" / "_aqueduct")),
                    mp.setattr(catalog_path, "BUILD_PATH", str(t / "prod" / "data" / "catalog.db")),
                    mp.setattr(config, "ROOT", str(t / "prod")),
                    mp.setattr(config, "STATE_DIR", str(t / "prod" / "data" / "_aqueduct")),
                    mp.setattr(config, "DATA_ROOT", str(t / "prod" / "data" / "clean_full")),
                    mp.setattr(config, "REGISTRY", str(t / "prod" / "updater" / "registry.yaml"))), "code's own checkout"),
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
