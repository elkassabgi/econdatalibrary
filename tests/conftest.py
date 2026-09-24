"""Suite-wide guards.

NO TEST STARTS A STATE SYNC (R1204). tests/test_derive_csv_bulk_blob_route.py ran derive_csv_bulk end to end,
and after a clean put the tool's _durable_clear started `python -m updater.run --pull-state` as a CHILD
process in the real checkout - where none of the test's patches reach (not the flag, not STATE_DB, not
R2Blob). With credentials in the environment that child downloads production state and replaces the
checkout's data/_aqueduct/state.db (the R340 class); tools/pretest.py runs this suite in the production
checkout, which has a .env. Worktrees and CI were safe only because they hold no credentials.

So every process a test starts is checked: a command line that carries --pull-state or --push-state is
refused, and the test FAILS at teardown even when the tool under test catches the refusal (a swallowed
error must not read as a pass)."""
import subprocess

import pytest

_FORBIDDEN = ("--pull-state", "--push-state")

pytest_plugins = ["pytester"]          # the guard's own can-fail test runs an isolated pytest


# BOTH GUARDS USE THEIR OWN MonkeyPatch, never the shared `monkeypatch` fixture: an autouse fixture that
# requests it creates it FIRST, so it is torn down LAST - after the test's other fixtures. A test that patched
# swap.stop to raise then had the rig's teardown run while the patch was still in place (R1204 follow-up:
# test_any_error_after_the_flip errored at teardown in the first whole-suite run with this file).
@pytest.fixture(autouse=True)
def _no_cloud_credentials_in_a_test(tmp_path_factory):
    """AND NO TEST HOLDS CLOUD CREDENTIALS. Several tools call pull_state/push_state IN-PROCESS, where the
    subprocess guard below cannot see them; and the production checkout, where tools/pretest.py runs this
    suite, has a .env. So every test runs with the R2 variables removed and core.r2_util reading a .env that
    does not exist: a real R2 client fails with "credentials are not set" instead of reaching the bucket.
    A test that needs a client fakes it (monkeypatch r2_util.client / creds), as they already do."""
    import os
    missing = str(tmp_path_factory.getbasetemp() / "no-such-dir" / ".env")
    with pytest.MonkeyPatch.context() as mp:
        for k in list(os.environ):
            if k.startswith(("R2_READ_", "R2_WRITE_", "CLOUDFLARE_API_TOKEN", "CF_API_TOKEN")):
                mp.delenv(k)
        # EMPTY, not absent (R1213): core.config.load_env() - which every connector's require() calls - does
        # os.environ.setdefault from the checkout's .env, so a deleted key came back. An empty one stays empty,
        # and core.r2_util ignores empty values.
        for side in ("READ", "WRITE"):
            for part in ("ENDPOINT", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"):
                mp.setenv(f"R2_{side}_{part}", "")
        mp.setenv("CLOUDFLARE_API_TOKEN", "")
        try:
            from core import r2_util
        except Exception:                                # noqa: BLE001 - a test tree without core/
            r2_util = None
        if r2_util is not None:
            mp.setattr(r2_util, "ENV", missing)
        try:
            from core import config as core_config
        except Exception:                                # noqa: BLE001
            core_config = None
        if core_config is not None and hasattr(core_config, "_DEFAULT"):
            mp.setattr(core_config, "_DEFAULT", missing)
        yield


@pytest.fixture(autouse=True)
def _no_production_writer_lock(tmp_path_factory):
    """AND NO TEST TAKES THE PRODUCTION WRITER LOCK. core.catalog_path.LOCK_PATH is E:\\econ_live\\state\\
    writer.lock - the machine's single-writer lock after T0. A test that forgot to point it elsewhere took it
    (2026-09-24, tests/test_probe_csv_freshness_selfhost.py; it existed and was not changed, but a test must
    never hold what the live updater holds). Every test starts with a path of its own that does not exist yet
    (a folder is not made: tests assert that no lock folder is created before T0); a test that sets its own
    path wins, since its setattr comes after this one."""
    import uuid
    try:
        from core import catalog_path
    except Exception:                                    # noqa: BLE001 - a test tree without core/
        yield
        return
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(catalog_path, "LOCK_PATH",
                   str(tmp_path_factory.getbasetemp() / "writer-locks" / uuid.uuid4().hex / "writer.lock"))
        yield


def pytest_configure(config):
    config.addinivalue_line("markers", "state_sync_refusal_expected: the guard's own can-fail test")


@pytest.fixture(autouse=True)
def _no_state_sync_from_a_test(request):
    started = []
    real = subprocess.Popen

    class _Guarded(real):
        def __init__(self, args, *a, **k):
            flat = args if isinstance(args, (str, bytes)) else " ".join(map(str, args))
            flat = flat.decode() if isinstance(flat, bytes) else flat
            if any(f in flat for f in _FORBIDDEN):
                started.append(flat)
                raise RuntimeError(f"a test started a state sync: {flat[:160]} (tests/conftest.py, R1204)")
            super().__init__(args, *a, **k)
    import os
    real_system = os.system

    def _system(cmd):
        if any(f in str(cmd) for f in _FORBIDDEN):          # R1213: os.system bypassed the Popen guard
            started.append(str(cmd))
            raise RuntimeError(f"a test started a state sync: {str(cmd)[:160]} (tests/conftest.py, R1204)")
        return real_system(cmd)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(subprocess, "Popen", _Guarded)
        mp.setattr(os, "system", _system)
        yield
    if request.node.get_closest_marker("state_sync_refusal_expected"):
        return
    assert not started, f"this test started a state sync (refused, but the attempt is the bug): {started}"
