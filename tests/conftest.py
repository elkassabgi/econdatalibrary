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
    with pytest.MonkeyPatch.context() as mp:
        for k in list(os.environ):
            if k.startswith(("R2_READ_", "R2_WRITE_", "CLOUDFLARE_API_TOKEN", "CF_API_TOKEN")):
                mp.delenv(k)
        try:
            from core import r2_util
        except Exception:                                # noqa: BLE001 - a test tree without core/
            r2_util = None
        if r2_util is not None:
            mp.setattr(r2_util, "ENV", str(tmp_path_factory.getbasetemp() / "no-such-dir" / ".env"))
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
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(subprocess, "Popen", _Guarded)
        yield
    if request.node.get_closest_marker("state_sync_refusal_expected"):
        return
    assert not started, f"this test started a state sync (refused, but the attempt is the bug): {started}"
