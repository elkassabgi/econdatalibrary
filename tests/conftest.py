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
def _no_production_paths(tmp_path_factory):
    """AND NO TEST REACHES THE MACHINE'S OWN SELF-HOSTING PATHS. core.catalog_path.LOCK_PATH is E:\\econ_live\\
    state\\writer.lock - the machine's single-writer lock after T0. A test that forgot to point it elsewhere
    took it (R1227, tests/test_probe_csv_freshness_selfhost.py; it existed and was not changed, but a test must
    never hold what the live updater holds). The lock was one of five (R1230): the T0 flag (a test that forgot
    it runs pre-T0 or post-T0 by what the MACHINE is, not what the test says), the served blob store, the live
    catalogue build, and the checkout's catalogue - which in the production checkout, where tools/pretest.py
    runs this suite, IS the live build. Every test starts with paths of its own that do not exist yet (no
    folder is made: tests assert that no lock folder is created before T0); a test that sets its own path
    wins, since its setattr comes after this one."""
    import uuid
    try:
        from core import catalog_path, cutover
    except Exception:                                    # noqa: BLE001 - a test tree without core/
        yield
        return
    try:
        from updater import blob
    except Exception:                                    # noqa: BLE001
        blob = None
    try:
        # econdl keeps its OWN copies of the flag and the build (it cannot import core), plus the checkout's
        # catalogue; imported the way core.derive_csv makes the updater import it (the checkout's copy first)
        import core.derive_csv  # noqa: F401
        from econdl import _catalog as econdl_catalog
    except Exception:                                    # noqa: BLE001 - a test tree without the client
        econdl_catalog = None
    own = tmp_path_factory.getbasetemp() / "machine-paths" / uuid.uuid4().hex
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(catalog_path, "LOCK_PATH", str(own / "state" / "writer.lock"))
        mp.setattr(cutover, "FLAG_PATH", str(own / "CUTOVER"))
        mp.setattr(catalog_path, "BUILD_PATH", str(own / "live" / "data" / "catalog.db"))
        mp.setattr(catalog_path, "CHECKOUT_PATH", str(own / "checkout" / "data" / "catalog.db"))
        if blob is not None and hasattr(blob, "SELFHOST_BLOB_ROOT"):
            mp.setattr(blob, "SELFHOST_BLOB_ROOT", str(own / "blobs"))
        if econdl_catalog is not None:
            for name, value in (("_CUTOVER_FLAG", cutover.FLAG_PATH), ("_BUILD_DB", catalog_path.BUILD_PATH),
                                ("_DEFAULT_DB", catalog_path.CHECKOUT_PATH)):
                if hasattr(econdl_catalog, name):
                    mp.setattr(econdl_catalog, name, value)
        yield


_PROCESS_KEYS = ("AQUEDUCT_BACKEND", "AQUEDUCT_DERIVE_WORKERS", "AQUEDUCT_DATA_ROOT", "AQUEDUCT_STATE_DIR")


@pytest.fixture(autouse=True)
def _no_process_leak(request):
    """AND NO TEST LEAVES THE PROCESS CHANGED. A tool that sets the store backend or changes directory when it is
    IMPORTED moved every later test onto R2 - 42 failures far from their cause (R1239), then one more from
    tools/rekey_ons_uk.py's import-time setdefault. The test that leaks now fails itself, and the values are put
    back so nothing after it inherits them. (This fixture requests nothing, so it is torn down after the test's
    own monkeypatch has undone its deliberate changes.)"""
    import os
    before = (os.getcwd(), {k: os.environ.get(k) for k in _PROCESS_KEYS})
    yield
    after = (os.getcwd(), {k: os.environ.get(k) for k in _PROCESS_KEYS})
    if after == before:
        return
    os.chdir(before[0])
    for k, v in before[1].items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    pytest.fail(f"{request.node.nodeid} left the process changed (cwd / store settings): {before} -> {after}",
                pytrace=False)


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



@pytest.fixture(autouse=True)
def _fresh_merge_served_cache():
    """AND NO TEST INHERITS ANOTHER TEST'S REGISTRY. derive._merge_served_sources() caches the registry's
    csv_merge_served set on the function (#79). A test that read it while the registry was faked left a set
    without ecb, and every later ecb merge test uploaded an unmerged CSV (found by the 2026-09-28 merge train,
    order-dependent: the full suite's order hid it). Cleared before and after each test."""
    from updater import derive
    derive._merge_served_sources._cache = None
    yield
    derive._merge_served_sources._cache = None


_ALARM_MODULES = ("updater.orchestrate", "updater.derive")


@pytest.fixture(autouse=True)
def _alarm_state_isolation():
    """NO TEST INHERITS ANOTHER TEST'S ALARM PLUMBING (2026-09-29, the Windows fence-test failures of 2026-09-28).

    tests/test_derive_fence_aware.py emulates SIGALRM with a SIGINT handler that runs orchestrate._deliver_alarm,
    and derive catches the trip as `except fence`, where fence is UnitTimeout imported LAZILY from
    sys.modules['updater.orchestrate']. So its results depend on process-wide state another test can leave behind:
      - orchestrate._DEFER_ALARM / _ALARM_PENDING (a deferred trip swallowed or delivered in the wrong test),
      - orchestrate.UNIT_TIMEOUT_FIRED (derive's _raise_if_fence_in_disguise turns a plain error into the fence),
      - a replaced sys.modules['updater.orchestrate' / 'updater.derive'] (two UnitTimeout classes: the trip is
        raised as one and caught as the other - "UnitTimeout has no attribute derive_partial", as on 2026-09-28),
        AND the `updater` package's attribute for it (derive._wait_slice reads `from . import orchestrate`, which is
        the package attribute, not sys.modules - a re-import rebinds both), AND orchestrate.UnitTimeout itself
        (importlib.reload keeps the module object but makes a new class),
      - the SIGINT handler.
    Each test starts clean, and a test that LEAVES any of these changed fails at its own teardown, naming it -
    instead of a fence test failing three files later with a message that points nowhere.

    Minimal order, measured on 2026-09-29: one test that pops and re-imports updater.orchestrate, run before
    tests/test_derive_fence_aware.py, gives 14 x "DID NOT RAISE UnitTimeout" on main; with this guard the
    re-importing test fails by name and the fence file passes."""
    import signal
    import sys
    import updater
    from updater import derive, orchestrate                     # noqa: F401 - both present BEFORE the snapshot,
    mods = {k: sys.modules.get(k) for k in _ALARM_MODULES}     # or a first import would read as a replacement
    attrs = {k: getattr(updater, k.rpartition(".")[2], None) for k in _ALARM_MODULES}
    fence_cls = orchestrate.UnitTimeout
    sigint = signal.getsignal(signal.SIGINT)
    orchestrate._DEFER_ALARM, orchestrate._ALARM_PENDING, orchestrate.UNIT_TIMEOUT_FIRED = False, None, False
    yield
    left = []
    for k, m in mods.items():
        if sys.modules.get(k) is not m:
            left.append(f"sys.modules[{k!r}] replaced")
            sys.modules[k] = m
        name = k.rpartition(".")[2]
        if getattr(updater, name, None) is not attrs[k]:
            left.append(f"the updater package's {name!r} attribute replaced")
            setattr(updater, name, attrs[k])
    orch = mods["updater.orchestrate"]
    if orch.UnitTimeout is not fence_cls:
        left.append("orchestrate.UnitTimeout is a different class (a reload?)")
        orch.UnitTimeout = fence_cls
    if orch._DEFER_ALARM or orch._ALARM_PENDING is not None:
        left.append(f"orchestrate._DEFER_ALARM={orch._DEFER_ALARM!r} / _ALARM_PENDING={orch._ALARM_PENDING!r}")
    orch._DEFER_ALARM, orch._ALARM_PENDING = False, None
    if signal.getsignal(signal.SIGINT) is not sigint:
        left.append("the SIGINT handler")
        signal.signal(signal.SIGINT, sigint)
    if left:
        pytest.fail("this test left process-wide alarm state changed (restored now): " + "; ".join(left))
