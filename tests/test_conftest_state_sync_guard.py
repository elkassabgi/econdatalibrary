"""tests/conftest.py's guard can fail (R1204): a test that starts a state sync is refused, in every spelling."""
import os
import subprocess
import sys

import pytest


@pytest.mark.state_sync_refusal_expected
@pytest.mark.parametrize("argv", [
    [sys.executable, "-m", "updater.run", "--pull-state"],
    [sys.executable, "-m", "updater.run", "--push-state", "--force"],
    f'"{sys.executable}" -m updater.run --pull-state',
])
def test_a_state_sync_started_by_a_test_is_refused(argv):
    with pytest.raises(RuntimeError, match="state sync"):
        subprocess.run(argv, shell=isinstance(argv, str))
    with pytest.raises(RuntimeError, match="state sync"):
        subprocess.Popen(argv, shell=isinstance(argv, str))


def test_no_test_holds_cloud_credentials(monkeypatch):
    """The in-process half: a real client cannot be built, even when the variables were set outside."""
    import os
    from core import r2_util
    assert not any(v for k, v in os.environ.items() if k.startswith(("R2_READ_", "R2_WRITE_")))   # present, EMPTY
    assert not os.path.exists(r2_util.ENV)
    assert r2_util.creds(write=True) is None and r2_util.creds(write=False) is None
    with pytest.raises(RuntimeError, match="credentials are not set"):
        r2_util.client(write=True)


@pytest.mark.state_sync_refusal_expected
def test_os_system_is_guarded_too():
    import os
    with pytest.raises(RuntimeError, match="state sync"):
        os.system(f'"{sys.executable}" -m updater.run --pull-state')


def test_no_test_holds_the_production_writer_lock():
    """The conftest points core.catalog_path.LOCK_PATH somewhere of this test's own (see the fixture)."""
    from core import catalog_path
    assert "econ_live" not in catalog_path.LOCK_PATH and not os.path.exists(catalog_path.LOCK_PATH)


def test_no_test_reaches_the_machine_s_self_hosting_paths():
    """R1230: the lock was one of five. The T0 flag, the served blob store, the live build and the checkout's
    catalogue are each a path of this test's own, none of them present, all under one folder of its own. The
    defaults are read from the SOURCE, since the attributes are what the guard changed."""
    import re
    from core import catalog_path, cutover
    from updater import blob
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    defaults = {}
    for mod, name in (("core/cutover.py", "FLAG_PATH"), ("updater/blob.py", "SELFHOST_BLOB_ROOT")):
        src = open(os.path.join(here, mod), encoding="utf-8").read()
        defaults[name] = re.search(rf'^{name} = r"([^"]+)"', src, re.M).group(1)
    live = {"FLAG_PATH": cutover.FLAG_PATH, "SELFHOST_BLOB_ROOT": blob.SELFHOST_BLOB_ROOT,
            "BUILD_PATH": catalog_path.BUILD_PATH, "CHECKOUT_PATH": catalog_path.CHECKOUT_PATH,
            "LOCK_PATH": catalog_path.LOCK_PATH}
    assert defaults == {"FLAG_PATH": r"C:\ProgramData\econ\CUTOVER", "SELFHOST_BLOB_ROOT": r"F:\econ_live\blobs"}
    same = lambda x, y: os.path.normcase(os.path.abspath(x)) == os.path.normcase(os.path.abspath(y))  # noqa: E731
    for name, default in defaults.items():
        assert not same(live[name], default), f"{name} is the machine's own"
    for name in ("BUILD_PATH", "CHECKOUT_PATH"):
        for default in (os.path.join(here, "data", "catalog.db"),
                        os.path.join(catalog_path.LIVE_STORE_ROOT, "data", "catalog.db")):
            assert not same(live[name], default), f"{name} is a real catalogue"
    assert not any(os.path.exists(v) for v in live.values()), live
    tops = {os.path.dirname(live["FLAG_PATH"]), os.path.dirname(live["SELFHOST_BLOB_ROOT"]),
            os.path.dirname(os.path.dirname(live["LOCK_PATH"]))}
    assert len(tops) == 1, tops
    assert not cutover.is_cut_over(), "a test that sets no flag of its own runs before T0, whatever the machine is"
    import core.derive_csv  # noqa: F401 - econdl's own copies, imported as the updater imports them
    from econdl import _catalog
    assert (_catalog._CUTOVER_FLAG, _catalog._BUILD_DB, _catalog._DEFAULT_DB) == \
        (live["FLAG_PATH"], live["BUILD_PATH"], live["CHECKOUT_PATH"])
    assert not _catalog._cut_over()


def test_the_default_env_file_is_not_the_checkout_s(monkeypatch):
    """R1218: a conftest without the core.config._DEFAULT patch survived. load_env() with no path reads
    _DEFAULT - the checkout's .env, which holds the write keys on the production checkout. Under the guard it
    names a file that does not exist, and a no-argument load_env() sets nothing - even for a key the guard
    did not empty, which the checkout's .env would have supplied."""
    import os
    from core import config as core_config
    assert not os.path.exists(core_config._DEFAULT)
    monkeypatch.delenv("R2_WRITE_ENDPOINT")
    core_config.load_env()
    assert "R2_WRITE_ENDPOINT" not in os.environ


def test_load_env_cannot_bring_the_keys_back(tmp_path):
    """R1213: core.config.load_env() setdefault()s from a .env; deleted keys came back from it. With a .env of
    this test's own: the R2 keys stay empty and r2_util still has no credentials."""
    import os
    from core import config as core_config, r2_util
    env = tmp_path / ".env"
    env.write_text("R2_WRITE_ENDPOINT=https://leak.invalid\nR2_WRITE_ACCESS_KEY_ID=k\n"
                   "R2_WRITE_SECRET_ACCESS_KEY=s\n", encoding="utf-8")
    core_config.load_env(str(env))
    assert os.environ["R2_WRITE_ENDPOINT"] == ""
    assert r2_util.creds(write=True) is None


def test_the_teardown_check_catches_a_swallowed_refusal(pytester):
    """R1213: the 'fails even if the tool swallows the refusal' claim, run for real in an isolated pytest."""
    here = os.path.dirname(os.path.abspath(__file__))
    pytester.makeconftest(open(os.path.join(here, "conftest.py"), encoding="utf-8").read())
    pytester.makepyfile(
        "import subprocess, sys\n"
        "def test_swallows():\n"
        "    try:\n"
        "        subprocess.run([sys.executable, '-m', 'updater.run', '--pull-state'])\n"
        "    except RuntimeError:\n"
        "        pass\n")
    r = pytester.runpytest("-p", "no:cacheprovider")
    r.assert_outcomes(passed=1, errors=1)


def test_a_test_that_leaves_the_process_changed_fails_and_the_next_does_not_inherit_it(pytester, monkeypatch):
    """R1239 and its repeat: an import-time backend setting turned later tests onto R2. The leaking test errors
    at teardown; the one after it sees the process as it was; a test that changes the backend through
    monkeypatch (and so undoes it) is fine."""
    here = os.path.dirname(os.path.abspath(__file__))
    pytester.makeconftest(open(os.path.join(here, "conftest.py"), encoding="utf-8").read())
    pytester.makepyfile(
        "import os\n"
        "def test_a_leaks():\n"
        "    os.environ['AQUEDUCT_BACKEND'] = 'r2'\n"
        "def test_b_after():\n"
        "    assert os.environ.get('AQUEDUCT_BACKEND') != 'r2'\n"
        "def test_c_monkeypatched(monkeypatch):\n"
        "    monkeypatch.setenv('AQUEDUCT_BACKEND', 'selfhost')\n"
        "    monkeypatch.chdir(os.path.dirname(os.getcwd()))\n")
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    r = pytester.runpytest("-p", "no:cacheprovider")
    r.assert_outcomes(passed=3, errors=1)
    assert "test_a_leaks left the process changed" in r.stdout.str()


def test_an_ordinary_process_still_runs():
    r = subprocess.run([sys.executable, "-c", "print('ok')"], capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "ok"


# ---- no test starts a real wrangler -------------------------------------------------------------------------
# Each spelling's PROGRAM and wrangler are paths that DO NOT EXIST under tmp_path: if the guard ever regressed,
# the process would fail to start - it can never reach the registry or Cloudflare.
def _wrangler_spellings(tmp):
    js = str(tmp / "node_modules" / "wrangler" / "bin" / "wrangler.js")
    node, npx, pnpm = str(tmp / "node"), str(tmp / "npx"), str(tmp / "pnpm")
    remote = ["d1", "execute", "econ-catalog", "--remote", "--command", "SELECT 1"]
    return [
        [node, js, *remote],
        [str(tmp / "wrangler.cmd"), "--version"],
        [str(tmp / "WRANGLER.EXE"), "--version"],
        [str(tmp / "bin" / "wrangler"), "--version"],
        [npx, "wrangler@4", *remote],                                         # the old loader's form
        [npx, "-y", "wrangler@3.114.0", "--version"],
        [pnpm, "dlx", "wrangler@latest", "--version"],
        [node, str(tmp / "x" / "wrangler.mjs"), "--version"],
        [node, str(tmp / "node_modules" / "wrangler" / "wrangler-dist" / "cli.js"), *remote],
        f'"{npx}" wrangler@4 d1 execute econ-catalog --remote --command "SELECT 1"',
        f'"{tmp / "cmd"}" /c "{npx}" wrangler --version',
        f'"{tmp / "bash"}" -c "{npx} -y wrangler@4 --version"',
    ]


def _guard():
    """The suite's conftest, loaded under a name of its own (pytester runs register other modules named
    `conftest`)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_suite_conftest_for_guard_tests", os.path.join(os.path.dirname(os.path.abspath(__file__)), "conftest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.wrangler_refusal_expected
def test_a_real_wrangler_started_by_a_test_is_refused(tmp_path):
    for argv in _wrangler_spellings(tmp_path):
        with pytest.raises(RuntimeError, match="real wrangler"):
            subprocess.run(argv, shell=isinstance(argv, str))
        with pytest.raises(RuntimeError, match="real wrangler"):
            subprocess.Popen(argv, shell=isinstance(argv, str))
    with pytest.raises(RuntimeError, match="real wrangler"):
        os.system(f'"{tmp_path / "npx"}" wrangler@4 --version')


def test_the_matcher_names_every_spelling(tmp_path):
    g = _guard()
    assert all(g._starts_a_wrangler(argv) for argv in _wrangler_spellings(tmp_path))


@pytest.mark.parametrize("argv", [
    ["grep", "-rn", "wrangler", "."],
    ["rg", "wrangler", "api/worker"],
    [sys.executable, "-m", "pytest", "-k", "wrangler"],
    "git log -- api/worker/wrangler.toml",
    [sys.executable, "-B", "standin.py", "wrangler", "secret", "put", "X"],     # a stand-in's argument
    [sys.executable, "-c", "print('ok')", "--name=no-wrangler-here", "wranglers"],
])
def test_a_mention_of_wrangler_is_not_refused(argv):
    """Negative controls: the guard refuses a wrangler that would RUN, not every mention of the word."""
    assert not _guard()._starts_a_wrangler(argv)


def test_an_ordinary_process_that_mentions_wrangler_still_runs():
    r = subprocess.run([sys.executable, "-c", "print('ok')", "--name=no-wrangler-here", "wranglers", "wrangler"],
                       capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "ok"


def test_only_the_guard_s_own_tests_may_carry_the_refusal_marker():
    """Ratchet: the marker skips the teardown check, so a marked test (or a module-level pytestmark) could hide a
    swallowed attempt. Only this file may use it, and never module-wide."""
    here = os.path.dirname(os.path.abspath(__file__))
    marker = "wrangler_refusal_" + "expected"
    import re
    users = sorted(os.path.relpath(os.path.join(d, f), here).replace("\\", "/")
                   for d, _, files in os.walk(here) for f in files if f.endswith(".py")
                   and marker in open(os.path.join(d, f), encoding="utf-8", errors="replace").read())
    assert users == ["conftest.py", "test_conftest_state_sync_guard.py"], users
    own = open(os.path.abspath(__file__), encoding="utf-8").read()
    assert not re.search(r"^pytestmark\s*=", own, re.M)


def test_the_teardown_check_catches_a_swallowed_wrangler_refusal(pytester, tmp_path):
    """A tool that catches the refusal must not turn the attempt into a pass."""
    here = os.path.dirname(os.path.abspath(__file__))
    pytester.makeconftest(open(os.path.join(here, "conftest.py"), encoding="utf-8").read())
    node = str(tmp_path / "node").replace("\\", "/")
    missing = str(tmp_path / "wrangler.js").replace("\\", "/")
    pytester.makepyfile(
        "import subprocess, sys\n"
        "def test_swallows():\n"
        "    try:\n"
        f"        subprocess.run([{node!r}, {missing!r}, '--version'])\n"
        "    except RuntimeError:\n"
        "        pass\n")
    r = pytester.runpytest("-p", "no:cacheprovider")
    r.assert_outcomes(passed=1, errors=1)
    assert "started a real wrangler" in r.stdout.str()
