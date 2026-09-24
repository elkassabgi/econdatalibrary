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


def test_an_ordinary_process_still_runs():
    r = subprocess.run([sys.executable, "-c", "print('ok')"], capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "ok"
