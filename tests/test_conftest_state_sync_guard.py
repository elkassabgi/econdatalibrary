"""tests/conftest.py's guard can fail (R1204): a test that starts a state sync is refused, in every spelling."""
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
    assert not any(k.startswith(("R2_READ_", "R2_WRITE_")) for k in os.environ)
    assert not os.path.exists(r2_util.ENV)
    assert r2_util.creds(write=True) is None and r2_util.creds(write=False) is None
    with pytest.raises(RuntimeError, match="credentials are not set"):
        r2_util.client(write=True)


def test_an_ordinary_process_still_runs():
    r = subprocess.run([sys.executable, "-c", "print('ok')"], capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "ok"
