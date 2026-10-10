"""tools/selfhost/deploy_edge.sh ends in an error when the PRODUCTION edge calls itself a soak worker.

SOAK = "1" belongs to the soak worker only (api/worker/wrangler.soak.toml). A variable of that name set by
hand on `econdl-api` survives a deploy and turns the page-view routes into 404s with no error anywhere. The
wrapper reads /v1/edge-status after the deploy; this test runs the REAL script with `git`, `npx`, `curl` and
`sleep` replaced by small stand-ins on PATH, so nothing is deployed and nothing goes to the network.

The control is the same run with `soak: false` (and with no `soak` key, an edge older than the field): the
script must end with exit 0, so a guard that always fails is caught too.
"""
import os
import shutil
import stat
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "tools", "selfhost", "deploy_edge.sh")
COMMIT = "c0ffee00" * 5

GIT = """#!/usr/bin/env bash
case "$*" in
  "rev-parse --show-toplevel") echo "$FAKE_TOP" ;;
  "rev-parse --abbrev-ref HEAD") echo main ;;
  "status --porcelain -- api/worker") ;;
  "fetch --quiet origin main") ;;
  "rev-parse HEAD"|"rev-parse origin/main") echo "$FAKE_COMMIT" ;;
  *) echo "unexpected git call: $*" >&2; exit 97 ;;
esac
"""
NPX = """#!/usr/bin/env bash
echo "npx $*" >> "$FAKE_LOG"
"""
CURL = """#!/usr/bin/env bash
echo "curl $*" >> "$FAKE_LOG"
n=$(grep -c '^curl ' "$FAKE_LOG")
if [ -f "$FAKE_STATUS.$n" ]; then cat "$FAKE_STATUS.$n"; else cat "$FAKE_STATUS"; fi
"""
SLEEP = """#!/usr/bin/env bash
exit 0
"""


def _run(tmp_path, status_body):
    bash = shutil.which("bash")
    if bash is None:
        assert os.name == "nt", "bash must exist where CI runs this test"
        pytest.skip("no bash on this Windows machine")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name, body in (("git", GIT), ("npx", NPX), ("curl", CURL), ("sleep", SLEEP)):
        p = bindir / name
        p.write_text(body, encoding="utf-8", newline="\n")
        p.chmod(p.stat().st_mode | stat.S_IEXEC)
    # the script calls `python`: make sure that name is this interpreter
    py = bindir / "python"
    py.write_text(f'#!/usr/bin/env bash\nexec "{sys.executable.replace(os.sep, "/")}" "$@"\n', encoding="utf-8", newline="\n")
    py.chmod(py.stat().st_mode | stat.S_IEXEC)
    top = tmp_path / "top"
    (top / "api" / "worker").mkdir(parents=True)
    status = tmp_path / "status.json"
    status.write_text(status_body, encoding="utf-8")
    log = tmp_path / "calls.log"
    log.write_text("", encoding="utf-8")
    env = dict(os.environ, FAKE_TOP=str(top).replace(os.sep, "/"), FAKE_COMMIT=COMMIT,
               FAKE_STATUS=str(status).replace(os.sep, "/"), FAKE_LOG=str(log).replace(os.sep, "/"),
               SELFHOST_EDGE="https://edge.example")
    env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
    r = subprocess.run([bash, SCRIPT], capture_output=True, text=True, env=env, timeout=120)
    return r, log.read_text(encoding="utf-8")


def _body(**extra):
    import json
    return json.dumps({"commit": COMMIT, "forward": False, "edge_state": "econ", "forward_raw": "",
                       "edge_state_raw": "", "origin_configured": False, **extra})


@pytest.mark.parametrize("body", [_body(soak=False), _body()])
def test_control_a_plain_edge_ends_the_deploy_with_success(tmp_path, body):
    r, calls = _run(tmp_path, body)
    assert r.returncode == 0, r.stderr
    assert f"verified: https://edge.example/v1/edge-status answers {COMMIT}" in r.stdout
    assert f"npx wrangler deploy --config wrangler.toml --var GIT_COMMIT:{COMMIT}" in calls, "the stand-in deploy ran"


def test_an_edge_that_says_soak_true_fails_the_deploy(tmp_path):
    r, calls = _run(tmp_path, _body(soak=True))
    assert r.returncode == 1
    assert "soak=yes" in r.stderr and "Remove the SOAK variable" in r.stderr
    assert "npx wrangler deploy" in calls


def test_a_status_that_cannot_be_read_for_the_soak_check_is_not_a_pass(tmp_path):
    """The commit is read by the first request and `soak` by the third (the second prints the status). When
    that third answer is not JSON the script cannot tell, and must not end with success."""
    (tmp_path / "status.json.3").write_text("<html>error 1033</html>", encoding="utf-8")
    r, calls = _run(tmp_path, _body(soak=False))
    assert calls.count("curl ") == 3
    assert r.returncode == 1
    assert "soak=unknown" in r.stderr
