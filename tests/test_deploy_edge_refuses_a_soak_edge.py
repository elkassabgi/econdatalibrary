"""tools/selfhost/deploy_edge.sh ends in an error when the PRODUCTION edge calls itself a soak worker.

SOAK = "1" belongs to the soak worker only (api/worker/wrangler.soak.toml). A variable of that name set by
hand as a SECRET on `econdl-api` survives a deploy and turns the page-view routes into 404s with no error anywhere. The
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


def _run(tmp_path, status_body, env_extra=None):
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
    (top / "api" / "worker").mkdir(parents=True, exist_ok=True)
    status = tmp_path / "status.json"
    status.write_text(status_body, encoding="utf-8")
    log = tmp_path / "calls.log"
    log.write_text("", encoding="utf-8")
    env = dict(os.environ, FAKE_TOP=str(top).replace(os.sep, "/"), FAKE_COMMIT=COMMIT,
               FAKE_STATUS=str(status).replace(os.sep, "/"), FAKE_LOG=str(log).replace(os.sep, "/"),
               SELFHOST_EDGE="https://edge.example", **(env_extra or {}))
    env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
    # THE REAL SCRIPT DEPLOYS. It may run only when every command it calls resolves to a stand-in.
    seen = subprocess.run([bash, "-c", "for c in git npx curl sleep python; do command -v $c; done"],
                          capture_output=True, text=True, env=env, timeout=60).stdout.split()
    assert len(seen) == 5 and all(os.path.basename(os.path.dirname(s)) == "bin" and tmp_path.name in s for s in seen), \
        f"the stand-ins are not first on PATH for {bash}: {seen} - the script was NOT run"
    r = subprocess.run([bash, SCRIPT], capture_output=True, text=True, env=env, timeout=120)
    return r, log.read_text(encoding="utf-8")


def test_a_name_override_for_wrangler_stops_the_production_deploy(tmp_path, monkeypatch):
    """wrangler 3.114.17 lets WRANGLER_CI_OVERRIDE_NAME replace the worker's name on deploy: the production
    config would go to another worker. The control is every other test of this file (the variable unset)."""
    monkeypatch.setenv("WRANGLER_CI_OVERRIDE_NAME", "econdl-api-soak")
    r, calls = _run(tmp_path, _body(soak=False))
    assert r.returncode == 1 and "npx" not in calls and "WRANGLER_CI_OVERRIDE_NAME" in r.stderr


@pytest.mark.parametrize("name", ["wrangler_ci_override_name", "Wrangler_CI_Override_Name"])
def test_the_name_override_in_another_letter_case_stops_the_production_deploy(tmp_path, monkeypatch, name):
    """AR-275: on Windows node reads environment names without regard to case; the guard must too."""
    monkeypatch.delenv("WRANGLER_CI_OVERRIDE_NAME", raising=False)
    r, calls = _run(tmp_path, _body(soak=False), env_extra={name: "econdl-api-soak"})
    assert r.returncode == 1 and "npx" not in calls and "WRANGLER_CI_OVERRIDE_NAME" in r.stderr


def test_the_lower_case_name_stops_the_production_deploy_in_a_turkish_locale(tmp_path, monkeypatch):
    """AR-278: in tr_TR.UTF-8 `grep -i` does not pair i with I; the guard's greps run in the C locale. Skipped,
    not passed, where the locale does not exist."""
    bash = shutil.which("bash")
    have = bash and "tr_tr.utf8" in subprocess.run([bash, "-c", "locale -a"], capture_output=True, text=True,
                                                   timeout=60).stdout.lower().replace("-", "").split()
    if not have:
        pytest.skip("no tr_TR.UTF-8 locale on this machine: the case cannot be made here")
    monkeypatch.delenv("WRANGLER_CI_OVERRIDE_NAME", raising=False)
    r, calls = _run(tmp_path, _body(soak=False), env_extra={"LC_ALL": "tr_TR.UTF-8", "wrangler_ci_override_name": "x"})
    assert r.returncode == 1 and "npx" not in calls and "WRANGLER_CI_OVERRIDE_NAME" in r.stderr
    sub = tmp_path / "control"
    sub.mkdir()
    r, calls = _run(sub, _body(soak=False), env_extra={"LC_ALL": "tr_TR.UTF-8"})
    assert r.returncode == 0 and "npx wrangler deploy" in calls, r.stderr


def test_a_list_of_names_that_cannot_be_read_stops_the_production_deploy(tmp_path, monkeypatch):
    """AR-278, case 13: an exported shell function named `compgen` gave the guard an empty list, which passed."""
    monkeypatch.delenv("WRANGLER_CI_OVERRIDE_NAME", raising=False)
    fn = {"BASH_FUNC_compgen%%": "() {  :\n}"}
    r, calls = _run(tmp_path, _body(soak=False), env_extra={**fn, "WRANGLER_CI_OVERRIDE_NAME": "econdl-api-soak"})
    assert r.returncode == 1 and "npx" not in calls and "could not be read" in r.stderr
    sub = tmp_path / "unset"
    sub.mkdir()
    r, calls = _run(sub, _body(soak=False), env_extra=fn)
    assert r.returncode == 1 and "npx" not in calls and "could not be read" in r.stderr


@pytest.mark.parametrize("line, refused", [("WRANGLER_CI_OVERRIDE_NAME=econdl-api-soak", True),
                                            ("wrangler_ci_override_name=econdl-api-soak", True),
                                            ("CLOUDFLARE_API_TOKEN=test-value", False)])
def test_a_dotenv_file_beside_the_config_with_the_override_stops_the_production_deploy(tmp_path, line, refused):
    """wrangler loads api/worker/.env; the file is git-ignored, so 'clean' cannot see it. The third case is the
    control: a .env with another name is no reason to refuse, and its value is never printed."""
    worker = tmp_path / "top" / "api" / "worker"
    worker.mkdir(parents=True)
    (worker / ".env").write_text(line + "\n", encoding="utf-8")
    r, calls = _run(tmp_path, _body(soak=False))
    assert (r.returncode == 1 and "npx" not in calls and ".env" in r.stderr) if refused else (r.returncode == 0 and "npx wrangler deploy" in calls)
    assert "test-value" not in r.stdout + r.stderr and "econdl-api-soak" not in r.stdout + r.stderr


def test_an_edge_that_never_answers_the_commit_fails_the_deploy(tmp_path):
    import json
    r, calls = _run(tmp_path, json.dumps({"commit": "0ld", "soak": False}))
    assert r.returncode == 1 and calls.count("curl ") == 6 and "does not answer" in r.stderr


def test_the_name_override_set_but_empty_stops_the_production_deploy(tmp_path):
    r, calls = _run(tmp_path, _body(soak=False), env_extra={"WRANGLER_CI_OVERRIDE_NAME": ""})
    assert r.returncode == 1 and "npx" not in calls and "WRANGLER_CI_OVERRIDE_NAME" in r.stderr


def _body(**extra):
    import json
    return json.dumps({"commit": COMMIT, "forward": False, "edge_state": "econ", "forward_raw": "",
                       "edge_state_raw": "", "origin_configured": False, **extra})


@pytest.mark.parametrize("body", [_body(soak=False), _body()])
def test_control_a_plain_edge_ends_the_deploy_with_success(tmp_path, body):
    r, calls = _run(tmp_path, body)
    assert r.returncode == 0, r.stderr
    assert f"verified: https://edge.example/v1/edge-status answers {COMMIT}" in r.stdout
    assert f"npx wrangler deploy --config wrangler.toml --var GIT_COMMIT:{COMMIT}\n" in calls, "the stand-in deploy ran, with one variable"


def test_an_edge_that_says_soak_true_fails_the_deploy(tmp_path):
    r, calls = _run(tmp_path, _body(soak=True))
    assert r.returncode == 1
    assert "soak=true" in r.stderr and "Remove the SOAK secret or variable" in r.stderr
    assert "npx wrangler deploy" in calls


def test_a_soak_value_that_is_not_a_boolean_is_not_a_pass(tmp_path):
    r, calls = _run(tmp_path, _body(soak="true"))
    assert r.returncode == 1 and "soak=unknown" in r.stderr and "could not be read" in r.stderr


def test_one_answer_gives_the_commit_and_the_soak_flag(tmp_path):
    """While a deploy spreads, two requests can be answered by two versions. The flag must come from the body
    that carried the commit: a later answer from the previous version (no `soak` key) changes nothing."""
    import json
    (tmp_path / "status.json.2").write_text(json.dumps({"commit": "0ld"}), encoding="utf-8")
    (tmp_path / "status.json.3").write_text(json.dumps({"commit": "0ld"}), encoding="utf-8")
    r, calls = _run(tmp_path, _body(soak=True))
    assert calls.count("curl ") == 1 and r.returncode == 1 and "soak=true" in r.stderr


def test_the_check_also_runs_when_the_commit_shows_on_a_later_try(tmp_path):
    import json
    (tmp_path / "status.json.1").write_text(json.dumps({"commit": "0ld"}), encoding="utf-8")
    (tmp_path / "status.json.2").write_text("<html>error 1033</html>", encoding="utf-8")
    r, calls = _run(tmp_path, _body(soak=True))
    assert calls.count("curl ") == 3 and r.returncode == 1 and "soak=true" in r.stderr
