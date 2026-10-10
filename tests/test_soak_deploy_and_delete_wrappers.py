"""tools/selfhost/deploy_soak.sh and delete_soak.sh act on the soak worker only, and say so truthfully.

The REAL scripts run with `git`, `npx`, `curl` and `sleep` replaced by stand-ins on PATH: nothing is deployed
or deleted and nothing goes to the network. The curl stand-in answers from a file per address, and the files
can change after the stand-in `npx` ran (as a deploy or a delete changes what an address answers).
Each failure case has the passing run beside it.
"""
import json
import os
import shutil
import stat
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEPLOY = os.path.join(ROOT, "tools", "selfhost", "deploy_soak.sh")
DELETE = os.path.join(ROOT, "tools", "selfhost", "delete_soak.sh")
COMMIT = "c0ffee00" * 5
PROD_COMMIT = "0ddba11" + "0" * 33

GIT = """#!/usr/bin/env bash
case "$*" in
  "rev-parse --show-toplevel") echo "$FAKE_TOP" ;;
  "rev-parse --abbrev-ref HEAD") echo "${FAKE_BRANCH:-main}" ;;
  "status --porcelain -- api/worker") ;;
  "fetch --quiet origin main") ;;
  "rev-parse HEAD"|"rev-parse origin/main") echo "$FAKE_COMMIT" ;;
  *) echo "unexpected git call: $*" >&2; exit 97 ;;
esac
"""
# after npx ran, the "after" files (when present) replace the answers of the two addresses
NPX = """#!/usr/bin/env bash
echo "npx $*" >> "$FAKE_LOG"
case "$*" in *--dry-run*) exit "${FAKE_DRY_EXIT:-0}" ;; esac
for who in soak prod; do
  if [ -f "$FAKE_DIR/$who.after" ]; then cp "$FAKE_DIR/$who.after" "$FAKE_DIR/$who"; fi
done
exit "${FAKE_NPX_EXIT:-0}"
"""
CURL = """#!/usr/bin/env bash
echo "curl $*" >> "$FAKE_LOG"
case "$*" in
  *soak.example*) who=soak ;;
  *prod.example*) who=prod ;;
  *) echo "unexpected curl call: $*" >&2; exit 96 ;;
esac
n=$(grep -c "^curl .*$who.example" "$FAKE_LOG")
if [ -f "$FAKE_DIR/$who.fail.$n" ]; then exit 7; fi
if [ -f "$FAKE_DIR/$who.fail" ]; then exit 7; fi
if [ -f "$FAKE_DIR/$who.$n" ]; then cat "$FAKE_DIR/$who.$n"; else cat "$FAKE_DIR/$who"; fi
"""
SLEEP = "#!/usr/bin/env bash\nexit 0\n"


def _status(commit, soak=None, forward=False):
    d = {"commit": commit, "forward": forward, "edge_state": "users" if forward else "econ",
         "forward_raw": "on" if forward else "", "edge_state_raw": "", "origin_configured": False}
    if soak is not None:
        d["soak"] = soak
    return json.dumps(d)


PROD = _status(PROD_COMMIT, soak=False)
SOAK_OK = _status(COMMIT, soak=True, forward=True)
GONE = "<html>There is nothing here yet</html>"


def _run(tmp_path, script, files, args=(), env_extra=None, typed="econdl-api-soak\n"):
    bash = shutil.which("bash")
    if bash is None:
        assert os.name == "nt", "bash must exist where CI runs this test"
        pytest.skip("no bash on this Windows machine")
    bindir, fake = tmp_path / "bin", tmp_path / "fake"
    bindir.mkdir()
    fake.mkdir()
    py = f'#!/usr/bin/env bash\nexec "{sys.executable.replace(os.sep, "/")}" "$@"\n'
    for name, body in (("git", GIT), ("npx", NPX), ("curl", CURL), ("sleep", SLEEP), ("python", py)):
        p = bindir / name
        p.write_text(body, encoding="utf-8", newline="\n")
        p.chmod(p.stat().st_mode | stat.S_IEXEC)
    for name, body in files.items():
        (fake / name).write_text(body, encoding="utf-8")
    top = tmp_path / "top"
    (top / "api" / "worker").mkdir(parents=True, exist_ok=True)
    if not (env_extra or {}).pop("NO_SOAK_CONFIG", None):
        (top / "api" / "worker" / "wrangler.soak.toml").write_text('name = "econdl-api-soak"\n', encoding="utf-8")
    log = tmp_path / "calls.log"
    log.write_text("", encoding="utf-8")
    fwd = lambda p: str(p).replace(os.sep, "/")                                    # noqa: E731
    env = dict(os.environ, FAKE_TOP=fwd(top), FAKE_COMMIT=COMMIT, FAKE_DIR=fwd(fake), FAKE_LOG=fwd(log),
               SELFHOST_SOAK="https://soak.example", SELFHOST_EDGE="https://prod.example", **(env_extra or {}))
    env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
    # THE REAL SCRIPTS DEPLOY AND DELETE. They may run only when every command they call resolves to a stand-in.
    seen = subprocess.run([bash, "-c", "for c in git npx curl sleep python; do command -v $c; done"],
                          capture_output=True, text=True, env=env, timeout=60).stdout.split()
    assert len(seen) == 5 and all(os.path.basename(os.path.dirname(s)) == "bin" and tmp_path.name in s for s in seen), \
        f"the stand-ins are not first on PATH for {bash}: {seen} - the script was NOT run"
    for name in ("WRANGLER_CI_OVERRIDE_NAME",):
        if name not in (env_extra or {}):
            env.pop(name, None)
    r = subprocess.run([bash, script, *args], capture_output=True, text=True, env=env, timeout=180, input=typed)
    calls = log.read_text(encoding="utf-8")
    return r, [ln for ln in calls.splitlines() if ln.startswith("npx ")]


DEPLOY_CMD = f"npx wrangler deploy --config wrangler.soak.toml --name econdl-api-soak --var GIT_COMMIT:{COMMIT}"

# ---------------------------------------------------------------- deploy_soak.sh


@pytest.mark.parametrize("prod", [PROD, _status(PROD_COMMIT)])            # the second: an edge older than the `soak` field
def test_deploy_names_the_soak_worker_and_its_config_and_verifies_both_addresses(tmp_path, prod):
    r, npx = _run(tmp_path, DEPLOY, {"prod": prod, "soak": GONE, "soak.after": SOAK_OK})
    assert r.returncode == 0, r.stderr
    assert npx == [DEPLOY_CMD]
    assert f"verified: https://soak.example answers {COMMIT} as a soak worker; production is unchanged" in r.stdout


def test_deploy_takes_no_argument(tmp_path):
    r, npx = _run(tmp_path, DEPLOY, {"prod": PROD, "soak": GONE, "soak.after": SOAK_OK}, args=["econdl-api"])
    assert r.returncode == 1 and npx == [] and "takes no argument" in r.stderr


def test_deploy_refuses_another_branch_and_an_unreadable_production(tmp_path):
    r, npx = _run(tmp_path, DEPLOY, {"prod": PROD, "soak": GONE}, env_extra={"FAKE_BRANCH": "selfhost/x"})
    assert r.returncode == 1 and npx == [] and "not main" in r.stderr
    for files in ({"prod": GONE, "soak": GONE}, {"prod": PROD, "prod.fail": "", "soak": GONE}):
        sub = tmp_path / f"case{len(files)}"
        sub.mkdir()
        r, npx = _run(sub, DEPLOY, files)
        assert r.returncode == 1 and npx == [], "no deploy when production cannot be read first"
        assert "cannot be read" in r.stderr


@pytest.mark.parametrize("after, words", [
    (GONE, "does not answer"),                                               # the address never answers the commit
    (_status("f" * 40, soak=True, forward=True), "does not answer"),          # an older deploy answers
    (_status(COMMIT, soak=False, forward=True), f"reads '{COMMIT} no yes users'"),      # not the soak config
    (_status(COMMIT, forward=True), f"reads '{COMMIT} none yes users'"),
    (_status(COMMIT, soak=True, forward=False), f"reads '{COMMIT} yes no econ'"),
    (json.dumps({"commit": COMMIT, "soak": True, "forward": True, "edge_state": "econ"}), f"reads '{COMMIT} yes yes econ'"),
])
def test_deploy_fails_when_the_soak_address_is_not_this_commit_as_a_soak_worker(tmp_path, after, words):
    r, npx = _run(tmp_path, DEPLOY, {"prod": PROD, "soak": GONE, "soak.after": after})
    assert r.returncode == 1 and npx == [DEPLOY_CMD]
    assert "FAILED" in r.stderr and words in r.stderr


@pytest.mark.parametrize("prod_after, words", [
    (_status(COMMIT, soak=False), "now '" + COMMIT),                          # the deploy landed on production
    (_status(PROD_COMMIT, soak=True), f"now '{PROD_COMMIT} yes no econ'"),
    (_status(PROD_COMMIT, soak=False, forward=True), f"now '{PROD_COMMIT} no yes users'"),   # same commit, another config
    (GONE, "now 'notjson'"),
])
def test_deploy_fails_when_production_changed(tmp_path, prod_after, words):
    r, _npx = _run(tmp_path, DEPLOY, {"prod": PROD, "prod.after": prod_after, "soak": GONE, "soak.after": SOAK_OK})
    assert r.returncode == 1
    assert "PRODUCTION worker changed" in r.stderr and words in r.stderr


def test_a_failed_wrangler_deploy_ends_the_script(tmp_path):
    r, npx = _run(tmp_path, DEPLOY, {"prod": PROD, "soak": GONE, "soak.after": SOAK_OK}, env_extra={"FAKE_NPX_EXIT": "3"})
    assert r.returncode == 3 and npx == [DEPLOY_CMD] and "verified" not in r.stdout



def test_one_answer_gives_all_four_words_while_a_deploy_spreads(tmp_path):
    """AR-274: three requests for three fields let a GOOD first deploy end with 'this is not the soak worker's
    config' when the second request was answered by a place the new worker had not reached. Now the answer
    that carries the commit is the only one read; the older answers before it are waited out."""
    r, npx = _run(tmp_path, DEPLOY, {"prod": PROD, "soak": GONE, "soak.after": SOAK_OK, "soak.1": GONE,
                                     "soak.2": _status("f" * 40, soak=True, forward=True)})
    assert r.returncode == 0, r.stderr
    calls = (tmp_path / "calls.log").read_text(encoding="utf-8")
    assert calls.count("soak.example") == 3 and calls.count("--max-time 30") == calls.count("curl ")


def test_a_production_answer_that_differs_in_any_word_or_at_any_of_three_reads_fails(tmp_path):
    """The read right after a deploy can come from the older version: the second and third still count."""
    changed = _status(PROD_COMMIT, soak=True, forward=True)
    for n in (2, 3, 4):                                        # call 1 is the read before the deploy
        sub = tmp_path / f"at{n}"
        sub.mkdir()
        r, _npx = _run(sub, DEPLOY, {"prod": PROD, f"prod.{n}": changed, "soak": GONE, "soak.after": SOAK_OK})
        assert r.returncode == 1 and "PRODUCTION worker changed" in r.stderr, n
    sub = tmp_path / "control"
    sub.mkdir()
    r, _npx = _run(sub, DEPLOY, {"prod": PROD, "prod.5": changed, "soak": GONE, "soak.after": SOAK_OK})
    assert r.returncode == 0, "control: there is no fifth read"


@pytest.mark.parametrize("prod", [_status(None, soak=False), '{"success": false}', _status("abc", soak=False),
                                  _status(PROD_COMMIT, soak=True)])
def test_no_deploy_and_no_delete_when_production_shows_no_commit_id_or_is_a_soak_worker(tmp_path, prod):
    """Without a commit id before, 'unchanged' after would compare nothing with nothing."""
    for script in (DEPLOY, DELETE):
        sub = tmp_path / os.path.basename(script)
        sub.mkdir()
        r, npx = _run(sub, script, {"prod": prod, "soak": SOAK_OK})
        assert r.returncode == 1 and npx == [] and "refused" in r.stderr


def test_a_name_override_for_wrangler_stops_the_deploy(tmp_path):
    """wrangler 3.114.17: WRANGLER_CI_OVERRIDE_NAME replaces --name on deploy, and api/worker/.env is loaded."""
    files = {"prod": PROD, "soak": GONE, "soak.after": SOAK_OK}
    r, npx = _run(tmp_path, DEPLOY, files, env_extra={"WRANGLER_CI_OVERRIDE_NAME": "econdl-api"})
    assert r.returncode == 1 and npx == [] and "WRANGLER_CI_OVERRIDE_NAME" in r.stderr
    sub = tmp_path / "empty"
    sub.mkdir()
    r, npx = _run(sub, DEPLOY, files, env_extra={"WRANGLER_CI_OVERRIDE_NAME": ""})
    assert r.returncode == 1 and npx == [], "set and empty is still set (wrangler then refuses for another reason)"
    sub = tmp_path / "dotenv"
    (sub / "top" / "api" / "worker").mkdir(parents=True)
    (sub / "top" / "api" / "worker" / ".env").write_text("WRANGLER_CI_OVERRIDE_NAME=econdl-api\n", encoding="utf-8")
    r, npx = _run(sub, DEPLOY, files)
    assert r.returncode == 1 and npx == [] and ".env" in r.stderr
    # ANOTHER LETTER CASE (AR-275): on Windows node reads `wrangler_ci_override_name` as the same variable
    for i, name in enumerate(("wrangler_ci_override_name", "Wrangler_Ci_Override_Name")):
        sub = tmp_path / f"case{i}"
        sub.mkdir()
        r, npx = _run(sub, DEPLOY, files, env_extra={name: "econdl-api"})
        assert r.returncode == 1 and npx == [] and "WRANGLER_CI_OVERRIDE_NAME" in r.stderr, name
    sub = tmp_path / "dotenv_lower"
    (sub / "top" / "api" / "worker").mkdir(parents=True)
    (sub / "top" / "api" / "worker" / ".env").write_text("wrangler_ci_override_name=econdl-api\n", encoding="utf-8")
    r, npx = _run(sub, DEPLOY, files)
    assert r.returncode == 1 and npx == [] and ".env" in r.stderr
    sub = tmp_path / "control"                                 # a .env with other names is no reason to refuse
    (sub / "top" / "api" / "worker").mkdir(parents=True)
    (sub / "top" / "api" / "worker" / ".env").write_text("CLOUDFLARE_API_TOKEN=test-value\nWRANGLER_LOG=debug\n", encoding="utf-8")
    r, npx = _run(sub, DEPLOY, files, env_extra={"WRANGLER_CI_OTHER": "1", "MY_WRANGLER_CI_OVERRIDE_NAME": "x"})
    assert r.returncode == 0 and npx == [DEPLOY_CMD], r.stderr
    assert "test-value" not in r.stdout + r.stderr

# ---------------------------------------------------------------- delete_soak.sh


DELETE_CMDS = ["npx wrangler delete --config wrangler.soak.toml --name econdl-api-soak --dry-run",
               "npx wrangler delete --config wrangler.soak.toml --name econdl-api-soak"]


def test_delete_runs_a_dry_run_then_deletes_the_soak_worker_by_name(tmp_path):
    r, npx = _run(tmp_path, DELETE, {"prod": PROD, "soak": SOAK_OK, "soak.after": GONE})
    assert r.returncode == 0, r.stderr
    assert npx == DELETE_CMDS
    assert "done: econdl-api-soak no longer answers; production is unchanged" in r.stdout


def test_delete_takes_no_argument_and_needs_the_soak_config(tmp_path):
    r, npx = _run(tmp_path, DELETE, {"prod": PROD, "soak": SOAK_OK}, args=["--name", "econdl-api"])
    assert r.returncode == 1 and npx == [] and "takes no argument" in r.stderr
    sub = tmp_path / "noconfig"
    sub.mkdir()
    r, npx = _run(sub, DELETE, {"prod": PROD, "soak": SOAK_OK}, env_extra={"NO_SOAK_CONFIG": "1"})
    assert r.returncode == 1 and npx == [] and "wrangler.soak.toml is missing" in r.stderr


def test_delete_refuses_when_production_cannot_be_read_first(tmp_path):
    r, npx = _run(tmp_path, DELETE, {"prod": GONE, "soak": SOAK_OK})
    assert r.returncode == 1 and npx == [] and "cannot be read" in r.stderr


def test_delete_fails_when_production_changed_or_the_soak_worker_still_answers(tmp_path):
    r, npx = _run(tmp_path, DELETE, {"prod": PROD, "prod.after": GONE, "soak": SOAK_OK, "soak.after": GONE})
    assert r.returncode == 1 and npx == DELETE_CMDS and "PRODUCTION worker's status changed" in r.stderr
    sub = tmp_path / "still"
    sub.mkdir()
    r, npx = _run(sub, DELETE, {"prod": PROD, "soak": SOAK_OK})                 # nothing changed: the worker is still there
    assert r.returncode == 1 and "is not gone" in r.stderr


def test_delete_asks_for_the_name_itself(tmp_path):
    """wrangler asks only in a terminal; anywhere else it answers yes by itself. The script's own question
    does not depend on that."""
    for i, typed in enumerate(("", "y\n", "econdl-api\n", "yes\n")):
        sub = tmp_path / f"t{i}"
        sub.mkdir()
        r, npx = _run(sub, DELETE, {"prod": PROD, "soak": SOAK_OK, "soak.after": GONE}, typed=typed)
        assert r.returncode == 1 and npx == [] and "nothing was deleted" in r.stderr, typed


def test_an_address_that_cannot_be_reached_is_not_a_deleted_worker(tmp_path):
    """AR-274: a failed request read as 'no longer answers' and the script said done with the worker there."""
    r, npx = _run(tmp_path, DELETE, {"prod": PROD, "soak": SOAK_OK, "soak.fail": ""})
    assert r.returncode == 1 and "could not be reached" in r.stderr and "done:" not in r.stdout
    sub = tmp_path / "slow"                                    # the delete spreads: JSON once more, then gone
    sub.mkdir()
    r, npx = _run(sub, DELETE, {"prod": PROD, "soak": SOAK_OK, "soak.after": GONE, "soak.1": SOAK_OK})
    assert r.returncode == 0 and "done:" in r.stdout


@pytest.mark.parametrize("n", [1, 3, 6])
def test_one_answer_that_is_not_json_from_a_worker_that_is_still_there_is_not_gone(tmp_path, n):
    """AR-275: nothing was deleted (wrangler ended with 0: the answer to its question was no); ONE answer of the
    soak address is an error page, every other one is the worker's JSON. The script must not say done."""
    r, npx = _run(tmp_path, DELETE, {"prod": PROD, "soak": SOAK_OK, f"soak.{n}": "<html>error code: 1101</html>"})
    assert len(npx) == 2 and r.returncode == 1 and "done:" not in r.stdout and "is not gone" in r.stderr


def test_a_failed_dry_run_stops_before_the_delete(tmp_path):
    """The stand-in exits non-zero only for the real call; here a wrangler that fails at once is shown by the
    script ending with its code and no 'done' line."""
    r, npx = _run(tmp_path, DELETE, {"prod": PROD, "soak": SOAK_OK, "soak.after": GONE}, env_extra={"FAKE_NPX_EXIT": "5"})
    assert r.returncode == 5 and "done:" not in r.stdout


def test_a_dry_run_that_fails_ends_the_script_before_the_delete(tmp_path):
    """The test above cannot fail a dry run (its stand-in ends 0 for --dry-run). This one does."""
    r, npx = _run(tmp_path, DELETE, {"prod": PROD, "soak": SOAK_OK, "soak.after": GONE}, env_extra={"FAKE_DRY_EXIT": "5"})
    assert r.returncode == 5 and len(npx) == 1 and npx[0].endswith("--dry-run") and "done:" not in r.stdout


def test_a_longer_commit_string_is_not_this_commit_and_the_sixth_try_still_counts(tmp_path):
    r, npx = _run(tmp_path, DEPLOY, {"prod": PROD, "soak": GONE, "soak.after": _status(COMMIT + "0", soak=True, forward=True)})
    assert r.returncode == 1 and "does not answer" in r.stderr
    sub = tmp_path / "sixth"
    sub.mkdir()
    r, npx = _run(sub, DEPLOY, {"prod": PROD, "soak": GONE, "soak.after": SOAK_OK, **{f"soak.{i}": GONE for i in range(1, 6)}})
    assert r.returncode == 0, r.stderr


def test_both_scripts_keep_lf_line_ends_and_name_only_the_soak_worker():
    for path in (DEPLOY, DELETE):
        raw = open(path, "rb").read()
        assert b"\r" not in raw, "bash fails on a carriage return"
        text = raw.decode("utf-8")
        assert 'SOAK_NAME="econdl-api-soak"' in text and 'SOAK_CONFIG="wrangler.soak.toml"' in text
        for line in text.splitlines():
            if "npx wrangler" in line:
                assert '--config "$SOAK_CONFIG" --name "$SOAK_NAME"' in line, line
