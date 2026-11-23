"""The econ edge worker (econdl-api) deploys ONLY through tools/selfhost/deploy_edge.sh (ledger R2038).

WHY. The 2026-11-21 deploy ran wrangler's own deploy command instead of the wrapper. Wrangler replaces the
plain vars with the empty [vars] of api/worker/wrangler.toml, so GIT_COMMIT was lost: /v1/edge-status reported
commit null for two days and tools/selfhost/watch_edge.py could not see worker changes that were not live.
The root cause was in the instructions: every deploy line an agent reads, and `npm run deploy`, gave the
command that drops GIT_COMMIT.

WHAT THIS PINS.
  1. `npm run deploy` in api/worker REFUSES (exit 1) and names the wrapper, also when a reader appends
     arguments (`npm run deploy -- --var GIT_COMMIT:x`). Its exact text is pinned BEFORE anything runs it: the
     conftest wrangler guard sees only the command Python starts, not a wrangler that npm or a shell starts, so
     a test that ran a regressed script in a checkout with node_modules would deploy production.
  2. A RATCHET over every TRACKED text file (tests/_repo_walk.tracked_files): each place that names a
     deploy-class wrangler command - deploy, publish (wrangler 3 keeps it as an alias of deploy), versions
     upload / deploy, rollback; any letter case; options between; a line break between; list form; a
     wrangler-action `command:` (a block scalar too), and EVERY use of the wrangler GitHub Action, which
     deploys when it names no command; `npm run deploy` - must sit on an allow-listed (path, phrase) with a
     reason.
     A new instruction anywhere fails here. A stale allow-list entry fails too.
  3. The instruction surfaces this change rewrote name the wrapper, and api/DEPLOY.md section 4 carries the
     whole recipe (Git Bash, a fresh clone of main, `npm ci`, the `verified:` receipt).
Planted shapes prove the matcher can fire; planted look-alikes prove it does not fire on everything.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _repo_walk  # noqa: E402

SELF = "tests/test_worker_deploy_only_via_wrapper.py"     # holds the planted shapes; scanned by the controls
PKG = os.path.join(ROOT, "api", "worker", "package.json")
WRAPPER = "tools/selfhost/deploy_edge.sh"

# The reviewed refusal. Trailing ` --`: npm appends a reader's arguments after the script, and without it node
# reads `--var` / `--dry-run` as ITS options, exits 9 with "bad option" and never prints the message (review
# 2026-11-23, condition 3). No `%`, `!`, `$`, backtick or double quote inside, so cmd.exe and sh read it alike.
EXPECTED_DEPLOY = ("node -e \"console.error('refused: deploy econdl-api only with tools/selfhost/deploy_edge.sh, "
                   "from a fresh clone of main, in Git Bash. It sets GIT_COMMIT and checks /v1/edge-status. "
                   "Steps: api/DEPLOY.md section 4 (ledger R2038).'); process.exit(1)\" --")

# --------------------------------------------------------------------------------------------- the matcher
_WR = re.compile(r"(?<![\w-])wrangler(?:@[^\s\"',\]]+)?(?:\.(?:cmd|exe|ps1|bat))?(?![\w./-])", re.IGNORECASE)
_TOK = re.compile(r"[^\s\"',\[\]\\`()]+")
_VALUE_OPTS = {"-c", "--config", "-e", "--env", "--name", "--cwd", "--experimental-json-config"}
_ACTION = re.compile(r"^[ \t-]*command:[ \t]*(?:[|>][-+]?[ \t]*\r?\n[ \t]*)?[\"']?(?!pages\b)"
                     r"(deploy|publish|versions|rollback)\b.*$", re.IGNORECASE | re.MULTILINE)
# Any use of the wrangler GitHub Action: with no `command:` it runs `wrangler deploy` (review AR, K2), so each
# use must be allow-listed with what it runs.
_ACTION_USE = re.compile(r"cloudflare/wrangler-action\b", re.IGNORECASE)
_NPM = re.compile(r"\bnpm\s+(?:run|run-script)\s+deploy\b", re.IGNORECASE)


def _line_span(text: str, start: int, end: int) -> tuple[int, str]:
    """(1-based line of `start`, the text from that line's start to the end of the line holding `end`, with
    whitespace collapsed) - the context an allow-list phrase is matched against."""
    a = text.rfind("\n", 0, start) + 1
    b = text.find("\n", end)
    b = len(text) if b < 0 else b
    return text.count("\n", 0, start) + 1, re.sub(r"\s+", " ", text[a:b]).strip()


def find_deploys(text: str) -> list[tuple[int, str, str]]:
    """Every deploy-class place in `text`: (line, kind, context)."""
    hits = []
    for m in _WR.finditer(text):
        window = text[m.end(): m.end() + 160]
        toks = [(t.group(0), t.start()) for t in _TOK.finditer(window)][:8]
        i, skip_next, sub = 0, False, None
        while i < len(toks):
            t = toks[i][0]
            if skip_next:
                skip_next = False
            elif t.startswith("-"):
                skip_next = t.lower() in _VALUE_OPTS
            else:
                word = t.lower().rstrip(".;:")
                if word in ("deploy", "publish", "rollback"):
                    sub = word
                elif word == "versions" and i + 1 < len(toks) and \
                        toks[i + 1][0].lower().rstrip(".;:") in ("upload", "deploy"):
                    sub = "versions " + toks[i + 1][0].lower().rstrip(".;:")
                    i += 1
                break
            i += 1
        if sub:
            line, ctx = _line_span(text, m.start(), m.end() + toks[i][1] + len(toks[i][0]))
            hits.append((line, "wrangler " + sub, ctx))
    for m in _ACTION.finditer(text):
        line, ctx = _line_span(text, m.start(), m.end())
        hits.append((line, "wrangler-action command", ctx))
    for m in _ACTION_USE.finditer(text):
        line, ctx = _step_block(text, m.start())
        hits.append((line, "wrangler-action use", ctx))
    for m in _NPM.finditer(text):
        line, ctx = _line_span(text, m.start(), m.end())
        hits.append((line, "npm run deploy", ctx))
    return hits


def _step_block(text: str, at: int) -> tuple[int, str]:
    """(line, context) for a wrangler-action use, with the WHOLE step as its context: the `uses:` line and every
    following line indented at least as deep as its key (its `with:` block). So an allow-list phrase can name
    what the step runs (`command: pages deploy ...`), and a second step without a command cannot borrow it
    (mutation run 3, M11b: a phrase on the `uses:` line alone allowed every use in the file)."""
    lines = text[text.rfind("\n", 0, at) + 1:].split("\n")
    first = lines[0].rstrip("\r")
    key_col = len(first) - len(first.lstrip(" -"))
    block = [first]
    for ln in lines[1:]:
        body = ln.rstrip("\r")
        if body.strip() and len(body) - len(body.lstrip(" ")) < key_col:
            break
        block.append(body)
    return text.count("\n", 0, at) + 1, re.sub(r"\s+", " ", " ".join(block)).strip()


# ------------------------------------------------------------------------------------------- the allow-list
# (path, a phrase from THAT line - never the bare command, which would exempt the whole file, reason).
# Everything else that matches is a deploy instruction for econdl-api, and it must name the wrapper instead.
ALLOWED = [
    (WRAPPER, "is easy to forget or get wrong", "the wrapper's header: why it exists"),
    (WRAPPER, "replace --name on `wrangler deploy`", "the wrapper's comment on WRANGLER_CI_OVERRIDE_NAME"),
    (WRAPPER, 'npx wrangler deploy --config wrangler.toml --var "GIT_COMMIT:$commit"', "THE deploy command"),
    (WRAPPER, "replaces plain variables with the ones in", "the wrapper's comment on SOAK"),
    ("tools/selfhost/deploy_soak.sh", "replace --name on `wrangler deploy`", "the soak worker's wrapper"),
    ("tools/selfhost/deploy_soak.sh", '--config "$SOAK_CONFIG" --name "$SOAK_NAME"',
     "the soak worker (econdl-api-soak), not econdl-api"),
    ("api/worker/wrangler.origin.toml", "Everything below is chosen so a", "says why that config can never deploy"),
    (".claude/skills/econ-updater/references/10-iron-rules.md", "`wrangler deploy --dry-run` (real esbuild)",
     "a dry run builds and uploads nothing"),
    (".claude/skills/econ-updater/references/50-queue.md", "guard imf_*_direct!)",
     "history: the 2026-08-06 IMF retirement plan as it was written"),
    ("WORKLOG.md", "classifies as `deploy`", "history: a 2026-08-30 work-log entry"),
    (".github/workflows/deploy-site.yml", "command: pages deploy catalog/site --project-name=econdatalibrary",
     "the step whose command is the static site's Pages deploy, not the worker"),
    ("assistant/package.json", '"deploy": "wrangler deploy"', "another worker (elkassabgidata-assistant)"),
    ("assistant/README.md", "elkassabgidata-assistant.elkassabgi.workers.dev", "another worker"),
    ("assistant/README.md", "ANTHROPIC_MAX_OUTPUT_TOKENS", "another worker"),
    ("mcp/package.json", '"deploy": "wrangler deploy"', "another worker (elkassabgidata-mcp)"),
    ("tests/test_deploy_edge_refuses_a_soak_edge.py", "npx wrangler deploy\" in calls",
     "the wrapper's own test: a stand-in npx records the deploy"),
    ("tests/test_deploy_edge_refuses_a_soak_edge.py", "--var GIT_COMMIT:{COMMIT}",
     "the wrapper's own test: the exact deploy line"),
    ("tests/test_soak_deploy_and_delete_wrappers.py", "DEPLOY_CMD = ", "the soak wrapper's own test"),
    ("tests/test_wrangler_origin_config.py", "would be used by", "says why an [env.x] block is refused"),
    ("tests/test_wrangler_toml_top_level_keys.py", "interactive account picker", "explains a config test"),
]

# The instruction surfaces this change rewrote: each must name the wrapper (positive control for the ratchet:
# removing the line does not satisfy it, replacing it does).
REWRITTEN = ["api/DEPLOY.md", "api/worker/README.md", "api/worker/wrangler.toml",
             ".claude/skills/econ-completion/SKILL.md",
             ".claude/skills/econ-completion/references/protocols.md",
             ".claude/skills/econ-completion/references/state-baseline.md",
             ".claude/skills/econ-updater/SKILL.md",
             ".claude/skills/econ-updater/references/00-architecture.md",
             ".claude/skills/econ-updater/references/20-per-source-procedure.md",
             ".claude/skills/econ-updater/references/30-serving-pipeline.md",
             "docs/ECONLIB_COMPLETION_PLAN.md", "PXWEB_PUBLISH_FLIP_PLAN.md",
             "tools/catalog_cepii_baci.py", "tools/catalog_imts_tables.py", "tools/cost/idb_backlink_check.py",
             "tools/cost/smoke_public.py", "tools/retire_source.py", "core/cutover.py"]
KNOWN = ["api/DEPLOY.md", "api/worker/package.json", ".claude/skills/econ-updater/SKILL.md",
         ".github/workflows/deploy-site.yml", WRAPPER]


def _scan(root: str = ROOT):
    tracked = _repo_walk.tracked_files(root)
    assert tracked is not None, f"{root} is not a git checkout: the ratchet cannot list what it must scan"
    missing = [k for k in KNOWN if k not in tracked]
    assert not missing, f"the tracked-file list lacks {missing}: the enumerator is wrong, the scan would be blind"
    hits, scanned = [], 0
    for rel in tracked:
        if rel == SELF:
            continue
        p = os.path.join(root, *rel.split("/"))
        if not os.path.isfile(p):               # tracked but deleted in this working tree
            continue
        with open(p, "rb") as fh:
            raw = fh.read()
        if b"\0" in raw[:8192]:
            continue
        scanned += 1
        for line, kind, ctx in find_deploys(raw.decode("utf-8", "replace")):
            hits.append((rel, line, kind, ctx))
    return hits, scanned


def test_no_deploy_instruction_outside_the_allow_list():
    hits, scanned = _scan()
    assert scanned >= len(KNOWN), scanned
    used = set()
    bad = []
    for rel, line, kind, ctx in hits:
        ok = [i for i, (path, phrase, _why) in enumerate(ALLOWED) if path == rel and phrase in ctx]
        if ok:
            used.update(ok)
        else:
            bad.append(f"{rel}:{line}: {kind} | {ctx[:160]}")
    assert not bad, ("deploy instructions for econdl-api must name tools/selfhost/deploy_edge.sh (Git Bash, a "
                     "fresh clone of main; api/DEPLOY.md section 4), never wrangler's own deploy command - it "
                     "drops GIT_COMMIT (ledger R2038). If a line is NOT such an instruction, add it to ALLOWED "
                     "with a reason:\n  " + "\n  ".join(bad))
    stale = [ALLOWED[i] for i in range(len(ALLOWED)) if i not in used]
    assert not stale, f"allow-list entries that match nothing (remove them, or the line moved): {stale}"


PLANTED = [
    "cd api/worker && npx wrangler deploy",
    "run: npx wrangler \\\n  deploy",
    'x = ["npx", "wrangler", "deploy"]',
    "npx wrangler -c wrangler.toml deploy",
    "npx wrangler --config=wrangler.toml deploy",
    "npx Wrangler publish",
    "npx wrangler versions upload",
    "npx wrangler versions deploy 2b4cb199@100%",
    "npx wrangler rollback ddbfa8db",
    "npx wrangler@3.114.17 deploy",
    r"node_modules\.bin\wrangler.cmd deploy",
    "WRANGLER DEPLOY",
    "      command: deploy",
    "    - command: 'publish'",
    "        with:\n          command: |\n            deploy --var X:y",
    "      - uses: cloudflare/wrangler-action@v3\n        with:\n          apiToken: t",
    "npm run deploy",
    "npm run-script deploy -- --var GIT_COMMIT:x",
]
LOOK_ALIKES = [
    "wrangler pages deploy catalog/site --project-name=econdatalibrary",
    "npx wrangler deployments list",
    "npx wrangler dev",
    "npx wrangler secret put SOME_TOKEN",
    "wrangler's own deploy command drops GIT_COMMIT",
    "api/worker/wrangler.toml deploy",
    "WRANGLER_CI_OVERRIDE_NAME deploy",
    "          command: pages deploy catalog/site",
    "          command: |\n            pages deploy catalog/site",
    "npm run typecheck",
    "the deploy is manual; see wrangler.soak.toml",
]


@pytest.mark.parametrize("text", PLANTED)
def test_control_every_deploy_shape_is_seen(text):
    assert len(find_deploys(text)) == 1, (text, find_deploys(text))


def test_control_a_second_action_step_cannot_borrow_the_first_steps_command():
    wf = ("jobs:\n  site:\n    steps:\n"
          "      - name: Deploy to Cloudflare Pages\n        uses: cloudflare/wrangler-action@v3\n        with:\n"
          "          apiToken: t\n          command: pages deploy catalog/site --project-name=econdatalibrary\n\n"
          "      # a comment at the step level ends the block\n"
          "      - name: x\n        uses: cloudflare/wrangler-action@v3\n        with:\n          apiToken: t\n")
    uses = [h for h in find_deploys(wf) if h[1] == "wrangler-action use"]
    assert [h[0] for h in uses] == [5, 12], uses
    assert "command: pages deploy catalog/site" in uses[0][2]
    assert "command:" not in uses[1][2] and "comment" not in uses[0][2], uses


@pytest.mark.parametrize("text", LOOK_ALIKES)
def test_control_look_alikes_are_not_seen(text):
    assert find_deploys(text) == [], (text, find_deploys(text))


def test_control_the_scan_sees_a_planted_line_in_a_tracked_file(tmp_path):
    """The whole path - tracked_files, the reader, the matcher, the allow-list - on a throwaway repo: a
    planted instruction in a tracked file fails, the same line in an UNTRACKED file is not scanned (as on CI),
    and a repo without the known files is refused as a blind enumerator."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}

    def git(*a):
        subprocess.run(["git", "-C", str(tmp_path), *a], check=True, capture_output=True, env=env)
    git("init", "-q")
    for k in KNOWN:
        p = tmp_path.joinpath(*k.split("/"))
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("placeholder\n", encoding="utf-8")
    (tmp_path / "docs").mkdir(exist_ok=True)
    (tmp_path / "docs" / "how.md").write_text("Deploy:\n\n    cd api/worker && npx wrangler deploy\n", encoding="utf-8")
    (tmp_path / "scratch.md").write_text("npx wrangler deploy\n", encoding="utf-8")
    git("add", *KNOWN, "docs/how.md")
    hits, _ = _scan(str(tmp_path))
    assert [(h[0], h[1]) for h in hits] == [("docs/how.md", 3)], hits
    # a list without the known files is a blind enumerator, not a clean tree
    git("rm", "-q", "--cached", KNOWN[0])
    with pytest.raises(AssertionError, match="enumerator is wrong"):
        _scan(str(tmp_path))
    # not a checkout at all: refused, never "nothing found"
    with pytest.raises(AssertionError, match="not a git checkout"):
        _scan(str(tmp_path / "docs"))


def test_the_rewritten_instructions_name_the_wrapper():
    lacking = []
    for rel in REWRITTEN:
        with open(os.path.join(ROOT, *rel.split("/")), encoding="utf-8") as fh:
            if WRAPPER not in fh.read():
                lacking.append(rel)
    assert not lacking, f"these instruction surfaces no longer name {WRAPPER}: {lacking}"
    with open(os.path.join(ROOT, "api", "DEPLOY.md"), encoding="utf-8") as fh:
        recipe = fh.read().split("## 4.", 1)[1].split("\n## ", 1)[0]
    for must in ("Git Bash", "never PowerShell", "FRESH clone of main", "npm ci", "bash tools/selfhost/deploy_edge.sh",
                 "verified: <edge>/v1/edge-status answers <sha>"):
        assert must in recipe, f"api/DEPLOY.md section 4 lacks {must!r}"
    assert "npm install" not in recipe, "npm install can change the lockfile's wrangler; the recipe says npm ci"


# ------------------------------------------------------------------------------------- npm run deploy refuses
def _deploy_script() -> str:
    with open(PKG, encoding="utf-8") as fh:
        pkg = json.load(fh)                         # an invalid file FAILS here, inside the test
    scripts = pkg.get("scripts") or {}
    assert not {"predeploy", "postdeploy"} & set(scripts), "a pre/post hook would run around the refusal"
    return scripts.get("deploy")


def test_npm_run_deploy_is_the_reviewed_refusal():
    assert _deploy_script() == EXPECTED_DEPLOY


@pytest.mark.parametrize("args", ["", "--dry-run", "--var GIT_COMMIT:x", "foo"])
def test_npm_run_deploy_refuses_through_the_shell(tmp_path, args):
    """Through npm and the platform shell (cmd.exe on Windows, sh on CI), in a folder with no node_modules."""
    script = _deploy_script()
    assert script == EXPECTED_DEPLOY, "refusing to RUN a deploy script that is not the reviewed refusal"
    if not (shutil.which("npm") and shutil.which("node")):
        if os.environ.get("GITHUB_ACTIONS") == "true":
            pytest.fail("npm/node are missing on CI: the pytest job sets up node, so this check must run there")
        pytest.skip("npm/node are not installed on this machine")
    shutil.copy(PKG, tmp_path / "package.json")
    env = {**os.environ, "npm_config_update_notifier": "false", "npm_config_fund": "false",
           "npm_config_cache": str(tmp_path / "npm-cache")}
    cmd = "npm run deploy" + (f" -- {args}" if args else "")
    r = subprocess.run(cmd, shell=True, cwd=str(tmp_path), capture_output=True, text=True, timeout=180, env=env)
    assert r.returncode == 1, (cmd, r.returncode, r.stdout[-500:], r.stderr[-500:])
    assert "refused: deploy econdl-api only with tools/selfhost/deploy_edge.sh" in r.stderr, r.stderr[-800:]
    assert not (tmp_path / "node_modules").exists()
