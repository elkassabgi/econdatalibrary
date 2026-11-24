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
     wrangler-action `command:` (a block scalar too), and EVERY use of a wrangler GitHub Action (any owner),
     which deploys when it names no command; `npm run deploy` - must sit on an allow-listed (path, phrase) with
     a reason. One exception: in a workflow or composite-action file a use of the action is never allowed by a
     phrase; it must be the `uses` of a step that item 4 parses and judges (a comment, a value or a
     commented-out step that names the action fails). Every other kind still runs on those files.
     A new instruction anywhere fails here. A stale allow-list entry fails too.
  3. The instruction surfaces this change rewrote name the wrapper, and api/DEPLOY.md section 4 carries the
     whole recipe (Git Bash, a fresh clone of main, `npm ci`, the `verified:` receipt).
  4. Every tracked workflow (.yml, .yaml) and composite action (action.yml, action.yaml) is PARSED, with
     duplicate keys refused, and every step whose `uses` contains "wrangler" is judged by what it runs: its
     `uses` must be the action an ACTION_STEPS entry pins (owner/repo, any letter case), at a ref that
     ACTION_REFS lists for it, compared exactly (a fork commit is reachable through the parent's name, so
     the name alone does not pin the code; review of PR #323, ledger R2072), its
     `command` input split per line as the action splits it (absent, null or blank = `deploy`, the action's
     default), each line EQUAL to the command of that entry, and every input it sets named by that entry.
     A `with` key is named exactly as the runner names it, INPUT_ + the key with each space replaced by `_`,
     upper-cased, and nothing trimmed: `"command "` is the input COMMAND_, so the action finds no command and
     runs `wrangler deploy` (review of PR #323, ledger R2060). A key that is not plain ASCII letters, digits,
     `_` or `-` is refused outright. An entry allows one step; a file that does not parse, and a shape the
     walk cannot judge, fail. Item 2's step-block phrase let a step with NO command pass when its block held
     the phrase in a comment or an env value (review of PR #320, condition C6; ledger R2042).
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
import yaml

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
# use must be allow-listed with what it runs. Any owner: a fork runs the same code (review of #320 C6, B1).
_ACTION_USE = re.compile(r"[\w.-]+/wrangler-action\b", re.IGNORECASE)
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
    # NO entry for .github/workflows/deploy-site.yml: its wrangler step is judged by the parse (ACTION_STEPS), and
    # a phrase here would let a comment or a value carry it again (review of #320 C6, B6)
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


def _judge_hits(hits, root: str = ROOT) -> tuple[set[int], list[str]]:
    """(ALLOWED indices used, bad lines) for the text hits. In a workflow or composite-action file a use of the
    wrangler action is NEVER allowed by a phrase: its line must hold the `uses` of a step the parse judges
    (wrangler_uses_lines), so a comment, a value or a commented-out step naming the action fails. Every other
    hit, in every file, needs an ALLOWED (path, phrase)."""
    used, bad, uses_lines = set(), [], {}
    for rel, line, kind, ctx in hits:
        if kind == "wrangler-action use" and _is_action_file(rel):
            if rel not in uses_lines:
                try:
                    uses_lines[rel] = wrangler_uses_lines(_read(root, rel))
                except yaml.YAMLError as e:
                    uses_lines[rel] = set()
                    bad.append(f"{rel}: does not parse, so no use of the action in it can be judged: {e}")
            if line not in uses_lines[rel]:
                bad.append(f"{rel}:{line}: {kind} that is not the `uses` of a step the parse judges (a comment, a "
                           f"value, a commented-out step) | {ctx[:160]}")
            continue
        ok = [i for i, (path, phrase, _why) in enumerate(ALLOWED) if path == rel and phrase in ctx]
        if ok:
            used.update(ok)
        else:
            bad.append(f"{rel}:{line}: {kind} | {ctx[:160]}")
    return used, bad


def test_no_deploy_instruction_outside_the_allow_list():
    hits, scanned = _scan()
    assert scanned >= len(KNOWN), scanned
    used, bad = _judge_hits(hits)
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


# ---------------------------------------------------------------- the wrangler GitHub Action, judged by parse
# Review of PR #320, condition C6 (ledger R2042): the text scan judged a wrangler-action use by its whole step
# block, so an allow phrase in a COMMENT (`# command: pages deploy ...`) or in an env value made a step with NO
# command read as allowed - and with no command the action runs `wrangler deploy` (its action.yml: the `command`
# input "Defaults to "deploy""). Here every tracked workflow (.yml and .yaml) and composite action is PARSED, and
# each step whose `uses` names a wrangler action (any owner, a fork, a docker image) is judged by what it really
# runs: its command, line by line, and every input it sets. In a workflow or action file a text mention of the
# action must be the `uses` of a step judged here (test_no_deploy_instruction_outside_the_allow_list); every
# other text rule still runs on those files.
DEPLOY_SITE = ".github/workflows/deploy-site.yml"

# (path, the action, the exact command, the inputs the step may set, reason). An entry allows ONE step of its
# file: its `uses` is the pinned action (owner/repo compared in any letter case; a fork, another owner, a
# docker image, a local path or a subdirectory is not it) at a ref ACTION_REFS lists for it, every line of its
# command equals the command exactly, and every input it sets is named here. Input names are compared as the
# runner hands them to the action (_input_name: spaces to `_`, upper case, NO trim), so a padded key is another
# input (R2060). A second step does not ride an entry (R2042), and an entry that allows no step is stale.
ACTION_STEPS = [
    (DEPLOY_SITE, "cloudflare/wrangler-action", "pages deploy catalog/site --project-name=econdatalibrary --branch=main",
     ("apiToken", "accountId", "command"), "the static site's Pages deploy, not the worker"),
]
# The refs at which each pinned action may be used, compared EXACTLY (a git ref is case-sensitive). The
# owner/repo pin alone does not pin the code that runs: GitHub resolves the ref on the named repository, and a
# commit that lives only in a fork is reachable through the parent's name (a fork-network sha, or
# `refs/pull/<n>/head`), so `cloudflare/wrangler-action@refs/pull/1/head` with the allowed inputs passed the
# owner/repo pin (review of PR #323, ledger R2072). A new ref (a version bump, a sha pin) goes here in the same
# commit that starts to use it; a listed ref that no allowed step uses is stale.
ACTION_REFS = {
    "cloudflare/wrangler-action": ("v3",),
}
ACTION_DEFAULT_COMMAND = "deploy"            # what the action runs when `command` is absent or blank
_PLAIN_INPUT = re.compile(r"[A-Za-z0-9_-]+")  # the only `with` key spelling a wrangler step may use
_PINNED_USES = re.compile(r"([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)@([^\s@]+)")   # owner/repo@ref, nothing around it


class _UniqueKeyLoader(yaml.SafeLoader):
    """yaml.safe_load, except that a mapping with a repeated key is refused: PyYAML keeps the LAST value
    silently, so a step with two `command:` keys would be judged on one of them. (YAML 1.1 still reads the bare
    key `on` as True; nothing here reads a top-level key but `jobs`, `runs` and `env`.)"""

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _value in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue                             # `<<:` merges; an explicit key may override a merged one
            key = self.construct_object(key_node, deep=deep)
            try:
                repeated = key in seen
            except TypeError:                        # an unhashable key: the base class raises the YAML error
                break
            if repeated:
                raise yaml.constructor.ConstructorError("while constructing a mapping", node.start_mark,
                                                        f"found the key {key!r} twice", key_node.start_mark)
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _load(text: str):
    return yaml.load(text, Loader=_UniqueKeyLoader)


def _input_name(key) -> str:
    """An input name as the runner hands it to the action, copied from its source and NOTHING more: actions/runner
    Handlers/Handler.cs AddInputsToEnvironment sets `INPUT_{key.Replace(' ', '_').ToUpperInvariant()}`, and
    @actions/core getInput reads `INPUT_` + name.replace(/ /g, '_').toUpperCase(). No trim: `"command "` is
    COMMAND_, a different input, and the action then finds no `command` (R2060). Outside ASCII Python's upper()
    is not .NET's ToUpperInvariant, which is why wrangler_step refuses a key that is not _PLAIN_INPUT."""
    return str(key).replace(" ", "_").upper()


def _is_wrangler(uses) -> bool:
    return isinstance(uses, str) and "wrangler" in uses.lower()


def action_of(uses) -> str | None:
    """The `owner/repo`, lower-cased, of `uses: owner/repo@ref` (GitHub resolves owner and repo in any letter
    case); None for any other shape (a docker image, a local path, a subdirectory, no ref, surrounding space)."""
    m = _PINNED_USES.fullmatch(uses) if isinstance(uses, str) else None
    return m.group(1).lower() if m else None


def ref_of(uses) -> str | None:
    """The `ref` of `uses: owner/repo@ref`, exactly as written (a git ref is case-sensitive); None for every
    shape action_of refuses."""
    m = _PINNED_USES.fullmatch(uses) if isinstance(uses, str) else None
    return m.group(2) if m else None


def _is_action_file(rel: str) -> bool:
    """A tracked file GitHub reads steps from: a workflow (.yml or .yaml) or a composite action."""
    low = rel.lower()
    return (low.startswith(".github/workflows/") and low.endswith((".yml", ".yaml"))) or \
        low.rsplit("/", 1)[-1] in ("action.yml", "action.yaml")


def action_lines(value: str) -> list[str]:
    """The commands the action runs for a `command` value, split as @actions/core getMultilineInput does: the
    value trimmed, split on newlines, empty strings dropped, each line trimmed (a blank line becomes '')."""
    return [ln.strip() for ln in value.strip().split("\n") if ln != ""]


def wrangler_step(step: dict) -> tuple[list[str], set[str], list[str]]:
    """(commands, input names, problems) of one step whose `uses` names a wrangler action."""
    w = step.get("with")
    w = {} if w is None else w
    if not isinstance(w, dict):
        return [], set(), [f"`with` is a {type(w).__name__}, not a mapping: cannot judge what the step runs"]
    names: dict[str, list] = {}
    problems = []
    for k in w:
        if not (isinstance(k, str) and _PLAIN_INPUT.fullmatch(k)):
            problems.append(f"the `with` key {k!r} is not a plain input name (ASCII letters, digits, `_`, `-`): the "
                            f"runner passes it untrimmed as {'INPUT_' + _input_name(k)!r}, which may not be the "
                            "input it looks like")
        names.setdefault(_input_name(k), []).append(k)
    problems += [f"`with` spells the {n} input {len(ks)} ways ({ks}): cannot tell which one the action reads"
                 for n, ks in names.items() if len(ks) > 1]
    keys = names.get(_input_name("command"), [])
    if len(keys) > 1:
        return [], set(names), problems
    cmd = w[keys[0]] if keys else None
    if cmd is None:
        return [ACTION_DEFAULT_COMMAND], set(names), problems + [
            "no `command`: the action runs its default, `wrangler deploy`"]
    if not isinstance(cmd, str):
        return [], set(names), problems + [f"`command` is a {type(cmd).__name__} ({cmd!r}), not text"]
    commands = action_lines(cmd)
    if not commands:
        return [ACTION_DEFAULT_COMMAND], set(names), problems + [
            "an empty `command`: the action runs its default, `wrangler deploy`"]
    return commands, set(names), problems


def _env_inputs(env, where: str) -> list[tuple[str, str]]:
    """An env block that sets INPUT_<NAME> could feed an input the step does not set: refused near a wrangler step.
    Broad on purpose (not _input_name): any key that holds INPUT_ in any letter case, anywhere, padded or not."""
    if env is None:
        return []
    if not isinstance(env, dict):
        return [(where, f"`env` is a {type(env).__name__}, not a mapping: cannot tell what it sets")]
    return [(where, f"`env` sets {k!r}, which the action could read as an input") for k in env
            if "INPUT_" in str(k).upper()]


def _walk(doc, rel: str) -> tuple[list[tuple[str, str]], list[tuple[str, dict]]]:
    """(problems, wrangler steps) of one parsed workflow (jobs.<id>.steps) or composite action (runs.steps).
    Every shape the walk cannot judge is a problem, never a skip."""
    if not isinstance(doc, dict):
        return [("<document>", f"the document is a {type(doc).__name__}, not a mapping")], []
    problems, groups, envs = [], [], [("env", doc.get("env"))]
    if rel.lower().rsplit("/", 1)[-1] in ("action.yml", "action.yaml"):
        runs = doc.get("runs")
        if runs is not None and not isinstance(runs, dict):
            problems.append(("runs", f"`runs` is a {type(runs).__name__}, not a mapping"))
        elif isinstance(runs, dict) and "steps" in runs:
            groups.append(("runs.steps", runs["steps"]))
    else:
        jobs = doc.get("jobs")
        if not isinstance(jobs, dict):
            return [("jobs", f"`jobs` is a {type(jobs).__name__}, not a mapping")], []
        for jid, job in jobs.items():
            if not isinstance(job, dict):
                problems.append((f"jobs.{jid}", f"the job is a {type(job).__name__}, not a mapping"))
                continue
            if _is_wrangler(job.get("uses")):
                problems.append((f"jobs.{jid}", f"the job-level `uses: {job['uses']}` names a wrangler action"))
            envs.append((f"jobs.{jid}.env", job.get("env")))
            if "steps" in job:
                groups.append((f"jobs.{jid}.steps", job["steps"]))
    steps = []
    for where, seq in groups:
        if not isinstance(seq, list):
            problems.append((where, f"`steps` is a {type(seq).__name__}, not a list"))
            continue
        for i, st in enumerate(seq):
            at = f"{where}[{i}]"
            if not isinstance(st, dict):
                problems.append((at, f"the step is a {type(st).__name__}, not a mapping"))
            elif "uses" in st and not isinstance(st["uses"], str):
                problems.append((at, f"`uses` is a {type(st['uses']).__name__}, not text"))
            elif _is_wrangler(st.get("uses")):
                steps.append((at, st))
                envs.append((f"{at}.env", st.get("env")))
            elif st.get("with") is not None and not isinstance(st["with"], dict):
                problems.append((at, f"`with` is a {type(st['with']).__name__}, not a mapping"))
    if steps:
        for where, env in envs:
            problems += _env_inputs(env, where)
    return problems, steps


def action_step_verdicts(text: str, rel: str) -> list[tuple[str, list[str], list[str], int | None]]:
    """[(where, commands, problems, the ACTION_STEPS index that allowed it)] for one workflow / composite-action
    file: one row per wrangler step, then one per shape the walk could not judge. A row passes when `problems` is
    empty. A file that does not parse RAISES (yaml.YAMLError): it is never skipped."""
    problems, steps = _walk(_load(text), rel)
    entries = [(i, action.lower(), cmd, {_input_name(k) for k in keys})
               for i, (path, action, cmd, keys, _why) in enumerate(ACTION_STEPS) if path == rel]
    used: set[int] = set()
    rows = []
    for where, step in steps:
        commands, inputs, bad = wrangler_step(step)
        uses = action_of(step.get("uses"))
        ref = ref_of(step.get("uses"))
        allowed_by = None
        if not bad:
            fits = [i for i, action, cmd, keys in entries
                    if uses == action and ref in ACTION_REFS.get(action, ())
                    and all(c == cmd for c in commands) and inputs <= keys]
            free = [i for i in fits if i not in used]
            if free:
                allowed_by = free[0]
                used.add(allowed_by)
            elif fits:
                bad.append(f"its ACTION_STEPS entry already allowed an earlier step in {rel}")
            else:
                bad.append(f"uses {step.get('uses')!r}, runs {commands} with inputs {sorted(inputs)}: no "
                           f"ACTION_STEPS entry for {rel} allows it")
        rows.append((where, commands, bad, allowed_by))
    return rows + [(where, [], [p], None) for where, p in problems]


def _node_pairs(node) -> list:
    """A mapping node's (key, value) pairs, explicit keys first, then those a `<<:` merge brings in."""
    own, merged = [], []
    for k, v in node.value:
        if k.tag == "tag:yaml.org,2002:merge":
            for m in (v.value if isinstance(v, yaml.SequenceNode) else [v]):
                if isinstance(m, yaml.MappingNode):
                    merged += _node_pairs(m)
        else:
            own.append((k, v))
    return own + merged


def _node_get(node, key: str):
    if isinstance(node, yaml.MappingNode):
        for k, v in _node_pairs(node):
            if isinstance(k, yaml.ScalarNode) and k.value == key:
                return v
    return None


def wrangler_uses_lines(text: str) -> set[int]:
    """The 1-based lines that hold the `uses` value of a step (or job) the walk above judges as a wrangler step:
    the only places in a workflow or action file where a text mention of the action is allowed."""
    root = yaml.compose(text, Loader=_UniqueKeyLoader)
    seqs, lines = [], set()

    def note(u):
        if isinstance(u, yaml.ScalarNode) and "wrangler" in u.value.lower():
            lines.add(u.start_mark.line + 1)
    jobs = _node_get(root, "jobs")
    if isinstance(jobs, yaml.MappingNode):
        for _k, job in _node_pairs(jobs):
            note(_node_get(job, "uses"))
            seqs.append(_node_get(job, "steps"))
    seqs.append(_node_get(_node_get(root, "runs"), "steps"))
    for seq in seqs:
        if isinstance(seq, yaml.SequenceNode):
            for st in seq.value:
                note(_node_get(st, "uses"))
    return lines


def _read(root: str, rel: str) -> str:
    with open(os.path.join(root, *rel.split("/")), encoding="utf-8-sig") as fh:
        return fh.read()


def _scan_action_steps(root: str = ROOT):
    """(bad, rows, files) over every tracked workflow / composite-action file; rows are (path, where, commands,
    problems, entry index). A file that does not parse is a `bad` line naming it."""
    tracked = _repo_walk.tracked_files(root)
    assert tracked is not None, f"{root} is not a git checkout: the parse check cannot list what it must read"
    files = [rel for rel in tracked if _is_action_file(rel) and os.path.isfile(os.path.join(root, *rel.split("/")))]
    assert DEPLOY_SITE in files, f"{DEPLOY_SITE} is not among the workflow files {files}: the enumerator is blind"
    bad, rows = [], []
    for rel in files:
        try:
            verdicts = action_step_verdicts(_read(root, rel), rel)
        except yaml.YAMLError as e:
            bad.append(f"{rel}: does not parse, so its steps cannot be judged: {e}")
            continue
        for where, commands, problems, entry in verdicts:
            rows.append((rel, where, commands, problems, entry))
            bad += [f"{rel} {where}: {p}" for p in problems]
    return bad, rows, files


def test_every_wrangler_step_runs_an_allowed_command():
    bad, rows, files = _scan_action_steps()
    assert len(files) >= 10, files                  # 10 tracked workflows on 899ffc3f5
    assert not bad, ("a step whose `uses` names a wrangler action must run exactly what an ACTION_STEPS entry "
                     "allows; with no command it runs `wrangler deploy`, which deploys econdl-api without "
                     "GIT_COMMIT (R2038):\n  " + "\n  ".join(bad))
    for i, (path, _action, cmd, _keys, _why) in enumerate(ACTION_STEPS):
        n = sum(1 for r in rows if r[4] == i)
        assert n == 1, f"ACTION_STEPS entry ({path}, {cmd!r}) allows {n} steps; it must allow exactly one (stale?)"
    # the positive control, by value: the whole tree has ONE wrangler step, deploy-site.yml's, and it is allowed
    assert [(r[0], r[1], r[2]) for r in rows] == [(DEPLOY_SITE, "jobs.deploy.steps[1]", [ACTION_STEPS[0][2]])], rows
    text = _read(ROOT, DEPLOY_SITE)
    assert wrangler_uses_lines(text) == {line for line, kind, _c in find_deploys(text) if kind == "wrangler-action use"}
    # ACTION_REFS lists exactly the (action, ref) pairs the allowed steps use: a listed ref no step uses is
    # stale, and every pinned action has its refs listed (R2072)
    assert set(ACTION_REFS) == {action.lower() for _p, action, _c, _k, _w in ACTION_STEPS}, ACTION_REFS
    _problems, steps = _walk(_load(text), DEPLOY_SITE)
    allowed_at = {r[1] for r in rows if r[0] == DEPLOY_SITE and r[4] is not None}
    used = {(action_of(s.get("uses")), ref_of(s.get("uses"))) for where, s in steps if where in allowed_at}
    assert used == {(a, ref) for a, refs in ACTION_REFS.items() for ref in refs}, used


_C6_PHRASE = "command: pages deploy catalog/site --project-name=econdatalibrary"   # 899ffc3f5's allow phrase
_CMD = "pages deploy catalog/site --project-name=econdatalibrary --branch=main"
_WF = "name: x\non:\n  workflow_dispatch:\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n" \
      "      - uses: actions/checkout@v6\n"
_REAL = ("      - name: Deploy to Cloudflare Pages\n        uses: cloudflare/wrangler-action@v3\n        with:\n"
         f"          apiToken: t\n          accountId: a\n          command: {_CMD}\n")
_WORKER = ("      - name: Deploy the worker\n        uses: cloudflare/wrangler-action@v3\n        with:\n"
           "          apiToken: t\n          accountId: a\n")
_STEP = "      - name: s\n        uses: cloudflare/wrangler-action@v3\n        with:\n          apiToken: t\n"
# Review of PR #323 (ledger R2060). F2: a padded key is ANOTHER input to the runner (no trim), so a step whose
# only command key is padded runs `wrangler deploy`. F3: a step that is not the pinned action, with the allowed
# inputs, runs code the ratchet never read. Each is planted through the pure judge AND as deploy-site.yml's own
# step through the full path (text scan + parse, on a throwaway repository).
_PADDED_OR_UNPINNED = {
    'the key "command " (quoted, trailing space)': _WORKER + f'          "command ": {_CMD}\n',
    'the key " command" (quoted, leading space)': _WORKER + f'          " command": {_CMD}\n',
    'the key "command<TAB>" (quoted)': _WORKER + f'          "command\\t": {_CMD}\n',
    "the key command<NBSP>, unquoted (invisible)": _WORKER + f"          command\u00a0: {_CMD}\n",
    'the key "apiToken " beside the real command': _REAL.replace("apiToken:", '"apiToken ":'),
    "a fork with the allowed inputs": _REAL.replace("cloudflare/wrangler-action@v3", "someone/wrangler-action@v3"),
    "a docker image with the allowed inputs": _REAL.replace("cloudflare/wrangler-action@v3", "docker://node-wrangler:3"),
    "a look-alike owner with the allowed inputs": _REAL.replace("cloudflare/wrangler-action@v3",
                                                                "cloudf1are/wrangler-action@v3"),
    # R2072: the pinned name at a ref ACTION_REFS does not list, each with the allowed inputs. A fork's commit is
    # reachable through the parent's name, so these could run code nobody reviewed.
    "a pull-request ref of the pinned action": _REAL.replace("wrangler-action@v3", "wrangler-action@refs/pull/1/head"),
    "a commit sha of the pinned action": _REAL.replace("wrangler-action@v3", "wrangler-action@" + "0123456789abcdef" * 2
                                                       + "01234567"),
    "another tag of the pinned action": _REAL.replace("wrangler-action@v3", "wrangler-action@v2"),
    "the listed tag in another letter case": _REAL.replace("wrangler-action@v3", "wrangler-action@V3"),
    "a branch of the pinned action": _REAL.replace("wrangler-action@v3", "wrangler-action@main"),
}
ACTION_PLANTED = {
    **_PADDED_OR_UNPINNED,
    # F3: other shapes that are not the pinned action, each with the allowed inputs
    "a subdirectory of the pinned repository": _REAL.replace("wrangler-action@v3", "wrangler-action/sub@v3"),
    "another repository of the owner": _REAL.replace("wrangler-action@v3", "wrangler-actions@v3"),
    "the pinned repository with no ref": _REAL.replace("wrangler-action@v3", "wrangler-action"),
    "a local action path": _REAL.replace("cloudflare/wrangler-action@v3", "./.github/actions/wrangler"),
    "a uses value with a leading space": _REAL.replace("cloudflare/wrangler-action@v3",
                                                       '" cloudflare/wrangler-action@v3"'),
    # a key outside ASCII: Python's upper() reads `ap<dotless i>Token` as APITOKEN; refused, not trusted
    "a key with a non-ASCII letter": _REAL.replace("apiToken:", "ap\u0131Token:"),
    "a key YAML reads as a number": _REAL + "          1: x\n",
    "a padded, lower-case env key that names an input": _REAL + '        env:\n          " input_command": deploy\n',
    # the two C6 shapes, each alone and after the real step
    "no command, the C6 phrase in a comment": _WORKER + f"          # {_C6_PHRASE}\n",
    "no command, the C6 phrase in a comment, after the real step": _REAL + _WORKER + f"          # {_C6_PHRASE}\n",
    "no command, the C6 phrase in an env value": _WORKER + f"        env:\n          NOTE: \"{_C6_PHRASE}\"\n",
    # B1: which steps
    "a fork of the action, no command": _REAL + "      - uses: someone/wrangler-action@v3\n        with:\n"
        "          apiToken: t\n",
    "the action in mixed case, no command": "      - uses: Cloudflare/Wrangler-Action@v3\n        with:\n"
        "          apiToken: t\n",
    "a docker image of wrangler, no command": "      - uses: docker://node-wrangler:3\n",
    "a flow-style step, no command": "      - {uses: cloudflare/wrangler-action@v3, with: {apiToken: t}}\n",
    "a second wrangler step without a command": _REAL + _STEP,
    "a second wrangler step with no with: at all": _REAL + "      - uses: cloudflare/wrangler-action@v3\n",
    "the allowed step twice": _REAL + _REAL,
    "an anchored step reused by alias in a second job": "      - &w\n        uses: cloudflare/wrangler-action@v3\n"
        f"        with:\n          apiToken: t\n          # {_C6_PHRASE}\n  other:\n    runs-on: x\n    steps:\n"
        "      - *w\n",
    "a merge key that inherits the action without its command": "      - &pages\n" + _REAL.split("\n", 1)[1]
        + "      - <<: *pages\n        with:\n          apiToken: t\n",
    # B2: the command
    "COMMAND: deploy beside the real command": _REAL + "          COMMAND: deploy\n",
    "Command: deploy alone": _STEP + "          Command: deploy\n",
    "command: ''": _STEP + "          command: ''\n",
    "command: null": _STEP + "          command:\n",
    "command: whitespace": _STEP + "          command: '   '\n",
    "a blank block scalar": _STEP + "          command: |\n\n",
    "command: deploy": _STEP + "          command: deploy\n",
    "wrangler versions upload": _STEP + "          command: versions upload\n",
    "a literal block: the real command, then deploy": _STEP + f"          command: |\n            {_CMD}\n"
        "            deploy\n",
    "a quoted two-line value, the C6 phrase in a comment": _STEP + f"          # {_C6_PHRASE}\n"
        f"          command: \"{_CMD}\\ndeploy\"\n",
    "the real command, then an expression line": _STEP + f"          command: |\n            {_CMD}\n"
        "            ${{ inputs.extra }}\n",
    "a whitespace line between two commands": _STEP + f"          command: \"{_CMD}\\n   \\n{_CMD}\"\n",
    "a line that starts with the real command": _STEP + f"          command: {_CMD} --commit-dirty=true\n",
    "a line that contains the real command": _STEP + f"          command: deploy --x '{_CMD}'\n",
    "the Pages command for another project": _STEP + "          command: pages deploy catalog/site --project-name=x\n",
    "a command that is a list": _STEP + "          command: [deploy]\n",
    "a command that is a mapping": _STEP + "          command: {deploy: 1}\n",
    "with: is a list": "      - name: s\n        uses: cloudflare/wrangler-action@v3\n        with: [deploy]\n",
    # B3: the whole input set
    "preCommands from a dispatch input": _REAL + "          preCommands: ${{ inputs.pre }}\n",
    "a postCommands block": _REAL + "          postCommands: |\n            cd api/worker\n            npx wrangler deploy\n",
    "workingDirectory": _REAL + "          workingDirectory: api/worker\n",
    "wranglerVersion": _REAL + "          wranglerVersion: '2.19.0'\n",
    "environment": _REAL + "          environment: production\n",
    "secrets": _REAL + "          secrets: X\n",
    "two spellings of an allowed input": _REAL + "          APITOKEN: t2\n",
    "a step env that feeds INPUT_COMMAND": _REAL + "        env:\n          INPUT_COMMAND: deploy\n",
    # B4: shapes the walk cannot judge
    "a step that is not a mapping": _REAL + "      - just a string\n",
    "a step whose uses is a list": _REAL + "      - uses: [cloudflare/wrangler-action@v3]\n",
}
_JOB_PLANTED = {
    # B4: whole-document shapes, each a complete file
    "a job-level uses that names a wrangler action": "on: push\njobs:\n  call:\n"
        "    uses: someone/wrangler-action/.github/workflows/deploy.yml@v1\n",
    "a job that is not a mapping": "on: push\njobs:\n  deploy: just text\n",
    "steps that are not a list": "on: push\njobs:\n  deploy:\n    runs-on: x\n    steps: {uses: x}\n",
    "jobs that are not a mapping": "on: push\njobs: [a, b]\n",
    "a document that is not a mapping": "- uses: cloudflare/wrangler-action@v3\n",
    "a workflow env that feeds INPUT_COMMAND": "on: push\nenv:\n  INPUT_COMMAND: deploy\n" + _WF.split("\n", 3)[3]
        + _REAL,
    "a non-wrangler step whose with: is text": "on: push\njobs:\n  deploy:\n    runs-on: x\n    steps:\n"
        "      - uses: actions/checkout@v6\n        with: text\n",
}
ACTION_ALLOWED_SHAPES = {
    "the real step": _REAL,
    "the real step with CRLF line ends": _REAL.replace("\n", "\r\n"),
    "a one-line literal block scalar": _REAL.replace(f"command: {_CMD}\n", f"command: |\n            {_CMD}\n"),
    "a folded block scalar": _REAL.replace(f"command: {_CMD}\n", f"command: >-\n            {_CMD}\n"),
    "inputs spelled in another case": _REAL.replace("command:", "Command:").replace("apiToken:", "APITOKEN:"),
    "the real step without accountId": _REAL.replace("          accountId: a\n", ""),
    "the action in another letter case": _REAL.replace("cloudflare/wrangler-action@v3", "Cloudflare/Wrangler-Action@v3"),
    "another action with command: deploy (not wrangler)": _REAL + "      - uses: some/other-action@v1\n"
        "        with:\n          command: deploy\n",
}


def test_parse_check_accepts_a_sha_pin_only_once_it_is_listed(monkeypatch):
    """A sha pin (the usual hardening) passes once ACTION_REFS lists that sha, and not before (R2072): the
    allow-list is the one place a ref is approved. Until 2026-11-24 any ref passed here."""
    sha = "0123456789abcdef" * 2 + "01234567"
    text = _WF + _REAL.replace("@v3", "@" + sha)
    assert any(r[2] for r in action_step_verdicts(text, DEPLOY_SITE))              # not listed: refused
    monkeypatch.setitem(ACTION_REFS, "cloudflare/wrangler-action", ("v3", sha))
    assert [(r[2], r[3]) for r in action_step_verdicts(text, DEPLOY_SITE)] == [([], 0)]
    upper = _WF + _REAL.replace("@v3", "@" + sha.upper())                         # exact, not case-folded
    assert any(r[2] for r in action_step_verdicts(upper, DEPLOY_SITE))


@pytest.mark.parametrize("name", list(ACTION_PLANTED))
def test_parse_check_refuses_a_step(name):
    rows = action_step_verdicts(_WF + ACTION_PLANTED[name], DEPLOY_SITE)
    assert any(r[2] for r in rows), (name, rows)
    if ACTION_PLANTED[name].startswith(_REAL + "      - "):     # a separate bad step after the real one: R2042's
        real = [r for r in rows if r[0] == "jobs.deploy.steps[1]"]   # control - the real step still passes
        assert [(r[2], r[3]) for r in real] == [([], 0)], (name, rows)


@pytest.mark.parametrize("name", list(_JOB_PLANTED))
def test_parse_check_refuses_a_shape_it_cannot_judge(name):
    rows = action_step_verdicts(_JOB_PLANTED[name], DEPLOY_SITE)
    assert any(r[2] for r in rows), (name, rows)


@pytest.mark.parametrize("name", list(ACTION_ALLOWED_SHAPES))
def test_parse_check_accepts_the_allowed_step(name):
    rows = action_step_verdicts(_WF + ACTION_ALLOWED_SHAPES[name], DEPLOY_SITE)
    assert [(r[2], r[3]) for r in rows] == [([], 0)], (name, rows)


@pytest.mark.parametrize("key, runner", [
    ("command", "COMMAND"), ("Command", "COMMAND"), ("apiToken", "APITOKEN"), ("api token", "API_TOKEN"),
    ("pre-commands", "PRE-COMMANDS"), ("command ", "COMMAND_"), (" command", "_COMMAND"),
    ("command\t", "COMMAND\t"), ("command\u00a0", "COMMAND\u00a0"), ("\tcommand", "\tCOMMAND")])
def test_input_names_map_as_the_runner_maps_them(key, runner):
    """By value, from the runner's source (Handler.cs AddInputsToEnvironment): spaces to `_`, upper case, no trim."""
    assert _input_name(key) == runner


def test_parse_check_refuses_a_repeated_key():
    for dup in (f"          command: {_CMD}\n          command: deploy\n",
                f"          command: deploy\n          command: {_CMD}\n"):   # PyYAML would keep the LAST
        with pytest.raises(yaml.YAMLError, match="twice"):
            action_step_verdicts(_WF + _STEP + dup, DEPLOY_SITE)


def test_parse_check_allows_nothing_in_another_file():
    rows = action_step_verdicts(_WF + _REAL, ".github/workflows/other.yml")
    assert len(rows) == 1 and rows[0][2] and rows[0][3] is None, rows


def _throwaway_repo(tmp_path, files: dict[str, str], untracked: dict[str, str] | None = None):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}

    def git(*a):
        subprocess.run(["git", "-C", str(tmp_path), *a], check=True, capture_output=True, env=env)
    tmp_path.mkdir(parents=True, exist_ok=True)
    git("init", "-q")
    for k in KNOWN:
        files.setdefault(k, "placeholder\n")
    for rel, text in {**files, **(untracked or {})}.items():
        p = tmp_path.joinpath(*rel.split("/"))
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    git("add", *files)
    return git


_REPO_PLANTED = {
    # each a tracked file beside an allowed deploy-site.yml; each must make the throwaway scan fail on it
    "a fork in a .yml workflow": (".github/workflows/fork.yml", _WF + "      - uses: someone/wrangler-action@v3\n"),
    "mixed case in a .yml workflow": (".github/workflows/case.yml", _WF + "      - uses: CloudFlare/WRANGLER-action@v3\n"),
    "a .yaml workflow": (".github/workflows/site.yaml", _WF + _STEP),
    "a composite action.yml": (".github/actions/deploy/action.yml", "name: d\nruns:\n  using: composite\n  steps:\n"
                               "    - uses: cloudflare/wrangler-action@v3\n      with:\n        apiToken: t\n"),
    "a composite action.yaml": ("tools/action.yaml", "name: d\nruns:\n  using: composite\n  steps:\n"
                                "    - uses: cloudflare/wrangler-action@v3\n"),
    "a workflow that does not parse": (".github/workflows/broken.yml", "on: push\njobs:\n  a: [\n"),
}


@pytest.mark.parametrize("name", list(_REPO_PLANTED))
def test_control_the_parse_check_reads_every_tracked_workflow_and_action(tmp_path, name):
    rel, text = _REPO_PLANTED[name]
    _throwaway_repo(tmp_path, {DEPLOY_SITE: _WF + _REAL, rel: text})
    bad, rows, files = _scan_action_steps(str(tmp_path))
    assert rel in files, files
    assert bad and all(b.startswith(rel) for b in bad), (name, bad)
    assert [r[4] for r in rows if r[0] == DEPLOY_SITE] == [0], rows       # the allowed step still passes


def test_control_the_parse_check_reads_tracked_files_only(tmp_path):
    """Untracked files are not read (CI checks out tracked files only), and a tree without deploy-site.yml is
    refused as a blind enumerator, never "nothing found"."""
    git = _throwaway_repo(tmp_path, {DEPLOY_SITE: _WF + _REAL}, untracked={".github/workflows/scratch.yml": _WF + _STEP})
    bad, rows, files = _scan_action_steps(str(tmp_path))
    assert (bad, [r[0] for r in rows]) == ([], [DEPLOY_SITE]) and ".github/workflows/scratch.yml" not in files
    git("rm", "-q", "--cached", DEPLOY_SITE)
    with pytest.raises(AssertionError, match="enumerator is blind"):
        _scan_action_steps(str(tmp_path))


_TEXT_PLANTED = {
    # B5: in a workflow whose wrangler step is allowed, a text mention of the action that is not a judged step's
    # `uses`, and a deploy-class command, each fail the text scan
    "a commented-out step": _WF + "      # - uses: cloudflare/wrangler-action@v3\n" + _REAL,
    "the action named in an env value": _WF + _REAL + "        env:\n          NOTE: cloudflare/wrangler-action@v3\n",
    "a run: npx wrangler deploy step": _WF + _REAL + "      - run: cd api/worker && npx wrangler deploy\n",
    "a with.command of deploy on another action": _WF + _REAL + "      - uses: some/other-action@v1\n        with:\n"
        "          command: deploy\n",
}


@pytest.mark.parametrize("name", list(_TEXT_PLANTED))
def test_control_the_text_scan_of_a_workflow_is_not_satisfied_by_a_phrase(tmp_path, name):
    planted, clean = tmp_path / "planted", tmp_path / "clean"
    _throwaway_repo(planted, {DEPLOY_SITE: _TEXT_PLANTED[name]})
    hits, _scanned = _scan(str(planted))
    _used, bad = _judge_hits(hits, str(planted))
    assert bad and all(b.startswith(DEPLOY_SITE) for b in bad), (name, bad)
    # the same file with only the real step is clean: the hit on the real `uses` line is the judged step
    _throwaway_repo(clean, {DEPLOY_SITE: _WF + _REAL})
    hits, _scanned = _scan(str(clean))
    assert [h[2] for h in hits] == ["wrangler-action use"] and _judge_hits(hits, str(clean)) == (set(), []), hits


def _full_path(root: str) -> tuple[list[str], list]:
    """Everything the ratchet reports for a tree: the text scan's bad lines plus the parse's, and the parse rows."""
    hits, _scanned = _scan(root)
    _used, text_bad = _judge_hits(hits, root)
    parse_bad, rows, _files = _scan_action_steps(root)
    return text_bad + parse_bad, rows


@pytest.mark.parametrize("name", list(_PADDED_OR_UNPINNED))
def test_control_a_padded_or_unpinned_deploy_site_step_fails_the_full_path(tmp_path, name):
    """F2 / F3 of the #323 review (R2060): deploy-site.yml's ONE wrangler step, padded or not the pinned action,
    fails the whole ratchet. Before the fix the four padded command keys and the fork passed it."""
    _throwaway_repo(tmp_path, {DEPLOY_SITE: _WF + _PADDED_OR_UNPINNED[name]})
    bad, rows = _full_path(str(tmp_path))
    assert bad and all(b.startswith(DEPLOY_SITE) for b in bad), (name, bad, rows)
    assert [(r[0], r[4]) for r in rows if r[1] == "jobs.deploy.steps[1]"] == [(DEPLOY_SITE, None)], (name, rows)


def test_control_the_padded_plants_hold_what_they_name():
    """By code point, so a plant that an editor or a replacement changed fails here instead of testing nothing."""
    p = _PADDED_OR_UNPINNED
    assert ('"command ":' in p['the key "command " (quoted, trailing space)']
            and '" command":' in p['the key " command" (quoted, leading space)']
            and '"command' + chr(92) + 't":' in p['the key "command<TAB>" (quoted)']
            and "command" + chr(0xA0) + ":" in p["the key command<NBSP>, unquoted (invisible)"]
            and '"apiToken ":' in p['the key "apiToken " beside the real command']
            and "ap" + chr(0x131) + "Token:" in ACTION_PLANTED["a key with a non-ASCII letter"])
    # and YAML keeps the NBSP in the unquoted key (a plain scalar trims only spaces and tabs)
    assert _load(_WF + p["the key command<NBSP>, unquoted (invisible)"])["jobs"]["deploy"]["steps"][1]["with"] == \
        {"apiToken": "t", "accountId": "a", "command" + chr(0xA0): _CMD}
    # R2072: each ref plant is the real step with ONLY its ref changed, and that ref is not listed
    for name, ref in (("a pull-request ref of the pinned action", "refs/pull/1/head"),
                      ("a commit sha of the pinned action", "0123456789abcdef" * 2 + "01234567"),
                      ("another tag of the pinned action", "v2"),
                      ("the listed tag in another letter case", "V3"),
                      ("a branch of the pinned action", "main")):
        step = _load(_WF + p[name])["jobs"]["deploy"]["steps"][1]
        assert step["uses"] == "cloudflare/wrangler-action@" + ref, (name, step["uses"])
        assert step["with"] == {"apiToken": "t", "accountId": "a", "command": _CMD}, name
        assert ref not in ACTION_REFS["cloudflare/wrangler-action"], name
    assert len("0123456789abcdef" * 2 + "01234567") == 40


def test_control_the_real_step_passes_the_full_path(tmp_path):
    _throwaway_repo(tmp_path, {DEPLOY_SITE: _WF + _REAL})
    bad, rows = _full_path(str(tmp_path))
    assert (bad, [(r[0], r[1], r[2], r[3], r[4]) for r in rows]) == \
        ([], [(DEPLOY_SITE, "jobs.deploy.steps[1]", [_CMD], [], 0)]), (bad, rows)


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
