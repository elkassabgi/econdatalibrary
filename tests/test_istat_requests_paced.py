"""TE: every code path in the repo that can send to an ISTAT host takes a token of the shared budget (DESIGN 12 TE,
C9b; verdict V11; review B phase 1, item 7).

THE LIMIT. ISTAT allows "5 queries per minute ... for each IP" and blocks for 1 to 2 days above it
(https://www.istat.it/en/classifications-and-tools/sdmx-web-services/, read 2026-11-24). The budget is shared by
every participant (jobs/_istat_pacer.py), so ONE unpaced requester anywhere breaks it for all of them.

THE SWEEP, over every TRACKED file (git ls-files; tests/ excluded; jobs/_istat_pacer.py is the pacer):
  1. A Python module is an ISTAT REQUESTER when it names an ISTAT host in a non-docstring string, or imports one
     of the four ISTAT modules (V11a: their HOSTS / base URLs), AND contains a send call (requests.*, a
     `.get/.post/.request/.send/.open(...)` with a timeout, urlopen, socket.create_connection, a socket
     `.connect((host, port))`, http.client connections, urllib3). In a requester EACH send call site must be
     preceded, in its own function, by a token: a call to `<pacer>.acquire`, or to a function of the module that
     calls it (review B: "the module calls acquire somewhere" passed a module that leaves one site unpaced).
  2. No tracked Python names an ISTAT IP literal (193.204.x.x) outside comments and docstrings: the pacer only
     takes tokens for *.istat.it names, so a request to an IP literal could not be paced.
  3. istat.py defines no current_vintage / probe: tools/audit_current_vintage.py and audit_vintage_stability.py
     call that on EVERY fetcher, unpaced.
  4. No tracked YAML/JSON carries an ISTAT URL except the two files no requester reads URLs from (the registry and
     the classification sidecar: prose and homepage links). updater/notices.yaml in particular, whose feeds
     jobs/watch_notices.py fetches daily ON THIS WORKSTATION at 12 per minute per host (V11b).
  5. No *.ps1 / *.sh / *.yml / *.cmd line sends to an ISTAT host (Invoke-WebRequest, Invoke-RestMethod, curl,
     wget).
Each rule is shown able to fail on a planted file, and the four known requesters are shown to be SEEN (a sweep
that sees nothing passes everything)."""
from __future__ import annotations

import ast
import os
import re
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACER = "jobs/_istat_pacer.py"
MEMBERS = {"updater/strategies/fetchers/istat.py", "jobs/ingest_istat_sliced.py", "tools/catalog_istat_flows.py",
           "tools/probe_istat_hosts.py"}
MEMBER_MODULES = {"updater.strategies.fetchers.istat", "jobs.ingest_istat_sliced", "ingest_istat_sliced",
                  "tools.catalog_istat_flows", "catalog_istat_flows", "tools.probe_istat_hosts", "probe_istat_hosts"}
# YAML/JSON files that carry ISTAT URLs no code fetches: the registry (metadata and prose, read by
# updater/registry.py) and the classification sidecar (homepage links shown on the site).
DATA_ALLOW = {"updater/registry.yaml": "registry prose; no requester reads a URL from it",
              "updater/_classifications.json": "classification prose; no requester reads a URL from it"}
HOST_RE = re.compile(r"(?<![a-z0-9.-])(?:[a-z0-9-]+\.)*istat\.it(?![a-z0-9-])", re.I)
IP_RE = re.compile(r"(?<![0-9.])193\.204\.\d{1,3}\.\d{1,3}(?![0-9])")
SHELL_SEND_RE = re.compile(r"Invoke-WebRequest|Invoke-RestMethod|\bcurl\b|\bwget\b|\biwr\b|\birm\b", re.I)
VERBS = {"get", "post", "put", "patch", "delete", "head", "request", "send", "open", "urlopen"}


def _tracked():
    try:
        out = subprocess.run(["git", "-C", ROOT, "ls-files", "-z"], capture_output=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as e:              # inability to measure is a failure
        pytest.fail(f"cannot list the tracked files: {e}")
    files = [f for f in out.decode("utf-8", "replace").split("\0") if f]
    assert len(files) > 500, f"premise: the tracked-file listing looks short ({len(files)})"
    return files


def _docstring_nodes(tree):
    ids = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.body:
            first = n.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) \
                    and isinstance(first.value.value, str):
                ids.add(id(first.value))
    return ids


def _strings(tree):
    """Non-docstring string constants (f-string parts included)."""
    skip = _docstring_nodes(tree)
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in skip]


def _imports(tree):
    mods, names = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            for a in n.names:
                mods.add(a.name)
                names.add(a.asname or a.name.split(".")[0])
        elif isinstance(n, ast.ImportFrom) and n.module:
            mods.add(n.module)
            for a in n.names:
                mods.add(f"{n.module}.{a.name}")
                names.add(a.asname or a.name)
    return mods, names


def _dotted(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _is_send(call: ast.Call) -> bool:
    f = call.func
    name = _dotted(f)
    last = name.rsplit(".", 1)[-1]
    kw = {k.arg for k in call.keywords}
    if name.startswith("requests.") and last in VERBS | {"Session"} and last != "Session":
        return True
    if last in ("urlopen", "create_connection", "HTTPConnection", "HTTPSConnection", "PoolManager"):
        return True
    if last in ("connect", "connect_ex") and call.args and isinstance(call.args[0], ast.Tuple):
        return True
    if last in VERBS and isinstance(f, ast.Attribute) and ("timeout" in kw or last == "urlopen"):
        return True
    if name.startswith("urllib3.") and last in ("request", "urlopen"):
        return True
    return False


def _token_functions(tree):
    """Names of the module's functions that take a token: they call `<x>.acquire(...)` or `acquire(...)`."""
    out = set()
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for n in ast.walk(fn):
                if isinstance(n, ast.Call) and _dotted(n.func).rsplit(".", 1)[-1] == "acquire":
                    out.add(fn.name)
                    break
    return out


def _functions_with_parents(tree):
    """[(function node, its own statements' calls)] for every function; calls in nested functions belong to them."""
    res = []
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            calls = []
            stack = list(ast.iter_child_nodes(fn))
            while stack:
                n = stack.pop()
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
                    continue
                if isinstance(n, ast.Call):
                    calls.append(n)
                stack.extend(ast.iter_child_nodes(n))
            res.append((fn, calls))
    return res


def check_python(rel: str, src: str) -> dict:
    """-> {"requester": bool, "sends": n, "offences": [...]} for one module's source."""
    tree = ast.parse(src, rel)
    strings = _strings(tree)
    mods, _names = _imports(tree)
    names_host = any(HOST_RE.search(s) or IP_RE.search(s) for s in strings)
    imports_member = any(m in MEMBER_MODULES or any(m.startswith(x + ".") for x in MEMBER_MODULES) for m in mods)
    offences = [f"{rel}: ISTAT IP literal {s[:60]!r} (the pacer cannot pace an IP literal)"
                for s in strings if IP_RE.search(s)]
    token_fns = _token_functions(tree) | {"acquire"}
    sends, unpaced = 0, []
    top_calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
    in_fn = set()
    for fn, calls in _functions_with_parents(tree):
        for c in calls:
            in_fn.add(id(c))
        tok_lines = [c.lineno for c in calls if _dotted(c.func).rsplit(".", 1)[-1] in token_fns]
        for c in calls:
            if _is_send(c):
                sends += 1
                if not any(t <= c.lineno for t in tok_lines):
                    unpaced.append(f"{rel}:{c.lineno} {_dotted(c.func)}(...) in {fn.name}() has no token before it")
    for c in top_calls:                                    # module-level sends have no function to pace them
        if id(c) not in in_fn and _is_send(c):
            sends += 1
            unpaced.append(f"{rel}:{c.lineno} {_dotted(c.func)}(...) at module level has no token")
    requester = (names_host or imports_member) and sends > 0
    if requester:
        offences += unpaced
        if not any("_istat_pacer" in m for m in mods):
            offences.append(f"{rel}: an ISTAT requester that does not import jobs._istat_pacer")
    return {"requester": requester, "sends": sends, "offences": offences}


def check_fetcher_has_no_vintage_probe(rel: str, src: str) -> list:
    tree = ast.parse(src, rel)
    return [f"{rel}: defines {n.name}() - tools/audit_current_vintage.py and audit_vintage_stability.py call it on "
            f"every fetcher, unpaced" for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in ("current_vintage", "probe")]


def check_data(rel: str, text: str) -> list:
    if rel in DATA_ALLOW:
        return []
    hits = [m.group(0) for m in re.finditer(r"https?://[^\s\"'<>]*", text) if HOST_RE.search(m.group(0))]
    hits += [m.group(0) for m in IP_RE.finditer(text)]
    return [f"{rel}: carries ISTAT URL(s) {hits[:3]} that a generic requester could fetch unpaced"] if hits else []


def check_shell(rel: str, text: str) -> list:
    out = []
    for i, line in enumerate(text.splitlines(), 1):
        if (HOST_RE.search(line) or IP_RE.search(line)) and SHELL_SEND_RE.search(line):
            out.append(f"{rel}:{i}: sends to an ISTAT host, unpaced: {line.strip()[:100]}")
    return out


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8-sig", errors="replace") as fh:
        return fh.read()


def _sweep(files):
    offences, requesters = [], {}
    for rel in files:
        if rel.startswith(("tests/", "data/")) or rel == PACER:
            continue
        if rel.endswith(".py"):
            try:
                got = check_python(rel, _read(rel))
            except SyntaxError as e:
                offences.append(f"{rel}: cannot parse ({e}) - inability to check is a failure")
                continue
            offences += got["offences"]
            if got["requester"]:
                requesters[rel] = got["sends"]
        elif rel.endswith((".yaml", ".yml", ".json")):
            offences += check_data(rel, _read(rel))
            if rel.endswith(".yml"):
                offences += check_shell(rel, _read(rel))
        elif rel.endswith((".ps1", ".sh", ".cmd", ".bat")):
            offences += check_shell(rel, _read(rel))
    offences += check_fetcher_has_no_vintage_probe("updater/strategies/fetchers/istat.py",
                                                   _read("updater/strategies/fetchers/istat.py"))
    return offences, requesters


def test_te_every_istat_send_site_in_the_repo_is_paced():
    offences, requesters = _sweep(_tracked())
    assert offences == [], "\n".join(offences)
    # POSITIVE CONTROL: the sweep SEES the four known requesters, each with its send sites (a sweep that sees
    # nothing passes everything): the fetcher's GET and TCP probe, the crawler's GET and connect, one each in the
    # tools.
    assert set(requesters) == MEMBERS, requesters
    assert all(requesters[m] >= 1 for m in MEMBERS) and requesters["updater/strategies/fetchers/istat.py"] >= 2 \
        and requesters["jobs/ingest_istat_sliced.py"] >= 2, requesters


# ------------------------------------------------------------------ the rules can fail (planted)

PLANTS = {
    "a_plain_requester": ('URL = "https://esploradati.istat.it/SDMXWS/rest/data/IT1,X/"\n'
                          "import requests\n\ndef f():\n    return requests.get(URL, timeout=5)\n"),
    "paced_once_unpaced_elsewhere": ("from jobs import _istat_pacer\nimport requests, socket\n"
                                     'H = "esploradati.istat.it"\n\n'
                                     "def a():\n    _istat_pacer.acquire('crawler', H, 'http', 'data')\n"
                                     "    return requests.get('https://' + H + '/x', timeout=5)\n\n"
                                     "def b():\n    return socket.create_connection((H, 443), timeout=6)\n"),
    "a_urllib_requester": ("import urllib.request\n\ndef f():\n"
                           "    return urllib.request.urlopen('https://sdmx.istat.it/SDMXWS/rest/', timeout=5)\n"),
    "an_ip_literal": "import requests\n\ndef f():\n    return requests.get('https://193.204.90.13/SDMXWS', timeout=5)\n",
    "imports_the_hosts": ("from updater.strategies.fetchers.istat import HOSTS\nimport requests\n\n"
                          "def f():\n    return [requests.get(b, timeout=5) for _l, b in HOSTS]\n"),
    "token_after_the_send": ("from jobs import _istat_pacer\nimport requests\n\n"
                             "def f(sess):\n    r = sess.get('https://esploradati.istat.it/x', timeout=5)\n"
                             "    _istat_pacer.acquire('updater', 'esploradati.istat.it', 'http', 'data')\n"
                             "    return r\n"),
}


@pytest.mark.parametrize("name", sorted(PLANTS))
def test_te_planted_requesters_are_flagged(name):
    got = check_python(f"planted/{name}.py", PLANTS[name])
    assert got["requester"] and got["offences"], got


def test_te_control_a_paced_requester_and_a_non_istat_requester_pass():
    paced = ("from jobs import _istat_pacer\nimport requests\n\n"
             "def f(sess):\n    tok = _istat_pacer.acquire('updater', 'esploradati.istat.it', 'http', 'data')\n"
             "    return sess.get('https://esploradati.istat.it/x', timeout=5)\n")
    other = "import requests\n\ndef f():\n    return requests.get('https://api.example.org/x', timeout=5)\n"
    assert check_python("p.py", paced) == {"requester": True, "sends": 1, "offences": []}
    assert check_python("o.py", other)["requester"] is False


def test_te_a_notices_feed_on_istat_and_a_shell_probe_are_flagged():
    assert check_data("updater/notices.yaml", "feeds:\n  - source: istat\n    url: https://www.istat.it/feed/\n")
    assert check_data("updater/notices.yaml", "feeds: []\n") == []
    assert check_shell("tools/x.ps1", "Invoke-WebRequest https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1")
    assert check_shell("tools/x.ps1", "Write-Host esploradati.istat.it") == []


def test_te_a_current_vintage_on_the_istat_fetcher_is_refused():
    src = _read("updater/strategies/fetchers/istat.py")
    assert check_fetcher_has_no_vintage_probe("istat.py", src) == []
    assert check_fetcher_has_no_vintage_probe("istat.py", src + "\n\ndef current_vintage(unit):\n    return None\n")
