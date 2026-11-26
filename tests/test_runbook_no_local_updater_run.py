"""Published instructions must never tell a person to run the updater locally or to dispatch bare.

A bare `python -m updater.run --source X --force` on a workstation writes outside CI's state
lineage (ledger R340, R406 failure class G). tools/gen_runbook.py printed exactly that command
into every generated runbook (steps 2 and 3 of the diagnose block), and the committed
docs/runbook/*.md, the cepii_baci registry entry, its ingest job's closing hint and the
econ-updater skill references said the same.

The sanctioned paths:
  - a cloud source: a manual dispatch THROUGH tools/econ_dispatch.py (rule M in its docstring:
    it waits for an empty concurrency group, a free lock and no desktop pass). A bare
    `gh workflow run` bypasses rule M;
  - a `run_location: local` source: the desktop pass, `tools/run_local_heavy.ps1 -Only X -Force`.
    A CI dispatch of it is wrong too: an explicit source overrides the routing, and these
    sources need more memory than a 16 GB runner has.
"""
from __future__ import annotations

import ast
import glob
import importlib.util
import os
import re
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

LOCAL_RUN = re.compile(r"-m\s+updater\.run\b")
BARE_DISPATCH = re.compile(r"^\s*gh\s+workflow\s+run\b")
MANUAL = "python tools/econ_dispatch.py manual --workflow updater-daily.yml"
SKILL_REFS = [os.path.join(ROOT, ".claude", "skills", "econ-updater", "references", n) for n in
              ("10-iron-rules.md", "20-per-source-procedure.md", "30-serving-pipeline.md")]


def _gen(monkeypatch, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, "tools", "gen_runbook.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # Hermetic: the ledger lives outside the repo and is absent on CI; do not depend on it.
    monkeypatch.setattr(mod, "LEDGER", os.path.join(ROOT, "no_such_ledger.md"))
    return mod


def _entries():
    with open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8") as f:
        data = yaml.safe_load(f)
    rows = data["sources"] if isinstance(data, dict) and "sources" in data else data
    return {r["source_id"]: r for r in rows if isinstance(r, dict) and r.get("source_id")}


def _diagnose_block(page):
    start = page.index("## 4. If it stops updating")
    return page[start:page.index("```", page.index("```bash", start) + 7)]


def _bare_dispatch_lines(text):
    return [ln for ln in text.splitlines() if BARE_DISPATCH.search(ln)]


def test_cloud_runbook_force_refresh_goes_through_the_dispatcher(monkeypatch):
    gen = _gen(monkeypatch, "gen_runbook_noloc1")
    reg = {"demo_cloud": {"source_id": "demo_cloud", "live": True}}
    page = gen.render("demo_cloud", reg, {}, {}, {}, set())
    block = _diagnose_block(page)
    assert not LOCAL_RUN.search(block), "runbook tells a person to run the updater locally"
    assert _bare_dispatch_lines(block) == [], "a bare gh workflow run bypasses rule M"
    lines = block.splitlines()
    assert f"{MANUAL} --source demo_cloud --manual-dry-run" in lines
    assert f"{MANUAL} --source demo_cloud --force" in lines
    # `--dry-run` is the DISPATCHER's own send-nothing flag, not the workflow's dry_run input.
    assert not re.search(r"--dry-run\b", block)


def test_local_routed_runbook_uses_the_desktop_pass_not_a_dispatch(monkeypatch):
    gen = _gen(monkeypatch, "gen_runbook_noloc2")
    reg = {"demo_local": {"source_id": "demo_local", "live": True, "run_location": "local"}}
    page = gen.render("demo_local", reg, {}, {}, {}, set())
    block = _diagnose_block(page)
    assert not LOCAL_RUN.search(block), "runbook tells a person to run the updater locally"
    assert "powershell -File tools\\run_local_heavy.ps1 -Only demo_local -Force" in block.splitlines()
    assert "powershell -File tools\\run_local_heavy.ps1 -Only demo_local -WhatIf" in block.splitlines()
    assert "gh workflow run" not in block and "econ_dispatch" not in block, \
        "a local-routed source must not be dispatched to CI"


def test_no_registry_source_renders_a_local_run_or_a_bare_dispatch(monkeypatch):
    gen = _gen(monkeypatch, "gen_runbook_noloc3")
    entries = _entries()
    assert len(entries) > 100, "registry reader found too few entries to be a real check"
    bad = []
    for sid, e in entries.items():
        text = "\n".join(gen.refresh_commands(sid, e))
        if LOCAL_RUN.search(text) or _bare_dispatch_lines(text):
            bad.append(sid)
    assert bad == [], f"refresh commands print a local updater run or a bare dispatch for {bad[:5]}"
    # Both branches are exercised by the real registry, so the check above cannot pass vacuously.
    assert any(e.get("run_location") == "local" for e in entries.values())
    assert any(e.get("run_location") != "local" for e in entries.values())


def test_registry_text_never_instructs_a_local_updater_run():
    found = []
    for sid, e in _entries().items():
        for key in ("force_refresh_procedure", "incremental_path", "keys_or_blockers"):
            for where in (e, e.get("matrix") or {}):
                val = where.get(key)
                text = "\n".join(map(str, val)) if isinstance(val, list) else str(val or "")
                if LOCAL_RUN.search(text):
                    found.append(f"{sid}.{key}")
    assert found == [], f"registry text tells a person to run the updater locally: {found}"


def _runbook_pages():
    pages = sorted(p for p in glob.glob(os.path.join(ROOT, "docs", "runbook", "*.md"))
                   if os.path.basename(p) != "README.md")
    assert len(pages) > 300, f"only {len(pages)} runbooks found - not a real check"
    return pages


def test_committed_runbooks_never_say_local_updater_run():
    """Scans every published docs/runbook/*.md (README included) for `-m updater.run`."""
    hits = []
    for p in glob.glob(os.path.join(ROOT, "docs", "runbook", "*.md")):
        with open(p, encoding="utf-8") as f:
            if LOCAL_RUN.search(f.read()):
                hits.append(os.path.basename(p))
    _runbook_pages()
    assert hits == [], f"{len(hits)} runbooks still say -m updater.run: {sorted(hits)[:5]}"


def test_committed_runbooks_match_the_generator(monkeypatch):
    """The published docs/runbook/*.md carry the generator's refresh lines, not the old local run."""
    gen = _gen(monkeypatch, "gen_runbook_noloc4")
    reg = gen.load_registry()
    stale = []
    for p in _runbook_pages():
        sid = os.path.basename(p)[:-3]
        with open(p, encoding="utf-8") as f:
            lines = f.read().splitlines()
        cmds = [ln for ln in gen.refresh_commands(sid, reg.get(sid, {})) if ln and not ln.startswith("#")]
        if not all(c in lines for c in cmds):
            stale.append(sid)
    assert stale == [], f"{len(stale)} runbooks do not carry the generator's refresh lines: {stale[:5]}"


def _code_lines(path):
    out, inside = [], False
    with open(path, encoding="utf-8") as f:
        for ln in f.read().splitlines():
            if ln.strip().startswith("```"):
                inside = not inside
                continue
            if inside:
                out.append(ln)
    return out


def test_skill_references_name_the_sanctioned_paths():
    for path in SKILL_REFS:
        name = os.path.basename(path)
        code = _code_lines(path)
        assert [ln for ln in code if LOCAL_RUN.search(ln)] == [], f"{name}: a code block runs updater.run locally"
        assert _bare_dispatch_lines("\n".join(code)) == [], f"{name}: a code block dispatches bare"
        with open(path, encoding="utf-8") as f:
            text = f.read()
        assert MANUAL in text, f"{name}: does not name the econ_dispatch.py manual path"
        assert "run_local_heavy.ps1 -Only" in text, f"{name}: does not name the desktop pass"
        # Inline commands too: no `gh workflow run updater-daily.yml -f ...` left as an instruction.
        assert not re.search(r"`gh workflow run updater-daily\.yml", text), f"{name}: inline bare dispatch"
    with open(SKILL_REFS[0], encoding="utf-8") as f:
        assert "Local = always `--source X`" not in f.read(), "10-iron-rules.md still sanctions a local --source run"


def test_cepii_ingest_closing_hint_names_the_desktop_pass():
    path = os.path.join(ROOT, "jobs", "ingest_cepii_baci.py")
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    hints = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "log" and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and "installs the raw files only" in arg.value:
                hints.append(arg.value)
    assert len(hints) == 1, f"expected one closing hint, found {len(hints)}"
    assert not LOCAL_RUN.search(hints[0]), "the hint tells a person to run the updater locally"
    assert "powershell -File tools\\run_local_heavy.ps1 -Only cepii_baci -Force" in hints[0]
