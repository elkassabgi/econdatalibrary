"""Published instructions must never tell a person to run the updater locally.

A bare `python -m updater.run --source X --force` on a workstation writes outside CI's state
lineage (ledger R340, R406 failure class G). tools/gen_runbook.py printed exactly that command
into every generated runbook (steps 2 and 3 of the diagnose block), and the cepii_baci registry
entry's force_refresh_procedure said the same.

The sanctioned paths:
  - a cloud source: a dispatch, `gh workflow run updater-daily.yml ... -f source=X -f force=true`;
  - a `run_location: local` source: the desktop pass, `tools/run_local_heavy.ps1 -Only X -Force`.
    A CI dispatch of it is wrong too: an explicit source overrides the routing, and these
    sources need more memory than a 16 GB runner has.
"""
from __future__ import annotations

import importlib.util
import os
import re
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

LOCAL_RUN = re.compile(r"-m\s+updater\.run\b")
DISPATCH = "gh workflow run updater-daily.yml -R elkassabgi/econdatalibrary"


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


def test_cloud_runbook_force_refresh_is_a_dispatch(monkeypatch):
    gen = _gen(monkeypatch, "gen_runbook_noloc1")
    reg = {"demo_cloud": {"source_id": "demo_cloud", "live": True}}
    page = gen.render("demo_cloud", reg, {}, {}, {}, set())
    block = _diagnose_block(page)
    assert not LOCAL_RUN.search(block), "runbook tells a person to run the updater locally"
    assert f"{DISPATCH} -f source=demo_cloud -f force=true" in block
    assert f"{DISPATCH} -f source=demo_cloud -f dry_run=true" in block


def test_local_routed_runbook_uses_the_desktop_pass_not_a_dispatch(monkeypatch):
    gen = _gen(monkeypatch, "gen_runbook_noloc2")
    reg = {"demo_local": {"source_id": "demo_local", "live": True, "run_location": "local"}}
    page = gen.render("demo_local", reg, {}, {}, {}, set())
    block = _diagnose_block(page)
    assert not LOCAL_RUN.search(block), "runbook tells a person to run the updater locally"
    assert "run_local_heavy.ps1 -Only demo_local -Force" in block
    assert "gh workflow run" not in block, "a local-routed source must not be dispatched to CI"


def test_no_registry_source_renders_a_local_updater_run(monkeypatch):
    gen = _gen(monkeypatch, "gen_runbook_noloc3")
    entries = _entries()
    assert len(entries) > 100, "registry reader found too few entries to be a real check"
    bad = [sid for sid, e in entries.items() if LOCAL_RUN.search("\n".join(gen.refresh_commands(sid, e)))]
    assert bad == [], f"refresh commands print a local updater run for {bad[:5]}"
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
