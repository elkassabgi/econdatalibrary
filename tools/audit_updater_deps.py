"""Is every third-party module the updater imports actually declared in requirements-updater.txt?

THE FAILURE THIS PREVENTS IS SILENT, WHICH IS WHY IT NEEDS A TOOL. A fetcher module that raises
ImportError makes `fetcher_implemented(<source>)` return False, so the orchestrator classifies
the source as PENDING — "no adapter built" — and skips it. There is no red step and nothing
names the missing package: the source simply never runs, forever, while the job goes green.

It has already happened twice, per requirements-updater.txt's own notes: a missing openpyxl made
edgar_jrc report "no adapter built" (CI run 28978133410), and a missing xlrd broke damodaran
(ModuleNotFoundError reported as a transient) AND sipri_polity (which reported "2/3 sub-units
returned 200 but parsed 0 rows" — the 2 being exactly its two .xls files). One absent dep, two
sources broken, neither naming the cause. lxml was the third, caught here before its first run.

Local imports prove nothing: everything is installed on this machine. What matters is whether
the package is DECLARED, so the runner gets it. So this walks the import graph statically —
every fetcher plus every `jobs.*` / `core.*` / `connectors.*` module they pull in — and reports
top-level third-party imports that requirements-updater.txt does not list.

Exit 1 on any undeclared import — safe to wire into the preflight workflow.

Usage:  python tools/audit_updater_deps.py
"""
from __future__ import annotations
import ast
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

REQ = os.path.join(ROOT, "requirements-updater.txt")
FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")

# Package name -> import name, where they differ.
ALIASES = {"pyyaml": "yaml", "pillow": "PIL", "beautifulsoup4": "bs4"}
# First-party roots that live in this repo, not on PyPI.
LOCAL_ROOTS = {"updater", "jobs", "core", "connectors", "clients", "econdl", "api", "tools"}


def _declared() -> set:
    out = set()
    for line in io.open(REQ, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"([A-Za-z0-9_.\-]+)", line)
        if m:
            name = m.group(1).lower()
            out.add(ALIASES.get(name, name).lower())
            out.add(name.replace("-", "_").lower())
    return out


def _module_path(dotted: str):
    p = os.path.join(ROOT, *dotted.split(".")) + ".py"
    return p if os.path.exists(p) else None


def _imports_of(path: str):
    """(third_party_roots, local_dotted_modules) from one file's top-level import statements."""
    try:
        tree = ast.parse(io.open(path, encoding="utf-8").read())
    except (SyntaxError, UnicodeDecodeError):
        return set(), set()
    third, local = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                root = a.name.split(".")[0]
                (local if root in LOCAL_ROOTS else third).add(
                    a.name if root in LOCAL_ROOTS else root)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                continue                                     # relative: same package, in-repo
            if not node.module:
                continue
            root = node.module.split(".")[0]
            (local if root in LOCAL_ROOTS else third).add(
                node.module if root in LOCAL_ROOTS else root)
    return third, local


def main() -> int:
    declared = _declared()
    stdlib = set(getattr(sys, "stdlib_module_names", set()))

    seen, queue, third_by_file = set(), [], {}
    for f in sorted(os.listdir(FETCHERS)):
        if f.endswith(".py"):
            queue.append(os.path.join(FETCHERS, f))

    while queue:
        path = queue.pop()
        if path in seen:
            continue
        seen.add(path)
        third, local = _imports_of(path)
        rel = os.path.relpath(path, ROOT).replace("\\", "/")
        for t in third:
            if t in stdlib or t.startswith("_"):
                continue
            third_by_file.setdefault(t, set()).add(rel)
        for dotted in local:
            p = _module_path(dotted)
            if p and p not in seen:
                queue.append(p)

    undeclared = {t: f for t, f in sorted(third_by_file.items())
                  if t.lower() not in declared}
    print(f"scanned {len(seen)} modules reachable from the fetchers")
    print(f"third-party roots imported: {len(third_by_file)}")
    for t in sorted(third_by_file):
        mark = "OK " if t.lower() in declared else "!! "
        print(f"  {mark}{t}")
    if undeclared:
        print(f"\nUNDECLARED ({len(undeclared)}) — a runner without these skips the source "
              f"silently as 'no adapter built':")
        for t, files in undeclared.items():
            print(f"  {t}: imported by {', '.join(sorted(files)[:3])}")
        return 1
    print("\nall third-party imports are declared in requirements-updater.txt")
    return 0


if __name__ == "__main__":
    sys.exit(main())
