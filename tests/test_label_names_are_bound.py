"""A failure label must not interpolate a name that is not bound — that turns a report into a crash.

WHAT HAPPENED. On 2026-09-03, labelling `bfs.py` I wrote `{tpath}` into six labels. bfs loops
`for item in catalog:` with `dbid = item.get("dbid", "")`; there is no `tpath` in that scope at
all — I had carried the identifier across from `scb.py`, patched an hour earlier. Every one of
those branches would have raised NameError the first time it fired, converting a diagnosable
failure into a crash: strictly worse than the bare count it replaced.

NOTHING ELSE WOULD HAVE CAUGHT IT. `import updater.strategies.fetchers.bfs` succeeds, because a
name inside a function body is resolved only when that line runs — and these lines run only on
failure, which is exactly when nobody wants a second bug. The whole suite was green.

The checker walks every `tally.*_unit(...)` call, collects the names its arguments interpolate,
and requires each to be bound in the function or an ENCLOSING one, at module level, or as a
builtin. It is deliberately conservative: it reports what it cannot see bound, so a pass means
"no obvious NameError", not "correct".

ITS FIRST RUN REPORTED SEVEN PROBLEMS AND ALL SEVEN WERE ITS OWN BLIND SPOTS — `_unctad.py`'s
`ds` is a parameter of the enclosing `make(ds, source)` closed over by the inner `update`, and
`imf_commodity.py`'s `FLOW` comes from `FLOW, AGENCY = "PCPS", "IMF.RES"`, a tuple target. Both
are now handled, because a checker whose every finding is false gets ignored, which is the
failure mode this session has recorded three times.
"""
from __future__ import annotations

import ast
import builtins
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")
METHODS = {"transient_unit", "structural_unit", "empty_unit", "no_time_unit",
           "deferred_unit", "added_unit"}


def _stored(node) -> set:
    return {t.id for t in ast.walk(node)
            if isinstance(t, ast.Name) and isinstance(t.ctx, (ast.Store, ast.Del))}


def _bound_in(fn) -> set:
    out = set()
    for a in list(fn.args.args) + list(fn.args.kwonlyargs):
        out.add(a.arg)
    if fn.args.vararg:
        out.add(fn.args.vararg.arg)
    if fn.args.kwarg:
        out.add(fn.args.kwarg.arg)
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            out.add(node.id)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            out.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            out |= {(a.asname or a.name).split(".")[0] for a in node.names}
    return out


def _unbound_label_names() -> list[tuple[str, int, str]]:
    problems = []
    for dirpath, dirnames, filenames in os.walk(FETCHERS):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in sorted(filenames):
            if not name.endswith(".py"):
                continue
            path = os.path.join(dirpath, name)
            try:
                tree = ast.parse(open(path, encoding="utf-8").read())
            except (SyntaxError, UnicodeDecodeError):
                continue

            module_level = {n.name for n in tree.body
                            if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
            for n in tree.body:
                if isinstance(n, ast.Assign):
                    for t in n.targets:
                        module_level |= _stored(t)
                elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name):
                    module_level.add(n.target.id)
                elif isinstance(n, (ast.Import, ast.ImportFrom)):
                    module_level |= {(a.asname or a.name).split(".")[0] for a in n.names}

            parents = {}
            for node in ast.walk(tree):
                for child in ast.iter_child_nodes(node):
                    parents[child] = node

            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                names = set(dir(builtins)) | module_level
                scope = fn
                while scope is not None:
                    if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        names |= _bound_in(scope)
                    scope = parents.get(scope)
                for call in ast.walk(fn):
                    if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                            and call.func.attr in METHODS):
                        used = {n.id for arg in list(call.args) + [k.value for k in call.keywords]
                                for n in ast.walk(arg)
                                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
                        for u in sorted(used - names):
                            try:
                                shown = os.path.relpath(path, ROOT)
                            except ValueError:
                                # tmp_path can be on another drive; relpath raises there
                                shown = path
                            problems.append((shown, call.lineno, u))
    return problems


def test_no_label_interpolates_an_unbound_name() -> None:
    problems = _unbound_label_names()
    assert not problems, (
        "these failure labels use a name that is not bound in scope — each would raise "
        "NameError the first time its branch fires, turning a report into a crash:\n  "
        + "\n  ".join(f"{f}:{ln} uses {u!r}" for f, ln, u in problems)
    )


def test_the_checker_is_not_vacuous(tmp_path) -> None:
    """It reported zero; prove it can report something (R501, R503)."""
    bad = tmp_path / "fetchers"
    bad.mkdir()
    (bad / "x.py").write_text(
        "def update(tally):\n"
        "    for item in []:\n"
        "        tally.transient_unit(f'{nope}: boom')\n",
        encoding="utf-8")

    global FETCHERS
    original, FETCHERS = FETCHERS, str(bad)
    try:
        found = _unbound_label_names()
    finally:
        FETCHERS = original
    assert any(u == "nope" for _, _, u in found), f"the checker missed a plain NameError: {found}"
