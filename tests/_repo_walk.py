"""The ONE folder walk the repo ratchets share (test_r2_cutover_guard, test_d1_remote, test_catalog_path).

R1178: each ratchet had its own walk, and each pruned folders by NAME at any depth - so a file under, say,
tools/x/data/ or updater/state/ was never scanned. The skip list names TOP-LEVEL folders only (the repo's
data, docs, tests, ...); below the top, only folders that never hold our code are pruned. One walk, one
pin (test_repo_walk.py), so the three cannot drift apart again.

In a git checkout the walk is over the TRACKED files (tracked_files): an untracked scratch file in the
production checkout once failed a ratchet there that CI, which checks out tracked files only, passed."""
from __future__ import annotations

import os
from typing import Iterator

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOP_LEVEL_SKIP = frozenset({".git", "node_modules", "data", "dist", "tests", "docs", "scratchpad", ".wrangler",
                            "__pycache__", ".claude", "logs", "state"})
ALWAYS_SKIP = frozenset({".git", "node_modules", "__pycache__", ".wrangler"})


def code_text(src: str, drop_functions: frozenset[str] = frozenset()) -> str:
    """The Python a ratchet should scan (AR-153): the parsed code printed back - so comments are gone and
    implicit concatenation is joined (`"catalog" ".db"` reads as 'catalog.db') - with DOCSTRINGS removed
    but every other string kept, triple-quoted SQL and shell text included (a regex that dropped every
    triple-quoted string hid code). `drop_functions` names functions to leave out (a narrow exemption,
    e.g. the one function that names a thing only to refuse it). A file that does not parse is returned
    unchanged: the ratchet then scans MORE, never less."""
    import ast                                                                  # noqa: PLC0415
    try:
        tree = ast.parse(src)
    except (SyntaxError, ValueError):
        return src
    def keep(n, top: bool) -> bool:
        if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str):
            return False              # a docstring, or any bare string statement: a no-op, never code
        # the exemption names MODULE-LEVEL functions only - a method or nested function of the same name is
        # still scanned (R1183 finding 6)
        return not (top and isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in drop_functions)

    for node in ast.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            stmts = getattr(node, field, None)
            if isinstance(stmts, list) and stmts and isinstance(stmts[0], ast.stmt):
                top = isinstance(node, ast.Module)
                setattr(node, field, [n for n in stmts if keep(n, top)] or ([ast.Pass()] if field == "body" else []))
    return ast.unparse(tree)


def read_code(path: str) -> str:
    """A code file's text as the interpreter reads it: UTF-8, with or without a byte-order mark. Ten jobs/
    files carry the mark (written by PowerShell); read as plain utf-8 they failed to parse and a ratchet that
    skipped them passed by not looking (R1209)."""
    with open(path, encoding="utf-8-sig") as fh:
        return fh.read()


def parse_code(path: str):
    """read_code, parsed. A file that does not parse RAISES - a ratchet never skips what it cannot read."""
    import ast
    return ast.parse(read_code(path), filename=path)


def tracked_files(root: str = ROOT) -> list[str] | None:
    """The repo-relative paths git TRACKS under `root` (its index: committed, or added and not yet committed),
    or None when `root` is not a git checkout (no .git directory or worktree file at `root`).

    Why the ratchets walk this list and not the folder (t0_ready 2026-11-16): an UNTRACKED scratch file in the
    production checkout (_verify_eia_resolver.py, never in git) failed the catalogue ratchet there while CI -
    which checks out tracked files only - was green. A file nobody committed is not code the repo ships, and
    the two places must judge the same tree. A git failure in a checkout RAISES: a ratchet never scans less
    because it could not ask."""
    if not os.path.exists(os.path.join(root, ".git")):
        return None
    import subprocess                                                           # noqa: PLC0415
    # GIT_DIR / GIT_WORK_TREE / GIT_INDEX_FILE from a calling hook would point git at another tree
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    # the repository git FINDS must be the one at `root`: an empty or broken .git inside another repository
    # makes git search upward and answer for the parent - an empty list here, a ratchet that scans nothing
    top = subprocess.run(["git", "-C", root, "rev-parse", "--show-toplevel"], capture_output=True, env=env)
    found_top = top.stdout.decode("utf-8", "surrogateescape").strip()
    if top.returncode != 0 or os.path.normcase(os.path.realpath(found_top)) != os.path.normcase(os.path.realpath(root)):
        raise RuntimeError(f"git ls-files failed in {root}: git resolves it to {found_top or 'no repository'!r} "
                           f"(exit {top.returncode}: {top.stderr.decode('utf-8', 'replace')[-300:]})")
    r = subprocess.run(["git", "-C", root, "ls-files", "-z", "--cached"], capture_output=True, env=env)
    if r.returncode != 0:
        raise RuntimeError(f"git ls-files failed in {root} (exit {r.returncode}): "
                           f"{r.stderr.decode('utf-8', 'replace')[-300:]}")
    # an unmerged path is listed once per stage: once each, in order
    return list(dict.fromkeys(p for p in r.stdout.decode("utf-8", "surrogateescape").split("\0") if p))


def code_files(extensions: tuple[str, ...], root: str = ROOT) -> Iterator[tuple[str, str]]:
    """(repo-relative path with forward slashes, absolute path) for every file ending in `extensions`.

    In a git checkout: the TRACKED files (tracked_files), with the same folder skips as the walk below; a
    tracked file deleted from the working tree is not there to read and is left out, as the walk would.
    Anywhere else (a test's temporary folder, an exported tree): the folder walk."""
    tracked = tracked_files(root)
    if tracked is not None:
        for rel in tracked:
            parts = rel.split("/")
            if parts[0] in TOP_LEVEL_SKIP or any(d in ALWAYS_SKIP for d in parts[1:-1]):
                continue
            if not parts[-1].endswith(extensions):
                continue
            p = os.path.join(root, *parts)
            if os.path.isfile(p):
                yield rel, p
        return
    for dirpath, dirs, files in os.walk(root):
        skip = TOP_LEVEL_SKIP if os.path.normcase(dirpath) == os.path.normcase(root) else ALWAYS_SKIP
        dirs[:] = [d for d in dirs if d not in skip]
        for f in files:
            if f.endswith(extensions):
                p = os.path.join(dirpath, f)
                yield os.path.relpath(p, root).replace(os.sep, "/"), p
