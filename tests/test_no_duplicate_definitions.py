"""No module may bind the same top-level name twice without the first binding being used.

WHY THIS TEST EXISTS. On 2026-09-02 commit 8b03b2b94 - titled "series writers: ONE DEFINITION" -
inserted a second copy of `_CONTENT_TYPES`, `_is_404`, `SKIPPED_IDENTICAL`, `_SKIPPED_LOCK`,
`_count_skip` and the whole `class R2Blob` ABOVE the originals in `updater/blob.py` instead of
editing them. Python binds the LAST definition, so the copy edited afterwards - the one that
replaced two `head_object` calls with one, and that was REPORTED to the owner as a live saving -
could never run. The bill was quoted on a fix that was dead code. Ledger R676.

Nothing caught it. It imports cleanly, every existing test passes, and both copies read as
correct in isolation. Only asking the class object which line it came from revealed it. Prose in
CLAUDE.md cannot prevent that; a test can.

THE RULE IS NOT "NEVER REBIND". Rebinding is ordinary and common in this repo:

    SERIES = [...]                                    # jobs/ingest_fred.py
    SERIES = [s for s in SERIES if ...]               # dedupe in place - the first is READ
    FINANCIAL_FIELDS = list(dict.fromkeys(FINANCIAL_FIELDS))
    _PARSE_EX_RAW = parse_cbs_period_ex               # jobs/ingest_cbs_nl.py
    def parse_cbs_period_ex(...):  # noqa: F811       # a decorator written longhand

What all of those have in common is that the FIRST binding is read before the second replaces
it. The dead-code case is the one where it is not: 150 lines of class body that nothing between
the two definitions ever mentions.

So the test flags a rebinding only when the name is never loaded between the two bindings, and
still honours an explicit `# noqa: F811` for anything it judges wrongly.
"""
from __future__ import annotations

import ast
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES = ("updater", "core", "jobs", "tools")
_DEF = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def _python_files() -> list[str]:
    out = []
    for pkg in PACKAGES:
        base = os.path.join(ROOT, pkg)
        if not os.path.isdir(base):
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in {".git", "__pycache__", ".venv"}]
            out.extend(os.path.join(dirpath, f) for f in filenames if f.endswith(".py"))
    return sorted(out)


def _loads_by_line(tree: ast.AST) -> list[tuple[int, str]]:
    """(line, name) for every read of a name, anywhere, including inside function bodies."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            out.append((node.lineno, node.id))
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            out.append((node.lineno, node.value.id))
    return out


def _duplicates(path: str) -> list[str]:
    """Top-level names rebound while the previous binding was never read."""
    try:
        source = open(path, encoding="utf-8").read()
    except (OSError, UnicodeDecodeError):
        return []
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []                       # a syntax error is a different test's problem

    lines = source.split("\n")
    loads = _loads_by_line(tree)
    seen: dict[str, int] = {}
    bad: list[str] = []

    for node in tree.body:
        if isinstance(node, _DEF):
            names = [node.name]
        elif isinstance(node, ast.Assign):
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        else:
            continue
        for name in names:
            first = seen.get(name)
            if first is not None:
                header = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                marked = "noqa: F811" in header or "noqa:F811" in header
                # Was the first binding read before this one replaced it? A read inside the
                # replacing statement itself (SERIES = [s for s in SERIES]) counts: it consumes
                # the old value. A read after it does not - that reads the NEW binding.
                used = any(n == name and first < ln <= node.end_lineno for ln, n in loads)
                if not marked and not used:
                    bad.append(
                        f"{name}: bound at line {first}, rebound at line {node.lineno} "
                        f"with nothing reading it in between"
                    )
            seen[name] = node.lineno
    return bad


@pytest.mark.parametrize("path", _python_files(), ids=lambda p: os.path.relpath(p, ROOT))
def test_no_dead_rebinding(path: str) -> None:
    dups = _duplicates(path)
    assert not dups, (
        f"{os.path.relpath(path, ROOT)}:\n  " + "\n  ".join(dups)
        + "\n\nPython binds the LAST definition, so the earlier one is dead and any edit to it "
          "does nothing. This is R676: a duplicated `class R2Blob` meant a measured billing fix "
          "was reported as live while the code that actually ran still paid twice.\n"
          "Delete the dead copy. If the shadowing is deliberate - as in ingest_cbs_nl.py, which "
          "captures `_PARSE_EX_RAW = parse_cbs_period_ex` and then wraps the name - that read "
          "already clears this check; mark it `# noqa: F811` only if it does not."
    )


def test_the_guard_actually_fires(tmp_path) -> None:
    """A guard that cannot fail is not a guard (R501/R503). Prove every direction."""
    dead = tmp_path / "dead.py"
    dead.write_text("class A:\n    x = 1\n\n\nclass A:\n    x = 2\n", encoding="utf-8")
    assert _duplicates(str(dead)), "missed a silently duplicated class - the R676 shape"

    consumed = tmp_path / "consumed.py"
    consumed.write_text("S = [3, 1, 3]\nS = sorted(set(S))\n", encoding="utf-8")
    assert not _duplicates(str(consumed)), "flagged a dedupe-in-place, which reads the first"

    wrapped = tmp_path / "wrapped.py"
    wrapped.write_text(
        "def f():\n    return 1\n\n\n_RAW = f\n\n\ndef f():\n    return _RAW() + 1\n",
        encoding="utf-8",
    )
    assert not _duplicates(str(wrapped)), "flagged a longhand decorator that captures the first"

    marked = tmp_path / "marked.py"
    marked.write_text("class A:\n    pass\n\n\nclass A:   # noqa: F811\n    pass\n",
                      encoding="utf-8")
    assert not _duplicates(str(marked)), "ignored an explicit noqa: F811"


def test_blob_defines_r2blob_once() -> None:
    """The specific regression, named, so a revert cannot pass quietly."""
    import updater.blob as blob                                       # noqa: PLC0415

    source = open(blob.__file__, encoding="utf-8").read()
    assert source.count("\nclass R2Blob:") == 1, "R2Blob is defined more than once again (R676)"
    # and the surviving one must be the complete class, not the subset that lacks these
    for method in ("delete", "list_keys", "size", "put_atomic", "_already_holds"):
        assert hasattr(blob.R2Blob, method), f"the bound R2Blob is missing {method}"
