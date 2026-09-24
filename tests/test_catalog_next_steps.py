"""After T0 a catalogue tool's closing NEXT line must not send anyone to the frozen cloud copy (R1234: seven tools
ended with "refresh_r2_catalog", which refuses after T0). core.catalog_path.next_steps keeps the pre-T0 line as it
was and, after T0, names the blue/green swap instead and marks the old steps as ones NOT to run. The ratchet: in
tools/ and core/, every printed text that names refresh_r2_catalog goes through next_steps."""
import ast
import os

import pytest

from core import catalog_path, cutover

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRE = "NEXT: python tools/refresh_r2_catalog.py <stamp>   (push the updated catalog to R2)"


def test_before_t0_the_line_is_unchanged(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag"))
    assert cutover.next_steps(PRE) == PRE


def test_after_t0_the_line_names_the_swap_and_marks_the_old_steps(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    out = cutover.next_steps(PRE)
    assert out.startswith("NEXT (after T0") and "tools/selfhost/swap.py" in out
    # the old line is KEPT (its other steps still stand) and only the three cloud steps are named as skipped
    # (R1238: "do NOT run" over the whole line forbade util.ts, registry and live-check steps)
    keep = "NEXT: util.ts removal + registry retire/count bump + deploy + live absence check + refresh_r2_catalog."
    out = cutover.next_steps(keep)
    assert " ".join(keep.split()) in out and "Do NOT run" not in out
    assert out.split("EXCEPT", 1)[1].startswith(" refresh_r2_catalog, sync_catalog_d1, the cloud `wrangler deploy`")
    assert "Commit any code, util.ts or registry edit" in out


NEEDLE = "refresh_r2_catalog"
# The only literals that may name it outside next_steps(...), each with its reason:
ALLOWED = {
    "core/catalog.py": "the schema's SQL comment says which tool regenerates catalog.db (not guidance)",
    "core/cutover.py": "CLOUD_STEPS - the list next_steps names as skipped",
}


def _label(path):
    try:
        return os.path.relpath(path, ROOT).replace(os.sep, "/")
    except ValueError:                           # another drive (the temp folder is on F:)
        return path


def _docstrings(tree):
    ids = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and body \
                and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                and isinstance(body[0].value.value, str):
            ids.add(id(body[0].value))
    return ids


def _unwrapped(path):
    """Every string literal naming the tool that is not a docstring and not inside a next_steps(...) call - so a
    variable, sys.stdout.write, logging, a concatenation, an argparse epilog or a SystemExit message is held too
    (R1238: the print()-only version let all eleven such escapes through)."""
    tree = ast.parse(open(path, encoding="utf-8-sig").read(), filename=path)
    docs, wrapped = _docstrings(tree), set()
    for c in ast.walk(tree):
        if isinstance(c, ast.Call) and getattr(c.func, "attr", getattr(c.func, "id", None)) == "next_steps":
            wrapped |= {id(n) for n in ast.walk(c)}
    return [f"{_label(path)}:{n.lineno}" for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and NEEDLE in n.value
            and id(n) not in docs and id(n) not in wrapped]


def _ps1_lines(path):
    return [f"{_label(path)}:{i}" for i, ln in enumerate(open(path, encoding="utf-8-sig", errors="replace"), 1)
            if NEEDLE in ln and not ln.lstrip().startswith("#")]


def test_nothing_names_refresh_r2_catalog_outside_next_steps():
    bad = []
    for top in ("tools", "core", "updater"):
        for dirpath, _dirs, files in os.walk(os.path.join(ROOT, top)):
            if "__pycache__" in dirpath:
                continue
            for f in files:
                p = os.path.join(dirpath, f)
                if f.endswith(".ps1"):
                    bad += _ps1_lines(p)
                elif f.endswith(".py") and f != "refresh_r2_catalog.py" and _label(p) not in ALLOWED:
                    bad += _unwrapped(p)
    assert bad == [], f"after T0 these would send a user to the frozen cloud copy: {bad}"


def test_the_allowed_files_still_need_their_exception():
    """An exception that is no longer needed is removed, so the list cannot grow stale."""
    for rel in ALLOWED:
        assert _unwrapped(os.path.join(ROOT, rel)), f"{rel} no longer names the tool - drop its exception"


ESCAPES = [
    'print("NEXT: refresh_r2_catalog")',
    'msg = "run refresh_r2_catalog"',
    'import sys; sys.stdout.write("refresh_r2_catalog next")',
    'import logging; logging.info("then refresh_r2_catalog")',
    'x = "NEXT: " + "refresh_r2_catalog"',
    'n = 1; y = f"step {n}: refresh_r2_catalog"',
    'raise SystemExit("now run refresh_r2_catalog")',
    'import argparse; argparse.ArgumentParser(epilog="then refresh_r2_catalog")',
]


@pytest.mark.parametrize("code", ESCAPES)
def test_the_ratchet_catches_each_escape(tmp_path, code):
    p = tmp_path / "t.py"
    p.write_text(code + "\nprint(cutover.next_steps('NEXT: refresh_r2_catalog'))\n", encoding="utf-8")
    assert _unwrapped(str(p)) == [f"{_label(str(p))}:1"], code


def test_a_ps1_line_is_caught(tmp_path):
    p = tmp_path / "t.ps1"
    p.write_text("# refresh_r2_catalog in a comment is fine\nSay 'then refresh_r2_catalog'\n", encoding="utf-8")
    assert _ps1_lines(str(p)) == [f"{_label(str(p))}:2"]


