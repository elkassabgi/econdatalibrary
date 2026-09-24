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
    before, _, after = out.partition("Do NOT run")
    assert "refresh_r2_catalog" not in before and "refresh_r2_catalog" in after, out


def _strings(node):
    """Every literal text inside `node`, f-string parts included."""
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            yield n.value


def _label(path):
    try:
        return os.path.relpath(path, ROOT)
    except ValueError:                           # another drive (the temp folder is on F:)
        return path


def _unwrapped_prints(path):
    tree = ast.parse(open(path, encoding="utf-8-sig").read(), filename=path)
    bad = []
    for call in ast.walk(tree):
        if not (isinstance(call, ast.Call) and getattr(call.func, "id", None) == "print"):
            continue
        wrapped = {id(s) for c in ast.walk(call) if isinstance(c, ast.Call)
                   and getattr(c.func, "attr", getattr(c.func, "id", None)) == "next_steps"
                   for s in ast.walk(c)}
        for n in ast.walk(call):
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and "refresh_r2_catalog" in n.value \
                    and id(n) not in wrapped:
                bad.append(f"{_label(path)}:{n.lineno}")
    return bad


def test_every_printed_refresh_r2_catalog_goes_through_next_steps():
    bad = []
    for top in ("tools", "core"):
        for dirpath, _dirs, files in os.walk(os.path.join(ROOT, top)):
            if "__pycache__" in dirpath:
                continue
            for f in files:
                if f.endswith(".py") and f != "refresh_r2_catalog.py":      # the tool itself refuses after T0
                    bad += _unwrapped_prints(os.path.join(dirpath, f))
    assert bad == [], f"after T0 these would send a user to the frozen cloud copy: {bad}"


def test_the_ratchet_can_fail(tmp_path):
    p = tmp_path / "t.py"
    p.write_text('print("NEXT: refresh_r2_catalog")\nprint(cutover.next_steps("NEXT: refresh_r2_catalog"))\n')
    assert _unwrapped_prints(str(p)) == [f"{_label(str(p))}:1"]
