"""Every reader of the worker gate must STOP when the gate cannot be read.

`api/worker/src/denylist.ts` is the 451 gate. Four places read it. Two were fixed on 2026-09-09
(core/gen_denylist.committed_gate and tools/gen_runbook.gated_ids); the other two kept their own
parsers, which returned an EMPTY SET for an empty file, a whitespace-only file, a prettier
singleQuote reformat, or a literal with no readable entries:

  * catalog/gen_site.py::load_denylisted - an empty set subtracts nothing, so every gated
    source's page offers "Free download" while the API answers 451;
  * tools/refresh_r2_catalog.py::_gate_ids - an empty set withholds nothing from the catalogue
    copy that refresh publishes.

Both now delegate to committed_gate, which raises GateParseError in those four shapes, reads both
quote styles, and keeps the one legitimate empty result: a checkout without the worker file.
The readers are exercised through their own source (extracted with ast), not by importing the
modules - gen_site runs the whole generator at import and refresh_r2_catalog needs zstandard/boto3.
"""
from __future__ import annotations

import ast
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import core.gen_denylist as gd                                   # noqa: E402

READERS = [
    (os.path.join(ROOT, "catalog", "gen_site.py"), "load_denylisted"),
    (os.path.join(ROOT, "tools", "refresh_r2_catalog.py"), "_gate_ids"),
]

GOOD = ('export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set<string>([\n'
        '  // "not_an_id" in a comment\n'
        '  "zz_alpha",\n'
        '  "zz_beta",\n'
        ']);\n')
SHAPES_THAT_MUST_RAISE = {
    "empty file": "",
    "whitespace only": "\n  \t\n",
    "no literal": 'export const SOMETHING_ELSE = new Set(["zz_alpha"]);\n',
    "literal with no readable entries": "export const NON_REDISTRIBUTABLE = new Set([\n  // nothing\n]);\n",
}


def _reader(path: str, name: str):
    """Compile just the named function from the file, in a namespace with what it needs."""
    tree = ast.parse(open(path, encoding="utf-8").read())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    mod = ast.Module(body=[fn], type_ignores=[])
    ns = {"os": os, "HERE": os.path.join(ROOT, "catalog")}
    exec(compile(mod, path, "exec"), ns)
    return ns[name]


@pytest.fixture
def gate_file(tmp_path, monkeypatch):
    p = tmp_path / "denylist.ts"
    monkeypatch.setattr(gd, "OUT", str(p))
    return p


@pytest.mark.parametrize("path,name", READERS, ids=[n for _, n in READERS])
def test_each_reader_delegates_to_committed_gate(path, name):
    fn = next(n for n in ast.parse(open(path, encoding="utf-8").read()).body
              if isinstance(n, ast.FunctionDef) and n.name == name)
    calls = {c.func.id for c in ast.walk(fn) if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)}
    assert "committed_gate" in calls, f"{name} no longer reads the gate through committed_gate"


@pytest.mark.parametrize("path,name", READERS, ids=[n for _, n in READERS])
def test_a_readable_gate_is_read(path, name, gate_file):
    gate_file.write_text(GOOD, encoding="utf-8")
    assert _reader(path, name)() == {"zz_alpha", "zz_beta"}


@pytest.mark.parametrize("path,name", READERS, ids=[n for _, n in READERS])
def test_a_single_quoted_reformat_is_read_not_emptied(path, name, gate_file):
    gate_file.write_text(GOOD.replace('"', "'"), encoding="utf-8")
    assert _reader(path, name)() == {"zz_alpha", "zz_beta"}


@pytest.mark.parametrize("shape", sorted(SHAPES_THAT_MUST_RAISE))
@pytest.mark.parametrize("path,name", READERS, ids=[n for _, n in READERS])
def test_an_unreadable_gate_stops_the_reader(path, name, shape, gate_file):
    gate_file.write_text(SHAPES_THAT_MUST_RAISE[shape], encoding="utf-8")
    with pytest.raises(gd.GateParseError):
        _reader(path, name)()


@pytest.mark.parametrize("path,name", READERS, ids=[n for _, n in READERS])
def test_a_checkout_without_the_worker_file_is_the_one_empty_result(path, name, gate_file):
    assert not gate_file.exists()
    assert _reader(path, name)() == set()
