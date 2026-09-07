"""The grain index reads five registries and a resolver. A rename must STOP the run, not default.

WHY (review of PR #12, 2026-09-07). `tools/audit_store_vs_catalog.py` classifies a source's grain
from five module attributes, and four of them were read with `getattr(mod, name, default)`.
Renaming any one reclassifies 5-39 sources with NO exception and the headline still prints - which
is the same "designed difference reported as a coverage gap" failure the whole file exists to fix,
running in the other direction. `_TABLE_GRAIN` already raised; the argument was simply never
applied to its four siblings, and CI pinned `_RESOLVERS` and `_resolve_file_grain` nowhere at all.

The second half is `_grain_from_resolver`'s `except Exception: continue`. That branch governs an
exclusion worth 1,012,069,333 keys. When the STORE path is wrong EVERY source raises, the function
returns {}, and the index still reports a normal count from the declaration lists with nothing
printed - the reviewer hit exactly that by running a copy of the tool whose module-level STORE
pointed at a nonexistent directory, and 16 of 16 sampled sources raised ResolveError in silence.
A DEFAULTED classification must not look like a measured one.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "audit_store_vs_catalog.py")


def _resolve_mod():
    """econdl lives under clients/python, which the tool puts on sys.path INSIDE grain_index().
    A test that imports it before that runs gets ModuleNotFoundError, so mirror what the tool does
    rather than depending on having called it first."""
    root = os.path.dirname(_HERE)
    for p in (os.path.join(root, "clients", "python"), root):
        if p not in sys.path:
            sys.path.insert(0, p)
    from econdl import _resolve as mod
    return mod


def _load(tmp_path=None):
    """Load the tool, and by default point ROOT at a TINY temp catalogue.

    THE REAL `data/catalog.db` IS 11.91 GB AND IS NOT IN A CI CHECKOUT. The first version of this
    file let `_grain_from_resolver` open the real one: 7 passed here and 5 failed in CI with
    `sqlite3.OperationalError: unable to open database file` - a test that passes only on the
    machine that wrote it, which is the same blindness `test_skill_check` already documents. The
    fixture holds two sources so the resolver loop actually runs.
    """
    spec = importlib.util.spec_from_file_location("_audit_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    if tmp_path is not None:
        root = str(tmp_path)
        os.makedirs(os.path.join(root, "data"), exist_ok=True)
        con = sqlite3.connect(os.path.join(root, "data", "catalog.db"))
        con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        con.executemany("INSERT INTO series VALUES (?, ?)",
                        [("alpha:1", "alpha"), ("beta:1", "beta")])
        con.commit()
        con.close()
        m.ROOT = root
    return m


@pytest.mark.parametrize("attr", ["_FLOW_GRAIN", "_DOT_TABLE_GRAIN", "_RESOLVERS",
                                  "_resolve_file_grain"])
def test_a_renamed_registry_stops_the_run(monkeypatch, tmp_path, attr):
    m = _load(tmp_path)
    _resolve = _resolve_mod()              # the same module object grain_index() imports
    monkeypatch.delattr(_resolve, attr)
    with pytest.raises(RuntimeError) as ex:
        m.grain_index()
    assert attr in str(ex.value)
    assert "do not default it" in str(ex.value).lower()


def test_every_registry_is_present_today(tmp_path):
    """The mirror: the guard above is only meaningful while the names it demands actually exist,
    and this is the line that fails when someone renames one on purpose."""
    _resolve = _resolve_mod()
    for attr in ("_FLOW_GRAIN", "_DOT_TABLE_GRAIN", "_RESOLVERS", "_resolve_file_grain"):
        assert hasattr(_resolve, attr), attr
    idx = _load(tmp_path).grain_index()
    assert isinstance(idx, dict) and idx


def test_a_store_path_that_resolves_nothing_says_so(monkeypatch, capsys, tmp_path):
    """The whole-index case, which is the one that misleads: point STORE at a directory that does
    not exist and every resolve raises. The classification is then DEFAULTED, and the run must say
    that in as many words rather than printing an ordinary-looking index."""
    m = _load(tmp_path)
    monkeypatch.setattr(m, "STORE", str(tmp_path / "no_such_store"))
    _resolve = _resolve_mod()

    def _boom(*_a, **_k):
        raise RuntimeError("simulated unresolvable store path")
    monkeypatch.setattr(_resolve, "resolve", _boom)
    m.grain_index()
    err = capsys.readouterr().err
    assert "resolver could not answer for" in err, err
    assert "EVERY source failed to resolve" in err, err
    assert "DEFAULTED, not measured" in err, err


def test_a_healthy_index_prints_no_warning(monkeypatch, capsys, tmp_path):
    """...and it must be quiet when nothing is wrong, or the warning becomes wallpaper. Every
    source in the fixture resolves, so nothing may be printed to stderr at all."""
    m = _load(tmp_path)
    _resolve = _resolve_mod()

    class _Res:
        predicate = ""
        key_col = ""
    monkeypatch.setattr(_resolve, "resolve", lambda *_a, **_k: _Res())
    m.grain_index()
    err = capsys.readouterr().err
    assert "resolver could not answer" not in err, err
    assert "EVERY source failed to resolve" not in err, err
