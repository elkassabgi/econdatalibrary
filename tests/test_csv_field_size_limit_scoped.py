"""csv.field_size_limit is raised AROUND a reader, never at import (2026-11-25, PR #332, CI run 45568040117).

csv.field_size_limit() is state of the whole process. Six modules set it when imported
(jobs/ingest_abs_full.py, jobs/ingest_bis_full.py, jobs/ingest_ecb.py, jobs/ingest_imf_full.py,
jobs/ingest_statcan.py, tools/apply_series_names.py), so a reader's result depended on import order: a
large-field reader worked only when one of them was imported first, and a test of the default limit failed
when one of them was imported earlier in the same pytest process. core/csv_limit.py now scopes the limit.

(a) importing any of those modules (or the fetchers that import them) leaves the limit at the default,
    and NO tracked code file calls csv.field_size_limit() at import time (a ratchet over the repo);
(b) each large-field reader still reads a planted 200,000-byte field in a FRESH process where nothing else
    was imported (tests/_csv_limit_probe.py), and puts the default back afterwards;
(c) the helper itself: nesting, threads that overlap, generators, and that it never lowers the limit;
(d) the updater runs every fetcher's probe and fetch inside the scope, so fetchers that got the large
    limit only by import order keep it, deterministically.
"""
from __future__ import annotations

import ast
import csv
import json
import os
import subprocess
import sys
import threading

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _repo_walk import code_files, parse_code  # noqa: E402
from core import csv_limit  # noqa: E402

DEFAULT = 131072
PROBE = os.path.join(ROOT, "tests", "_csv_limit_probe.py")

# every module that used to set the limit at import, and every module that imports one of them at import
IMPORTERS = [
    "jobs.ingest_abs_full", "jobs.ingest_bis_full", "jobs.ingest_ecb", "jobs.ingest_imf_full",
    "jobs.ingest_statcan", "apply_series_names", "updater.strategies.fetchers.abs",
    "updater.strategies.fetchers.bis", "updater.strategies.fetchers.statcan", "updater.orchestrate",
]
READERS = ["abs", "bis_full", "ecb", "imf", "statcan", "apply_series_names", "bis_check_body", "bis_bulk_rows"]


def _env(tmp_path):
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + os.path.join(ROOT, "tools")
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    return env


def _run(args, tmp_path):
    return subprocess.run([sys.executable, *args], cwd=ROOT, env=_env(tmp_path),
                          capture_output=True, text=True, timeout=300)


# ---- (a) import leaves the default ------------------------------------------------------------------------
@pytest.mark.parametrize("mod", IMPORTERS)
def test_a_import_leaves_the_default_limit(mod, tmp_path):
    code = ("import csv, importlib, sys\n"
            f"before = csv.field_size_limit()\n"
            f"importlib.import_module({mod!r})\n"
            "print(before, csv.field_size_limit())\n")
    r = _run(["-c", code], tmp_path)
    assert r.returncode == 0, r.stderr[-3000:]
    before, after = (int(x) for x in r.stdout.split()[-2:])
    assert before == DEFAULT, f"a fresh interpreter starts at {before}, not {DEFAULT}"
    assert after == DEFAULT, f"importing {mod} changed csv.field_size_limit() to {after}"


def test_a_importing_all_of_them_together_leaves_the_default(tmp_path):
    code = ("import csv, importlib\n"
            f"for m in {IMPORTERS!r}:\n"
            "    importlib.import_module(m)\n"
            "print(csv.field_size_limit())\n")
    r = _run(["-c", code], tmp_path)
    assert r.returncode == 0, r.stderr[-3000:]
    assert int(r.stdout.split()[-1]) == DEFAULT


def _import_time_calls(tree):
    """Calls of csv.field_size_limit / field_size_limit with an argument that run when the module is
    IMPORTED: anywhere except inside a function or lambda body (class bodies run at import too)."""
    found = []

    def visit(node, in_func):
        for child in ast.iter_child_nodes(node):
            inner = in_func or isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda))
            if isinstance(child, ast.Call) and not in_func and (child.args or child.keywords):
                f = child.func
                name = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
                if name == "field_size_limit":
                    found.append(child.lineno)
            visit(child, inner)

    visit(tree, False)
    return found


def test_a_no_code_file_sets_the_limit_at_import():
    hits, scanned = [], 0
    for rel, path in code_files((".py",)):
        scanned += 1
        for line in _import_time_calls(parse_code(path)):
            hits.append(f"{rel}:{line}")
    assert scanned > 500, f"the walk saw only {scanned} .py files - it is not looking at the repo"
    assert not hits, ("csv.field_size_limit() is called at import time - it changes every reader in the "
                      "process; wrap the reader with core.csv_limit instead: " + ", ".join(hits))


def test_a_the_ratchet_sees_an_import_time_call():
    """Negative control: the scan must flag the exact shape that was removed, and must not flag a call
    inside a function."""
    planted = ast.parse("import csv, sys\ncsv.field_size_limit(min(sys.maxsize, 2_147_483_647))\n"
                        "class K:\n    csv.field_size_limit(5)\n"
                        "def f():\n    csv.field_size_limit(7)\n    return csv.field_size_limit()\n")
    assert _import_time_calls(planted) == [2, 4]


# ---- (b) each reader reads a large field in a fresh process -----------------------------------------------
@pytest.mark.parametrize("case", READERS)
def test_b_reader_reads_a_large_field_in_a_fresh_process(case, tmp_path):
    r = _run([PROBE, case, str(tmp_path)], tmp_path)
    lines = [ln for ln in r.stdout.splitlines() if ln.startswith("{")]
    assert lines, f"probe printed no result (rc {r.returncode}): {r.stderr[-3000:]}"
    out = json.loads(lines[-1])
    assert out["error"] is None, f"{case}: the reader failed on a 200,000-byte field: {out['error']}"
    assert out["limit_at_start"] == DEFAULT
    assert out["limit_after_import"] == DEFAULT, f"{case}: importing its module set {out['limit_after_import']}"
    assert out["limit_after_read"] == DEFAULT, f"{case}: the reader left the limit at {out['limit_after_read']}"
    assert r.returncode == 0 and out["ok"], out


def test_b_the_probe_can_fail_without_the_helper(tmp_path):
    """Negative control: a plain csv.reader on the same planted field fails in a fresh process, so a
    passing probe means the helper did the work, not a lenient default."""
    code = ("import csv, io\n"
            "try:\n"
            "    list(csv.reader(io.StringIO('a,' + 'x' * 200000 + '\\n')))\n"
            "    print('READ')\n"
            "except csv.Error:\n"
            "    print('REFUSED')\n")
    r = _run(["-c", code], tmp_path)
    assert r.stdout.strip() == "REFUSED", r.stdout + r.stderr


# ---- (c) the helper ----------------------------------------------------------------------------------------
@pytest.fixture
def default_limit():
    old = csv.field_size_limit(DEFAULT)
    try:
        yield
    finally:
        csv.field_size_limit(old)


def test_c_scope_raises_then_restores(default_limit):
    with csv_limit.field_size_limit(1 << 24):
        assert csv.field_size_limit() == 1 << 24
        with csv_limit.field_size_limit(10_000_000):
            assert csv.field_size_limit() == 1 << 24, "an inner, smaller scope must not lower the limit"
        assert csv.field_size_limit() == 1 << 24
    assert csv.field_size_limit() == DEFAULT


def test_c_restores_after_an_exception(default_limit):
    with pytest.raises(RuntimeError):
        with csv_limit.field_size_limit(1 << 24):
            raise RuntimeError("boom")
    assert csv.field_size_limit() == DEFAULT


def test_c_never_lowers_a_higher_base():
    old = csv.field_size_limit(1 << 26)
    try:
        with csv_limit.field_size_limit(1 << 20):
            assert csv.field_size_limit() == 1 << 26
        assert csv.field_size_limit() == 1 << 26
    finally:
        csv.field_size_limit(old)


def test_c_generator_holds_the_limit_for_its_whole_iteration(default_limit):
    @csv_limit.with_field_size_limit(1 << 24)
    def gen():
        yield csv.field_size_limit()
        yield csv.field_size_limit()

    g = gen()
    assert csv.field_size_limit() == DEFAULT, "creating the generator must not raise the limit yet"
    assert next(g) == 1 << 24
    assert next(g) == 1 << 24
    assert list(g) == []
    assert csv.field_size_limit() == DEFAULT
    g2 = gen()
    next(g2)
    g2.close()
    assert csv.field_size_limit() == DEFAULT, "closing a generator early must restore the limit"


def test_c_overlapping_threads_do_not_restore_under_each_other(default_limit):
    """Thread A opens, B opens, A closes while B still reads: B must still have the large limit. A plain
    save/restore pair fails this (A would put back 131072 under B)."""
    a_in, b_in, a_out = threading.Event(), threading.Event(), threading.Event()
    seen = {}

    def a():
        with csv_limit.field_size_limit(1 << 24):
            a_in.set()
            b_in.wait(10)
        a_out.set()

    def b():
        a_in.wait(10)
        with csv_limit.field_size_limit(1 << 24):
            b_in.set()
            a_out.wait(10)
            seen["b_after_a_closed"] = csv.field_size_limit()
            seen["b_reads"] = len(next(csv.reader(["x" * 200_000]))[0])

    ta, tb = threading.Thread(target=a), threading.Thread(target=b)
    ta.start(); tb.start(); ta.join(20); tb.join(20)
    assert seen == {"b_after_a_closed": 1 << 24, "b_reads": 200_000}
    assert csv.field_size_limit() == DEFAULT


# ---- (d) the updater scope ---------------------------------------------------------------------------------
def test_d_every_fetcher_probe_and_fetch_runs_inside_the_scope():
    tree = parse_code(os.path.join(ROOT, "updater", "orchestrate.py"))
    targets = {"detect_change": 0, "run": 0}
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        items = [ast.unparse(i.context_expr) for i in node.items]
        for stmt in ast.walk(ast.Module(body=node.body, type_ignores=[])):
            if (isinstance(stmt, ast.Call) and isinstance(stmt.func, ast.Attribute)
                    and isinstance(stmt.func.value, ast.Name) and stmt.func.value.id == "strat"
                    and stmt.func.attr in targets):
                assert "_csv_field_size_limit(FETCH_CSV_FIELD_LIMIT)" in items, (
                    f"strat.{stmt.func.attr}() at line {stmt.lineno} runs outside the csv field-limit scope")
                targets[stmt.func.attr] += 1
    assert targets == {"detect_change": 1, "run": 1}, targets


def test_d_the_values_did_not_change():
    import jobs.ingest_abs_full as A
    import jobs.ingest_bis_full as B
    import jobs.ingest_ecb as E
    import jobs.ingest_imf_full as I
    import jobs.ingest_statcan as S
    from updater import orchestrate as O
    big = min(sys.maxsize, 2_147_483_647)
    assert (A.CSV_FIELD_LIMIT, B.CSV_FIELD_LIMIT, E.CSV_FIELD_LIMIT, I.CSV_FIELD_LIMIT) == (big,) * 4
    assert S.CSV_FIELD_LIMIT == 1 << 24
    assert O.FETCH_CSV_FIELD_LIMIT == big
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import apply_series_names as N
    assert N.CSV_FIELD_LIMIT == 10_000_000
