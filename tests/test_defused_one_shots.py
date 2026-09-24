"""Completed one-shots that deleted or rewrote R2 objects AT IMPORT stay defused (plan step 1, 2026-09-24).

tools/_delete_statcan_r2.py deleted every series/statcan%3A* and clean_full/statcan/ object when run or
imported - and statcan was RESTORED to R2 on 2026-09-05 and is served. tools/trim_bfs_corrupt_tail.py
rewrote R2's clean_full/bfs/bfs.parquet at import, outside the merge path, with a safety check that its
docstring promised and its code never had. tools/purge_unpermitted_r2.py was already defused. Each must
refuse before it touches anything - run as a script and as an import."""
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFUSED = ["tools/_delete_statcan_r2.py", "tools/trim_bfs_corrupt_tail.py", "tools/purge_unpermitted_r2.py"]


@pytest.mark.parametrize("rel", DEFUSED)
def test_it_refuses_as_a_script(rel, tmp_path):
    # no credentials and a scratch cwd: even a regression that got past the refusal could reach nothing
    env = {k: v for k, v in os.environ.items() if not k.startswith(("R2_", "CLOUDFLARE_", "CF_"))}
    r = subprocess.run([sys.executable, "-B", os.path.join(ROOT, rel)], cwd=str(tmp_path), env=env,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode != 0, f"{rel} ran: {r.stdout[-300:]}"
    assert "DEFUSED" in (r.stdout + r.stderr).upper() or "defused" in (r.stdout + r.stderr), (r.stdout, r.stderr)


def _harmless(stmt) -> bool:
    """An import, a constant, or a path computation - nothing that can open a client or touch a store."""
    import ast
    if isinstance(stmt, (ast.Import, ast.ImportFrom)):
        return True
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
        return True
    calls = [n for n in ast.walk(stmt) if isinstance(n, ast.Call)]
    ok_call = all(ast.unparse(c.func).startswith(("os.path.", "sys.path.insert")) for c in calls)
    return isinstance(stmt, (ast.Assign, ast.AnnAssign, ast.Expr)) and ok_call


@pytest.mark.parametrize("rel", DEFUSED)
def test_nothing_runs_before_the_refusal(rel):
    """Only imports, constants and path arithmetic may come before the raise: nothing above it can reach R2."""
    import ast
    tree = ast.parse(open(os.path.join(ROOT, rel), encoding="utf-8").read())
    for stmt in tree.body:
        if isinstance(stmt, ast.Raise):
            assert "SystemExit" in ast.unparse(stmt.exc), rel
            return
        assert _harmless(stmt), f"{rel}: runs before its refusal - {ast.unparse(stmt)[:100]}"
    raise AssertionError(f"{rel}: no module-level refusal")


def test_the_structural_rule_can_fail():
    import ast
    for code in ("import boto3\ns3 = boto3.client('s3')\nraise SystemExit('x')\n",
                 "from core import r2_util\nr2_util.client().delete_objects()\nraise SystemExit('x')\n"):
        tree = ast.parse(code)
        assert not all(_harmless(s) for s in tree.body if not isinstance(s, ast.Raise)), code
