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


DEAD = "http://127.0.0.1:9"                 # the discard port: nothing answers


def _dead_env() -> dict:
    """This process's environment with every cloud credential removed and every R2 variable set to the dead
    endpoint (an environment value wins over a .env in core.config.load_env and core.r2_util.load_env)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("R2_", "CLOUDFLARE_", "CF_"))}
    for side in ("READ", "WRITE"):
        env[f"R2_{side}_ENDPOINT"] = DEAD
        env[f"R2_{side}_ACCESS_KEY_ID"] = "dead-key"
        env[f"R2_{side}_SECRET_ACCESS_KEY"] = "dead-secret"
    return env


@pytest.mark.parametrize("rel", DEFUSED)
def test_it_refuses_as_a_script(rel, tmp_path):
    """Run a COPY from tmp_path/tools/, never the checkout's file (R1210). The tools find their .env from their
    own path (ROOT/.env - and the production checkout has one with the write keys), so running the real file
    with the variables merely removed let a regression that lost the refusal LIST AND DELETE before the test
    could fail. The copy's ROOT is tmp_path: no .env, no core/ to import; and every R2 variable is set to a
    dead endpoint, which wins over any .env in both loaders."""
    dest = tmp_path / "tools" / os.path.basename(rel)
    dest.parent.mkdir()
    dest.write_bytes(open(os.path.join(ROOT, rel), "rb").read())
    env = _dead_env()
    # the repo on the path: purge_unpermitted_r2 imports core before its refusal. Safe because every R2
    # variable is the dead endpoint, and an environment value wins over the checkout's .env
    env["PYTHONPATH"] = ROOT
    r = subprocess.run([sys.executable, "-B", str(dest)], cwd=str(tmp_path), env=env,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode != 0, f"{rel} ran: {r.stdout[-300:]}"
    assert "DEFUSED" in (r.stdout + r.stderr).upper() or "defused" in (r.stdout + r.stderr), (r.stdout, r.stderr)


@pytest.mark.parametrize("rel", DEFUSED)
def test_a_copy_without_the_refusal_cannot_reach_anything(rel, tmp_path):
    """The can-fail half (R1210): with the refusal REMOVED, the copied script still reaches nothing - it stops
    at its first first-party import (no core/ beside the copy), and no endpoint it could read answers."""
    import ast
    src = open(os.path.join(ROOT, rel), encoding="utf-8").read()
    tree = ast.parse(src)
    raise_stmt = next(n for n in tree.body if isinstance(n, ast.Raise))
    lines = src.splitlines(keepends=True)
    armed = "".join(lines[:raise_stmt.lineno - 1] + lines[raise_stmt.end_lineno:])
    dest = tmp_path / "tools" / os.path.basename(rel)
    dest.parent.mkdir()
    dest.write_text(armed, encoding="utf-8")
    env = _dead_env()                                   # and NO repo on the path: nothing of core/ to import
    env.pop("PYTHONPATH", None)
    r = subprocess.run([sys.executable, "-B", str(dest)], cwd=str(tmp_path), env=env,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode != 0 and "DEFUSED" not in (r.stdout + r.stderr).upper(), "precondition: armed copy ran"
    assert "ModuleNotFoundError" in r.stderr or "ImportError" in r.stderr, r.stderr[-400:]
    assert "TOTAL DELETED" not in r.stdout and "uploaded" not in r.stdout, r.stdout[-400:]


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
