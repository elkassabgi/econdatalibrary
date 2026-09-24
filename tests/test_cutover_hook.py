"""tools/selfhost/cutover_hook.py - the PreToolUse hook for hand and agent writes (plan change 5)."""
import json
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import cutover_hook as H  # noqa: E402

FLAG_NAMES = [
    r"New-Item -ItemType File C:\ProgramData\econ\CUTOVER",
    r"Remove-Item 'C:\ProgramData\econ\CUTOVER'",
    r"del c:\programdata\econ\cutover",
    r"icacls C:\ProgramData\econ /grant Users:F",
    "rm -f /c/ProgramData/econ/CUTOVER",
    r"Remove-Item $env:ProgramData\econ -Recurse",
    r"type %ProgramData%\econ\CUTOVER",
]
WRITES = [
    "npx wrangler r2 object put econ-data/series/x.csv --file x.csv",
    "wrangler r2 object delete econ-data/_aqueduct/stats.json --remote",
    'npx wrangler d1 execute econ-catalog --remote --command "DELETE FROM series"',
    "npx wrangler d1 execute --remote econ-catalog-climate --file fix.sql",
    "wrangler d1 migrations apply econ-catalog --remote",
    "npx wrangler d1 execute 1a6d0755-ecef-46d0-a478-46cad1cf064c --remote --command 'SELECT 1'",
    "curl -X POST https://api.cloudflare.com/client/v4/accounts/a/d1/database/e34114f2-c0be-43d9-bcb5-798a3952414c/query",
    "aws s3 cp x.csv s3://econ-data/series/x.csv --endpoint-url https://r2",
    "aws s3api delete-object --bucket econ-data --key x",
    "rclone sync ./blobs r2:econ-data",
]
FINE = [
    "npx wrangler r2 object get econ-data/series/x.csv --file x.csv",
    "npx wrangler r2 object put hfdatalibrary-data/x.csv --file x.csv",
    'npx wrangler d1 execute econ-catalog --local --command "SELECT 1"',
    'npx wrangler d1 execute hfdatalibrary-db --remote --command "SELECT 1"',
    "npx wrangler r2 object put econ-data-archive/x --file x",
    "git log -1",
    "python -m pytest tests/test_cutover.py",
    r"Get-ChildItem C:\ProgramData\econometrics",
]


@pytest.fixture
def flag(tmp_path):
    p = tmp_path / "CUTOVER"
    p.write_text("")
    return str(p)


@pytest.fixture
def no_flag(tmp_path):
    return str(tmp_path / "absent" / "CUTOVER")


@pytest.mark.parametrize("cmd", FLAG_NAMES)
def test_the_flag_folder_is_refused_always(cmd, no_flag, flag):
    assert H.decide(cmd, no_flag) and H.decide(cmd, flag)


@pytest.mark.parametrize("cmd", WRITES)
def test_write_roads_are_refused_only_after_t0(cmd, no_flag, flag):
    assert H.decide(cmd, no_flag) is None, "before T0 nothing changes"
    assert H.decide(cmd, flag), cmd


@pytest.mark.parametrize("cmd", FINE)
def test_everything_else_runs(cmd, flag):
    assert H.decide(cmd, flag) is None, cmd


def test_an_unreadable_flag_counts_as_cut_over(monkeypatch):
    def stat(_p, *a, **k):
        raise PermissionError(13, "denied")
    monkeypatch.setattr(H.os, "stat", stat)
    assert H.cut_over() is True


def _run(stdin: str) -> str:
    return subprocess.run([sys.executable, "-B", os.path.join(ROOT, "tools", "selfhost", "cutover_hook.py")],
                          input=stdin, capture_output=True, text=True, timeout=60).stdout


def test_the_hook_protocol():
    out = _run(json.dumps({"tool_name": "Bash", "tool_input": {"command": FLAG_NAMES[0]}}))
    d = json.loads(out)["hookSpecificOutput"]
    assert d["permissionDecision"] == "deny" and "CUTOVER" in d["permissionDecisionReason"]
    assert _run(json.dumps({"tool_name": "Bash", "tool_input": {"command": "git status"}})) == ""
    assert _run("not json") == "", "fails open on unreadable input"


def test_the_real_flag_path_is_the_one_core_uses():
    from core import cutover
    assert H.FLAG_PATH == cutover.FLAG_PATH
