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
    # R1178: the 8.3 name, ALLUSERSPROFILE and the .NET special folder
    r"del C:\PROGRA~3\econ\CUTOVER",
    r"Remove-Item $env:ALLUSERSPROFILE\econ\CUTOVER",
    r"Join-Path ([Environment]::GetFolderPath('CommonApplicationData')) econ",
    # R1179: the folder and `econ` need not be next to each other
    r"Remove-Item ${env:ProgramData}\econ\CUTOVER",
    r"Join-Path $env:ProgramData econ",
    r"Remove-Item (Join-Path $env:ProgramData 'econ') -Recurse",
    "python -c \"import os; os.remove(os.path.join(os.environ['PROGRAMDATA'], 'econ', 'CUTOVER'))\"",
    r"Remove-Item 'C:\Users\All Users\econ\CUTOVER'",
    r'del "C:\Documents and Settings\All Users\econ\CUTOVER"',
    r"del C:\ProgramData\.\econ\CUTOVER",
    r"cd C:\ProgramData; Remove-Item econ -Recurse",
    r'del "C:\ProgramData"\econ\CUTOVER',
    r"del %SystemDrive%\ProgramData\econ\CUTOVER",
    r"del \\?\C:\ProgramData\econ\CUTOVER",
    r"del \\localhost\c$\ProgramData\econ\CUTOVER",
]
# Spellings the rule does NOT catch - stated, not hidden: the folder's ACL is the protection (plan 3.5).
FLAG_NAMES_NOT_CAUGHT = [r"$p='C:\Program'+'Data\econ'; rm $p"]
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
    # R1178: whole-database / whole-bucket commands the off-machine check cannot see
    "npx wrangler d1 delete econ-catalog -y",
    "wrangler d1 time-travel restore econ-catalog-climate --timestamp 2026-10-01",
    "wrangler r2 bucket delete econ-data",
    "wrangler r2 bucket lifecycle add econ-data rule --expire-days 1",
    # R1178: every spelling of wrangler
    "npx wrangler@4 r2 object put econ-data/x --file x",
    "wrangler.cmd d1 execute econ-catalog --remote --command \"DELETE FROM series\"",
    "node node_modules/wrangler/bin/wrangler.js d1 execute econ-catalog --remote --file f.sql",
    # R1178: continued onto the next line (PowerShell backtick, POSIX backslash)
    "npx wrangler d1 execute `\n  econ-catalog --remote --command \"DELETE FROM series\"",
    "npx wrangler d1 execute \\\n  econ-catalog --remote --file f.sql",
    "aws s3 rb s3://econ-data --force",
    # R1179: binding names, options before the subcommand, bucket-level verbs, the REST path, other clients
    "npx wrangler d1 execute CATALOG --remote --command \"DELETE FROM series\"",
    "npx wrangler d1 migrations apply CATALOG_CLIMATE --remote",
    "npx wrangler --config api/worker/wrangler.toml d1 execute econ-catalog --remote --command \"DELETE FROM series\"",
    "npx wrangler -c api/worker/wrangler.toml d1 delete econ-catalog -y",
    "npx wrangler --cwd api/worker r2 object put econ-data/x --file x",
    "npx wrangler r2 bulk put econ-data --filename list.json",
    "aws --endpoint-url https://acct.r2.cloudflarestorage.com s3 rm s3://econ-data --recursive",
    "aws --profile r2 s3 rb s3://econ-data --force",
    "aws s3api put-bucket-lifecycle-configuration --bucket econ-data --lifecycle-configuration file://l.json",
    "aws s3api copy-object --bucket econ-data --key a --copy-source econ-data/b",
    "aws s3api put-bucket-policy --bucket econ-data --policy file://p.json",
    "curl -X DELETE https://api.cloudflare.com/client/v4/accounts/ACC/r2/buckets/econ-data",
    "rclone --config r.conf sync ./x r2:econ-data",
    "rclone bisync ./x r2:econ-data",
    "rclone copyurl https://x r2:econ-data/k",
    "rclone touch r2:econ-data/k",
    "npx wrangler d1 execute ECON-CATALOG --remote --command \"DELETE FROM series\"",
    "npx wrangler d1 execute econ-catalog --remote=true --command \"DELETE FROM series\"",
    "npx wrangler d1 execute ^\r\n econ-catalog --remote --file f.sql",
    "npx -y wrangler d1 time-travel restore econ-catalog --bookmark x",
    "pnpm dlx wrangler r2 bucket delete econ-data",
    "bunx wrangler d1 delete 1A6D0755-ECEF-46D0-A478-46CAD1CF064C",
    "s5cmd rm s3://econ-data/*",
    "mc rm --recursive --force r2/econ-data",
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
    "npx wrangler d1 list",
    "npx wrangler r2 bucket list",
    "npx wrangler d1 time-travel info hfdatalibrary-db",
    # R1179 finding 7: separate lines are separate commands
    "npx wrangler d1 execute hfdatalibrary-db --remote --command \"SELECT 1\"\necho econ-catalog done",
    # binding names are case-sensitive, as in wrangler: hf's lower-case `catalog` is not the binding
    "npx wrangler d1 execute hfdatalibrary-db --remote --file dist/catalog.sql",
    "wrangler r2 bucket info econ-data",
    "wrangler d1 info econ-catalog",
    r"[Environment]::GetFolderPath('CommonApplicationData')",      # the folder alone, no econ
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


@pytest.mark.parametrize("cmd", FLAG_NAMES_NOT_CAUGHT)
def test_the_known_gaps_are_still_gaps(cmd, flag):
    """If one of these starts being caught, move it to FLAG_NAMES; the docstring's honesty depends on it."""
    assert H.decide(cmd, flag) is None


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
