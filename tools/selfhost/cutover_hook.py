"""PreToolUse hook for Claude Code: hand and agent writes to the retired econ cloud copy
(docs/ECON_SELF_HOSTING_PLAN.md, section 3, change 5 - "Hand and agent writes").

INSTALLED BY AHMED, not by an agent: the plan says the user-global settings change is shown to him before
it is made. It belongs in the USER-GLOBAL ~/.claude/settings.json (hooks are per project, and the R251
ban was once hf-only), as a PreToolUse hook on Bash and PowerShell:

    {"matcher": "Bash|PowerShell",
     "hooks": [{"type": "command", "command": "python <repo>/tools/selfhost/cutover_hook.py"}]}

Two rules:
  1. ALWAYS, from install: refuse any command line that names the flag folder (see _FOLDER below - the
     folder's ACL is the real protection; this catches plain mistakes only). The flag is Ahmed's to create at
     T0 (elevated); an agent must never create, move or delete it - "not cut over" is the state that
     allows writes.
  2. ONCE THE FLAG EXISTS: refuse the command-line roads to the retired copy - wrangler r2 object
     put/delete on the econ bucket, wrangler d1 execute / migrations apply --remote on the econ
     catalogue databases (by name or id), the D1 REST path with their ids, and aws/rclone writes to the
     bucket. Reads after T0 go through core/d1_remote.py with a read-only token instead.

Rule 1 also refuses harmless commands that only MENTION the folder (a grep for it, a commit message typed
on the command line). That friction is deliberate: text naming the path goes through a file instead
(`git commit -F`, a script), where the rule does not look.

What it is NOT: it stops mistakes, not intent. The hook and settings are editable by the same user; it
cannot see writes made inside scripts (those are stopped by core/r2_util's guard, core/d1_remote.py and
the revoked write key; the daily off-machine check, tools/selfhost/watch_edge.py, is the proof); and it
cannot resolve a database name or id held in a shell VARIABLE. For the whole-database and whole-bucket
commands (d1 delete / time-travel, r2 bucket ...) the off-machine check is blind, so on this desktop -
where wrangler's OAuth login is account-wide - this hook is the one preventive layer: step 6a narrows the
login's token, which is the real fix (R1178).

FAILS OPEN on unreadable input, like the D1 cost hook: a guard that blocks all work when it breaks is a
guard that gets disabled.
"""
from __future__ import annotations

import json
import os
import re
import sys

FLAG_PATH = r"C:\ProgramData\econ\CUTOVER"
ECON_BUCKET = "econ-data"
ECON_D1 = ("econ-catalog", "econ-catalog-climate",
           "1a6d0755-ecef-46d0-a478-46cad1cf064c", "e34114f2-c0be-43d9-bcb5-798a3952414c")
# The worker's BINDING names: wrangler resolves `d1 execute CATALOG` through the config (R1179). Matched
# case-SENSITIVELY, as wrangler matches them, so hf's `--file dist/catalog.sql` is not a hit.
ECON_D1_BINDINGS = ("CATALOG_CLIMATE", "CATALOG")

# The flag folder. NOT COMPLETE, and it cannot be: a shell reaches a folder by more spellings than a pattern
# can list (Join-Path, a variable, `cd` then a relative name, the "All Users" junction, python's os.environ).
# The folder's ACL (Users read only, owner Administrators - plan 3.5) is the real protection; this rule only
# catches the plain mistakes. It refuses a line that names the ProgramData folder in any of the spellings
# below ANYWHERE and also names `econ` or `CUTOVER` anywhere (R1179: adjacency let `Join-Path
# $env:ProgramData econ` and `"C:\ProgramData"\econ` through).
_FOLDER = re.compile(r"programdata|progra~\d|allusersprofile|all\s+users[\\/]|commonapplicationdata", re.I)
_FLAG_WORD = re.compile(r"(?<![\w-])(econ|cutover)(?![\w-])", re.I)
_D1_NAME = "|".join(re.escape(n) for n in ECON_D1)
_BINDING = "(?-i:" + "|".join(ECON_D1_BINDINGS) + ")"
# Options between the levels of a subcommand (`d1 -c x.toml execute`, `r2 object --config=x put`): wrangler
# accepts them there (R1184 measured `d1 -c wrangler.toml delete econ-catalog` getting through).
_O = r"(?:\s+-\S*(?:\s+[^-\s]\S*)?)*"
_B = re.escape(ECON_BUCKET)
# wrangler in every spelling a shell reaches it by: wrangler, wrangler@4, wrangler.cmd, .../wrangler.js, ...
# followed by ANY options before the subcommand (`wrangler -c x.toml d1 ...`, R1179).
_W = r"\bwrangler(?:@[\w.\-]+)?(?:\.cmd|\.js|\.mjs|\.ps1|\.exe)?\b.*?"
# aws / rclone, with any global options before the subcommand (`aws --endpoint-url e s3 rm`, R1179)
_AWS = r"\baws\b.*?\bs3(?:api)?\s+"
_RCLONE = r"\brclone\b.*?\b"
_WRITE_ROADS = [
    re.compile(rf"{_W}\br2{_O}\s+object{_O}\s+(put|delete)\b.*?\b{_B}(?![\w-])", re.I),
    # whole-bucket changes the off-machine check cannot see (R1178)
    re.compile(rf"{_W}\br2{_O}\s+bucket{_O}\s+(delete|lifecycle|cors|notification|sippy|domain|dev-url|lock|update"
               rf"|catalog)\b.*?\b{_B}(?![\w-])", re.I),
    re.compile(rf"{_W}\bd1{_O}\s+(execute|migrations{_O}\s+apply)\b(?=.*--remote).*?(?<![\w-])({_D1_NAME})(?![\w-])",
               re.I),
    # deleting or rewinding a whole database - also invisible to the off-machine check (R1178)
    re.compile(rf"{_W}\bd1{_O}\s+(delete|time-travel)\b.*?(?<![\w-])({_D1_NAME})(?![\w-])", re.I),
    # a BINDING name only as the database argument (R1184: CATALOG in an hf command's SQL is not a database)
    re.compile(rf"{_W}\bd1{_O}\s+(execute|migrations{_O}\s+apply)\b(?=.*--remote){_O}\s+[\"']?{_BINDING}(?![\w-])",
               re.I),
    re.compile(rf"{_W}\bd1{_O}\s+(delete|time-travel{_O}\s+restore){_O}\s+[\"']?{_BINDING}(?![\w-])", re.I),
    re.compile(rf"/d1/database/({_D1_NAME})\b", re.I),
    re.compile(rf"/r2/buckets/{_B}(?![\w-])", re.I),                       # the REST API (R1179)
    # the verb anywhere after `s3` / `s3api`: options may come first (`aws s3api --bucket econ-data delete-object`)
    re.compile(rf"\baws\b.*?\bs3(?:api)?\b(?=.*?(?<![\w-])(cp|mv|rm|sync|rb|put-\S+|delete-\S+|copy-object"
               rf"|create-multipart-upload|upload-part\S*|complete-multipart-upload|abort-multipart-upload"
               rf"|restore-object)(?![\w-])).*?{_B}(?![\w-])", re.I),
    re.compile(rf"{_RCLONE}(copy|copyto|copyurl|move|moveto|sync|bisync|delete|deletefile|purge|rcat|touch|mkdir"
               rf"|rmdir|rmdirs|settier|dedupe|backend)\b.*?{_B}(?![\w-])", re.I),
    # the other S3 command-line clients (R1179)
    re.compile(rf"\b(s5cmd|mc)\b.*?\b(rm|cp|mv|sync|rb|mirror|put|pipe|del)\b.*?{_B}(?![\w-])", re.I),
    re.compile(rf"{_W}\br2{_O}\s+bulk\b.*?\b{_B}(?![\w-])", re.I),
]
# NOTE: the aws / rclone / s5cmd / mc roads refuse DOWNLOADS from the bucket too (`aws s3 cp s3://econ-data/x .`,
# `rclone copy r2:econ-data ./x`): after T0 the bucket is the frozen copy, and the reads that step 6b needs
# go through core.r2_util.cloud_client, not a shell (fail closed; none of these CLIs is installed today).


def _segments(command: str) -> list[str]:
    """The separate COMMANDS in `command`. A line continued onto the next (PowerShell backtick, POSIX
    backslash, cmd caret) is joined first (R1178); then the text is cut at ; && || | and newlines that are
    OUTSIDE quotes. A newline inside a quoted --command "..." stays part of its command (R1184: cutting at
    every physical line let `--remote` or the database name after that newline through), and an hf command
    and a mention of econ in the NEXT command are not one write (R1179 finding 7). An unclosed quote keeps
    the rest as one segment: more text scanned together, never less."""
    text = re.sub(r"[`\\^]\r?\n", " ", command)
    out, cur, quote, i = [], [], None, 0
    while i < len(text):
        ch = text[i]
        if quote:
            if ch in "\\`" and quote == '"' and i + 1 < len(text):     # an escaped character inside "..."
                cur.append(ch + text[i + 1])
                i += 2
                continue
            cur.append(" " if ch in "\r\n" else ch)
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
            cur.append(ch)
        elif ch in "\r\n;|" or text.startswith("&&", i):
            out.append("".join(cur))
            cur = []
            if text.startswith("&&", i) or text.startswith("||", i):
                i += 1
        else:
            cur.append(ch)
        i += 1
    out.append("".join(cur))
    return [x for x in out if x.strip()] or [""]


def cut_over(flag_path: str = FLAG_PATH) -> bool:
    """The same answer as core/cutover.is_cut_over (kept separate: the hook runs outside the repo)."""
    try:
        os.stat(flag_path)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except Exception:  # noqa: BLE001 - cannot tell = cut over
        return True
    return True


def decide(command: str, flag_path: str = FLAG_PATH) -> str | None:
    """The reason to deny `command`, or None to let it run."""
    # Rule 1 reads the WHOLE command, not per segment: `cd C:\ProgramData; Remove-Item econ` is one act in two
    # commands. A false refusal here costs a `-F file`; a miss costs the flag (R1184 finding 5, weighed).
    whole = " ".join(_segments(command))
    if _FOLDER.search(whole) and _FLAG_WORD.search(whole):
        return ("REFUSED: this command names the ProgramData folder AND `econ` or `CUTOVER` - it may reach the "
                "econ CUTOVER flag folder (C:\\ProgramData\\econ), which only Ahmed creates or changes, elevated, "
                "at T0 (docs/ECON_SELF_HOSTING_PLAN.md step 6a). Mentioning both on purpose? Put the text in a "
                "file (git commit -F, a script) - the rule reads only command lines.")
    if cut_over(flag_path):
        for line in _segments(command):
            for road in _WRITE_ROADS:
                m = road.search(line)
                if not m:
                    continue
                return (f"REFUSED after T0: '{m.group(0)[:80]}' touches the retired econ cloud copy. econ is "
                        "self-hosted: write locally; read D1 through core/d1_remote.py (read-only token) and R2 "
                        "through core.r2_util.cloud_client.")
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        tool_input = payload.get("tool_input") or {}
        command = tool_input.get("command") or ""
        if not isinstance(command, str):
            return 0
    except Exception:  # noqa: BLE001 - fail open
        return 0
    why = decide(command)
    if why:
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                                                 "permissionDecisionReason": why}}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
