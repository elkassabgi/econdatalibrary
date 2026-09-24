"""PreToolUse hook for Claude Code: hand and agent writes to the retired econ cloud copy
(docs/ECON_SELF_HOSTING_PLAN.md, section 3, change 5 - "Hand and agent writes").

INSTALLED BY AHMED, not by an agent: the plan says the user-global settings change is shown to him before
it is made. It belongs in the USER-GLOBAL ~/.claude/settings.json (hooks are per project, and the R251
ban was once hf-only), as a PreToolUse hook on Bash and PowerShell:

    {"matcher": "Bash|PowerShell",
     "hooks": [{"type": "command", "command": "python <repo>/tools/selfhost/cutover_hook.py"}]}

Two rules:
  1. ALWAYS, from install: refuse any command that names the flag path. The flag is Ahmed's to create at
     T0 (elevated); an agent must never create, move or delete it - "not cut over" is the state that
     allows writes.
  2. ONCE THE FLAG EXISTS: refuse the command-line roads to the retired copy - wrangler r2 object
     put/delete on the econ bucket, wrangler d1 execute / migrations apply --remote on the econ
     catalogue databases (by name or id), the D1 REST path with their ids, and aws/rclone writes to the
     bucket. Reads after T0 go through core/d1_remote.py with a read-only token instead.

Rule 1 also refuses harmless commands that only MENTION the folder (a grep for it, a commit message typed
on the command line). That friction is deliberate: text naming the path goes through a file instead
(`git commit -F`, a script), where the rule does not look.

What it is NOT: it stops mistakes, not intent. The hook and settings are editable by the same user, and it
cannot see writes made inside scripts - those are stopped by core/r2_util's guard, core/d1_remote.py and
the revoked write key; the daily off-machine check (tools/selfhost/watch_edge.py) is the proof.

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

# The flag folder in any spelling a shell accepts: C:\ProgramData\econ, $env:ProgramData\econ,
# %ProgramData%\econ, /c/ProgramData/econ, with either slash and any case.
_FLAG_NAME = re.compile(r"programdata%?\)?[\\/]+econ(?![\w-])", re.I)
_D1_NAME = "|".join(re.escape(n) for n in ECON_D1)
_WRITE_ROADS = [
    re.compile(rf"wrangler\s+r2\s+object\s+(put|delete)\b[^\n]*\b{re.escape(ECON_BUCKET)}(?![\w-])", re.I),
    re.compile(rf"wrangler\s+d1\s+(execute|migrations\s+apply)\b(?=[^\n]*--remote)[^\n]*\b({_D1_NAME})(?![\w-])", re.I),
    re.compile(rf"wrangler\s+d1\s+(execute|migrations\s+apply)\b[^\n]*\b({_D1_NAME})(?![\w-])(?=[^\n]*--remote)", re.I),
    re.compile(rf"/d1/database/({_D1_NAME})\b", re.I),
    re.compile(rf"\baws\s+s3(api)?\s+(cp|mv|rm|sync|put-object|delete-object|delete-objects)\b[^\n]*"
               rf"{re.escape(ECON_BUCKET)}(?![\w-])", re.I),
    re.compile(rf"\brclone\s+(copy|copyto|move|moveto|sync|delete|deletefile|purge|rcat)\b[^\n]*"
               rf"{re.escape(ECON_BUCKET)}(?![\w-])", re.I),
]


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
    if _FLAG_NAME.search(command):
        return ("REFUSED: this command names the econ CUTOVER flag folder (C:\\ProgramData\\econ). Only Ahmed "
                "creates or changes it, elevated, at T0 (docs/ECON_SELF_HOSTING_PLAN.md step 6a).")
    if cut_over(flag_path):
        for road in _WRITE_ROADS:
            m = road.search(command)
            if m:
                return (f"REFUSED after T0: '{m.group(0)[:80]}' writes to the retired econ cloud copy. econ is "
                        "self-hosted; read D1 through core/d1_remote.py (read-only token), write locally.")
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
