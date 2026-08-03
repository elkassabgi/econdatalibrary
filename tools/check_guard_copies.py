"""Do the versioned copies of the reboot-survival scripts still match the live files?

WHY. Everything that survives a reboot on this workstation depends on three files that
`.gitignore:41` correctly excludes (they pin absolute E:\\ paths and this box's python.exe), so
until 2026-08-03 the only copy of the fleet definition was on one disk, unversioned. tools/machine/
now holds a copy of each — and a copy that nobody checks is a copy that is wrong when you need it,
which is during a rebuild, when the live file is already gone.

This is the check. It does not sync anything: silently overwriting either side would be a fine way
to lose an edit made on the machine or to resurrect a stale definition on top of a good one. It
reports, and the human decides which side is right.

    python tools/check_guard_copies.py          # exit 1 on any drift
    python tools/check_guard_copies.py --diff   # show the differing lines
"""
from __future__ import annotations
import argparse
import difflib
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MACHINE = os.path.join(ROOT, "tools", "machine")
STARTUP = os.path.join(os.environ.get("APPDATA", ""), "Microsoft", "Windows",
                       "Start Menu", "Programs", "Startup")

PAIRS = [
    (os.path.join(MACHINE, "EconGuard.cmd.startup-copy"),
     os.path.join(STARTUP, "EconGuard.cmd")),
    (os.path.join(MACHINE, "RELAUNCH_GUARD_LOOP.ps1.workstation-copy"),
     os.path.join(ROOT, "RELAUNCH_GUARD_LOOP.ps1")),
    (os.path.join(MACHINE, "RELAUNCH_GUARD.ps1.workstation-copy"),
     os.path.join(ROOT, "RELAUNCH_GUARD.ps1")),
]


def _read(p):
    try:
        with open(p, encoding="utf-8", errors="replace") as fh:
            return fh.read().splitlines()
    except OSError:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--diff", action="store_true", help="print the differing lines")
    a = ap.parse_args()

    drift = 0
    for copy_path, live_path in PAIRS:
        name = os.path.basename(live_path)
        c, l = _read(copy_path), _read(live_path)
        if c is None:
            print(f"  {name:<28} NO VERSIONED COPY at {copy_path}")
            drift += 1
            continue
        if l is None:
            # Not automatically a fault: on any machine that is not the workstation the live file
            # is SUPPOSED to be absent, and the copy is the thing being preserved.
            print(f"  {name:<28} live file absent — expected off the workstation, "
                  f"copy is intact ({len(c)} lines)")
            continue
        if c == l:
            print(f"  {name:<28} in sync ({len(l)} lines)")
            continue
        drift += 1
        print(f"  {name:<28} DRIFTED — copy {len(c)} lines, live {len(l)} lines")
        if a.diff:
            for line in list(difflib.unified_diff(c, l, "versioned copy", "live", lineterm=""))[:40]:
                print(f"      {line}")

    if drift:
        print(f"\n{drift} file(s) drifted or missing. Decide which side is right, then re-copy — "
              f"this tool deliberately does NOT sync, because overwriting either direction "
              f"silently loses one of them.")
        return 1
    print("\nall reboot-survival scripts match their versioned copies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
