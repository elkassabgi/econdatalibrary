"""Print the comma-separated source_ids routed to the workstation (run_location: local).

A separate file rather than a here-string inside run_local_heavy.ps1: Windows PowerShell 5.1
reads a BOM-less .ps1 as ANSI, and embedding another language in a here-string is one more
way for that to go wrong silently. Used by tools/run_local_heavy.ps1 so the source list has
exactly one definition - the registry.
"""
import io
import os
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
reg = os.path.join(ROOT, "updater", "registry.yaml")

try:
    d = yaml.safe_load(io.open(reg, encoding="utf-8")) or {}
except Exception as e:                                       # noqa: BLE001
    print(f"registry unreadable: {e!r}", file=sys.stderr)
    sys.exit(1)

ids = sorted({e.get("source_id") for e in (d.get("sources") or [])
              if e.get("run_location") == "local" and e.get("source_id")})
print(",".join(ids))
