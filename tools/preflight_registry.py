"""Fast, dependency-light registry preflight — catches at PUSH time what would otherwise
take the whole nightly refresh offline at 06:00 UTC.

WHY. `updater/config.py:EXPECTED_SOURCE_COUNT` is a deliberate tripwire: it makes anyone
adding a source say so out loud, which is how the "source added but invisible to the
orchestrator" class got closed. But its failure mode is total, not partial. A stale count
makes `registry.validate()` return a problem and `orchestrate.py` raise SystemExit — so the
run aborts before touching a single source. On 2026-07-30 nine IMF sources went in across two
commits without a bump; from 01:37 UTC every run would have exited immediately, and the only
place that would have shown up was a red 06:00 cron.

A tripwire whose alarm is a nightly outage is the wrong alarm. This runs on push, needs only
PyYAML, and finishes in about a second.

CHECKS
  1. registry.yaml parses, and EXPECTED_SOURCE_COUNT matches the number of entries
  2. no duplicate source_id
  3. every entry has a strategy and a cadence
  4. `live` is a real boolean where present (live: "yes" would silently widen the perimeter)
  5. every `live: true` source with a fetcher-backed strategy HAS its fetcher module on disk —
     a live source whose adapter is missing is a guaranteed RED-UNRUN

Exit 1 with the specific problems listed. Usage: python tools/preflight_registry.py
"""
from __future__ import annotations
import os
import re
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY = os.path.join(ROOT, "updater", "registry.yaml")
CONFIG = os.path.join(ROOT, "updater", "config.py")
FETCHER_DIR = os.path.join(ROOT, "updater", "strategies", "fetchers")

# Strategies the orchestrator resolves through a per-source fetcher module. MUST stay in step
# with orchestrate._has_adapter and health._adapter_ready; those two have drifted apart before.
FETCHER_BACKED = {"extend_by_date", "overwrite_if_changed", "sdmx_delta",
                  "manual_vintage", "bulk_snapshot_if_changed"}


def main() -> int:
    problems = []

    reg = yaml.safe_load(open(REGISTRY, encoding="utf-8"))
    sources = reg.get("sources") or []

    src = open(CONFIG, encoding="utf-8").read()
    m = re.search(r"^EXPECTED_SOURCE_COUNT\s*=\s*(\d+)", src, re.M)
    if not m:
        problems.append("EXPECTED_SOURCE_COUNT not found in updater/config.py")
    else:
        expected = int(m.group(1))
        if expected != len(sources):
            problems.append(
                f"EXPECTED_SOURCE_COUNT is {expected} but registry.yaml has {len(sources)} "
                f"sources. Bump the constant AND add a dated changelog line above it saying "
                f"which sources were added and why — orchestrate.py raises SystemExit on this "
                f"mismatch, so leaving it stale takes the ENTIRE nightly refresh offline.")

    seen = set()
    for e in sources:
        sid = e.get("source_id")
        if not sid:
            problems.append(f"entry with no source_id: {str(e)[:80]}")
            continue
        if sid in seen:
            problems.append(f"duplicate source_id: {sid}")
        seen.add(sid)
        if not e.get("strategy"):
            problems.append(f"{sid}: missing strategy")
        if not e.get("cadence"):
            problems.append(f"{sid}: missing cadence")
        if "live" in e and not isinstance(e.get("live"), bool):
            problems.append(f"{sid}: live must be a boolean, got {e.get('live')!r}")
        if e.get("live") is True and e.get("strategy") in FETCHER_BACKED:
            if not os.path.exists(os.path.join(FETCHER_DIR, f"{sid}.py")):
                problems.append(
                    f"{sid}: live=true with strategy {e['strategy']} but no fetcher module "
                    f"updater/strategies/fetchers/{sid}.py — it can only ever be RED-UNRUN")

    if problems:
        print(f"REGISTRY PREFLIGHT FAILED ({len(problems)} problem(s)):")
        for p in problems:
            print(f"  - {p}")
        return 1
    live = sum(1 for e in sources if e.get("live") is True)
    print(f"registry preflight OK: {len(sources)} sources, {live} live")
    return 0


if __name__ == "__main__":
    sys.exit(main())
