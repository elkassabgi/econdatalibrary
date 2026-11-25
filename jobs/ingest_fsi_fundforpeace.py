#!/usr/bin/env python3
"""Fragile States Index (FSI) ingest - Fund for Peace. COLD START ONLY.

Source: https://fragilestatesindex.org/excel/
License: CC BY-NC-SA 4.0 (non-commercial; attribution to "The Fund for Peace" with a link to its web site where
possible; ShareAlike). See DATABASE_LICENSES_VERBATIM.md.
Coverage: about 178 countries a year, 2006 to the newest year the excel page links.
series_key: FSI_FP:{indicator}:{country}
Output: data/clean_full/fsi_fundforpeace/fsi_fundforpeace.parquet

Run: python jobs/ingest_fsi_fundforpeace.py
  main() writes ONLY when NO store exists, and then through the fetcher's own update()
  (updater/strategies/fetchers/fsi_fundforpeace.py: the same discovery, year checks, refusals and merge). On an
  existing store it writes NOTHING and prints the command to use instead (python -m updater.run --source
  fsi_fundforpeace --force): a write outside the orchestrator books no state row, runs no CSV phase and queues no
  catalogue sync (R549). The old main() walked a hard-coded YEAR_URLS list (dead WordPress paths) and a deleted
  GitHub mirror and wrote with a local pq.write_table, outside the never-shrink merge. That list, its parser and
  that write were removed on 2026-11-25.
"""
from __future__ import annotations
import os, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
FORCED_RUN = "python -m updater.run --source fsi_fundforpeace --force"


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    """Cold start only, through the fetcher. Returns the Result, or None when a store exists (nothing written)."""
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from updater import blob, config                              # noqa: PLC0415
    from updater.strategies.fetchers import fsi_fundforpeace as F  # noqa: PLC0415
    path = os.path.join(config.source_dir(F.SOURCE), "fsi_fundforpeace.parquet")
    if blob.exists(path):
        log(f"fsi_fundforpeace: a store exists ({path}); this script writes only on a cold start and wrote "
            f"nothing. To refresh it, run from the repo root: {FORCED_RUN}")
        return None
    res = F.update(None, None)
    log(f"fsi_fundforpeace: {res.status}: {res.error}")
    return res


if __name__ == "__main__":
    main()
