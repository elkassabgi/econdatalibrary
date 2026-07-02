#!/usr/bin/env python3
"""Serially ingest every workflow-built connector into the catalog (the verification step).

Each connector runs in its OWN subprocess with a timeout, so a single hang/crash can't
stall the batch. This is the real end-to-end test: a connector only "passes" if it
actually fetches, normalizes, license-gates, writes Parquet, and upserts the catalog.

Run: python jobs/ingest_all.py
"""
import os
import subprocess
import sys
import time

ROOT = r"D:/research/econfindatalibrary"
RUNNER = os.path.join(ROOT, "jobs", "run_connector.py")

# 23 connectors, lightest/fastest first, heavier API crawlers (oecd, imf) last.
SOURCES = [
    "frankfurter", "defillama", "treasury", "fed_board", "fhfa", "zillow",
    "ember", "owid", "penn_world_table", "worldbank_pink", "boe", "bis",
    "statcan", "abs", "wikidata", "dbnomics", "worldbank_esg", "census",
    "faostat", "ilostat", "ecb", "oecd", "imf",
]

results = []
for src in SOURCES:
    t0 = time.time()
    try:
        p = subprocess.run([sys.executable, RUNNER, src],
                           capture_output=True, text=True, timeout=600)
        out = (p.stdout or "") + (p.stderr or "")
        line = [ln for ln in out.splitlines() if "ingested" in ln]
        if p.returncode == 0 and line:
            st, summ = "OK", line[-1].strip()
        else:
            st = "ERR"
            tail = [ln for ln in out.strip().splitlines() if ln.strip()]
            summ = (tail[-1] if tail else "no output")[:200]
        dur = round(time.time() - t0, 1)
        results.append((src, st, dur, summ))
        print(f"{st:4} {src:18} {dur:>6}s  {summ[:120]}", flush=True)
    except subprocess.TimeoutExpired:
        results.append((src, "TIMEOUT", 600.0, ""))
        print(f"TIMEOUT {src}", flush=True)
    except Exception as e:  # noqa: BLE001
        results.append((src, "EXC", round(time.time() - t0, 1), str(e)[:160]))
        print(f"EXC  {src}: {e}", flush=True)

print("\n==================== SUMMARY ====================")
for src, st, dur, summ in results:
    print(f"  {st:8} {src:18} {dur:>6}s  {summ[:90]}")
ok = sum(1 for r in results if r[1] == "OK")
print(f"\n{ok}/{len(results)} connectors ingested OK")
