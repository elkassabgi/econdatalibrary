#!/usr/bin/env python3
"""Orchestrate the rest of the NOAA crawl after the GSOM download is underway.

Waits for the already-running GSOM download to finish (detected by the count of
.csv files stabilizing at the published total OR a sweep finding < N missing),
then runs: GSOY download -> build (both) -> verify. Resumable: re-running picks
up where files already exist.

This is launched in the background; progress goes to data/ingest_noaa_full.log.
"""
import json
import os
import subprocess
import sys
import time

ROOT = r"D:/research/econfindatalibrary"
RAW = os.path.join(ROOT, "data", "raw", "noaa")
PY = sys.executable
JOB = os.path.join(ROOT, "jobs", "ingest_noaa.py")


def count_csv(ds):
    d = os.path.join(RAW, ds)
    if not os.path.isdir(d):
        return 0
    return sum(1 for n in os.listdir(d) if n.endswith(".csv"))


def published_total(ds):
    p = os.path.join(RAW, f"{ds}_station_ids.txt")
    with open(p, encoding="utf-8") as f:
        return sum(1 for ln in f if ln.strip())


def run(args):
    print(f">>> RUN {' '.join(args)}", flush=True)
    r = subprocess.run([PY, JOB] + args, cwd=ROOT)
    print(f">>> EXIT {r.returncode} for {' '.join(args)}", flush=True)
    return r.returncode


def wait_until_downloaded(ds, settle_secs=90, poll=15):
    """Wait until the dataset's CSV count reaches >=99.5% of published OR stops
    growing for settle_secs (download finished / saturated)."""
    target = published_total(ds)
    print(f"WAIT {ds}: target={target:,}", flush=True)
    last = -1
    stable_for = 0
    while True:
        c = count_csv(ds)
        if c >= int(target * 0.995):
            print(f"WAIT {ds}: reached {c:,}/{target:,} (>=99.5%)", flush=True)
            return c
        if c == last:
            stable_for += poll
            if stable_for >= settle_secs:
                print(f"WAIT {ds}: stalled at {c:,}/{target:,} for {settle_secs}s -> proceed/retry",
                      flush=True)
                return c
        else:
            stable_for = 0
            last = c
            print(f"WAIT {ds}: {c:,}/{target:,}", flush=True)
        time.sleep(poll)


def main():
    t0 = time.time()
    # 1) GSOM download is already running externally; wait for it.
    wait_until_downloaded("gsom")
    # Re-run gsom download once to sweep any 404/failed (skips existing, fast).
    run(["--download", "--dataset", "gsom", "--workers", "6"])
    # 2) GSOY download
    run(["--download", "--dataset", "gsoy", "--workers", "6"])
    wait_until_downloaded("gsoy")
    # 3) Build both datasets -> grouped parquet
    run(["--build", "--dataset", "all"])
    # 4) Verify + write aggregate meta
    run(["--verify", "--dataset", "all"])
    print(f"ALL DONE in {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
