#!/usr/bin/env python3
"""Durable single-pass runner for the OECD crawl, designed to be re-invoked by the
Windows Task Scheduler every few minutes.

Behaviour per invocation:
  * If the catalog is already DONE -> exit immediately.
  * If another instance's crawler is still alive (heartbeat lock fresh) -> exit (so we
    never run two crawlers at once -> stays under OECD's request quota).
  * Otherwise launch ONE crawler pass (ingest_oecd.py, fully resumable) and wait for it
    to finish or die, refreshing the heartbeat while it runs.

Because ingest_oecd skips any dataflow that already has a Parquet, repeated invocations
march the whole 1509-dataflow catalog to completion even if individual processes get
reaped by the environment.

Scheduled-task command:
  python jobs/run_oecd_until_done.py --workers 1 --version-fallback
"""
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OECD_RAW = os.path.join(ROOT, "data", "raw", "oecd")
RUNLOG = os.path.join(OECD_RAW, "run.log")
SUPLOG = os.path.join(OECD_RAW, "supervisor.log")
LOCK = os.path.join(OECD_RAW, "crawler.lock")          # heartbeat file
DONE_MARK = "DONE in"
LOCK_STALE_SECS = 180                                   # lock older than this = dead


def slog(msg):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        with open(SUPLOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass
    print(line, flush=True)


def already_done():
    try:
        with open(RUNLOG, encoding="utf-8") as f:
            return any(line.startswith(DONE_MARK) for line in f)
    except OSError:
        return False


def lock_fresh():
    try:
        age = time.time() - os.path.getmtime(LOCK)
        return age < LOCK_STALE_SECS
    except OSError:
        return False


def touch_lock():
    try:
        with open(LOCK, "w", encoding="utf-8") as f:
            f.write(str(time.time()))
    except OSError:
        pass


def main():
    passthru = sys.argv[1:]
    if already_done():
        slog("already DONE -> nothing to do")
        return
    if lock_fresh():
        slog("another crawler is alive (fresh lock) -> exiting")
        return

    cmd = [sys.executable, "-u", os.path.join(ROOT, "jobs", "ingest_oecd.py")] + passthru
    slog(f"launching crawler: {' '.join(passthru)}")
    touch_lock()
    with open(RUNLOG, "a", encoding="utf-8") as out:
        out.write(f"\n===== runner pass @ {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n")
        out.flush()
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT)
        # refresh heartbeat while the crawler runs
        while True:
            rc = proc.poll()
            if rc is not None:
                break
            touch_lock()
            time.sleep(30)
    slog(f"crawler exited rc={rc}; done={already_done()}")
    # drop the lock so the next scheduled invocation can resume promptly
    try:
        os.remove(LOCK)
    except OSError:
        pass


if __name__ == "__main__":
    main()
