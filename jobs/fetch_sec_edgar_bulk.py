#!/usr/bin/env python3
"""Phase-0 bootstrap: download the SEC EDGAR bulk archives for the initial backfill.

  companyfacts.zip  (~1.39 GB) -- every company's XBRL financial-statement facts (the NUMBERS we host)
  submissions.zip   (~1.54 GB) -- every filing's metadata (the POINTERS we index; documents stay on sec.gov)

Polite by design: descriptive User-Agent (SEC requires it or returns 403), streamed
to disk, resumable (HTTP Range), retried. Source: U.S. SEC EDGAR (public domain).

Run:  python jobs/fetch_sec_edgar_bulk.py
"""
import hashlib
import os
import time

import requests

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"  # SEC requires a descriptive UA
DEST = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "sec_edgar")
FILES = {
    "companyfacts.zip": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
    "submissions.zip":  "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
}


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def download(name: str, url: str, dest_dir: str) -> bool:
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, name)
    tmp = path + ".part"
    pos = os.path.getsize(tmp) if os.path.exists(tmp) else 0
    headers = {"User-Agent": UA}
    if pos:
        headers["Range"] = f"bytes={pos}-"
        log(f"{name}: resuming at {pos/1e6:.1f} MB")
    with requests.get(url, headers=headers, stream=True, timeout=120) as r:
        if r.status_code not in (200, 206):
            log(f"{name}: HTTP {r.status_code} -- abort")
            return False
        total = int(r.headers.get("Content-Length", 0)) + (pos if r.status_code == 206 else 0)
        mode = "ab" if (pos and r.status_code == 206) else "wb"
        done = pos if mode == "ab" else 0
        last = time.time()
        with open(tmp, mode) as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                done += len(chunk)
                if time.time() - last > 15:
                    pct = f"{done/total*100:.1f}%" if total else "?"
                    log(f"{name}: {done/1e6:.0f} MB ({pct})")
                    last = time.time()
    os.replace(tmp, path)
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    log(f"{name}: DONE {os.path.getsize(path)/1e6:.0f} MB  sha256={h.hexdigest()[:16]}...")
    return True


def main() -> None:
    dest = os.path.abspath(DEST)
    log(f"SEC EDGAR bulk download -> {dest}")
    ok = True
    for name, url in FILES.items():
        for attempt in range(1, 6):
            try:
                if download(name, url, dest):
                    break
            except Exception as e:  # noqa: BLE001
                log(f"{name}: attempt {attempt} failed: {e}")
                time.sleep(10)
        else:
            ok = False
            log(f"{name}: GAVE UP after retries")
        time.sleep(1)  # be polite between files
    log("ALL DONE" if ok else "FINISHED WITH ERRORS")


if __name__ == "__main__":
    main()
