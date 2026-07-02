#!/usr/bin/env python3
"""Crawl Eurostat -- download every dataset as a gzipped TSV (CONCURRENT, the big bulk).

Reads the catalogue TOC, extracts dataset/table codes, downloads each via the SDMX 2.1
dissemination API (format=TSV&compressed=true -- the `files/` bulk path is 410 Gone).
Concurrent (thread pool), resumable (skips files already on disk), retry/backoff on
429/5xx, logs progress. Source: Eurostat (CC BY 4.0). Carve-outs applied later at serve.

Run: python jobs/fetch_eurostat_bulk.py
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
HERE = os.path.dirname(__file__)
DEST = os.path.abspath(os.path.join(HERE, "..", "data", "raw", "eurostat"))
TOC = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt"
DATA = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{code}/?format=TSV&compressed=true"
WORKERS = 6
RETRIES = 4


def log(m):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {m}", flush=True)


def dataset_codes():
    r = requests.get(TOC, headers={"User-Agent": UA}, timeout=120)
    r.raise_for_status()
    os.makedirs(DEST, exist_ok=True)
    with open(os.path.join(DEST, "_toc.txt"), "w", encoding="utf-8") as f:
        f.write(r.text)
    seen, out = set(), []
    for line in r.text.splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        code = parts[1].strip().strip('"')
        typ = parts[2].strip().strip('"').lower()
        if typ in ("dataset", "table") and code and code not in seen:
            seen.add(code)
            out.append(code)
    return out


def fetch_one(code):
    path = os.path.join(DEST, code + ".tsv.gz")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return "skip", os.path.getsize(path)
    url = DATA.format(code=code)
    for attempt in range(1, RETRIES + 1):
        try:
            with requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=300) as r:
                if r.status_code == 200:
                    tmp = f"{path}.{os.getpid()}.part"
                    n = 0
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(1 << 20):
                            f.write(chunk)
                            n += len(chunk)
                    if n > 0:
                        os.replace(tmp, path)
                        return "ok", n
                    return "empty", 0
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(2 * attempt)
                    continue
                return f"http{r.status_code}", 0
        except Exception:
            time.sleep(2 * attempt)
    return "fail", 0


def main():
    os.makedirs(DEST, exist_ok=True)
    codes = dataset_codes()
    log(f"Eurostat crawl: {len(codes):,} datasets, {WORKERS} workers -> {DEST}")
    ok = skip = fail = done = 0
    total = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_one, c): c for c in codes}
        for fut in as_completed(futs):
            code = futs[fut]
            try:
                status, n = fut.result()
            except Exception:
                status, n = "exc", 0
            if status == "ok":
                ok += 1
                total += n
            elif status == "skip":
                skip += 1
                total += n
            else:
                fail += 1
                log(f"  {code}: {status}")
            done += 1
            if done % 200 == 0:
                log(f"  {done:,}/{len(codes):,}  ok={ok} skip={skip} fail={fail}  ~{total/1e9:.2f} GB")
    log(f"DONE  ok={ok} skip={skip} fail={fail}  total~{total/1e9:.2f} GB")


if __name__ == "__main__":
    main()
