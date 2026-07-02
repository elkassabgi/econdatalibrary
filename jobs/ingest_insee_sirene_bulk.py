#!/usr/bin/env python3
"""INSEE SIRENE full bulk download from data.gouv.fr.

Downloads all 6 Parquet stock files published monthly on data.gouv.fr.
The public API caps at offset=10000; the bulk files have the complete 29M+ records.

Files (updated monthly, current as of 2026-06-01):
  StockUniteLegale          - all legal units (siren)         ~0.7 GB
  StockEtablissement        - all establishments (siret)      ~2.2 GB
  StockUniteLegaleHistorique  - historic unit periods          ~0.8 GB
  StockEtablissementHistorique - historic establishment periods ~0.9 GB
  StockEtablissementLiensSuccession - succession links         ~0.1 GB
  StockDoublons             - duplicate flags                  ~0.0 GB

License: Licence Ouverte / Open Licence 2.0 — commercial redistribution OK.
Attribution: Source: INSEE SIRENE (Licence Ouverte 2.0) www.insee.fr

Run: python jobs/ingest_insee_sirene_bulk.py
"""
import os, sys, time
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT = os.path.join(ROOT, "data", "clean_full", "insee_sirene")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

DATAGOUV_API = "https://www.data.gouv.fr/api/1/datasets/5b7ffc618b4c4169d30727e0/"


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def get_urls():
    """Fetch current Parquet URLs from data.gouv.fr API (gets latest monthly update)."""
    r = requests.get(DATAGOUV_API, headers=UA, timeout=30)
    r.raise_for_status()
    urls = {}
    for res in r.json().get("resources", []):
        if res.get("format", "").lower() != "parquet":
            continue
        title = res.get("title", "")
        url = res.get("url", "")
        sz = res.get("filesize", 0)
        # Extract stock name from title
        for name in ["StockUniteLegaleHistorique", "StockEtablissementHistorique",
                     "StockEtablissementLiensSuccession", "StockDoublons",
                     "StockEtablissement", "StockUniteLegale"]:
            if name.lower() in title.lower():
                if name not in urls:
                    urls[name] = (url, sz)
                break
    return urls


def download(name, url, size_bytes):
    out_path = os.path.join(OUT, f"{name}.parquet")
    if os.path.exists(out_path):
        n = 0
        try: n = pq.read_metadata(out_path).num_rows
        except Exception: pass
        if n > 0:
            log(f"{name}: already on disk ({n:,} rows)"); return n
    log(f"{name}: downloading {size_bytes/1e9:.1f} GB...")
    tmp = out_path + ".part"
    pos = os.path.getsize(tmp) if os.path.exists(tmp) else 0
    hdrs = {**UA}
    if pos:
        hdrs["Range"] = f"bytes={pos}-"
        log(f"  resuming at {pos/1e6:.0f} MB")
    for attempt in range(5):
        try:
            with requests.get(url, headers=hdrs, stream=True, timeout=600) as r:
                if r.status_code not in (200, 206):
                    log(f"  HTTP {r.status_code}"); return 0
                total = int(r.headers.get("Content-Length", 0)) + pos
                mode = "ab" if (pos and r.status_code == 206) else "wb"
                done = pos if mode == "ab" else 0
                last = time.time()
                with open(tmp, mode) as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk); done += len(chunk)
                        if time.time() - last > 15:
                            log(f"  {name}: {done/1e6:.0f}/{total/1e6:.0f} MB"); last = time.time()
            os.replace(tmp, out_path)
            break
        except Exception as e:
            log(f"  {name}: attempt {attempt+1} ERR: {e}"); time.sleep(10*(attempt+1))
    else:
        log(f"{name}: GAVE UP"); return 0

    n = pq.read_metadata(out_path).num_rows
    log(f"{name}: DONE {n:,} rows ({os.path.getsize(out_path)/1e9:.1f} GB Parquet)")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    log("Fetching current SIRENE bulk Parquet URLs from data.gouv.fr...")
    urls = get_urls()
    log(f"Found {len(urls)} Parquet stock files:")
    for name, (url, sz) in sorted(urls.items()):
        log(f"  {name}: {sz/1e9:.1f} GB")
    total = 0
    for name in ["StockUniteLegale", "StockEtablissement",
                 "StockUniteLegaleHistorique", "StockEtablissementHistorique",
                 "StockEtablissementLiensSuccession", "StockDoublons"]:
        if name in urls:
            total += download(name, *urls[name])
    log(f"GRAND TOTAL: {total:,} SIRENE records across all stock files")


if __name__ == "__main__":
    main()
