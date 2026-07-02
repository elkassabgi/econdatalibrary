#!/usr/bin/env python3
"""Test the efotw.org download URL."""
import requests

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}

url = "https://efotw.org/economic-freedom/dataset?type=excel"
try:
    r = requests.get(url, headers=UA, timeout=60, stream=True)
    ct = r.headers.get("content-type", "")
    cd = r.headers.get("content-disposition", "")
    cl = r.headers.get("content-length", "?")
    print(f"status: {r.status_code}")
    print(f"content-type: {ct}")
    print(f"content-disposition: {cd}")
    print(f"content-length: {cl}")
    # Read first 1KB
    chunk = next(r.iter_content(1024), b"")
    print(f"first bytes hex: {chunk[:16].hex()}")
    print(f"first bytes: {repr(chunk[:20])}")
    # Excel XLSX starts with PK\x03\x04 (zip magic)
    if chunk[:4] == b"PK\x03\x04":
        print("  => This IS an XLSX file!")
    else:
        print("  => Not an XLSX zip file")
        print(f"  => HTML preview: {chunk[:200]}")
except Exception as e:
    print(f"ERR: {e}")

# Try a few more possible direct URLs
print("\n--- More direct URL tries ---")
for url2 in [
    "https://efotw.org/economic-freedom/download-data",
    "https://efotw.org/economic-freedom/dataset-download",
    "https://efotw.org/sites/default/files/2024-09/economic-freedom-of-the-world-2024-dataset.xlsx",
    "https://efotw.org/sites/default/files/2023-09/economic-freedom-of-the-world-2023-dataset.xlsx",
]:
    try:
        resp = requests.head(url2, headers=UA, timeout=10, allow_redirects=True)
        ct2 = resp.headers.get("content-type","")
        print(f"{resp.status_code} {resp.url[-80:]}  ct={ct2[:30]}")
    except Exception as e:
        print(f"ERR: {e}")
