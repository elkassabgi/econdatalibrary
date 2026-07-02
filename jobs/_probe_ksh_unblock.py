#!/usr/bin/env python3
"""Single gentle probe: prints CSV if KSH WAF block has cleared, WAF/ERR otherwise."""
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
}
try:
    r = requests.get("https://www.ksh.hu/stadat_files/nep/en/nep0001.csv", headers=HEADERS, timeout=45)
    head = r.content[:400].lstrip().lower()
    if r.status_code == 200 and not (head.startswith(b"<html") or head.startswith(b"<!doctype") or b"request rejected" in head):
        print("CSV")
    else:
        print("WAF")
except Exception:
    print("ERR")
