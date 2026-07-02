#!/usr/bin/env python3
import requests, re

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}
r = requests.get("https://efotw.org/economic-freedom/dataset", headers=UA, timeout=30)
print(f"status: {r.status_code}, len: {len(r.content)}")

# Find xlsx links
for m in re.findall(r'href=["\'](https?://[^"\']+\.xlsx[^"\']*)["\']', r.text, re.I)[:10]:
    print(f"XLSX: {m}")
# Find download/file links
for m in re.findall(r'href=["\'](https?://[^"\']*(?:download|dataset|efw|file|excel)[^"\']*)["\']', r.text, re.I)[:15]:
    print(f"DL: {m}")
# Also check for form actions
for m in re.findall(r'action=["\'](https?://[^"\']+)["\']', r.text, re.I)[:5]:
    print(f"FORM: {m}")

# Try downloading EFW data directly from fraserinstitute
urls_to_try = [
    "https://www.fraserinstitute.org/sites/default/files/economic-freedom-of-the-world-2024-dataset.xlsx",
    "https://www.fraserinstitute.org/sites/default/files/economic-freedom-of-the-world-2023-dataset.xlsx",
    "https://efotw.org/sites/default/files/economic-freedom-of-the-world-2024-dataset.xlsx",
    "https://efotw.org/sites/default/files/efw2024.xlsx",
]
for url in urls_to_try:
    try:
        resp = requests.head(url, headers=UA, timeout=15, allow_redirects=True)
        print(f"HEAD {resp.status_code} {len(resp.content)} {url[-60:]}")
    except Exception as e:
        print(f"ERR: {e}")
