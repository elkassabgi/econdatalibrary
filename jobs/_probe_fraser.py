#!/usr/bin/env python3
import requests, re

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}
r = requests.get("https://www.fraserinstitute.org/economic-freedom/dataset", headers=UA, timeout=30)
print(f"status: {r.status_code}, len: {len(r.content)}")

# Find xlsx links
matches = re.findall(r'href=["\']([^"\']+\.xlsx[^"\']*)["\']', r.text, re.I)
for m in matches[:10]:
    print(f"XLSX: {m}")

# Find download links
matches2 = re.findall(r'href=["\']([^"\']+)["\']', r.text, re.I)
download_links = [m for m in matches2 if any(x in m.lower() for x in ["download", "dataset", "efw", "excel"])]
for m in download_links[:15]:
    print(f"DL: {m}")
