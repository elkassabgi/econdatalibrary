#!/usr/bin/env python3
"""Probe efotw.org for EFW data download."""
import requests, re

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}

# Try efotw.org dataset page
r = requests.get("https://efotw.org/economic-freedom/dataset", headers=UA, timeout=30)
print(f"efotw dataset page: {r.status_code}, len={len(r.content)}")

# Find any file links
all_links = re.findall(r'href=["\'](https?://[^"\']+|/[^"\']+)["\']', r.text)
# Filter for likely data files
for link in all_links:
    if any(x in link.lower() for x in [".xlsx", ".csv", ".zip", "download", "data", "freedom"]):
        print(f"  LINK: {link[:120]}")

# Also look for form actions
for m in re.findall(r'action=["\'](/[^"\']*|https?://[^"\']*)["\']', r.text):
    print(f"  FORM: {m[:120]}")

# Try direct URL patterns on efotw.org
print("\n--- HEAD checks ---")
for url in [
    "https://efotw.org/sites/default/files/economic-freedom-of-the-world-2024-dataset.xlsx",
    "https://efotw.org/sites/default/files/economic-freedom-of-the-world-2023-dataset.xlsx",
    "https://efotw.org/sites/default/files/efw_2024.xlsx",
    "https://efotw.org/economic-freedom/dataset?type=excel",
    "https://efotw.org/download",
    "https://efotw.org/sites/default/files/economic-freedom-of-world-2024-dataset.xlsx",
    "https://www.fraserinstitute.org/sites/default/files/economic_freedom_of_the_world_2024_dataset.xlsx",
    "https://www.fraserinstitute.org/sites/default/files/economic-freedom-of-the-world-annual-report-dataset-2024.xlsx",
]:
    try:
        resp = requests.head(url, headers=UA, timeout=10, allow_redirects=True)
        print(f"{resp.status_code} {resp.headers.get('content-length','?')} bytes  {url[-80:]}")
    except Exception as e:
        print(f"ERR: {e}")
