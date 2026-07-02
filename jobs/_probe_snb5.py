#!/usr/bin/env python3
"""Check SNB CSV format."""
import requests, io, csv

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}

r = requests.get("https://data.snb.ch/api/cube/devkum/data/csv/en", headers=UA, timeout=30)
print(f"devkum CSV: {r.status_code}, {len(r.content)} bytes")
print(f"Encoding: {r.encoding}")
# Show first 10 rows
text = r.content.decode("utf-8-sig")  # utf-8-sig strips BOM
lines = text.split("\n")
for i, line in enumerate(lines[:15]):
    print(f"  row {i}: {repr(line[:120])}")

# Also show last 3 rows
print("  ...")
for line in lines[-5:]:
    print(f"  tail: {repr(line[:120])}")
