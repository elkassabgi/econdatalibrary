import requests, json

BASE = "https://andmed.stat.ee/api/v1/en/stat"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Load catalog
import os
with open(r"D:/research/econfindatalibrary/data/clean_full/stat_estonia/_catalog.json") as f:
    tables = json.load(f)

print(f"Catalog: {len(tables)} tables")
print(f"First 3: {tables[:3]}")

# Try to get metadata for the first table
t = tables[0]
meta_url = f"{BASE}/{t['path']}/metadata"
r = requests.get(meta_url, headers=UA, timeout=30)
print(f"GET {meta_url}: {r.status_code}")
if r.status_code == 200:
    meta = r.json()
    print(json.dumps(meta, indent=2)[:2000])