#!/usr/bin/env python3
"""Try to find SNB cube catalog by probing with Accept: application/json."""
import requests, json

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}
JAX = {**UA, "Accept": "application/json"}

# The 404 for DEVKUM/dimensions came back as JSON — so the real API IS there
# Try catalog endpoints with JSON accept
tests = [
    "https://data.snb.ch/api/cube/list/en",
    "https://data.snb.ch/api/cube/catalog/en",
    "https://data.snb.ch/api/series/list/en",
    "https://data.snb.ch/api/catalog/en",
    "https://data.snb.ch/api/en/cube",
    "https://data.snb.ch/api/en/series",
    # Try known SNB cube IDs
    "https://data.snb.ch/api/cube/DEVKUM/dimensions/en",
    "https://data.snb.ch/api/cube/ZIMSNB/dimensions/en",
    "https://data.snb.ch/api/cube/GELDM/dimensions/en",
    "https://data.snb.ch/api/cube/SNBDKK/dimensions/en",
    "https://data.snb.ch/api/cube/ZINSNB/dimensions/en",
    "https://data.snb.ch/api/cube/WECHKURSE/dimensions/en",
    "https://data.snb.ch/api/cube/DEVISENKURSE/dimensions/en",
    "https://data.snb.ch/api/cube/table/list/en",
    "https://data.snb.ch/api/table/list/en",
]

for url in tests:
    try:
        r = requests.get(url, headers=JAX, timeout=10)
        ct = r.headers.get("content-type", "")
        body = r.text[:200] if r.status_code in [200, 400, 404] else ""
        is_json = "json" in ct
        print(f"{r.status_code} {'JSON' if is_json else 'HTML':4s} {url[-60:]}")
        if is_json:
            try:
                d = r.json()
                if isinstance(d, list):
                    print(f"  list[{len(d)}]: {str(d[0])[:120]}")
                else:
                    print(f"  {str(d)[:200]}")
            except:
                print(f"  raw: {body[:150]}")
    except Exception as e:
        print(f"ERR: {e}  {url[-55:]}")
