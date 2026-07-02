#!/usr/bin/env python3
"""Probe SNB cube API to understand data format."""
import requests

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}
JAX = {**UA, "Accept": "application/json"}

# 1. Get dimensions for devkum
r = requests.get("https://data.snb.ch/api/cube/devkum/dimensions/en", headers=JAX, timeout=15)
print(f"devkum dimensions: {r.status_code}")
import json
if r.status_code == 200:
    d = r.json()
    print(json.dumps(d, indent=2)[:2000])

# 2. Try to download CSV data (no dimSel)
r2 = requests.get("https://data.snb.ch/api/cube/devkum/data/csv/en", headers=UA, timeout=30)
print(f"\ndevkum CSV (no dimSel): {r2.status_code}, len={len(r2.content)}")
if r2.status_code == 200:
    print(r2.text[:600])

# 3. Try JSON data
r3 = requests.get("https://data.snb.ch/api/cube/devkum/data/json/en", headers=JAX, timeout=30)
print(f"\ndevkum JSON (no dimSel): {r3.status_code}, len={len(r3.content)}")
if r3.status_code == 200:
    d3 = r3.json()
    print(str(d3)[:500])

# 4. Try snbmonagg dimensions
r4 = requests.get("https://data.snb.ch/api/cube/snbmonagg/dimensions/en", headers=JAX, timeout=15)
print(f"\nsnbmonagg dimensions: {r4.status_code}")
if r4.status_code == 200:
    print(json.dumps(r4.json(), indent=2)[:1500])
