#!/usr/bin/env python3
"""Probe SNB API to find working endpoints."""
import requests, json

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}

# First, see what the home page HTML says
r = requests.get("https://data.snb.ch/", headers=UA, timeout=15)
print("HOME HTML snippet:")
print(r.text[:600])
print("---")

# Try fetching the actual API catalog with curl-like Accept header
for url in [
    "https://data.snb.ch/api/series/catalog/en",
    "https://data.snb.ch/api/cube/DEVKUM/data/monthly/2020-01/2020-12/en",  # known series
    "https://data.snb.ch/api/cube/DEVKUM/dimensions/en",
    "https://data.snb.ch/api/en/cube/DEVKUM/data",
]:
    h = {**UA, "Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
    try:
        r = requests.get(url, headers=h, timeout=10)
        ct = r.headers.get("content-type", "")
        print(f"{r.status_code} {url}  ct={ct[:30]}")
        if "json" in ct:
            d = r.json()
            print(f"  JSON: {str(d)[:200]}")
    except Exception as e:
        print(f"ERR {e}  {url}")

# Check SNB SDMX
for url in [
    "https://www.snb.ch/sdmx/2.1/dataflow",
    "https://www.snb.ch/sdmx/v2.1/dataflow/SNB",
    "https://data.snb.ch/sdmx",
    "https://stats.snb.ch/",
]:
    try:
        r = requests.get(url, headers=UA, timeout=10)
        ct = r.headers.get("content-type", "")
        print(f"{r.status_code} {url}  ct={ct[:25]}  len={len(r.content)}")
    except Exception as e:
        print(f"ERR {e}  {url}")
