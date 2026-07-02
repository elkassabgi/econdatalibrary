#!/usr/bin/env python3
"""Probe World Bank extra databases for indicator counts and API response."""
import requests, time

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://api.worldbank.org/v2"

SOURCES = [
    (12, "gfdd"),
    (14, "gender"),
    (15, "gem"),
    (29, "edstats"),
    (37, "poverty"),
    (40, "jobs"),
    (41, "doing_biz"),
]

for sid, name in SOURCES:
    url = f"{BASE}/source/{sid}/indicator"
    try:
        r = requests.get(url, params={"format": "json", "per_page": 1, "page": 1},
                         headers=UA, timeout=30)
        if r.status_code == 200:
            j = r.json()
            if len(j) >= 1:
                total = j[0].get("total", 0)
                pages = j[0].get("pages", 0)
                print(f"  {name} (source {sid}): {total} indicators, {pages} pages")
            else:
                print(f"  {name}: unexpected response: {str(r.text)[:100]}")
        else:
            print(f"  {name}: HTTP {r.status_code}")
    except Exception as e:
        print(f"  {name}: ERR {e}")
    time.sleep(0.5)

# Also test fetching one gfdd indicator to see if it works
print("\nTesting gfdd indicator fetch...")
r = requests.get(f"{BASE}/source/12/indicator",
                 params={"format": "json", "per_page": 5, "page": 1}, headers=UA, timeout=30)
if r.status_code == 200:
    j = r.json()
    if len(j) >= 2:
        inds = j[1][:3]
        for ind in inds:
            print(f"  Sample indicator: {ind.get('id')}")
        # Try fetching data for first indicator
        if inds:
            test_ind = inds[0]["id"]
            r2 = requests.get(f"{BASE}/country/all/indicator/{test_ind}",
                              params={"format": "json", "per_page": 5}, headers=UA, timeout=30)
            print(f"  Data fetch {test_ind}: {r2.status_code}, {str(r2.text)[:200]}")
