#!/usr/bin/env python3
import requests, re, json

UA = {"User-Agent": "Mozilla/5.0 Econ-Fin Data Library admin@hfdatalibrary.com"}

# Check SNB SPA for JS files and API hints
r = requests.get("https://data.snb.ch/", headers=UA, timeout=15)
print(f"SNB home: {r.status_code}, len: {len(r.content)}")
scripts = re.findall(r'src=["\'](/[^"\']+\.js)["\']', r.text)
print(f"JS files: {scripts[:10]}")

# Look at one JS file for API calls
if scripts:
    js_url = "https://data.snb.ch" + scripts[0]
    jr = requests.get(js_url, headers=UA, timeout=15)
    print(f"JS {jr.status_code}: {js_url}")
    # Find API patterns
    for m in re.findall(r'"(/api/[^"]{5,60})"', jr.text)[:20]:
        print(f"  API route: {m}")

# Try some specific SNB API patterns
# SNB might use a different API version or base path
for url in [
    "https://data.snb.ch/api/v2/series/metadata/en",
    "https://data.snb.ch/api/en/series/metadata",
    "https://data.snb.ch/api/cube/metadata/en",
    "https://data.snb.ch/api/en/cubes",
    "https://data.snb.ch/api/cubes/en",
    "https://data.snb.ch/api/ts/list/en",
    "https://data.snb.ch/api/series/list/en?limit=5",
]:
    try:
        resp = requests.get(url, headers={**UA, "Accept": "application/json"}, timeout=10)
        ct = resp.headers.get("content-type", "")
        body = resp.text[:150] if resp.status_code == 200 else ""
        print(f"{resp.status_code} {url[-60:]} ct={ct[:25]}")
        if "json" in ct:
            print(f"  >> {body[:150]}")
    except Exception as e:
        print(f"ERR: {e}")
