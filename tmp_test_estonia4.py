import requests, json, datetime as dt, re

BASE = "https://andmed.stat.ee/api/v1/en/stat"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
path = "keskkond/pollumajanduskeskkond/KK208.PX"

# Get data
url = f"{BASE}/{path}"
meta = requests.get(url, headers=UA, timeout=30).json()
variables = meta.get("variables", [])

body = {
    "query": [{"code": v["code"], "selection": {"filter":"item","values": v["values"][:3]}} for v in variables],
    "response": {"format": "json-stat2"}
}
r = requests.post(url, json=body, headers=UA, timeout=60)
data = r.json()

print("=== JSON-stat2 dimension structure ===")
dims = data.get("dimension", {})
for did, dim_data in dims.items():
    cat = dim_data.get("category", {})
    cat_idx = cat.get("index", {})
    cat_label = cat.get("label", {})
    print(f"  {did}: index type={type(cat_idx).__name__}, len={len(cat_idx) if cat_idx else 0}")
    print(f"    index[:3]={list(cat_idx.items())[:3] if isinstance(cat_idx,dict) else list(cat_idx)[:3]}")

print("\n=== id and size ===")
print(f"id: {data.get('id')}")
print(f"size: {data.get('size')}")
print(f"values[:10]: {data.get('value',[])[:10]}")