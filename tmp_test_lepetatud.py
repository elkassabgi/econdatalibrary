import requests, json

BASE = "https://andmed.stat.ee/api/v1/en/stat"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

with open(r"D:/research/econfindatalibrary/data/clean_full/stat_estonia/_catalog.json") as f:
    tables = json.load(f)

# Find Lepetatud tables
lep = [t for t in tables if t["path"].startswith("Lepetatud")]
print(f"Lepetatud tables: {len(lep)}")
print(f"First 3: {lep[:3]}")

# Test one
path = lep[0]["path"]
url = f"{BASE}/{path}"
r = requests.get(url, headers=UA, timeout=30)
if r.status_code == 200:
    meta = r.json()
    vars = meta.get("variables", [])
    print(f"\n{path}: {len(vars)} variables")
    for v in vars[:3]:
        print(f"  {v['code']}: {len(v.get('values',[]))} values, first={v.get('values',[])[:2]}, time={v.get('time')}")
    
    # Try POST
    body = {"query": [{"code": v["code"], "selection": {"filter":"item","values": v["values"][:5]}} for v in vars], 
            "response": {"format": "json-stat2"}}
    r2 = requests.post(url, json=body, headers=UA, timeout=30)
    if r2.status_code == 200:
        data = r2.json()
        vals = data.get("value", [])
        print(f"  POST values: {vals[:5]}, n={len(vals)}")
else:
    print(f"GET {r.status_code}")