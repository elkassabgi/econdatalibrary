import requests, json

BASE = "https://andmed.stat.ee/api/v1/en/stat"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
path = "keskkond/pollumajanduskeskkond/KK208.PX"

# Get metadata 
url = f"{BASE}/{path}/"
meta = requests.get(url, headers=UA, timeout=30).json()
variables = meta.get("variables", [])
body = {
    "query": [{"code": v["code"], "selection": {"filter":"item","values": v["values"][:3]}} for v in variables],
    "response": {"format": "json-stat2"}
}

# POST with trailing slash
r = requests.post(url, json=body, headers=UA, timeout=60)
print(f"POST with slash {url[-50:]}: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    vals = data.get("value", [])
    print(f"  values: {vals[:5]}, n_vals={len(vals)}")
else:
    print(f"  Error: {r.text[:200]}")
    
# POST without trailing slash
url2 = url.rstrip("/")
r2 = requests.post(url2, json=body, headers=UA, timeout=60)
print(f"POST no slash {url2[-50:]}: {r2.status_code}")
if r2.status_code == 200:
    data2 = r2.json()
    vals2 = data2.get("value", [])
    print(f"  values: {vals2[:5]}, n_vals={len(vals2)}")