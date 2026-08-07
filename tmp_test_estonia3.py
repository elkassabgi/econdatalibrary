import requests, json

BASE = "https://andmed.stat.ee/api/v1/en/stat"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
path = "keskkond/pollumajanduskeskkond/KK208.PX"

# Test GET with and without trailing slash
for url in [f"{BASE}/{path}/", f"{BASE}/{path}"]:
    r = requests.get(url, headers=UA, timeout=30)
    print(f"GET {url[-50:]}: {r.status_code}")
    if r.status_code == 200:
        meta = r.json()
        print(f"  Keys: {list(meta.keys())}")
        print(f"  vars: {[v['code'] for v in meta.get('variables',[])]}")
        
# Test POST with no-slash URL (correct body format)
url = f"{BASE}/{path}"
meta = requests.get(url, headers=UA, timeout=30).json()
variables = meta.get("variables", [])
print(f"\nVariables: {[v['code'] for v in variables]}")

body = {
    "query": [{"code": v["code"], "selection": {"filter":"item","values": v["values"][:3]}} for v in variables],
    "response": {"format": "json-stat2"}
}
r2 = requests.post(url, json=body, headers=UA, timeout=60)
print(f"POST {url[-50:]}: {r2.status_code}")
if r2.status_code == 200:
    data = r2.json()
    print(f"  JSON keys: {list(data.keys())}")
    print(f"  id: {data.get('id')}, size: {data.get('size')}, values[:5]: {data.get('value',[])[:5]}")
else:
    print(f"  Error: {r2.text[:200]}")