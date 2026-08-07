import requests, json

BASE = "https://andmed.stat.ee/api/v1/en/stat"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

path = "keskkond/pollumajanduskeskkond/KK208.PX"

# Try different URL patterns
urls = [
    f"{BASE}/{path}",
    f"https://andmed.stat.ee/api/v1/en/stat/{path}",
    f"https://andmed.stat.ee/api/v1/en/table/{path}",
    f"https://andmed.stat.ee/api/v1/stat/{path}",
]

for url in urls[:2]:
    r = requests.get(url, headers=UA, timeout=30)
    print(f"GET {url[-60:]}: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(json.dumps(data, indent=2)[:1000])
        break
    else:
        print(f"  Response: {r.text[:200]}")