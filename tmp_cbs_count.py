import requests
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
url = "https://odata4.cbs.nl/CBS/85477NED/Observations?$count=true&$top=1"
try:
    r = requests.get(url, headers=UA, timeout=30)
    if r.status_code == 200:
        data = r.json()
        total = data.get("@odata.count", "?")
        print(f"85477NED total obs: {total:,}")
    else:
        print(f"HTTP {r.status_code}")
except Exception as e:
    print(f"Error: {e}")