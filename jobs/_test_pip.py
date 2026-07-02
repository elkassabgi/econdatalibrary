import requests
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
try:
    r = requests.get(
        "https://api.worldbank.org/pip/v1/pip",
        params={"country": "CHN", "year": "2019", "povline": 2.15,
                "fill_gaps": "false", "format": "json"},
        headers=UA, timeout=20
    )
    print(f"Status: {r.status_code} Size: {len(r.content)}")
    if r.status_code == 200:
        print(r.text[:300])
except Exception as e:
    print(f"ERR: {e}")
