import requests
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
urls = [
    ("PWT11",    "https://dataverse.nl/api/access/datafile/554105"),
    ("SWIID_sum","https://raw.githubusercontent.com/fsolt/swiid/master/data/swiid_summary.csv"),
    ("SWIID_src","https://raw.githubusercontent.com/fsolt/swiid/master/data/swiid_source.csv"),
    ("MAD23",    "https://dataverse.nl/api/access/datafile/421302"),
    ("MAD23b",   "https://dataverse.nl/api/access/datafile/392395"),
]
for name, url in urls:
    try:
        r = requests.get(url, headers=UA, timeout=20, allow_redirects=True, stream=True)
        chunk = b""
        for c in r.iter_content(2048):
            chunk += c
            if len(chunk) >= 2048:
                break
        ct = r.headers.get("Content-Type", "?")
        print(f"{name}: {r.status_code} ct={ct[:30]} first={chunk[:60]}")
    except Exception as e:
        print(f"{name}: ERR {str(e)[:80]}")
