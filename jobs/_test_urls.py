import requests
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
tests = [
    ("SWIID CSV",  "https://raw.githubusercontent.com/fsolt/swiid/master/data/swiid9_5.csv"),
    ("SWIID ZIP",  "https://github.com/fsolt/swiid/releases/latest/download/swiid.zip"),
    ("WPB data",   "https://www.prisonstudies.org/sites/default/files/resources/downloads/wpb_stats_2024.xlsx"),
    ("PWT1001",    "https://www.rug.nl/ggdc/docs/pwt1001.xlsx"),
    ("Maddison23", "https://www.rug.nl/ggdc/historicaldevelopment/maddison/data/mpd2023.xlsx"),
    ("IMF WoRLD",  "https://data.imf.org/api/SDMX/WORLD/2.0/data/FISCAL_MONITOR/A.%40.FCSE_GDP/"),
    ("ADB knoema", "https://knoema.com/api/1.0/data/ADB_KI"),
]
for name, url in tests:
    try:
        r = requests.head(url, headers=UA, timeout=15, allow_redirects=True)
        cl = r.headers.get("content-length", "?")
        print(f"{name}: {r.status_code} {cl}b  {url[-60:]}")
    except Exception as e:
        print(f"{name}: ERR {str(e)[:80]}")
