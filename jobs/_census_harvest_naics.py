"""Harvest the full NAICS code universe (the exports/naics endpoint 500s when
asked to enumerate all codes, so we iterate codes as predicates instead). We
collect codes from the WORKING imports/naics endpoint across several months and
years, then cache to data/raw/census/naics_codes.json."""
import json
import os
import time
import urllib.request

ROOT = r"D:/research/econfindatalibrary"
RAW = os.path.join(ROOT, "data", "raw", "census")
k = open(os.path.join(ROOT, ".env")).read().split("CENSUS_API_KEY=")[1].split()[0]
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def get(u, to=90):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=to)
        return json.loads(r.read())
    except Exception:
        return None


def main():
    codes = set()
    # sample a spread of months across the full 2010..now range to catch codes
    # that appear/disappear over time, from BOTH import and export-equivalent
    # NAICS classifications (imports/naics works; its code set == exports' set).
    months = []
    for y in range(2010, 2027):
        for m in ("01", "06", "12"):
            months.append(f"{y}-{m}")
    for mth in months:
        u = (f"https://api.census.gov/data/timeseries/intltrade/imports/naics"
             f"?get=NAICS&CTY_CODE=-&time={mth}&key={k}")
        j = get(u)
        if j and len(j) > 1:
            ni = j[0].index("NAICS")
            for row in j[1:]:
                v = row[ni]
                if v and v != "-":
                    codes.add(v)
        time.sleep(0.1)
    codes = sorted(codes)
    json.dump(codes, open(os.path.join(RAW, "naics_codes.json"), "w"))
    print(f"harvested {len(codes)} NAICS codes -> naics_codes.json", flush=True)
    print("sample:", codes[:10], "...", codes[-5:], flush=True)


if __name__ == "__main__":
    main()
