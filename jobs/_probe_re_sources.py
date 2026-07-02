#!/usr/bin/env python3
"""One-shot probe of KSH STADAT, GUS DBW, ADB KIDB endpoint shapes. Writes findings to tmp file."""
import json, time, io, csv
import requests

OUTF = open(r"D:\research\econfindatalibrary\tmp_probe_re.txt", "w", encoding="utf-8")
def w(*a):
    print(*a, file=OUTF, flush=True)

BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
}
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# ---------- 1. KSH ----------
w("=" * 30, "KSH toc.json")
try:
    r = requests.get("https://www.ksh.hu/stadat_files/toc.json", headers=BROWSER, timeout=60)
    w("status", r.status_code, "ctype", r.headers.get("content-type"))
    j = r.json()
    w("top-level type:", type(j).__name__)
    if isinstance(j, dict):
        w("keys:", list(j.keys())[:20])
        for k, v in j.items():
            if isinstance(v, list) and v:
                w(f"key '{k}' is list len {len(v)}; first 2 entries:")
                w(json.dumps(v[:2], ensure_ascii=False, indent=1)[:2000])
                break
    elif isinstance(j, list):
        w("len:", len(j))
        w(json.dumps(j[:3], ensure_ascii=False, indent=1)[:2500])
except Exception as e:
    w("ERR", repr(e))

time.sleep(0.6)
for tid, theme in (("gdp0001", "gdp"), ("ara0001", "ara")):
    w("=" * 30, f"KSH {tid}.csv")
    try:
        r = requests.get(f"https://www.ksh.hu/stadat_files/{theme}/en/{tid}.csv", headers=BROWSER, timeout=60)
        w("status", r.status_code, "ctype", r.headers.get("content-type"), "bytes", len(r.content))
        raw = r.content
        try:
            txt = raw.decode("utf-8-sig")
            w("decoded as utf-8-sig")
        except UnicodeDecodeError:
            txt = raw.decode("cp1250", errors="replace")
            w("decoded as cp1250")
        lines = txt.splitlines()
        w("n lines:", len(lines))
        for ln in lines[:14]:
            w("LINE>", repr(ln[:300]))
        w("...last 2 lines:")
        for ln in lines[-2:]:
            w("LINE>", repr(ln[:300]))
    except Exception as e:
        w("ERR", repr(e))
    time.sleep(0.6)

# ---------- 2. GUS DBW ----------
G = "https://api-dbw.stat.gov.pl/api/1.2.0"
w("=" * 30, "GUS area-area")
areas = []
try:
    r = requests.get(f"{G}/area/area-area?lang=en", headers=UA, timeout=60)
    w("status", r.status_code, "ctype", r.headers.get("content-type"))
    j = r.json()
    w("type:", type(j).__name__)
    if isinstance(j, dict):
        w("keys:", list(j.keys()))
        for k, v in j.items():
            if isinstance(v, list):
                areas = v
                break
    elif isinstance(j, list):
        areas = j
    w("n areas:", len(areas))
    w(json.dumps(areas[:4], ensure_ascii=False, indent=1)[:1800])
    # find candidate areas that look leaf-like / have variables flag
    flags = set()
    for a in areas[:200]:
        if isinstance(a, dict):
            flags.update(a.keys())
    w("union of area keys:", sorted(flags))
except Exception as e:
    w("ERR", repr(e))

time.sleep(1.2)
# pick an area with variables
cand = None
for a in areas:
    if isinstance(a, dict) and (a.get("czy-zmienne") is True or a.get("czyZmienne") is True or str(a.get("czy-zmienne")).lower() == "true"):
        cand = a
        break
if cand is None and areas:
    cand = areas[-1]
w("candidate area:", json.dumps(cand, ensure_ascii=False)[:400] if cand else None)

var = None
if cand:
    aid = cand.get("id")
    w("=" * 30, f"GUS area-variable id-obszaru={aid}")
    try:
        r = requests.get(f"{G}/area/area-variable?id-obszaru={aid}&lang=en", headers=UA, timeout=60)
        w("status", r.status_code)
        j = r.json()
        w("type:", type(j).__name__)
        lst = j if isinstance(j, list) else None
        if isinstance(j, dict):
            w("keys:", list(j.keys()))
            for k, v in j.items():
                if isinstance(v, list):
                    lst = v
                    break
        if lst:
            w("n vars:", len(lst))
            w(json.dumps(lst[:3], ensure_ascii=False, indent=1)[:1500])
            var = lst[0]
    except Exception as e:
        w("ERR", repr(e))

time.sleep(1.2)
meta = None
if var:
    vid = var.get("id")
    w("=" * 30, f"GUS variable-meta id-zmiennej={vid}")
    try:
        r = requests.get(f"{G}/variable/variable-meta?id-zmiennej={vid}&lang=en", headers=UA, timeout=60)
        w("status", r.status_code)
        meta = r.json()
        w(json.dumps(meta, ensure_ascii=False, indent=1)[:3500])
    except Exception as e:
        w("ERR", repr(e))

time.sleep(1.2)
w("=" * 30, "GUS periods-dictionary")
try:
    r = requests.get(f"{G}/dictionaries/periods-dictionary?lang=en", headers=UA, timeout=60)
    w("status", r.status_code)
    j = r.json()
    w("type:", type(j).__name__)
    if isinstance(j, dict):
        w("keys:", list(j.keys()))
    w(json.dumps(j, ensure_ascii=False, indent=1)[:2500])
except Exception as e:
    w("ERR", repr(e))

time.sleep(1.2)
# attempt a data call using whatever meta gave us
if var and meta is not None:
    vid = var.get("id")
    # hunt for section + period + year hints anywhere in meta
    sections, periods, years = set(), set(), set()
    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                kl = k.lower()
                if isinstance(v, (int, str)) and str(v).strip().isdigit():
                    if "przekroj" in kl: sections.add(int(v))
                    elif "okres" in kl and "typ" not in kl: periods.add(int(v))
                    elif kl in ("rok", "rok-poczatek", "rok-koniec", "rok-od", "rok-do"): years.add(int(v))
                walk(v)
        elif isinstance(o, list):
            for it in o: walk(it)
    walk(meta)
    w("meta-derived sections:", sorted(sections)[:10], "periods:", sorted(periods)[:15], "years:", sorted(years)[:10])
    sec = sorted(sections)[0] if sections else 2
    per = 282 if 282 in periods or not periods else sorted(periods)[0]
    yr = max(years) if years else 2022
    for page in (0, 1):
        url = f"{G}/variable/variable-data-section?id-zmienna={vid}&id-przekroj={sec}&id-rok={yr}&id-okres={per}&ile-na-stronie=5000&numer-strony={page}&lang=en"
        w("=" * 30, "GUS data", url)
        try:
            r = requests.get(url, headers=UA, timeout=60)
            w("status", r.status_code)
            j = r.json()
            if isinstance(j, dict):
                w("keys:", list(j.keys()))
                d = j.get("data")
                if isinstance(d, list):
                    w("n rows:", len(d))
                    w(json.dumps(d[:3], ensure_ascii=False, indent=1)[:1500])
            else:
                w(str(j)[:800])
        except Exception as e:
            w("ERR", repr(e))
        time.sleep(1.2)
        break  # only page 0 probe

# ---------- 3. ADB ----------
A = "https://kidb.adb.org/api"
w("=" * 30, "ADB dataflows XML")
try:
    r = requests.get(f"{A}/v4/sdmx/structure/dataflow/all/all/", headers=UA, timeout=120)
    w("status", r.status_code, "ctype", r.headers.get("content-type"), "bytes", len(r.content))
    txt = r.text
    w("first 900 chars:")
    w(txt[:900])
    import re as _re
    flows = _re.findall(r"<(?:[\w.]+:)?Dataflow\b[^>]*\bid=\"([^\"]+)\"[^>]*>", txt)
    agencies = _re.findall(r"<(?:[\w.]+:)?Dataflow\b[^>]*\bagencyID=\"([^\"]+)\"", txt)
    w("n Dataflow elems:", len(flows), "ids sample:", flows[:15])
    w("agencies:", sorted(set(agencies)))
except Exception as e:
    w("ERR", repr(e))

time.sleep(1.2)
w("=" * 30, "ADB indicators EO_NA")
for url in (f"{A}/dataflow/indicators/EO_NA", f"{A}/api/dataflow/indicators/EO_NA"):
    try:
        r = requests.get(url, headers=UA, timeout=120)
        w(url, "->", r.status_code, r.headers.get("content-type"), len(r.content))
        if r.status_code == 200:
            try:
                j = r.json()
                w("type:", type(j).__name__)
                if isinstance(j, dict):
                    w("keys:", list(j.keys())[:20])
                w(json.dumps(j, ensure_ascii=False)[:2200])
            except Exception as e2:
                w("not json:", r.text[:500])
            break
    except Exception as e:
        w("ERR", repr(e))
    time.sleep(1.2)

time.sleep(1.2)
w("=" * 30, "ADB data A.NGDP_XDC. (all economies)")
try:
    r = requests.get(f"{A}/v4/sdmx/data/ADB,EO_NA/A.NGDP_XDC.?format=sdmx-csv", headers=UA, timeout=180)
    w("status", r.status_code, "ctype", r.headers.get("content-type"), "bytes", len(r.content))
    lines = r.text.splitlines()
    w("n lines:", len(lines))
    for ln in lines[:5]:
        w("LINE>", ln[:300])
except Exception as e:
    w("ERR", repr(e))

OUTF.close()
print("probe done")
