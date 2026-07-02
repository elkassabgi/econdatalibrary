"""Title insee_bdm series from INSEE BDM's OFFICIAL series titles (verbatim).

The INSEE SDMX data response carries TITLE_EN (and TITLE_FR) on every <Series>
element, keyed by IDBANK. We fetch each dataflow with firstNObservations=1 (full
series list, tiny payload -> avoids the 413 that full-data fetches hit), parse
IDBANK -> TITLE_EN (fallback TITLE_FR), and write dist/titles/insee_bdm.json keyed
by the catalog id `insee_bdm:<idbank>`. Only idbanks that exist in catalog.db are
emitted. Labels are copied VERBATIM from INSEE — no fabrication.

    python core/title_insee_bdm.py
"""
from __future__ import annotations
import glob, html, json, os, re, sqlite3, time, urllib.request, urllib.error

_THIS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_THIS, ".."))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
DATADIR = os.path.join(ROOT, "data", "clean_full", "insee_bdm")
OUT = os.path.join(ROOT, "dist", "titles", "insee_bdm.json")
BASE = "https://api.insee.fr/series/BDM/V1"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126",
      "Accept": "application/xml"}
SERIES_RE = re.compile(r"<(?:\w+:)?Series\b([^>]*)>")
ATTR_RE = re.compile(r'(\w+)="([^"]*)"')


def fetch(url: str, tries: int = 5) -> str | None:
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(2 * (i + 1)); continue
            return f"HTTP {e.code}"
        except Exception:
            time.sleep(2 * (i + 1))
    return None


def main() -> None:
    conn = sqlite3.connect(CATALOG)
    valid = {r[0].split(":", 1)[1] for r in
             conn.execute("SELECT series_id FROM series WHERE source_id='insee_bdm'")}
    conn.close()
    print(f"catalog insee_bdm idbanks: {len(valid):,}")
    flows = sorted(os.path.splitext(os.path.basename(f))[0]
                   for f in glob.glob(os.path.join(DATADIR, "*.parquet")))
    print(f"dataflows to title: {len(flows)}")

    titles: dict[str, str] = {}
    failed = []
    for i, flow in enumerate(flows, 1):
        xml = fetch(f"{BASE}/data/{flow}?firstNObservations=1")
        if not xml or xml.startswith("HTTP"):
            failed.append((flow, xml or "no response"))
            print(f"  [{i}/{len(flows)}] {flow}: FAIL {xml}", flush=True)
            continue
        n = 0
        for m in SERIES_RE.finditer(xml):
            a = dict(ATTR_RE.findall(m.group(1)))
            idb = a.get("IDBANK", "")
            if not idb or idb not in valid:
                continue
            title = a.get("TITLE_EN") or a.get("TITLE_FR")
            if not title:
                continue
            titles[f"insee_bdm:{idb}"] = html.unescape(title).strip()
            n += 1
        print(f"  [{i}/{len(flows)}] {flow}: {n} titled", flush=True)

    json.dump(titles, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\nwrote {OUT}: {len(titles):,} / {len(valid):,} idbanks titled")
    if failed:
        print(f"failed dataflows ({len(failed)}): {[f for f,_ in failed]}")


if __name__ == "__main__":
    main()
