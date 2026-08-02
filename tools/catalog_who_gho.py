"""Catalogue WHO GHO series that the store publishes but the catalogue does not know.

WHY THIS EXISTS. Migrating who_rs/who_hwf/who_sdg off the banned DBnomics mirror onto WHO's
own API (updater/strategies/fetchers/_who_gho.py) did not just restore freshness — WHO serves
series the mirror never carried. Measured 2026-08-02: who_sdg returns 29,088 keys against
28,160 catalogued, i.e. 10,339 series that are FETCHED and STORED but invisible, because a
series nobody catalogued is a series nobody can find or download.

DIMENSION NAMES COME FROM THE DATA, NOT A GUESS. A title is
    {IndicatorName} - {Spatial} - {Dim1} - {Dim2} - {Dim3}
rendered with WHO's display names. The obvious approach — fetch /DIMENSION/REGION and
/DIMENSION/COUNTRY and hope — fails silently: SDG series carry spatial codes like "143"
(Central Asia) that are in NEITHER, and a missing lookup would quietly emit a title with a
raw code in it. Every GHO row states which dimension it used (SpatialDimType, DimNType), so
the needed dimension codes are COLLECTED FROM THE ROWS and only those are fetched. A code that
still has no display name is reported, never silently rendered raw.

Existing rows are never touched: this only INSERTs ids absent from the catalogue, and it
reuses the source's own license_id and metadata template so new rows are indistinguishable in
shape from the ones already there.

Usage:
    python tools/catalog_who_gho.py who_sdg [--dry-run] [--limit N]
"""
from __future__ import annotations
import argparse
import collections
import datetime as dt
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = "https://ghoapi.azureedge.net/api"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
PREFIX = {"who_rs": "WHO_RS", "who_hwf": "WHO_HWF", "who_sdg": "WHO_SDG"}


def get(url, tries=4):
    last = None
    for a in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=180) as f:
                return json.loads(f.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                return None
            last = f"HTTP {e.code}"
        except Exception as e:                      # noqa: BLE001
            last = f"{type(e).__name__}: {e}"
        time.sleep(min(2 ** a, 20))
    raise SystemExit(f"GHO GET failed after {tries} tries: {url}  ({last})")


def key_of(v, prefix):
    code, spatial = v.get("IndicatorCode"), v.get("SpatialDim")
    if not code or spatial is None:
        return None
    parts = [str(code), str(spatial)]
    for i in (1, 2, 3):
        dv = v.get(f"Dim{i}")
        if dv:
            parts.append(str(dv))
    return f"{prefix}:" + ".".join(parts) + ".A"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source", choices=sorted(PREFIX))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="cap indicators (smoke test only)")
    a = ap.parse_args()
    src, pref = a.source, PREFIX[a.source]

    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(ROOT, "data", "catalog.db")
    con = sqlite3.connect(cat)
    have = {r[0] for r in con.execute("SELECT series_id FROM series WHERE source_id=?", (src,))}
    if not have:
        raise SystemExit(f"{src}: no existing catalogue rows — refusing to invent a template")
    tmpl = con.execute("SELECT license_id, metadata FROM series WHERE source_id=? LIMIT 1",
                       (src,)).fetchone()
    license_id, metadata = tmpl
    print(f"{src}: {len(have):,} catalogued today | license_id={license_id}")

    inds = sorted({sid.split(f"{src}:{pref}:", 1)[1].split(".")[0]
                   for sid in have if f"{src}:{pref}:" in sid})
    if a.limit:
        inds = inds[:a.limit]
    print(f"  indicators: {len(inds)}")

    names = {d["IndicatorCode"]: d.get("IndicatorName") or d["IndicatorCode"]
             for d in get(f"{BASE}/Indicator")["value"]}

    rows_by_key = {}
    dim_needed = collections.defaultdict(set)      # dimension code -> value codes seen
    for n, code in enumerate(inds, 1):
        d = get(f"{BASE}/{urllib.parse.quote(code)}")
        vals = (d or {}).get("value") or []
        for v in vals:
            k = key_of(v, pref)
            yr = v.get("TimeDim")
            if not k or yr is None or v.get("NumericValue") is None:
                continue
            try:
                y = int(yr)
            except (TypeError, ValueError):
                continue
            rec = rows_by_key.get(k)
            if rec is None:
                rows_by_key[k] = rec = {"ind": v["IndicatorCode"], "lo": y, "hi": y, "dims": []}
                sdt = v.get("SpatialDimType")
                if sdt:
                    dim_needed[sdt].add(str(v.get("SpatialDim")))
                    rec["dims"].append((sdt, str(v.get("SpatialDim"))))
                for i in (1, 2, 3):
                    dv, dtp = v.get(f"Dim{i}"), v.get(f"Dim{i}Type")
                    if dv and dtp:
                        dim_needed[dtp].add(str(dv))
                        rec["dims"].append((dtp, str(dv)))
            else:
                rec["lo"] = min(rec["lo"], y)
                rec["hi"] = max(rec["hi"], y)
        print(f"    [{n}/{len(inds)}] {code}: {len(vals):,} rows", flush=True)
        time.sleep(0.2)

    # Display names ONLY for the dimensions the data actually used.
    disp = {}
    for dcode in sorted(dim_needed):
        d = get(f"{BASE}/DIMENSION/{urllib.parse.quote(dcode)}/DimensionValues")
        for x in (d or {}).get("value", []):
            disp[(dcode, str(x.get("Code")))] = x.get("Title") or str(x.get("Code"))
    missing = {(dc, vc) for dc, vcs in dim_needed.items() for vc in vcs if (dc, vc) not in disp}
    if missing:
        print(f"  WARNING: {len(missing)} dimension value(s) have no WHO display name; "
              f"their titles keep the raw code: {sorted(missing)[:6]}")

    new = [k for k in rows_by_key if f"{src}:{k}" not in have]
    print(f"\n  WHO keys {len(rows_by_key):,} | already catalogued "
          f"{len(rows_by_key) - len(new):,} | NEW {len(new):,}")
    if not new:
        print("  nothing to insert."); return 0

    ins = []
    for k in sorted(new):
        rec = rows_by_key[k]
        bits = [names.get(rec["ind"], rec["ind"])]
        bits += [disp.get(dv, dv[1]) for dv in rec["dims"]]
        ins.append((f"{src}:{k}", src, " - ".join(bits), None, None, None, None, license_id,
                    f"{rec['lo']}-01-01", f"{rec['hi']}-01-01", None, metadata))
    print("  sample new rows:")
    for r in ins[:3]:
        print(f"    {r[0]}\n      {r[2][:120]}   [{r[8]} .. {r[9]}]")
    if a.dry_run:
        print("\n  --dry-run: nothing written.")
        return 0

    con.executemany(
        "INSERT OR IGNORE INTO series (series_id, source_id, title, frequency, unit, geography,"
        " category, license_id, start_date, end_date, last_updated, metadata)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", ins)
    con.commit()
    now = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (src,)).fetchone()[0]
    print(f"\n  INSERTED {len(ins):,} — {src} catalogue {len(have):,} -> {now:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
