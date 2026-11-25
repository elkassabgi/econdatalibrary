#!/usr/bin/env python3
"""UNDP Human Development Report (HDR) composite indices - 190+ countries.

License: CC BY 3.0 IGO
Source: https://hdr.undp.org/data-center/documentation-and-downloads
No API key required (direct CSV download from hdr.undp.org).

This module holds the ONE composite-CSV parser (ingest_composite_csv), which the fetcher
updater/strategies/fetchers/undp_hdr.py imports.

Run: python jobs/ingest_undp_hdr.py
  COLD START (and any manual re-run) goes THROUGH THE FETCHER: main() calls the fetcher's own update(), so it
  uses the same edition discovery, the same refusals and the same merge. The old main() walked a hard-coded
  2023-24 list (the HDR23-24 composite CSV, a 2024 annex zip, an HDI trends XLSX and five 2023-24 annex XLSX
  files) and wrote whatever answered, so after HDR25 a deleted store came back as an older edition. That list
  and the annex-XLSX parser it fed were removed on 2026-11-25.
"""
from __future__ import annotations
import datetime as dt, io, os, re, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# The HDI rank column is a label, not a series: "hdi_rank_2023" in HDR25, "hdi_rank_2022" in HDR23-24, and
# whatever year the next edition stamps. Matched by pattern, so a new edition's rank column is never minted as
# ~200 new "hdi_rank:<ISO>" series (R22). Only hdi_rank: gii_rank_<Y> and gdi_group_<Y> stay series, as before.
_HDI_RANK_COL = re.compile(r"^hdi_rank_\d{4}$")


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def ingest_composite_csv(content: bytes, all_keys, all_dates, all_vals, strict: bool = False,
                         max_year: int | None = None) -> int:
    """Parse the composite time-series CSV.

    Supports two layouts:
    1. Long format:  iso3, year, indicator_columns...
    2. Wide format:  iso3, indicator_1990, indicator_1991, ... (col names like 'hdi_2022')

    strict=True re-raises a parse error after logging it (the fetcher names it as a structural break); the
    default keeps the old behaviour of logging it and returning the count so far.

    max_year bounds a wide column's year from above. It was a fixed 2030, a date that would silently drop every
    column from 2031 on (R159). The fetcher passes its run year + 1 (HDR data ends two years before its edition,
    so no real column is later); None takes the UTC year now + 1.
    """
    import csv
    if max_year is None:
        max_year = dt.datetime.now(dt.timezone.utc).year + 1
    count = 0
    try:
        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        headers = reader.fieldnames or []
        log(f"  CSV headers ({len(headers)}): {headers[:15]}")

        iso_col = next((h for h in headers if h.lower() in ("iso3", "iso_code", "country_code", "code")), None)
        yr_col  = next((h for h in headers if h.lower() == "year"), None)
        skip_meta = {"country", "country_name", "hdicode", "region", "hdi_rank", iso_col, yr_col}
        skip_meta |= {h for h in headers if h and _HDI_RANK_COL.match(h)}

        if yr_col:
            # Long format: one row per country/year
            val_cols = [h for h in headers if h not in skip_meta and h]
            log(f"  Long format: iso_col={iso_col}, yr_col={yr_col}, {len(val_cols)} value cols")
            for row in reader:
                iso = str(row.get(iso_col, "")).strip()[:20] if iso_col else "WLD"
                if not iso: continue
                try:
                    yr = int(float(str(row.get(yr_col, "")).strip()))
                    obs_date = dt.date(yr, 12, 31)
                except (ValueError, TypeError):
                    continue
                for col in val_cols:
                    v_raw = row.get(col, "")
                    if not v_raw or str(v_raw).strip() in ("", "..", "NA", "n.a.", "N/A"):
                        continue
                    try:
                        v = float(str(v_raw).replace(",", ""))
                        if v != v: continue
                    except (ValueError, TypeError):
                        continue
                    all_keys.append(f"{col.lower().strip()}:{iso}")
                    all_dates.append(obs_date)
                    all_vals.append(v)
                    count += 1
        else:
            # Wide format: columns named like 'hdi_1990', 'gni_per_capita_2022'
            # Split on last underscore to extract year
            wide_cols = []  # (col_name, series_name, year)
            for h in headers:
                if h in skip_meta or not h:
                    continue
                m = re.search(r"_(\d{4})$", h)
                if m:
                    yr = int(m.group(1))
                    if 1980 <= yr <= max_year:
                        series = h[:m.start()].lower().strip()
                        wide_cols.append((h, series, yr))
            log(f"  Wide format: {len(wide_cols)} year-indicator columns")
            for row in reader:
                iso = str(row.get(iso_col, "")).strip()[:20] if iso_col else "WLD"
                if not iso: continue
                for col, series, yr in wide_cols:
                    v_raw = row.get(col, "")
                    if not v_raw or str(v_raw).strip() in ("", "..", "NA", "n.a.", "N/A"):
                        continue
                    try:
                        v = float(str(v_raw).replace(",", ""))
                        if v != v: continue
                    except (ValueError, TypeError):
                        continue
                    all_keys.append(f"{series}:{iso}")
                    all_dates.append(dt.date(yr, 12, 31))
                    all_vals.append(v)
                    count += 1

    except Exception as e:
        log(f"  CSV parse error: {e}")
        if strict:
            raise
        import traceback; traceback.print_exc()
    return count


def main():
    """Cold start or manual re-run, through the fetcher (see the module docstring)."""
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from updater.strategies.fetchers import undp_hdr   # noqa: PLC0415
    res = undp_hdr.update(None, None)
    log(f"undp_hdr: {res.status}: {res.error}")
    return res


if __name__ == "__main__":
    main()
