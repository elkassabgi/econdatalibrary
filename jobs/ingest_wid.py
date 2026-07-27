#!/usr/bin/env python3
"""World Inequality Database (WID) — income & wealth inequality, 200+ countries, 1820–present.

License: NOT CC BY 4.0 — that line was here for months and was never verified.
  WID publishes no data-reuse licence: wid.world/data/ and the homepage carry no CC
  string, no creativecommons.org link and no licence text (checked RENDERED, not just
  fetched — both are JS shells), and wid.world/terms/ 404s.
  What we actually hold is a written grant, 2026-07-27, from info@wid.world: "Yes, you
  can use the data for educational purpose", with a CC variant supplied only as an
  IMAGE and therefore still unconfirmed in text. Conditions attached: keep the data
  current with their releases and follow their methodological notes
  (https://wid.world/methodology/#library-methodological-notes).
  Full thread: REDISTRIBUTION_EMAIL_TRAIL.md.
  STATUS: GATED. Not in the updater registry, 0 catalog rows, nothing served. Do not
  record a license_id or publish anything until the CC variant is confirmed in text —
  NC alone is fine for us, NC-SA would bind our own catalogue metadata.
Source: https://wid.world/data/
No API key required.

Coverage:
  * Pre-tax and post-tax income shares (top 1%, top 10%, bottom 50%)
  * Wealth shares and averages
  * National income, GDP, population
  * Gini coefficients
  * All countries + US states + German states + regions
  * ~1820-present (varies by country)

Bulk download: https://wid.world/bulk_download/WID_data_{ISO}.csv
Format: country;variable;percentile;year;value;age;pop

Run: python jobs/ingest_wid.py
"""
from __future__ import annotations
import datetime as dt, os, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "wid")
BASE = "https://wid.world/bulk_download"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3

# All available WID country/region codes (from zip file listing)
COUNTRY_CODES = [
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AN", "AO", "AR", "AS", "AT", "AU",
    "AW", "AZ", "Al", "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL",
    "BM", "BN", "BO", "BQ", "BR", "BS", "BT", "BW", "BY", "BZ",
    "CA", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN", "CN-RU", "CN-UR",
    "CO", "CR", "CS", "CU", "CV", "CW", "CY", "CZ",
    "DD", "DE", "DE-BD", "DE-BY", "DE-HB", "DE-HE", "DE-HH", "DE-PR", "DE-SN", "DE-WU",
    "DJ", "DK", "DM", "DO", "DZ",
    "EC", "EE", "EG", "EH", "ER", "ES", "ET",
    "FI", "FJ", "FK", "FM", "FO", "FR",
    "GA", "GB", "GD", "GE", "GG", "GH", "GI", "GL", "GM", "GN", "GQ", "GR", "GT",
    "GU", "GW", "GY",
    "HK", "HN", "HR", "HT", "HU",
    "ID", "IE", "IL", "IM", "IN", "IQ", "IR", "IS", "IT",
    "JE", "JM", "JO", "JP",
    "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KS", "KW", "KY", "KZ",
    "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY",
    "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP",
    "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ",
    "NA", "NC", "NE", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ",
    "OA", "OA-MER", "OB", "OB-MER", "OC", "OC-MER", "OD", "OD-MER",
    "OE", "OE-MER", "OH", "OH-MER", "OI", "OI-MER", "OJ", "OJ-MER",
    "OK", "OK-MER", "OL", "OL-MER", "OM", "ON", "ON-MER", "OO", "OO-MER",
    "OP", "OP-MER", "OQ", "OQ-MER",
    "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PR", "PS", "PT", "PW", "PY",
    "QA", "QB", "QB-MER", "QC", "QC-MER", "QD", "QD-MER", "QE", "QE-MER",
    "QF", "QF-MER", "QG", "QG-MER", "QH", "QH-MER", "QI", "QI-MER",
    "QJ", "QJ-MER", "QK", "QK-MER", "QL", "QL-MER", "QM", "QM-MER",
    "QN", "QN-MER", "QO", "QO-MER", "QP", "QP-MER", "QQ", "QQ-MER",
    "QR", "QR-MER", "QS", "QS-MER", "QT", "QT-MER", "QU", "QU-MER",
    "QV", "QV-MER", "QW", "QW-MER", "QX", "QX-MER", "QY", "QY-MER",
    "RO", "RS", "RU", "RW",
    "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SK", "SL", "SM", "SN",
    "SO", "SR", "SS", "ST", "SU", "SV", "SW", "SX", "SY", "SZ",
    "TC", "TD", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TR", "TT",
    "TV", "TW", "TZ",
    "UA", "UG",
    "US", "US-AK", "US-AL", "US-AR", "US-AZ", "US-CA", "US-CO", "US-CT",
    "US-DC", "US-DE", "US-FL", "US-GA", "US-HI", "US-IA", "US-ID", "US-IL",
    "US-IN", "US-KS", "US-KY", "US-LA", "US-MA", "US-MD", "US-ME", "US-MI",
    "US-MN", "US-MO", "US-MS", "US-MT", "US-NC", "US-ND", "US-NE", "US-NH",
    "US-NJ", "US-NM", "US-NV", "US-NY", "US-OH", "US-OK", "US-OR", "US-PA",
    "US-RI", "US-SC", "US-SD", "US-TN", "US-TX", "US-UT", "US-VA", "US-VT",
    "US-WA", "US-WI", "US-WV", "US-WY",
    "UY", "UZ",
    "VA", "VC", "VE", "VG", "VI", "VN", "VU",
    "WF", "WO", "WO-MER", "WS",
    "XA", "XA-MER", "XB", "XB-MER", "XC", "XE", "XF", "XF-MER",
    "XI", "XK", "XL", "XL-MER", "XM", "XM-MER", "XN", "XN-MER",
    "XQ", "XQ-MER", "XR", "XR-MER", "XS", "XS-MER",
    "YE", "YU",
    "ZA", "ZM", "ZW", "ZZ",
    # PPP variants
    "OA-PPP", "OB-PPP", "OC-PPP", "OD-PPP", "OE-PPP", "OH-PPP", "OI-PPP",
    "OJ-PPP", "OK-PPP", "OL-PPP", "QE-PPP", "QF-PPP", "QL-PPP", "QM-PPP",
    "QP-PPP", "WO-PPP", "XB-PPP", "XF-PPP", "XL-PPP", "XN-PPP", "XR-PPP", "XS-PPP",
]

LOG_FILE = os.path.join(OUT, "_wid_log.txt")


def log(m):
    msg = f"[{time.strftime('%H:%M:%S')}] {m}"
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("ascii", "replace").decode(), flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


def fetch_country(code: str) -> list[tuple[str, dt.date, float]]:
    """Download and parse WID CSV for one country/region."""
    url = f"{BASE}/WID_data_{code}.csv"
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 404:
                return []
            if r.status_code != 200:
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
                    continue
                return []
            # Parse CSV: country;variable;percentile;year;value;age;pop
            results = []
            lines = r.text.split("\n")
            for line in lines[1:]:  # skip header
                line = line.strip()
                if not line:
                    continue
                parts = line.split(";")
                if len(parts) < 5:
                    continue
                try:
                    _, variable, percentile, yr_str, val_str = parts[0], parts[1], parts[2], parts[3], parts[4]
                    age = parts[5] if len(parts) > 5 else ""
                    pop = parts[6] if len(parts) > 6 else ""
                    yr = int(yr_str)
                    v = float(val_str)
                    d = dt.date(yr, 12, 31)
                    # Series key: WID:{variable}:{percentile}:{age}:{pop}:{country}
                    key = f"WID:{variable}:{percentile}:{age}:{pop}:{code}"
                    results.append((key, d, v))
                except (ValueError, TypeError):
                    continue
            return results
        except Exception as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                log(f"  ERR {code}: {e}")
    return []


def main():
    """Write one parquet per country to avoid OOM on 150M+ row dataset."""
    import re as _re
    os.makedirs(OUT, exist_ok=True)

    # Done set: check existing per-country parquet files first
    done: set[str] = set()
    existing = [f for f in os.listdir(OUT) if f.endswith(".parquet") and f != "wid.parquet"]
    for fname in existing:
        done.add(fname[:-8])  # strip ".parquet"

    # If no per-country files yet, parse log to find countries already saved in legacy wid.parquet
    # (Avoids loading 100M+ rows of legacy parquet into RAM which causes OOM)
    if not existing and os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, encoding="utf-8") as f:
                log_text = f.read()
            # Lines like: "  [280/423] SN: 428,179 obs, total 106,064,359"
            # Also lines like: "  Checkpoint: 95,888,122 rows" — use last checkpoint row count
            matches = _re.findall(r"\[(\d+)/\d+\] ([A-Z][A-Z0-9\-]+):", log_text)
            last_checkpoint_idx = 0
            for m in _re.finditer(r"Checkpoint: ([\d,]+) rows", log_text):
                last_checkpoint_idx = int(m.group(1).replace(",", ""))
            # Find the iteration index of the last checkpoint save (every 50 countries)
            # The checkpoint at i=250 saves countries 1-250
            checkpoint_counts = [int(x) for x in _re.findall(r"Checkpoint: ([\d,]+) rows", log_text.replace(",", ""))]
            if checkpoint_counts:
                # Find which country index corresponds to the last checkpoint
                # checkpoint fires at i%50==0, so last checkpoint = floor(max_i / 50) * 50
                all_indices = [int(m[0]) for m in matches]
                last_ckpt_i = max((i for i in all_indices if i % 50 == 0), default=0)
                log(f"Last checkpoint at country index {last_ckpt_i}")
                # All countries up to last_ckpt_i are in the legacy parquet
                for idx, code in matches:
                    if int(idx) <= last_ckpt_i:
                        done.add(code)
            log(f"Parsed log: {len(done)} countries in legacy checkpoint parquet")
        except Exception as e:
            log(f"Warning: could not parse log for done set: {e}")

    todo = [c for c in COUNTRY_CODES if c not in done]
    log(f"WID: {len(todo)}/{len(COUNTRY_CODES)} country codes to fetch ({len(done)} done)")

    total_obs = 0
    for fname in existing:
        fp = os.path.join(OUT, fname)
        try:
            total_obs += pq.read_metadata(fp).num_rows
        except Exception:
            pass

    for i, code in enumerate(todo, 1):
        rows = fetch_country(code)
        n = len(rows)
        if rows:
            keys  = [r[0] for r in rows]
            dates = [r[1] for r in rows]
            vals  = [r[2] for r in rows]
            tbl = pa.table({
                "series_key": pa.array(keys,  pa.string()),
                "obs_date":   pa.array(dates, pa.date32()),
                "value":      pa.array(vals,  pa.float64()),
            })
            pq.write_table(tbl, os.path.join(OUT, f"{code}.parquet"), compression="zstd")
            total_obs += n
        if n > 0 or i % 20 == 0:
            log(f"  [{i}/{len(todo)}] {code}: {n:,} obs, cumulative {total_obs:,}")
        time.sleep(RATE)

    log(f"DONE: {total_obs:,} total WID observations across {len(done)+len(todo)} countries")


if __name__ == "__main__":
    main()
