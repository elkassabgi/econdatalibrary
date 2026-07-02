#!/usr/bin/env python3
"""Statistics New Zealand — national accounts, trade, population, labour.

License: Creative Commons Attribution 4.0
Source: https://www.stats.govt.nz/large-datasets/csv-files-for-download/
No API key required (direct CSV/Excel download).

Coverage:
  * GDP quarterly (expenditure, income, production approaches)
  * Balance of payments, trade in goods and services
  * Labour force survey (employment, unemployment, wages)
  * CPI components and sub-indices
  * Dwelling consents and sales
  * Population (national, sub-national)
  * Business financial statistics

Run: python jobs/ingest_statsnz.py
"""
from __future__ import annotations
import csv, datetime as dt, io, os, re, time, zipfile
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "stats_nz")
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# Stats NZ bulk CSV downloads — curated set of key time-series datasets
# Format: (url, series_prefix, description)
NZ_BASE = "https://www.stats.govt.nz/assets/Uploads"

# For each dataset: (topic_folder, filename_stem, prefix, description)
# The connector tries {period} variants from newest to oldest
DATASETS_DYNAMIC = [
    # quarterly topics: try Dec-2024, Sep-2024, Jun-2024, Mar-2024, Dec-2023
    ("Gross-domestic-product", "gross-domestic-product", "gdp_quarterly", "Quarterly GDP",
     ["December-2024-quarter", "September-2024-quarter", "June-2024-quarter", "March-2024-quarter", "December-2023-quarter"]),
    ("Consumers-price-index", "consumers-price-index", "cpi", "Consumers Price Index",
     ["March-2025-quarter", "December-2024-quarter", "September-2024-quarter", "June-2024-quarter"]),
    ("Labour-market-statistics", "labour-market-statistics", "labour", "Labour Market Statistics",
     ["December-2024-quarter", "September-2024-quarter", "June-2024-quarter", "March-2024-quarter"]),
    ("Balance-of-payments", "balance-of-payments-and-international-investment-position", "bop", "Balance of Payments",
     ["September-2024", "June-2024", "March-2024", "December-2023"]),
    ("Producers-price-index", "producers-price-index", "ppi", "Producers Price Index",
     ["December-2024-quarter", "September-2024-quarter", "June-2024-quarter"]),
    ("Retail-trade-survey", "retail-trade-survey", "retail", "Retail Trade Survey",
     ["December-2024-quarter", "September-2024-quarter", "June-2024-quarter"]),
    ("Business-financial-statistics", "business-financial-statistics", "biz_finance", "Business Financial Statistics",
     ["Year-ended-March-2024", "Year-ended-March-2023"]),
    ("Overseas-merchandise-trade", "overseas-merchandise-trade", "trade", "Merchandise Trade",
     ["April-2025", "March-2025", "February-2025", "January-2025", "December-2024"]),
    ("Building-consents-issued", "building-consents-issued", "building", "Building Consents",
     ["January-2025", "December-2024", "November-2024", "October-2024"]),
    ("International-travel-and-migration", "international-travel-and-migration", "travel", "Travel & Migration",
     ["December-2024", "November-2024", "October-2024"]),
    # Annual
    ("National-accounts-income-and-expenditure",
     "national-accounts-income-and-expenditure",
     "gdp_annual", "Annual GDP",
     ["Year-ended-March-2024", "Year-ended-March-2023"]),
    ("Estimated-resident-population-for-New-Zealand",
     "estimated-resident-population-for-new-zealand",
     "population", "Estimated Resident Population",
     ["2024", "2023"]),
]

def build_datasets():
    """Build list of (url, prefix, desc) by testing which URLs exist."""
    import requests
    result = []
    for topic, stem, prefix, desc, periods in DATASETS_DYNAMIC:
        found = False
        for period in periods:
            url = f"{NZ_BASE}/{topic}/{topic}-{period}/Download-data/{stem}-{period.lower()}.csv"
            try:
                r = requests.head(url, headers=UA, timeout=10, allow_redirects=True)
                if r.status_code == 200:
                    result.append((url, prefix, desc))
                    found = True
                    break
            except Exception:
                pass
        if not found:
            log(f"  No working URL found for {prefix}")
    return result

DATASETS = build_datasets  # call at runtime

# Stats NZ Infoshare API (alternative — structured time series)
INFOSHARE_BASE = "https://infoshare.stats.govt.nz/infoshare/downloaddata"


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def try_download(url: str, timeout: int = 120) -> bytes | None:
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.status_code == 200 and len(r.content) > 500:
            return r.content
        log(f"  HTTP {r.status_code}: {url[-70:]}")
    except Exception as e:
        log(f"  ERR: {e}")
    return None


def parse_statsnz_date(s: str) -> dt.date | None:
    """Parse various Stats NZ date formats."""
    s = (s or "").strip()
    try:
        # 2024Q3 or 2024-Q3
        m = re.match(r"(\d{4})[.\-Q]?Q(\d)", s, re.IGNORECASE)
        if m:
            q = int(m.group(2))
            return dt.date(int(m.group(1)), (q-1)*3+1, 1)
        # YYYY-MM or YYYY.MM or March 2024
        m = re.match(r"(\d{4})[-.](\d{2})$", s)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        # March 2024 or Mar-2024
        months = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                  "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
        m = re.match(r"([A-Za-z]{3})\W*(\d{4})", s)
        if m:
            mon = months.get(m.group(1).lower())
            if mon:
                return dt.date(int(m.group(2)), mon, 1)
        # Full: YYYY-MM-DD
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            return dt.date.fromisoformat(s[:10])
        # Annual YYYY
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        # Year ended March YYYY: "March 2024"
        m = re.match(r"Year ended [A-Za-z]+ (\d{4})", s)
        if m:
            return dt.date(int(m.group(1)), 12, 31)
    except (ValueError, TypeError):
        pass
    return None


def ingest_csv(content: bytes, prefix: str) -> list[tuple[str, dt.date, float]]:
    """Parse a Stats NZ CSV file (typically long format with Period, Value columns)."""
    results = []
    try:
        # Try UTF-8 then Latin-1
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")

        reader = csv.DictReader(io.StringIO(text))
        headers = [h.strip().lower() for h in (reader.fieldnames or [])]
        log(f"  Headers ({len(headers)}): {headers[:10]}")

        # Find key columns
        period_col = next((f for f in (reader.fieldnames or []) if
                           f.strip().lower() in ("period", "date", "year", "quarter", "month", "time")), None)
        value_col  = next((f for f in (reader.fieldnames or []) if
                           f.strip().lower() in ("value", "data_value", "figure", "amount")), None)
        series_col = next((f for f in (reader.fieldnames or []) if
                           f.strip().lower() in ("series_reference", "series_id", "series_title_1",
                                                  "variable", "indicator", "series_name", "label")), None)
        # Fallback to subject/category only if no better option
        if series_col is None:
            series_col = next((f for f in (reader.fieldnames or []) if
                               f.strip().lower() in ("subject", "category")), None)

        if period_col is None or value_col is None:
            log(f"  Could not identify period/value columns in {prefix}")
            return results

        for row in reader:
            period = row.get(period_col, "").strip()
            v_raw  = row.get(value_col, "").strip()
            if not period or not v_raw or v_raw.upper() in ("", "C", "S", "NA", "N/A", "."):
                continue
            obs_date = parse_statsnz_date(period)
            if obs_date is None:
                continue
            try:
                v = float(v_raw.replace(",", ""))
                if v != v:
                    continue
            except (ValueError, TypeError):
                continue

            # Build series key
            if series_col:
                sid = str(row.get(series_col, "")).strip()[:50]
                if sid:
                    series_key = f"{prefix}:{sid}"
                else:
                    series_key = prefix
            else:
                series_key = prefix
            results.append((series_key, obs_date, v))
    except Exception as e:
        log(f"  CSV parse error ({prefix}): {e}")
    return results


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "stats_nz.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"Already done: {n:,} rows"); return

    all_keys, all_dates, all_vals = [], [], []
    seen: set[tuple] = set()

    datasets = build_datasets()
    log(f"Found {len(datasets)} working Stats NZ datasets")
    for url, prefix, desc in datasets:
        log(f"Downloading {prefix}: {url[-70:]}")
        content = try_download(url)
        if not content:
            # Try year-variant URL (remove last year number from URL)
            base_url = re.sub(r"-\d{4}-quarter", "", url)
            base_url = re.sub(r"-\d{4}\.csv", ".csv", base_url)
            if base_url != url:
                content = try_download(base_url)
        if not content:
            log(f"  Skipping {prefix}")
            continue

        # Handle zip files
        if content[:4] == b"PK\x03\x04":
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith(".csv")]
                    if csv_files:
                        content = z.read(csv_files[0])
                        log(f"  Extracted {csv_files[0]} from zip")
            except Exception:
                pass

        results = ingest_csv(content, prefix)
        log(f"  {prefix}: {len(results):,} observations")
        for key, d, v in results:
            tok = (key, d)
            if tok not in seen:
                seen.add(tok)
                all_keys.append(key)
                all_dates.append(d)
                all_vals.append(v)
        time.sleep(0.5)

    if not all_vals:
        log("0 observations — check Stats NZ URLs"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} Stats NZ observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
