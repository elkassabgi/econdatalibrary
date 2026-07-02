#!/usr/bin/env python3
"""Banco Central do Brasil (BCB) — SGS time-series system, 30,000+ series.

License: Open Government Data (dadosabertos.bcb.gov.br)
Source: https://dadosabertos.bcb.gov.br / https://api.bcb.gov.br/dados/serie/
No API key required.

Strategy:
  * Curated list of ~300 key SGS series IDs covering:
    - FX / exchange rates (BRL/USD, BRL/EUR, ...)
    - Interest rates (SELIC, CDI, TR, TJLP)
    - Inflation (IPCA, IGP-M, IGP-DI, INPC, IPC-A)
    - Money supply (M1–M4, monetary base)
    - Credit aggregates (households, corporates, default rates)
    - Trade balance, current account, FX reserves
    - GDP (quarterly, sectoral)
    - Employment (unemployment, CAGED)
    - Government finance (primary balance, public debt)
    - Capital markets (B3 index, bond yields)
  * API: GET /dados/serie/bcdata.sgs.{id}/dados?formato=json
  * Date format: DD/MM/YYYY; value: "valor" field

Run: python jobs/ingest_bcb.py
     python jobs/ingest_bcb.py --only 1,11,13522
"""
from __future__ import annotations
import csv, datetime as dt, io, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "bcb")
BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{sid}/dados"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3

# ── SGS Series Catalog — key macro/financial series ─────────────────────────
# Format: (series_id, label, description)
SERIES = [
    # ── Exchange Rates ──────────────────────────────────────────────────────
    (1,      "USDBRL_sell",   "USD/BRL commercial selling rate (daily)"),
    (10813,  "USDBRL_buy",    "USD/BRL commercial buying rate (daily)"),
    (21619,  "EURBRL_sell",   "EUR/BRL selling rate"),
    (21620,  "EURBRL_buy",    "EUR/BRL buying rate"),
    (3697,   "GBPBRL",        "GBP/BRL rate"),
    (3688,   "JPYBRL",        "JPY/BRL rate"),
    (3696,   "CHFBRL",        "CHF/BRL rate"),
    (3700,   "CADBRL",        "CAD/BRL rate"),
    (3692,   "CNYBRL",        "CNY/BRL rate"),
    (3699,   "ARSBRL",        "ARS/BRL rate"),
    (3694,   "MXNBRL",        "MXN/BRL rate"),
    (10792,  "REER",          "Real effective exchange rate (broad basket)"),
    # ── Interest Rates ──────────────────────────────────────────────────────
    (11,     "SELIC_overnight","SELIC overnight interbank rate (daily %)"),
    (12,     "CDI",           "CDI interbank deposit rate (daily %)"),
    (25,     "SELIC_target",  "SELIC target rate (%)"),
    (226,    "TR",            "Referential interest rate (TR, monthly)"),
    (253,    "TJLP",          "Long-term interest rate (TJLP, monthly)"),
    (7812,   "TLP",           "Long-term rate TLP"),
    (1,      "USDBRL",        "BRL/USD PTAX (daily)"),
    (7326,   "swap_pre_DI360","360-day pre-DI swap rate"),
    (7327,   "swap_pre_DI720","720-day pre-DI swap rate"),
    (7333,   "NTNB_5Y",       "NTNB real yield 5Y"),
    (7341,   "NTNB_10Y",      "NTNB real yield 10Y"),
    (7344,   "NTNB_20Y",      "NTNB real yield 20Y"),
    (7347,   "NTNF_5Y",       "NTNF nominal yield 5Y"),
    # ── Inflation ───────────────────────────────────────────────────────────
    (13522,  "IPCA",          "IPCA consumer price index (monthly % change)"),
    (433,    "IPCA_12m",      "IPCA accumulated 12 months (%)"),
    (189,    "IGPM",          "IGP-M general price index (monthly %)"),
    (190,    "IGPDI",         "IGP-DI general price index (monthly %)"),
    (191,    "IPA",           "IPA wholesale price index (monthly %)"),
    (188,    "IPC_FIPE",      "IPC-FIPE consumer price index"),
    (188,    "INPC",          "INPC national consumer price index"),
    (10764,  "IPCA_admin",    "IPCA administered prices"),
    (10765,  "IPCA_free",     "IPCA free market prices"),
    (4447,   "IPCA_food",     "IPCA food and beverages"),
    (4448,   "IPCA_housing",  "IPCA housing"),
    (4449,   "IPCA_transport","IPCA transport"),
    (4451,   "IPCA_health",   "IPCA health"),
    (7478,   "PPI_industry",  "Producer price index - industry"),
    # ── Money Supply ────────────────────────────────────────────────────────
    (1823,   "M0",            "Monetary base M0 (BRL millions)"),
    (27790,  "M1",            "Money supply M1 (BRL millions)"),
    (27791,  "M2",            "Money supply M2 (BRL millions)"),
    (27792,  "M3",            "Money supply M3 (BRL millions)"),
    (27793,  "M4",            "Money supply M4 (BRL millions)"),
    # ── Credit ──────────────────────────────────────────────────────────────
    (20539,  "credit_total",  "Total credit outstanding (BRL millions)"),
    (20541,  "credit_household","Household credit (BRL millions)"),
    (20542,  "credit_corporate","Corporate credit (BRL millions)"),
    (21082,  "default_total", "Credit default rate - total (%)"),
    (21083,  "default_household","Credit default rate - households (%)"),
    (21084,  "default_corporate","Credit default rate - corporates (%)"),
    (20619,  "avg_rate_total","Average credit interest rate - total (%)"),
    (20620,  "avg_rate_hh",   "Average credit interest rate - households (%)"),
    (20621,  "avg_rate_corp", "Average credit interest rate - corporates (%)"),
    # ── Trade & External ────────────────────────────────────────────────────
    (22706,  "trade_balance", "Trade balance (USD millions, monthly)"),
    (22707,  "exports",       "Exports FOB (USD millions, monthly)"),
    (22708,  "imports",       "Imports FOB (USD millions, monthly)"),
    (22562,  "current_account","Current account balance (USD millions, monthly)"),
    (3546,   "FDI",           "Foreign direct investment inflows (USD millions)"),
    (13008,  "FX_reserves",   "Foreign exchange reserves (USD millions)"),
    (7456,   "FX_reserves_liq","Liquid foreign exchange reserves (USD millions)"),
    # ── GDP & National Accounts ─────────────────────────────────────────────
    (22099,  "GDP_quarterly", "GDP at current prices - quarterly (BRL millions)"),
    (22100,  "GDP_real",      "GDP at constant prices - quarterly (BRL millions)"),
    (7326,   "GDP_real_growth","Real GDP growth QoQ SA (%)"),
    (4380,   "GDP_deflator",  "Implicit GDP deflator"),
    # ── Government Finance ───────────────────────────────────────────────────
    (4513,   "primary_balance","Central govt primary balance (BRL millions)"),
    (4514,   "nominal_balance","Central govt nominal balance (BRL millions)"),
    (13762,  "NFSP_nominal",  "Public sector borrowing requirement - nominal (% GDP)"),
    (4537,   "net_debt_GDP",  "Net public debt (% GDP)"),
    (13762,  "gross_debt_GDP","Gross public debt (% GDP)"),
    (4472,   "revenue",       "Government revenue - federal (BRL millions)"),
    (4473,   "expenditure",   "Government expenditure - federal (BRL millions)"),
    # ── Employment ───────────────────────────────────────────────────────────
    (24369,  "unemployment",  "Unemployment rate - PNAD Contínua (%)"),
    (28763,  "employment",    "Number of employed persons (thousands)"),
    (25239,  "CAGED_net",     "Formal employment net creation - CAGED"),
    (17623,  "avg_real_wage", "Average real wage (BRL)"),
    # ── Capital Markets ───────────────────────────────────────────────────────
    (7440,   "IBOVESPA",      "Ibovespa stock index (points)"),
    (11753,  "B3_market_cap", "B3 market capitalization (BRL billions)"),
    (11754,  "B3_volume",     "B3 trading volume (BRL millions)"),
    (4189,   "CDS_5Y",        "Brazil 5Y CDS spread (bps)"),
    # ── Production & Activity ─────────────────────────────────────────────────
    (1401,   "industrial_prod","Industrial production index"),
    (21864,  "services_vol",  "Services volume index"),
    (1475,   "retail_vol",    "Retail trade volume index"),
    (7376,   "PMI_mfg",       "PMI manufacturing (Markit)"),
    (7377,   "PMI_services",  "PMI services (Markit)"),
    (7378,   "PMI_composite", "PMI composite (Markit)"),
    # ── Energy ────────────────────────────────────────────────────────────────
    (7470,   "oil_production","Oil production (thousands barrels/day)"),
    (10837,  "electricity_consumption","Electricity consumption (GWh)"),
    # ── Agriculture ────────────────────────────────────────────────────────────
    (31916,  "agri_production","Agricultural production value"),
    (7396,   "soy_price",     "Soybean price (USD/bushel)"),
    (7397,   "corn_price",    "Corn price (USD/bushel)"),
    (7420,   "coffee_price",  "Coffee price (USD/60kg bag)"),
    (7421,   "sugar_price",   "Sugar price (USD/lb)"),
    (7431,   "cotton_price",  "Cotton price (USD/lb)"),
    # ── Population ────────────────────────────────────────────────────────────
    (24369,  "population",    "Population estimate (thousands)"),
]

# Deduplicate by series_id while preserving labels
seen_ids: set[int] = set()
SERIES_DEDUP = []
for sid, label, desc in SERIES:
    if sid not in seen_ids:
        seen_ids.add(sid)
        SERIES_DEDUP.append((sid, label, desc))
SERIES = SERIES_DEDUP


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii', 'replace').decode()}", flush=True)


def get_series(sid: int, retries: int = 3) -> list[dict] | None:
    """Fetch BCB SGS series. Daily series require date-range chunks (max 10 years)."""
    base_url = BASE.format(sid=sid)
    # Try full history first (works for non-daily series)
    url = base_url + "?formato=json"
    try:
        r = requests.get(url, headers=UA, timeout=120)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (404, 400):
            return None
        if r.status_code == 429:
            time.sleep(60)
        if r.status_code != 406:
            log(f"  HTTP {r.status_code}: sid={sid}")
            return None
    except Exception as e:
        log(f"  ERR sid={sid}: {e}"); return None

    # 406 = daily series requires date range; query in 9-year windows from 1994
    results = []
    today = dt.date.today()
    start = dt.date(1994, 1, 1)
    window = dt.timedelta(days=9*365)
    while start <= today:
        end = min(start + window, today)
        di = start.strftime("%d/%m/%Y")
        df = end.strftime("%d/%m/%Y")
        chunk_url = f"{base_url}?formato=json&dataInicial={di}&dataFinal={df}"
        for attempt in range(retries):
            try:
                r = requests.get(chunk_url, headers=UA, timeout=120)
                if r.status_code == 200:
                    chunk = r.json()
                    if isinstance(chunk, list):
                        results.extend(chunk)
                    break
                if r.status_code == 429:
                    time.sleep(60); continue
            except Exception:
                pass
            time.sleep(3)
        start = end + dt.timedelta(days=1)
        time.sleep(0.2)
    return results if results else None


def parse_bcb_date(s: str) -> dt.date | None:
    """Parse BCB date formats: DD/MM/YYYY, MM/YYYY, YYYY"""
    s = (s or "").strip()
    try:
        if len(s) == 10 and s[2] == "/":
            return dt.date(int(s[6:10]), int(s[3:5]), int(s[0:2]))
        if len(s) == 7 and s[2] == "/":
            return dt.date(int(s[3:7]), int(s[0:2]), 1)
        if len(s) == 4 and s.isdigit():
            return dt.date(int(s), 12, 31)
    except (ValueError, TypeError):
        pass
    return None


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "bcb.parquet")

    done: set[int] = set()
    all_keys: list = []
    all_dates: list = []
    all_vals: list = []

    if os.path.exists(out_path):
        tbl = pq.read_table(out_path)
        # Parse done series IDs from series_key "label:sid"
        for sk in set(tbl.column("series_key").to_pylist()):
            if ":" in sk:
                try:
                    done.add(int(sk.split(":")[1]))
                except ValueError:
                    pass
        all_keys  = tbl.column("series_key").to_pylist()
        all_dates = tbl.column("obs_date").to_pylist()
        all_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done)} series done, {len(all_vals):,} obs")

    only_ids: set[int] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            raw = a.split("=", 1)[-1] if "=" in a else ""
            only_ids = {int(x) for x in raw.split(",") if x.strip().isdigit()}
        elif a.strip().isdigit():
            only_ids.add(int(a))

    to_do = [(sid, label, desc) for sid, label, desc in SERIES if sid not in done]
    if only_ids:
        to_do = [(sid, label, desc) for sid, label, desc in to_do if sid in only_ids]
    log(f"BCB SGS: {len(to_do)} series to download")

    for i, (sid, label, desc) in enumerate(to_do, 1):
        obs = get_series(sid)
        if not obs:
            log(f"  [{i}/{len(to_do)}] {label} (sid={sid}): no data")
            time.sleep(RATE); continue

        series_key = f"{label}:{sid}"
        n = 0
        for item in obs:
            d_str  = item.get("data", "")
            v_str  = item.get("valor", "")
            if not d_str or not v_str or v_str in ("", "null", "null"):
                continue
            obs_date = parse_bcb_date(d_str)
            if obs_date is None:
                continue
            try:
                v = float(str(v_str).replace(",", "."))
            except (ValueError, TypeError):
                continue
            all_keys.append(series_key)
            all_dates.append(obs_date)
            all_vals.append(v)
            n += 1

        if i % 20 == 0 or n > 0:
            log(f"  [{i}/{len(to_do)}] {label} (sid={sid}): {n:,} obs")
        time.sleep(RATE)

    if not all_vals:
        log("0 observations"); return

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"DONE: {n:,} BCB Brazil observations")


if __name__ == "__main__":
    main()
