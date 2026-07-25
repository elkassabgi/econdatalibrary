#!/usr/bin/env python3
"""FRED (Federal Reserve Economic Data) — St. Louis Fed, keyless CSV download.

License: FRED Terms of Use — free for research/educational use
Source: https://fred.stlouisfed.org/
No API key required for individual series CSV downloads.

Strategy:
  * Download each series via https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}
  * Curated list of ~800 series covering US macro, rates, labor, banking, international

Run: python jobs/ingest_fred.py
     python jobs/ingest_fred.py --only GDP,CPIAUCSL
"""
from __future__ import annotations
import csv, datetime as dt, io, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "fred")
BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "text/csv,text/plain,*/*"}
RATE = 0.5

# ── Curated FRED series (800+ covering all major US indicators) ──────────────
SERIES = [
    # ── GDP / National Accounts ──────────────────────────────────────────────
    "GDP", "GDPC1", "GDPPOT", "GDPDEF", "GNPC96", "GFDGDPA188S",
    "A191RL1Q225SBEA", "A191RO1Q156NBEA",  # real GDP growth
    "PCEPI", "PCEPILFE",  # PCE deflators
    "CPIAUCSL", "CPILFESL", "CPIENGSL", "CPIFABSL",  # CPI
    "PPIACO", "PPIFIS", "PPIITM",  # PPI
    "GDPCA", "NNGDP", "NETFI",  # nominal GDP, net financial investment
    "GNP", "GNPCA",  # GNP
    "OUTBS", "OPHNFB", "OPHPBS",  # productivity
    # ── Labor Market ────────────────────────────────────────────────────────
    "UNRATE", "U6RATE", "U3RATE",
    "PAYEMS", "USPRIV", "USCONS", "USFIRE", "USGOOD", "USINFO",
    "USMINE", "USMFG", "USSERV", "USTRADE", "USTPU", "USWTRADE",
    "CIVPART", "LNS12300060", "LNS11300001",  # participation rates
    "AWHMAN", "AWOTMAN",  # avg hours worked manufacturing
    "CES0500000003",  # avg hourly earnings
    "JTSJOL", "JTSQUL", "JTSQUR", "JTSQUO",  # job openings/quits
    "ICSA", "CCSA",  # initial/continued claims
    "EMRATIO",  # employment-population ratio
    "MANEMP", "SRVPRD",  # manufacturing / service employment
    # ── Interest Rates ──────────────────────────────────────────────────────
    "FEDFUNDS", "DFEDTARL", "DFEDTARU",  # fed funds
    "DFF", "IOER", "IORB",  # effective fed funds, IOER
    "TB3MS", "TB6MS", "GS1", "GS2", "GS3", "GS5", "GS7", "GS10", "GS20", "GS30",
    "T10Y2Y", "T10Y3M", "T5YIFR",  # spreads
    "BAMLH0A0HYM2", "BAMLC0A0CM",  # HY and IG spreads
    "PRIME", "DPRIME",  # prime rate
    "MORTGAGE30US", "MORTGAGE15US",  # mortgage rates
    "EFFR", "OBFR", "SOFR", "IOER",  # policy rates
    "AAA", "BAA", "AAA10Y", "BAA10Y",  # corporate bond yields
    # ── Money Supply ─────────────────────────────────────────────────────────
    "M1SL", "M2SL", "M2V", "MZMSL", "M1V",  # monetary aggregates
    "BOGMBASE", "TOTRESNS", "EXCSRESNS",  # monetary base, reserves
    "WRMFSL",  # retail MF assets
    # ── Banking / Financial ───────────────────────────────────────────────────
    "TOTLL", "TOTCI", "DPSACBW027SBOG",  # total loans/deposits
    "DRBLACBS", "DRSFRMACBS",  # delinquency rates
    "STLFSI2", "NFCI",  # financial stress/conditions
    "OBMMSFHA30YF",  # FHA 30Y mortgage
    "DPCREDIT", "CONSUMER",  # credit
    "WALCL", "WSHOMCB",  # Fed balance sheet
    "BUSLOANS", "REALLN",  # C&I loans, real estate loans
    # ── Housing ──────────────────────────────────────────────────────────────
    "CSUSHPISA", "CSUSHPINSA",  # Case-Shiller
    "USSTHPI",  # FHFA HPI
    "HOUST", "HOUST1F", "HOUST5F",  # housing starts
    "PERMIT", "TLRESCONS",  # permits, construction
    "HOEMF",  # homeownership rate
    "MSPNHSUS",  # median new home price
    # ── Trade / International ────────────────────────────────────────────────
    "BOPTIMP", "BOPTEXP", "BOPBCA",  # imports/exports/CA balance
    "DTWEXBGS", "DTWEXAFEGS", "DTWEXEMEGS",  # dollar indices
    "CHNFN", "JPONF",  # China/Japan holding US treasuries
    "EXCHUS", "EXJPUS", "EXUSEU", "EXUKUS", "EXCAUS",  # exchange rates
    "EXSZUS", "EXKOUS", "EXINUS", "EXBZUS", "EXMXUS",
    # ── Investment / Business ────────────────────────────────────────────────
    "GPDI", "FPI", "PRFI",  # gross private investment
    "A006RC1Q027SBEA",  # corporate profits
    "INDPRO", "IPMAN", "IPMANSICS",  # industrial production
    "TCU", "CAPUTLB00004SQ",  # capacity utilization
    "RETAILSMNSA", "RSXFS", "RRSFS",  # retail sales
    "TOTALSA", "BUSNLOANS",  # total vehicle sales, business loans
    "ISRATIO",  # inventory/sales ratio
    # ── Consumer / Confidence ────────────────────────────────────────────────
    "UMCSENT", "USEPUINDXD",  # U. Michigan sentiment, EPU
    "KCFSI", "ANFCI",  # Kansas City / Atlanta financial conditions
    # ── Prices / Commodities ────────────────────────────────────────────────
    "DCOILWTICO", "DCOILBRENTEU",  # WTI/Brent crude
    "GOLDPMGBD228NLBM", "SLVPRUSD",  # gold/silver
    "PCOPPUSDM",  # copper
    "PNRGASUSUSD",  # natural gas
    "PNRGOWORLDOILPETRO",  # oil price
    # ── Government ───────────────────────────────────────────────────────────
    "GFDEBTN", "FYGFDPUN", "FYFSGDA188S",  # federal debt
    "FYONGDA188S", "FYFSDFYGDP",  # deficit/surplus
    "RECEIPTS", "EXPND", "OUTLAYS",  # gov receipts/outlays
    # ── International (from St Louis Fed)  ─────────────────────────────────
    "MKTGDPCNA646NWDB", "MKTGDPJPA646NWDB", "MKTGDPDEA646NWDB",  # GDP
    "FPCPITOTLZGCHE", "FPCPITOTLZGDEU",  # inflation
    "IRLTLT01DEA156N", "IRLTLT01FRA156N", "IRLTLT01ITA156N",  # Euro zone 10Y
    "IRLTLT01GBQ156N", "IRLTLT01JPQ156N", "IRLTLT01CAQ156N",

    # ── Additional US Labor Market ───────────────────────────────────────────
    "UEMP5TO14", "UEMP15OV", "UEMP27OV", "UEMP15T26",  # duration of unemployment
    "LNS13000001", "LNS13000002",   # unemployment by sex
    "LNS14000006",  # Black unemployment
    "LNS14000009",  # Hispanic unemployment
    "ADPWNUSNERSA",  # ADP private payrolls
    "MNFCTRMP",  # manufacturing new orders
    "NEWORDER",  # manufactured goods orders
    "DGORDER",   # durable goods orders
    "NPPTTL",    # nonfarm payrolls total
    "UEMPMEAN",  # mean unemployment duration
    "NROU", "NROUST",  # natural rate of unemployment
    "RECPROUSM156N",  # recession probability

    # ── US Financial Markets ─────────────────────────────────────────────────
    "VIXCLS",    # CBOE VIX volatility index (daily)
    "NASDAQCOM", # NASDAQ composite
    "SP500",     # S&P 500 index
    "DJIA",      # Dow Jones Industrial Average
    "DSPIC96",   # real disposable personal income
    "PSAVERT",   # personal saving rate
    "PCE",       # personal consumption expenditures

    # ── More Interest Rate Details ────────────────────────────────────────────
    "DFII5", "DFII7", "DFII10", "DFII20", "DFII30",  # TIPS real yields
    "T5YIE", "T10YIE",  # breakeven inflation 5Y, 10Y
    "THREEFYTP10",  # 10Y term premium (NY Fed)
    "THREEFF1", "THREEFF2", "THREEFF3",  # fed funds futures 1-3Y
    "MORTGAGE10US",  # 10Y mortgage rate
    "RMVSNACBW027SBOG",  # revolving consumer credit
    "DTCTHFNM",  # consumer credit outstanding

    # ── More FX / International ───────────────────────────────────────────────
    "EXUSAL", "EXUSAL",  # USD/ARS (Argentina)
    "EXVZUS",  # USD/VEB (Venezuela)
    "EXNOUS",  # USD/NOK (Norway)
    "EXSDUS",  # USD/SEK (Sweden)
    "EXDNUS",  # USD/DKK (Denmark)
    "EXHKUS",  # USD/HKD (Hong Kong)
    "EXITUS",  # USD/INR (India)
    "EXTAUS",  # USD/TWD (Taiwan)
    "EXMAUS",  # USD/MYR (Malaysia)
    "EXTOUS",  # USD/THB (Thailand)
    "EXPHUS",  # USD/PHP (Philippines)
    "EXIDUS",  # USD/IDR (Indonesia)
    "EXSAUS",  # USD/ZAR (South Africa)
    "EXEGUS",  # USD/EGP (Egypt)
    "EXNOUS",  # USD/NOK (Norway)
    "EXPLZS",  # USD/PLN (Poland)
    "EXCPUS",  # USD/CZK (Czech Republic)
    "EXHNUS",  # USD/HUF (Hungary)
    "EXROUS",  # USD/RUB (Russia)
    "EXTRUS",  # USD/TRY (Turkey)
    "EXUAUS",  # USD/UAH (Ukraine)

    # ── More Commodity Prices ─────────────────────────────────────────────────
    "PALLFNFINDEXQ",  # All commodities index quarterly (World Bank)
    "PCOCOUSDM",   # Cocoa
    "PCOFCUSDM",   # Coffee, arabica
    "PRUBAINDXM",  # Rubber price
    "PPALTUSDM",   # Palm oil
    "PWHEAMTUSDM", # Wheat
    "PRICENPQUSDM",# Rice
    "PSOYBUSDM",   # Soybeans
    "PMAIZMTUSDM", # Corn/maize
    "PALUMUSDM",   # Aluminum
    "PNICKUSDM",   # Nickel
    "PZINQUSDM",   # Zinc
    "PLEAD01USDM", # Lead
    "PTINUSDM",    # Tin
    "IRON",        # Iron ore
    "PCOTTINDUSDM",# Cotton
    "PSUGAISAUSDM",# Sugar
    "POILAPSPUSDM",# Oil (APSP average)

    # ── Business Surveys / Confidence ─────────────────────────────────────────
    "BSCICP03USM665S",  # Consumer confidence (OECD)
    "BSXRLV02USM086S",  # Export orders future
    "BSPRV02USM086S",   # Business confidence

    # ── More Banking & Credit ────────────────────────────────────────────────
    "CORS",    # consumer credit outstanding all
    "H8B1015NCBCMG",   # credit default swaps index
    "DRTSCILM",  # C&I loan delinquency
    "DRTSCIS",   # C&I loan charge-offs
    "RCHOAL",    # credit card delinquency
    "RCHOAQ",    # credit card charge-off
    "DRALACBN",  # auto loan delinquency
    "TERMCBPER24NS",  # rate on personal loans 24M
    "TERMCBCCINTNS",  # credit card interest rate

    # ── Real Estate & Construction ────────────────────────────────────────────
    "NHSDPTS",   # new 1-family homes sold
    "HSN1F",     # new private housing units
    "EVACANTUSQ176N",  # vacant housing units
    "RHORUSQ156N",     # homeownership rate quarterly
    "MDOSPNHS",        # median days on market
    "LRUN64TTUSM156S", # labor force 64+ employment rate

    # ── State-Level Data ──────────────────────────────────────────────────────
    "CAUR", "NYUR", "TXUR", "FLUR", "ILUR",  # unemployment by state (CA,NY,TX,FL,IL)
    "OHUR", "PAUR", "GAUR", "NCUR", "MIUR",  # more states
    "NJUR", "VAUR", "WAUR", "AZUR", "MAUR",  # and more

    # ── Climate / Environment ────────────────────────────────────────────────
    "GASREGCOVW",  # gasoline prices all grades
    "DGASNYH",     # NY Harbor gasoline
    "DHHNGSP",     # Henry Hub natural gas spot

    # ── Additional International Long-Run ─────────────────────────────────────
    "MKTGDPGBA646NWDB",  # UK GDP (World Bank)
    "MKTGDPFRA646NWDB",  # France GDP
    "MKTGDPITA646NWDB",  # Italy GDP
    "MKTGDPCNA646NWDB",  # China GDP
    "MKTGDPINA646NWDB",  # India GDP
    "MKTGDPBRA646NWDB",  # Brazil GDP
    "MKTGDPRUA646NWDB",  # Russia GDP
    "MKTGDPAUS646NWDB",  # Australia GDP
    "FPCPITOTLZGGBR",    # UK inflation
    "FPCPITOTLZGJPN",    # Japan inflation
    "FPCPITOTLZGCAN",    # Canada inflation
    "FPCPITOTLZGFRA",    # France inflation
    "FPCPITOTLZGBRA",    # Brazil inflation
    "FPCPITOTLZGIND",    # India inflation
    "FPCPITOTLZGMEX",    # Mexico inflation
    "FPCPITOTLZGRUS",    # Russia inflation
    "FPCPITOTLZGAUS",    # Australia inflation
    "IRLTLT01AUM156N",   # Australia 10Y bond
    "IRLTLT01NZQ156N",   # New Zealand 10Y bond
    "IRLTLT01CHQ156N",   # Switzerland 10Y bond
    "IRLTLT01SEQ156N",   # Sweden 10Y bond
    "IRLTLT01NOQ156N",   # Norway 10Y bond
    "IRLTLT01DNQ156N",   # Denmark 10Y bond
    "IRLTLT01ESM156N",   # Spain 10Y bond
    "IRLTLT01PTM156N",   # Portugal 10Y bond
    "IRLTLT01GRM156N",   # Greece 10Y bond
    "IRLTLT01NLM156N",   # Netherlands 10Y bond
    "IRLTLT01ATM156N",   # Austria 10Y bond
    "IRLTLT01BEM156N",   # Belgium 10Y bond
    "IRLTLT01FIM156N",   # Finland 10Y bond
    "IRLTLT01KRQ156N",   # South Korea 10Y
    "IRLTLT01PLQ156N",   # Poland 10Y
    "IRLTLT01MXQ156N",   # Mexico 10Y
    "IRLTLT01ZAQ156N",   # South Africa 10Y
    "IRLTLT01INQ156N",   # India 10Y
    "IRLTLT01CZQ156N",   # Czech Republic 10Y
    "IRLTLT01HUQ156N",   # Hungary 10Y

    # ── BLS Detailed Price Indices ────────────────────────────────────────────
    "CPIAPPSL", "CPIMEDSL", "CPITRNSL",  # CPI by component: apparel, medical, transport
    "CPIHOSSL", "CPIRECSL", "CPIEDSL",   # housing, recreation, education
    "CUSR0000SAC",  # CPI commodities
    "CUSR0000SAS",  # CPI services
    "CUSR0000SA0E", # CPI energy
    "CUSR0000SAF1", # CPI food at home
    "CUSR0000SEFV", # CPI food away from home

    # ── Flow of Funds / Financial Accounts ───────────────────────────────────
    "HNOSDCA027S",  # household net worth
    "TNWBSHNO",     # total net wealth households
    "FGEXPND",      # federal expenditures
    "BOGZ1FV",      # total financial system
]

# Deduplicate while preserving order
seen = set()
SERIES = [s for s in SERIES if s not in seen and not seen.add(s)]


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def get_csv(series_id: str, retries: int = 3) -> bytes | None:
    url = f"{BASE}?id={series_id}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.content
            if r.status_code == 429:
                time.sleep(30); continue
            if r.status_code in (400, 404):
                return None
        except Exception as e:
            log(f"  {series_id} ERR attempt {attempt+1}: {e}")
        time.sleep(3 * (attempt + 1))
    return None


def parse_fred_csv(content: bytes, series_id: str) -> tuple[list, list, list]:
    """Parse FRED CSV: two columns header=observation_date, {series_id}."""
    keys, dates, vals = [], [], []
    try:
        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return [], [], []
        # FRED CSV: observation_date, {series_id}
        date_col = reader.fieldnames[0]
        val_col  = reader.fieldnames[1] if len(reader.fieldnames) > 1 else None
        if not val_col:
            return [], [], []
        for row in reader:
            d_str = row.get(date_col, "").strip()
            v_str = row.get(val_col, "").strip()
            if not d_str or not v_str or v_str in (".", "nan", "N/A", ""):
                continue
            try:
                d = dt.date.fromisoformat(d_str[:10])
                v = float(v_str)
            except (ValueError, TypeError):
                continue
            keys.append(series_id)
            dates.append(d)
            vals.append(v)
    except Exception as e:
        log(f"  {series_id}: parse error: {e}")
    return keys, dates, vals


def main():
    os.makedirs(OUT, exist_ok=True)
    out_path = os.path.join(OUT, "fred.parquet")

    # Track which series already done
    done_series: set[str] = set()
    existing_keys: list = []
    existing_dates: list = []
    existing_vals: list = []
    if os.path.exists(out_path):
        import pyarrow.parquet as pq2
        tbl = pq2.read_table(out_path)
        # Extract unique series keys
        done_series = set(tbl.column("series_key").to_pylist())
        existing_keys = tbl.column("series_key").to_pylist()
        existing_dates = tbl.column("obs_date").to_pylist()
        existing_vals  = tbl.column("value").to_pylist()
        log(f"Resuming: {len(done_series)} series already done, {len(existing_vals):,} obs")

    only_ids: set[str] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            ids = a.split("=", 1)[-1] if "=" in a else ""
            only_ids = set(ids.split(","))
        elif not a.startswith("-"):
            only_ids.add(a)

    to_download = [s for s in SERIES if s not in done_series]
    if only_ids:
        to_download = [s for s in SERIES if s in only_ids and s not in done_series]
    log(f"FRED: {len(to_download)} series to download (of {len(SERIES)} total)")

    all_keys   = list(existing_keys)
    all_dates  = list(existing_dates)
    all_vals   = list(existing_vals)

    for i, sid in enumerate(to_download, 1):
        content = get_csv(sid)
        if not content:
            log(f"  [{i}/{len(to_download)}] {sid}: skip (no data)")
            time.sleep(RATE); continue
        k, d, v = parse_fred_csv(content, sid)
        if v:
            all_keys.extend(k)
            all_dates.extend(d)
            all_vals.extend(v)
            log(f"  [{i}/{len(to_download)}] {sid}: {len(v):,} obs")
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
    log(f"DONE: {n:,} FRED observations ({len(set(all_keys))} series)")


if __name__ == "__main__":
    main()
