#!/usr/bin/env python3
"""KSH Hungary (Központi Statisztikai Hivatal) — STADAT tables via toc.json + CSV.

License: Free to use with attribution to KSH (www.ksh.hu)
Source: https://www.ksh.hu/stadat (the SDMX host is dead; this is the working channel)
No API key required.

Coverage: all ~1,632 STADAT tables across 27 themes (ara prices, gdp national
accounts, mun labour, nep population, ene energy, ...), English edition.

Channel:
  * Catalog: GET https://www.ksh.hu/stadat_files/toc.json
  * Data:    GET https://www.ksh.hu/stadat_files/{theme}/en/{id}.csv
             where theme = first 3 chars of the table id.

Notes:
  * KSH's F5 WAF rejects minimal clients → full browser headers are required
    (this script intentionally does NOT use the library User-Agent).
  * CSVs are semicolon-delimited presentation tables: title row, one or more
    header rows (quoted cells may contain newlines), comma decimal separators,
    space thousands separators, 'x'/'..' placeholders.
  * Time may live in the first column (Year rows) or in the column headers;
    both orientations are handled. Tables with no parseable time dimension
    are logged and skipped.

Output: one parquet per theme (first 3 chars of table id), long format
        {series_key, obs_date, value}; resumable (skips existing parquets).
series_key: KSH:{table_id}:{row_label}:{col_label} (separators sanitized).

Run: python jobs/ingest_ksh_stadat.py
"""
from __future__ import annotations
import csv, datetime as dt, io, json, os, random, re, time
from collections import defaultdict
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "ksh_stadat")
BASE = "https://www.ksh.hu/stadat_files"
# F5 WAF rejects minimal clients — full browser headers required for this source.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
}
RATE = 1.2          # 0.5s tripped the F5 WAF on a re-crawl; be gentler
CATALOG_FILE = os.path.join(OUT, "_catalog.json")
WAF_SLEEPS = [60, 120, 300, 600, 900]
WAF_FAILS = [0]     # consecutive tables lost to the WAF; main() aborts at 3


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def _is_waf_page(content: bytes) -> bool:
    head = content[:600].lstrip().lower()
    return (b"request rejected" in head or head.startswith(b"<html")
            or head.startswith(b"<!doctype"))


def get_bytes(url: str) -> bytes | None:
    """Fetch with F5-WAF awareness: the WAF serves HTTP 200 'Request Rejected'
    HTML when throttling. Sleep on an escalating schedule and retry; if the
    block persists ~30+ min for this URL, give up (caller counts WAF losses
    and aborts the run rather than mass-skipping tables)."""
    waf_i = 0
    for attempt in range(12):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                if _is_waf_page(r.content):
                    if waf_i >= len(WAF_SLEEPS):
                        log(f"  WAF still rejecting after {sum(WAF_SLEEPS)}s of backoff: {url[-50:]}")
                        WAF_FAILS[0] += 1
                        return None
                    s = WAF_SLEEPS[waf_i]
                    waf_i += 1
                    log(f"  WAF rejection — sleeping {s}s ({url[-40:]})")
                    time.sleep(s)
                    continue
                WAF_FAILS[0] = 0
                return r.content
            if r.status_code == 404:
                WAF_FAILS[0] = 0
                return None
            if r.status_code in (403, 406, 429, 503):
                log(f"  HTTP {r.status_code} (WAF/throttle), backing off: {url[-60:]}")
                time.sleep(60 * min(attempt + 1, 5))
                continue
            log(f"  HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  ERR: {e}")
        time.sleep(5 * (attempt + 1))
        if attempt >= 4:
            return None
    return None


# ---------------------------------------------------------------- time parsing
MONTHS = {}
for i, names in enumerate([
    ("january", "jan", "januar", "január"),
    ("february", "feb", "febr", "februar", "február", "febuary"),  # KSH EN files contain the 'Febuary' typo
    ("march", "mar", "marc", "márc", "marcius", "március"),
    ("april", "apr", "ápr", "aprilis", "április"),
    ("may", "maj", "máj", "majus", "május"),
    ("june", "jun", "jún", "junius", "június"),
    ("july", "jul", "júl", "julius", "július"),
    ("august", "aug", "augusztus"),
    ("september", "sep", "sept", "szep", "szept", "szeptember"),
    ("october", "oct", "okt", "oktober", "október"),
    ("november", "nov"),
    ("december", "dec"),
], start=1):
    for n in names:
        MONTHS[n] = i
ROMAN_Q = {"i": 1, "ii": 2, "iii": 3, "iv": 4}


def _yr_ok(y: int) -> bool:
    return 1800 <= y <= 2100


def parse_time(s: str) -> dt.date | None:
    """Parse KSH period labels: 1995 | 2023. Q1 | 2023. I. negyedév | 2023. január | 2023. 01. ..."""
    if not s:
        return None
    t = re.sub(r"\s+", " ", s.strip())
    t = re.sub(r"\((?:\d+|[a-z])\)$", "", t).strip()      # footnote refs "(1)"
    t = t.rstrip("*+°^").strip()
    low = t.lower()
    try:
        m = re.fullmatch(r"(\d{4})\.?", low)
        if m:
            y = int(m.group(1))
            return dt.date(y, 12, 31) if _yr_ok(y) else None
        m = re.fullmatch(r"(\d{4})\.?\s*[qk]\s*\.?\s*([1-4])\.?(?:\s*(?:negyed[ée]v|quarter))?\.?", low)
        if m:
            y, q = int(m.group(1)), int(m.group(2))
            return dt.date(y, (q - 1) * 3 + 1, 1) if _yr_ok(y) else None
        m = re.fullmatch(r"[qk]\s*([1-4])[\s.]+(\d{4})\.?", low)
        if m:
            q, y = int(m.group(1)), int(m.group(2))
            return dt.date(y, (q - 1) * 3 + 1, 1) if _yr_ok(y) else None
        m = re.fullmatch(r"[qk]\s*1\s*[–\-−]\s*[qk]\s*4[\s.]+(\d{4})\.?", low)   # Q1–Q4 2022 = full year
        if m:
            y = int(m.group(1))
            return dt.date(y, 12, 31) if _yr_ok(y) else None
        m = re.fullmatch(r"(\d{1,2})\.?\s+([a-záéíóöőúüű]+)\.?\s+(\d{4})\.?", low)   # 30 June 2021
        if m:
            dd, name, y = int(m.group(1)), m.group(2), int(m.group(3))
            mm = MONTHS.get(name)
            if mm and _yr_ok(y):
                return dt.date(y, mm, dd)
            return None
        m = re.fullmatch(r"([a-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})\.?", low)             # June 30, 2021
        if m:
            name, dd, y = m.group(1), int(m.group(2)), int(m.group(3))
            mm = MONTHS.get(name)
            if mm and _yr_ok(y):
                return dt.date(y, mm, dd)
            return None
        m = re.fullmatch(r"(\d{4})\.?\s*(i{1,2})\.?\s*f[ée]l[ée]v\.?", low)   # half-year
        if m:
            y, h = int(m.group(1)), len(m.group(2))
            return dt.date(y, 1 if h == 1 else 7, 1) if _yr_ok(y) else None
        m = re.fullmatch(r"(\d{4})\.?\s*(i{1,3}|iv)\.?\s*(?:negyed[ée]v|n[ée]|quarter)\.?", low)
        if m:
            y, q = int(m.group(1)), ROMAN_Q[m.group(2)]
            return dt.date(y, (q - 1) * 3 + 1, 1) if _yr_ok(y) else None
        m = re.fullmatch(r"(\d{4})\.?\s*([a-záéíóöőúüű]+)\.?", low)           # 2023. január
        if m:
            y, name = int(m.group(1)), m.group(2)
            mm = MONTHS.get(name)
            if mm and _yr_ok(y):
                return dt.date(y, mm, 1)
            return None
        m = re.fullmatch(r"([a-z]+)\.?\s+(\d{4})\.?", low)                    # January 2023
        if m:
            name, y = m.group(1), int(m.group(2))
            mm = MONTHS.get(name)
            if mm and _yr_ok(y):
                return dt.date(y, mm, 1)
            return None
        m = re.fullmatch(r"(\d{4})\.?\s+(\d{1,2})\.?(?:\s*(?:h[óo]|month))?\.?", low)  # 2023. 01.
        if m:
            y, mm = int(m.group(1)), int(m.group(2))
            return dt.date(y, mm, 1) if (_yr_ok(y) and 1 <= mm <= 12) else None
        m = re.fullmatch(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})\.?", low)
        if m:
            y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return dt.date(y, mm, dd) if _yr_ok(y) else None
        m = re.fullmatch(r"(\d{4})[-/](\d{1,2})", low)
        if m:
            y, mm = int(m.group(1)), int(m.group(2))
            return dt.date(y, mm, 1) if (_yr_ok(y) and 1 <= mm <= 12) else None
    except (ValueError, KeyError):
        return None
    return None


MISSING = {"", "-", "–", "—", "x", "..", "...", "…", ":", ".", "n.a.", "na", "n/a", "·"}


def parse_subperiod(s: str):
    """Sub-year period labels found in KSH 'Period' columns, to be combined with
    a Year column: January / január / Q1 / 1st quarter / I. negyedév / 1. félév.
    Returns (kind, n) with kind in M/Q/H/Y, or None."""
    if not s:
        return None
    t = re.sub(r"\s+", " ", s.strip())
    t = re.sub(r"\((?:\d+|[a-z])\)$", "", t).strip().rstrip("*+°^").strip().lower().rstrip(".")
    if not t:
        return None
    if t in ("total", "year", "annual", "yearly", "year total", "éves", "év", "összesen", "altogether"):
        return ("Y", 0)
    m = re.fullmatch(r"([a-záéíóöőúüű]+)\s*[–\-−]\s*([a-záéíóöőúüű]+)", t)
    if m:  # cumulated ranges: only January–December equals the full year; others skipped
        if MONTHS.get(m.group(1)) == 1 and MONTHS.get(m.group(2)) == 12:
            return ("Y", 0)
        return None
    if t in MONTHS:
        return ("M", MONTHS[t])
    m = re.fullmatch(r"q\s*([1-4])", t)
    if m:
        return ("Q", int(m.group(1)))
    m = re.fullmatch(r"([1-4])\s*(?:st|nd|rd|th)?\s*quarter", t)
    if m:
        return ("Q", int(m.group(1)))
    m = re.fullmatch(r"quarter\s*([1-4])", t)
    if m:
        return ("Q", int(m.group(1)))
    m = re.fullmatch(r"(i{1,3}|iv)\.?\s*(?:negyed[ée]v|n[ée]|quarter)", t)
    if m:
        return ("Q", ROMAN_Q[m.group(1)])
    m = re.fullmatch(r"([1-4])\.?\s*negyed[ée]v", t)
    if m:
        return ("Q", int(m.group(1)))
    m = re.fullmatch(r"(?:1st|first|i)\.?\s*(?:half|f[ée]l[ée]v)(?:-?year)?", t)
    if m:
        return ("H", 1)
    m = re.fullmatch(r"(?:2nd|second|ii)\.?\s*(?:half|f[ée]l[ée]v)(?:-?year)?", t)
    if m:
        return ("H", 2)
    m = re.fullmatch(r"(\d{1,2})\.?\s*(?:h[óo]|month)", t)
    if m and 1 <= int(m.group(1)) <= 12:
        return ("M", int(m.group(1)))
    m = re.fullmatch(r"(\d{1,2})", t)
    if m and 1 <= int(m.group(1)) <= 12:
        return ("M", int(m.group(1)))
    return None


def parse_time_col(s: str) -> tuple[dt.date | None, str]:
    """Column-header time parser. Handles plain periods plus headers that embed
    a measure label with the year ('underweight, 2009' → (2009-12-31, 'underweight'))."""
    d = parse_time(s)
    if d is not None:
        return d, ""
    t = re.sub(r"\s+", " ", (s or "").strip())
    m = re.fullmatch(r"(.+?)[,;]\s*((?:18|19|20)\d{2})\.?", t)
    if m:
        y = int(m.group(2))
        if _yr_ok(y):
            return dt.date(y, 12, 31), m.group(1).strip(" ,;")
    return None, ""


_TIMELIKE_CUM = re.compile(r"[qk]\s*[1-4]\s*[–\-−]\s*[qk]\s*[1-4][\s.]+\d{4}\.?", re.IGNORECASE)


def looks_timelike(c: str) -> bool:
    """Used only for time-in-columns detection: real periods plus cumulated
    quarter ranges (which may not yield a date but mark a time header row)."""
    if parse_time_col(c)[0] is not None:
        return True
    return bool(_TIMELIKE_CUM.fullmatch(re.sub(r"\s+", " ", c.strip())))


def combine_subperiod(year: int, sp: tuple) -> dt.date | None:
    kind, n = sp
    try:
        if kind == "M":
            return dt.date(year, n, 1)
        if kind == "Q":
            return dt.date(year, (n - 1) * 3 + 1, 1)
        if kind == "H":
            return dt.date(year, 1 if n == 1 else 7, 1)
        if kind == "Y":
            return dt.date(year, 12, 31)
    except ValueError:
        return None
    return None


def parse_num(s: str) -> float | None:
    """Hungarian/English numbers: '5 855,6' → 5855.6; placeholders → None."""
    if s is None:
        return None
    t = s.strip()
    if t.lower() in MISSING:
        return None
    t = t.rstrip("*+").strip()
    t = t.replace("\xa0", "").replace(" ", "").replace(" ", "")
    t = t.replace(",", ".")
    if not re.fullmatch(r"[+-]?\d+(\.\d+)?", t):
        return None
    try:
        v = float(t)
        return v if v == v else None
    except ValueError:
        return None


def sanitize(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    s = s.replace(":", "/").replace(";", ",")
    return s[:240]


def make_key(tid: str, row_lab: str, col_lab: str) -> str:
    parts = ["KSH", tid] + [p for p in (sanitize(row_lab), sanitize(col_lab)) if p]
    return ":".join(parts)


def _ffill(row: list[str]) -> list[str]:
    """Forward-fill merged header cells (never bleed col0 into the data columns)."""
    out = list(row)
    cur = ""
    for j in range(1, len(out)):
        if out[j]:
            cur = out[j]
        else:
            out[j] = cur
    return out


# ---------------------------------------------------------------- table parser
def parse_table(tid: str, txt: str) -> tuple[list[tuple[str, dt.date, float]], str | None]:
    rdr = csv.reader(io.StringIO(txt), delimiter=";")
    raw = [[(c or "").strip() for c in row] for row in rdr]
    rows = [r for r in raw if any(c for c in r)]
    if not rows:
        return [], "empty file"
    W = max(len(r) for r in rows)
    if W < 2:
        return [], "single column"
    rows = [r + [""] * (W - len(r)) for r in rows]

    # drop leading title rows (only first cell populated), max 3 — keep the text
    title, drops = "", 0
    while rows and drops < 3 and rows[0][0] and not any(c for c in rows[0][1:]):
        title = f"{title} {rows[0][0]}".strip()
        rows.pop(0)
        drops += 1
    if len(rows) < 2:
        return [], "no data rows"

    # ---- orientation 1: time in first column ----
    col0_time = [i for i, r in enumerate(rows) if parse_time(r[0]) is not None]
    if col0_time and col0_time[0] <= 10 and len(col0_time) >= 2:
        return emit_time_in_rows(tid, rows, col0_time[0], W), None

    # ---- orientation 2: time in a header row ----
    for i in range(min(8, len(rows))):
        cells = [c for c in rows[i][1:] if c]
        if len(cells) < 2:
            continue
        n_t = sum(1 for c in cells if looks_timelike(c))
        if n_t >= 2 and n_t >= 0.4 * len(cells):
            return emit_time_in_cols(tid, rows, i, W), None

    # weak fallback: a single time-bearing row in col0
    if col0_time and col0_time[0] <= 10:
        return emit_time_in_rows(tid, rows, col0_time[0], W), None

    # ---- orientation 3: year only in the table title, months/quarters as
    # divider rows (KSH 'current year' snapshot tables) ----
    yrs = re.findall(r"\b((?:18|19|20)\d{2})\b", title)
    if yrs and _yr_ok(int(yrs[-1])):
        res = emit_title_year(tid, rows, int(yrs[-1]), W)
        if res:
            return res, None
    return [], "no parseable time dimension"


def _cell_class(data, j):
    """Count numeric vs. textual cells in column j, ignoring missing markers."""
    n_num = n_txt = 0
    for r in data:
        c = r[j]
        if not c or c.lower() in MISSING:
            continue
        if parse_num(c) is not None:
            n_num += 1
        else:
            n_txt += 1
    return n_num, n_txt


def emit_time_in_rows(tid, rows, r0, W):
    header = rows[:r0]
    section = ""
    # trailing header rows with only col0 populated are block dividers, not headers
    while header and header[-1][0] and not any(c for c in header[-1][1:]):
        section = header[-1][0]
        header = header[:-1]
    data = rows[r0:]
    H = [_ffill(h) for h in header[:-1]] + header[-1:] if header else []

    def col_label(j):
        parts = [h[j] for h in H if j < len(h) and h[j]]
        return " ".join(parts) if parts else f"col{j}"

    # classify columns: numeric-majority (among non-missing cells) → value column
    val_col = {}
    for j in range(1, W):
        n_num, n_txt = _cell_class(data, j)
        ok = n_num >= 1 and n_num >= 0.6 * (n_num + n_txt)
        if ok and col_label(j).lower().strip() in ("code", "kód", "ksh code", "ksh-kód"):
            ok = False
        val_col[j] = ok
    label_cols = [j for j in range(1, W) if not val_col[j]]

    # detect a 'Period' column (months/quarters) to combine with the Year column
    sub_col = None
    for j in label_cols:
        vals = [r[j] for r in data if r[j] and r[j].lower() not in MISSING]
        if len(vals) >= 2:
            n_sp = sum(1 for c in vals if parse_subperiod(c) is not None)
            if n_sp >= 0.3 * len(vals):
                sub_col = j
                break
    other_label_cols = [j for j in label_cols if j != sub_col]

    out = []
    cur0 = ""
    for r in data:
        c0 = r[0] or cur0                              # merged Year cells → forward-fill
        d0 = parse_time(c0)
        if d0 is None:
            if r[0] and not any(c for c in r[1:]):
                section = r[0]                         # block divider row
                continue
            continue                                   # footnote / source rows
        cur0 = c0
        if sub_col is not None:
            sp = parse_subperiod(r[sub_col])
            if sp is None:
                continue                               # unmapped range/period label
            d = combine_subperiod(d0.year, sp)
            if d is None:
                continue
        else:
            d = d0
        row_lab = " ".join(p for p in [section] + [r[j] for j in other_label_cols] if p)
        for j in range(1, W):
            if not val_col[j]:
                continue
            v = parse_num(r[j])
            if v is None:
                continue
            out.append((make_key(tid, row_lab, col_label(j)), d, v))
    return out


def emit_time_in_cols(tid, rows, t_idx, W):
    # header block: everything through the time row, plus up to 2 following
    # sub-header rows that contain no numeric cells
    hdr_end = t_idx + 1
    extra = 0
    while hdr_end < len(rows) and extra < 2:
        tail = [c for c in rows[hdr_end][1:] if c]
        if not tail or any(parse_num(c) is not None for c in tail):
            break
        hdr_end += 1
        extra += 1

    header, data = rows[:hdr_end], rows[hdr_end:]
    time_row = rows[t_idx]
    col_date, col_resid = {}, {}
    for j in range(1, W):
        d, resid = parse_time_col(time_row[j])
        col_date[j] = d
        col_resid[j] = resid
    others = [_ffill(h) for k, h in enumerate(header) if k < t_idx] + \
             [h for k, h in enumerate(header) if t_idx < k < hdr_end]

    def col_label(j):
        parts = [h[j] for h in others if j < len(h) and h[j]]
        if col_resid.get(j):
            parts.append(col_resid[j])
        return " ".join(parts)

    # undated columns that are mostly text act as extra row-label columns
    extra_label_cols = []
    for j in range(1, W):
        if col_date.get(j) is not None:
            continue
        n_num, n_txt = _cell_class(data, j)
        if (n_num + n_txt) and n_num < 0.6 * (n_num + n_txt):
            extra_label_cols.append(j)

    out = []
    section = ""
    for r in data:
        has_num = any(parse_num(r[j]) is not None for j in range(1, W) if col_date.get(j))
        if not has_num:
            if r[0] and not any(c for c in r[1:]):
                section = r[0]                         # group divider row
            continue
        row_lab = " ".join(p for p in [section, r[0]] + [r[j] for j in extra_label_cols] if p)
        for j in range(1, W):
            d = col_date.get(j)
            if d is None:
                continue
            v = parse_num(r[j])
            if v is None:
                continue
            out.append((make_key(tid, row_lab, col_label(j)), d, v))
    return out


def emit_title_year(tid, rows, year, W):
    """Orientation 3: single-year snapshot tables — the year lives in the title,
    months/quarters appear as divider rows ('January;;;;'), categories in col0."""
    hdr, i = [], 0
    while i < len(rows):
        r = rows[i]
        only0 = bool(r[0]) and not any(c for c in r[1:])
        has_num = any(parse_num(c) is not None for c in r[1:] if c)
        if only0 or has_num:
            break
        hdr.append(r)
        i += 1
    if not hdr:
        return []
    H = [_ffill(h) for h in hdr[:-1]] + hdr[-1:]

    def col_label(j):
        parts = [h[j] for h in H if j < len(h) and h[j]]
        return " ".join(parts) if parts else f"col{j}"

    data = rows[i:]
    val_col = {}
    for j in range(1, W):
        n_num, n_txt = _cell_class(data, j)
        val_col[j] = n_num >= 1 and n_num >= 0.6 * (n_num + n_txt)
    label_cols = [j for j in range(1, W) if not val_col[j]]
    if not any(val_col.values()):
        return []

    out, section, period = [], "", None
    for r in data:
        if r[0] and not any(c for c in r[1:]):
            sp = parse_subperiod(r[0])
            if sp is not None:
                period = sp                      # month/quarter divider
            else:
                section = r[0]                   # block divider
            continue
        if period is None:
            continue
        d = combine_subperiod(year, period)
        if d is None:
            continue
        row_lab = " ".join(p for p in ([section, r[0]] + [r[j] for j in label_cols]) if p)
        for j in range(1, W):
            if not val_col[j]:
                continue
            v = parse_num(r[j])
            if v is None:
                continue
            out.append((make_key(tid, row_lab, col_label(j)), d, v))
    return out


# ---------------------------------------------------------------- catalog
def load_catalog() -> list[dict]:
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, encoding="utf-8") as f:
            tables = json.load(f)
        log(f"Loaded catalog: {len(tables)} tables")
        return tables
    log("Fetching KSH STADAT toc.json ...")
    raw = get_bytes(f"{BASE}/toc.json")
    if raw is None:
        log("FATAL: cannot fetch toc.json")
        return []
    j = json.loads(raw.decode("utf-8-sig"))
    tables = [t for t in j.get("tables", []) if isinstance(t, dict) and t.get("id")]
    os.makedirs(OUT, exist_ok=True)
    with open(CATALOG_FILE, "w", encoding="utf-8") as f:
        json.dump(tables, f, ensure_ascii=False)
    log(f"Catalog: {len(tables)} tables")
    return tables


def main():
    os.makedirs(OUT, exist_ok=True)
    tables = load_catalog()
    if not tables:
        return

    by_theme: dict[str, list] = defaultdict(list)
    for t in tables:
        tid = str(t["id"])
        theme = tid[:3].lower()
        if re.fullmatch(r"[a-z]{3}", theme):
            by_theme[theme].append(t)
    log(f"Processing {sum(len(v) for v in by_theme.values())} tables in {len(by_theme)} themes")

    total_obs = 0
    for theme in sorted(by_theme.keys()):
        theme_tables = by_theme[theme]
        out_path = os.path.join(OUT, f"{theme}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  Skip {theme}: {n:,} rows")
            total_obs += n
            continue

        log(f"  Theme '{theme}': {len(theme_tables)} tables")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()

        for i, t in enumerate(theme_tables):
            tid = str(t["id"])
            try:
                raw = get_bytes(f"{BASE}/{theme}/en/{tid}.csv")
                time.sleep(RATE + random.uniform(0.0, 0.8))   # jitter — uniform cadence looks bot-like
                if raw is None:
                    log(f"    [{i+1}/{len(theme_tables)}] {tid}: not found / fetch failed")
                    if WAF_FAILS[0] >= 3:
                        log("ABORT: persistent WAF block. Finished themes are kept; "
                            "rerun this script later to resume (this theme restarts).")
                        return
                    continue
                try:
                    txt = raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    txt = raw.decode("cp1250", errors="replace")
                rows, why = parse_table(tid, txt)
                n = 0
                for key, d, v in rows:
                    tok = (key, d)
                    if tok not in seen:
                        seen.add(tok)
                        all_keys.append(key)
                        all_dates.append(d)
                        all_vals.append(v)
                        n += 1
                if why:
                    log(f"    [{i+1}/{len(theme_tables)}] {tid}: skip ({why})")
                else:
                    log(f"    [{i+1}/{len(theme_tables)}] {tid}: {n:,} obs")
            except Exception as e:
                log(f"    [{i+1}/{len(theme_tables)}] {tid} ERR: {e}")

        if all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n = pq.read_metadata(out_path).num_rows
            log(f"  {theme}: {n:,} obs saved")
            total_obs += n
        else:
            log(f"  {theme}: 0 obs, no parquet written")

    log(f"DONE: {total_obs:,} total KSH STADAT observations")


if __name__ == "__main__":
    main()
