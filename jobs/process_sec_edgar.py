#!/usr/bin/env python3
r"""Turn the downloaded SEC EDGAR bulk into point-in-time series (the NUMBERS).

This is the *process* step that pairs with jobs/fetch_sec_edgar_bulk.py. The fetch
job dropped two archives in data/raw/sec_edgar/:

  companyfacts.zip  -- every filer's XBRL financial-statement facts (the values we host)
  submissions.zip   -- every filing's metadata (the POINTERS we index; see notes below)

What this module does
---------------------
It reads companyfacts.zip member-by-member (each ``CIK##########.json`` is one
filer), maps the numeric CIK to a stock ticker via company_tickers.json, and walks
``facts."us-gaap".<tag>.units.<unit>`` -- a list of reported data points, each with
``end`` (the period-end the figure describes), ``val`` (the reported number), and
``filed`` (the date that filing hit EDGAR). Every point becomes one point-in-time
Observation:

    series_id    = "sec_edgar:<TICKER>:<tag>:<unit>"
    obs_date     = parse(end)            # what period the number describes
    value        = float(val)
    version      = "clean"
    vintage_date = parse(filed)          # when that number was first available (ALFRED-style)

License: us-public-domain (U.S. SEC EDGAR is public domain; re-serveable -- see
core/licenses.py).

Why keep ``filed`` as the vintage
---------------------------------
A single period-end is reported, then *restated*, across several filings. Keeping
``filed`` as ``vintage_date`` preserves the as-first-released vs as-revised history
(the same point-in-time discipline ALFRED applies to FRED). We therefore do NOT
collapse multiple vintages of the same ``end`` into one row -- each filing's value is
its own Observation.

A note on the one genuine collision this can create: XBRL sometimes reports two facts
with the *same* ``end`` but different period *durations* (e.g. a fiscal-year total and
its closing quarter both end 2018-09-29). They share ``end`` and often ``filed``, yet
are different numbers. The connector contract keys nothing, so we emit both faithfully
and stash the period ``start``, ``form``, ``fp`` and ``accn`` (accession no.) in each
Observation's ``flags`` so the duration/source context survives for any downstream
de-duplication or frame-aware reshaping.

This module is import-safe and side-effect free at import time. Run it directly to
SMOKE-TEST on a handful of large filers (AAPL, MSFT, AMZN, JPM, KO):

    python D:/research/econfindatalibrary/jobs/process_sec_edgar.py

It deliberately does NOT touch data/catalog.db or data/clean/ -- it only reads the raw
archives and prints what it would have produced. Wiring the output into the catalog /
Parquet store is the job of jobs/run_connector.py, exactly as for every other source.


===========================================================================
FILING POINTERS from submissions.zip  (the INDEX side -- outlined, not built here)
===========================================================================
companyfacts gives us the *numbers*. submissions.zip gives us the *pointers*: one
``CIK##########.json`` per filer holding filing metadata only -- we never host the
documents, we link back to sec.gov. (Very prolific filers spill older filings into
paginated siblings ``CIK##########-submissions-NNN.json``, listed under
``filings.files`` of the main member.)

Shape of a submissions member::

    {
      "cik": "0000320193", "name": "Apple Inc.",
      "tickers": ["AAPL"], "exchanges": ["Nasdaq"],
      "sic": 3571, "sicDescription": "Electronic Computers",
      "fiscalYearEnd": "0928", "stateOfIncorporation": "CA", ...,
      "filings": {
        "recent": {                       # PARALLEL ARRAYS (columnar), index i = one filing
          "accessionNumber":   ["0000320193-18-000145", ...],
          "filingDate":        ["2018-11-05", ...],
          "reportDate":        ["2018-09-29", ...],
          "acceptanceDateTime":["2018-11-05T18:01:00.000Z", ...],
          "form":              ["10-K", ...],
          "primaryDocument":   ["aapl-20180929.htm", ...],
          "primaryDocDescription": ["10-K", ...],
          "isXBRL": [...], "isInlineXBRL": [...], "size": [...],
          "fileNumber": [...], "items": [...], ...
        },
        "files": [ {"name": "CIK0000320193-submissions-001.json",
                    "filingCount": 1234,
                    "filingFrom": "1994-01-26", "filingTo": "2015-05-11"} ]
      }
    }

A filing pointer record would therefore carry, per filing i:
  - ticker / CIK / company name / SIC
  - accession number, form type (10-K, 10-Q, 8-K, 4, ...)
  - filing date, report (period) date, acceptance datetime
  - primary document name + description, isXBRL flags, size

...and the canonical sec.gov URLs, built (not stored) from CIK + accession number:
  accn      = "0000320193-18-000145"
  accn_nodash = accn.replace("-", "")            -> "000032019318000145"
  cik_int   = int(cik)                            -> 320193   (NO zero-padding in the path)
  folder    = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}/"
  index     = folder + f"{accn}-index.htm"        # the filing's human index page
  primary   = folder + primaryDocument            # the actual 10-K/10-Q/8-K document

We index those pointers (form, dates, URL) for search/links; the documents themselves
stay on sec.gov. ``iter_filing_pointers()`` below is a ready-to-use generator that
yields exactly these records (it is NOT exercised by the smoke test, which stays on the
fast companyfacts path, but it is import-clean and unit-checkable).
"""
from __future__ import annotations

import json
import os
import sys
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Iterable, Iterator, Optional

# Make the connector contract importable whether run from repo root or elsewhere.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from connectors.base import Observation, SeriesMeta  # noqa: E402

# --- locations (Windows D:/ paths; no POSIX /d/ here) ----------------------
RAW_DIR = os.path.join(ROOT, "data", "raw", "sec_edgar")
COMPANYFACTS_ZIP = os.path.join(RAW_DIR, "companyfacts.zip")
SUBMISSIONS_ZIP = os.path.join(RAW_DIR, "submissions.zip")
TICKERS_JSON = os.path.join(RAW_DIR, "company_tickers.json")

SOURCE_ID = "sec_edgar"
LICENSE_ID = "us-public-domain"
ATTRIBUTION = "Source: U.S. Securities and Exchange Commission (EDGAR), public domain."
HOMEPAGE = "https://www.sec.gov/edgar"
TAXONOMY = "us-gaap"   # we host the us-gaap facts (the financial-statement taxonomy)


# ===========================================================================
# CIK <-> ticker map
# ===========================================================================
def load_cik_to_ticker(path: str = TICKERS_JSON) -> dict[int, str]:
    """company_tickers.json is ``{"0": {"cik_str": 320193, "ticker": "AAPL", ...}, ...}``.

    Returns ``{cik_int: TICKER}``. When several share classes map to one CIK (rare in
    this file), the first one encountered wins -- deterministic given file order.
    """
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    out: dict[int, str] = {}
    for rec in raw.values():
        try:
            cik = int(rec["cik_str"])
            ticker = str(rec["ticker"]).strip()
        except (KeyError, TypeError, ValueError):
            continue
        if ticker and cik not in out:
            out[cik] = ticker
    return out


def cik_member_name(cik: int) -> str:
    """companyfacts/submissions members are zero-padded to 10 digits: CIK0000320193.json."""
    return f"CIK{cik:010d}.json"


# ===========================================================================
# companyfacts.zip  ->  series  (the NUMBERS)
# ===========================================================================
def _parse_iso(s: object) -> Optional[date]:
    """Lenient ISO-date parse: 'YYYY-MM-DD' (EDGAR's format). Returns None on junk."""
    if not isinstance(s, str) or len(s) < 10:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _coerce_float(v: object) -> Optional[float]:
    """val is normally int/float; guard against null/blank/garbage defensively."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(",", "").strip())
        except ValueError:
            return None
    return None


def _frequency_hint(points: list[dict]) -> str:
    """Cheap frequency guess from the XBRL 'frame' tags on the points.

    Frames look like CY2018 (annual), CY2018Q3 (quarterly), CY2018Q3I (instant).
    Purely advisory metadata for the catalog -- the values themselves are unchanged.
    """
    has_q = has_a = False
    for p in points:
        fr = p.get("frame")
        if not isinstance(fr, str):
            continue
        if "Q" in fr:
            has_q = True
        else:
            has_a = True
    if has_q and not has_a:
        return "Q"
    if has_a and not has_q:
        return "A"
    return "irregular"


def series_meta(ticker: str, tag: str, unit: str, *, label: Optional[str],
                description: Optional[str], frequency: str) -> SeriesMeta:
    """Build the SeriesMeta for one (ticker, tag, unit) triple."""
    sid = f"{SOURCE_ID}:{ticker}:{tag}:{unit}"
    title = f"{ticker} - {label or tag} ({unit})"
    return SeriesMeta(
        series_id=sid,
        title=title,
        frequency=frequency,
        unit=unit,
        geography="US",
        category="company-financials",
        license_id=LICENSE_ID,
        metadata={
            "ticker": ticker,
            "taxonomy": TAXONOMY,
            "tag": tag,
            "unit": unit,
            "label": label,
            "description": description,
        },
    )


def observations_for_unit(series_id: str, points: list[dict]) -> list[Observation]:
    """One Observation per reported data point -- ALL vintages kept (point-in-time).

    Each XBRL point carries end/val/filed plus context (start, form, fp, accn). We map
    end->obs_date, val->value, filed->vintage_date, and preserve the rest in ``flags`` so
    a fiscal-year total and its closing quarter (same ``end``) stay distinguishable.
    """
    out: list[Observation] = []
    for p in points:
        obs_date = _parse_iso(p.get("end"))
        value = _coerce_float(p.get("val"))
        if obs_date is None or value is None:
            continue
        flags: tuple = tuple(
            f"{k}={p[k]}" for k in ("start", "form", "fp", "accn") if p.get(k)
        )
        out.append(
            Observation(
                series_id=series_id,
                obs_date=obs_date,
                value=value,
                version="clean",
                flags=flags,
                vintage_date=_parse_iso(p.get("filed")),
            )
        )
    out.sort(key=lambda o: (o.obs_date, o.vintage_date or date.min))
    return out


def iter_company_series(
    cik_to_ticker: dict[int, str],
    *,
    only_ciks: Optional[set[int]] = None,
    zip_path: str = COMPANYFACTS_ZIP,
) -> Iterator[tuple[SeriesMeta, list[Observation]]]:
    """Stream (SeriesMeta, [Observation]) for the us-gaap facts of each filer.

    Reads companyfacts.zip member by member (1.39 GB archive, ~20k members) so memory
    stays flat -- only one filer's JSON is resident at a time. Pass ``only_ciks`` to
    restrict to a subset (used by the smoke test); otherwise every mapped filer with
    facts is processed. Filers absent from the ticker map are skipped (no ticker -> no
    series id we can publish under).
    """
    if only_ciks is not None:
        wanted_members = {cik_member_name(c): c for c in only_ciks
                          if c in cik_to_ticker}
    else:
        wanted_members = None  # take everything in the archive

    with zipfile.ZipFile(zip_path) as zf:
        names = (sorted(wanted_members) if wanted_members is not None
                 else zf.namelist())
        for name in names:
            if not name.endswith(".json"):
                continue
            # CIK0000320193.json -> 320193
            try:
                cik = int(name[3:-5])
            except ValueError:
                continue
            ticker = cik_to_ticker.get(cik)
            if not ticker:
                continue  # only emit series for filers we can name with a ticker
            try:
                with zf.open(name) as fh:
                    doc = json.load(fh)
            except (KeyError, json.JSONDecodeError, zipfile.BadZipFile):
                continue
            gaap = (doc.get("facts") or {}).get(TAXONOMY) or {}
            for tag, tagdoc in gaap.items():
                units = (tagdoc or {}).get("units") or {}
                label = tagdoc.get("label")
                description = tagdoc.get("description")
                for unit, points in units.items():
                    if not isinstance(points, list) or not points:
                        continue
                    meta = series_meta(
                        ticker, tag, unit,
                        label=label, description=description,
                        frequency=_frequency_hint(points),
                    )
                    obs = observations_for_unit(meta.series_id, points)
                    if obs:
                        yield meta, obs


# ===========================================================================
# submissions.zip  ->  filing POINTERS  (the INDEX -- metadata + sec.gov URL only)
# ===========================================================================
@dataclass
class FilingPointer:
    """One filing's metadata + its canonical sec.gov URLs. We index this; the document
    stays on sec.gov. (Outlined deliverable -- not consumed by the smoke test.)"""
    ticker: Optional[str]
    cik: int
    company: str
    sic: Optional[str]
    form: str
    accession: str
    filing_date: Optional[date]
    report_date: Optional[date]
    acceptance_datetime: Optional[str]
    primary_document: Optional[str]
    primary_doc_description: Optional[str]
    is_xbrl: bool
    size: Optional[int]
    index_url: str        # the filing's human index page on sec.gov
    primary_url: str      # the primary document on sec.gov (we LINK, never host)
    metadata: dict = field(default_factory=dict)


def filing_urls(cik: int, accession: str, primary_document: Optional[str]) -> tuple[str, str]:
    """Build (index_url, primary_url) on sec.gov from CIK + accession number.

    Path uses the UN-padded integer CIK and the de-dashed accession number, e.g.
      https://www.sec.gov/Archives/edgar/data/320193/000032019318000145/...
    """
    accn_nodash = accession.replace("-", "")
    folder = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accn_nodash}/"
    index_url = f"{folder}{accession}-index.htm"
    primary_url = folder + (primary_document or "")
    return index_url, primary_url


def _filing_pointers_from_recent(cik: int, company: str, ticker: Optional[str],
                                 sic: Optional[str], recent: dict) -> Iterator[FilingPointer]:
    """``recent`` is columnar (parallel arrays). Zip the columns back into records."""
    accns = recent.get("accessionNumber") or []
    n = len(accns)

    def col(key: str) -> list:
        v = recent.get(key) or []
        return v if len(v) == n else (list(v) + [None] * (n - len(v)))

    forms = col("form")
    filing_dates = col("filingDate")
    report_dates = col("reportDate")
    accept_dts = col("acceptanceDateTime")
    primary_docs = col("primaryDocument")
    primary_descs = col("primaryDocDescription")
    is_xbrls = col("isXBRL")
    sizes = col("size")

    for i in range(n):
        accession = accns[i]
        if not accession:
            continue
        index_url, primary_url = filing_urls(cik, accession, primary_docs[i])
        yield FilingPointer(
            ticker=ticker,
            cik=cik,
            company=company,
            sic=str(sic) if sic is not None else None,
            form=forms[i] or "",
            accession=accession,
            filing_date=_parse_iso(filing_dates[i]),
            report_date=_parse_iso(report_dates[i]),
            acceptance_datetime=accept_dts[i],
            primary_document=primary_docs[i],
            primary_doc_description=primary_descs[i],
            is_xbrl=bool(is_xbrls[i]),
            size=sizes[i] if isinstance(sizes[i], int) else None,
            index_url=index_url,
            primary_url=primary_url,
        )


def iter_filing_pointers(
    cik_to_ticker: dict[int, str],
    *,
    only_ciks: Optional[set[int]] = None,
    zip_path: str = SUBMISSIONS_ZIP,
    include_older: bool = True,
) -> Iterator[FilingPointer]:
    """Stream FilingPointer records from submissions.zip (metadata + sec.gov URL only).

    For each filer's main member we read ``filings.recent``; when ``include_older`` we
    also pull the paginated ``filings.files`` siblings (older filings) named
    ``CIK##########-submissions-NNN.json``. This is the INDEX path -- it never downloads
    or stores filing documents. Provided as the working outline of the pointer side; the
    smoke test below stays on the companyfacts (numbers) path.
    """
    with zipfile.ZipFile(zip_path) as zf:
        available = set(zf.namelist())
        targets = (sorted(cik_member_name(c) for c in only_ciks)
                   if only_ciks is not None else None)
        members = targets if targets is not None else sorted(
            n for n in available if "-submissions-" not in n and n.endswith(".json"))
        for name in members:
            if name not in available:
                continue
            try:
                with zf.open(name) as fh:
                    doc = json.load(fh)
            except (KeyError, json.JSONDecodeError, zipfile.BadZipFile):
                continue
            cik_raw = doc.get("cik")
            try:
                cik = int(cik_raw)
            except (TypeError, ValueError):
                continue
            company = doc.get("name") or ""
            ticker = cik_to_ticker.get(cik) or (
                (doc.get("tickers") or [None])[0])
            sic = doc.get("sic")
            filings = doc.get("filings") or {}
            recent = filings.get("recent") or {}
            yield from _filing_pointers_from_recent(cik, company, ticker, sic, recent)
            if include_older:
                for pg in (filings.get("files") or []):
                    pg_name = pg.get("name")
                    if not pg_name or pg_name not in available:
                        continue
                    try:
                        with zf.open(pg_name) as fh2:
                            page = json.load(fh2)
                    except (KeyError, json.JSONDecodeError, zipfile.BadZipFile):
                        continue
                    # older pages are themselves a columnar block of the same shape
                    yield from _filing_pointers_from_recent(cik, company, ticker, sic, page)


# ===========================================================================
# SMOKE TEST  (reads only; never writes catalog.db or data/clean/)
# ===========================================================================
SMOKE_TICKERS = ["AAPL", "MSFT", "AMZN", "JPM", "KO"]


def _smoke_test() -> int:
    for label, path in (
        ("company_tickers.json", TICKERS_JSON),
        ("companyfacts.zip", COMPANYFACTS_ZIP),
    ):
        if not os.path.exists(path):
            print(f"[smoke] MISSING {label}: {path}", file=sys.stderr)
            return 1

    cik_to_ticker = load_cik_to_ticker()
    print(f"[smoke] CIK->ticker map: {len(cik_to_ticker):,} tickers")

    ticker_to_cik = {t: c for c, t in cik_to_ticker.items()}
    want_ciks: set[int] = set()
    for t in SMOKE_TICKERS:
        c = ticker_to_cik.get(t)
        if c is None:
            print(f"[smoke] WARNING: {t} not in ticker map")
        else:
            want_ciks.add(c)
    print(f"[smoke] processing {len(want_ciks)} filers: "
          f"{', '.join(SMOKE_TICKERS)}")

    n_series = n_obs = 0
    per_ticker: dict[str, int] = {}
    aapl_revenue: Optional[Observation] = None
    aapl_revenue_meta: Optional[SeriesMeta] = None
    samples: list[tuple[str, int, Observation]] = []

    for meta, obs in iter_company_series(cik_to_ticker, only_ciks=want_ciks):
        n_series += 1
        n_obs += len(obs)
        tkr = meta.metadata["ticker"]
        per_ticker[tkr] = per_ticker.get(tkr, 0) + 1
        if meta.series_id == "sec_edgar:AAPL:Revenues:USD":
            aapl_revenue_meta = meta
            aapl_revenue = obs[-1]  # last by (obs_date, vintage)
        if len(samples) < 5:
            samples.append((meta.series_id, len(obs), obs[-1]))

    print("\n[smoke] ============ RESULTS ============")
    print(f"[smoke] series produced : {n_series:,}")
    print(f"[smoke] observations     : {n_obs:,}")
    print(f"[smoke] series per ticker:")
    for t in SMOKE_TICKERS:
        print(f"          {t:<6} {per_ticker.get(t, 0):>6,} series")

    print("\n[smoke] sample series (id | #obs | last obs):")
    for sid, k, o in samples:
        print(f"          {sid}")
        print(f"              {k:,} obs; last {o.obs_date} = {o.value:,.0f} "
              f"(filed {o.vintage_date})")

    print("\n[smoke] AAPL Revenues (us-gaap:Revenues, USD) last value:")
    if aapl_revenue is not None and aapl_revenue_meta is not None:
        o = aapl_revenue
        print(f"          series_id : {aapl_revenue_meta.series_id}")
        print(f"          title     : {aapl_revenue_meta.title}")
        print(f"          last point: {o.obs_date} = ${o.value:,.0f} "
              f"(vintage/filed {o.vintage_date})")
        print(f"          flags     : {o.flags}")
        print("          note: Apple retired the bare us-gaap:Revenues tag after FY2018 "
              "(switched to RevenueFromContractWithCustomerExcludingAssessedTax),\n"
              "                so this tag's last point is its FY2018 closing quarter -- "
              "expected, not a gap in our parse.")
    else:
        print("          (sec_edgar:AAPL:Revenues:USD not found -- check inputs)")

    # Tiny pointer demo so the INDEX path is exercised too (metadata + URL only).
    if os.path.exists(SUBMISSIONS_ZIP):
        aapl_cik = ticker_to_cik.get("AAPL")
        if aapl_cik is not None:
            print("\n[smoke] filing POINTER demo (submissions.zip -- metadata + sec.gov URL, "
                  "NOT the document):")
            gen = iter_filing_pointers(cik_to_ticker, only_ciks={aapl_cik},
                                       include_older=False)
            fp = next(gen, None)
            if fp is not None:
                print(f"          {fp.ticker} {fp.form} accn={fp.accession} "
                      f"filed={fp.filing_date} report={fp.report_date}")
                print(f"          index : {fp.index_url}")
                print(f"          doc   : {fp.primary_url}")
            gen.close()
    else:
        print("\n[smoke] submissions.zip not present -- skipping pointer demo.")

    print("\n[smoke] done. (No writes to data/catalog.db or data/clean/.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_smoke_test())
