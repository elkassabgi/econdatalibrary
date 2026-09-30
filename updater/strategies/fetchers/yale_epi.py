"""S1 fetcher — Yale Environmental Performance Index (EPI), biennial country index.

CC BY-NC-SA 4.0 (Yale Center for Environmental Law & Policy) — NOT CC BY 4.0, which
this docstring claimed until 2026-07-28. Yale states verbatim on epi.yale.edu/about-epi:
"This work is under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License", and the downloads page links creativecommons.org/licenses/
by-nc-sa/4.0/. So re-use is NON-COMMERCIAL, attribution is REQUIRED, and derivatives
must be shared alike. Single grouped parquet
clean_full/yale_epi/yale_epi.parquet, schema (series_key, obs_date, value),
series_key 'EPI:{variable}:{iso}'. The EPI results CSV is re-estimated/re-published
each release (the 2024 edition is a CSV, now read from Yale's Dataverse archive - see KNOWN_URLS; wide format: code/iso/country columns
plus ~146 indicator columns). We re-fetch the WHOLE table and MERGE (dedup
series_key+obs_date, new wins on revision, never-shrink). One sub-unit (the CSV);
a 200 that parses 0 rows from a real body is structural.

RELEASES ARE DISCOVERED, NOT HARDCODED (2026-07-28). This fetcher used to pin
`epi2024results.csv` and probe the vintage of that one URL. Yale published EPI 2026
on 2026-07-07 at a DIFFERENT url — `epi2026results2026-07-07.xlsx` — so the pinned
file never changed, the probe never moved, and the source sat on 2024 data reporting
success while a whole new edition went unnoticed. Watching a fixed URL answers "did
this file change", not "did the publisher release something", and for a biennial
index those diverge exactly when it matters (ledger R73).

So the downloads page is scraped for every `epiYYYYresults*.{csv,xlsx}` link and all
of them are fetched. A page redesign that yields NO links is reported structurally
rather than silently falling back to a stale pin — but the known-good URLs are still
attempted, so a scrape failure costs a red run, never data.
"""
from __future__ import annotations
import csv
import datetime as dt
import hashlib
import io
import os
import re

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

SOURCE = "yale_epi"
DEDUP = ("series_key", "obs_date")

HOME = "https://epi.yale.edu/"
DOWNLOADS = "https://epi.yale.edu/downloads"          # the pre-September-2026 page; 404 since
# THE SITE MOVED (found 2026-09-29, daily gate run 36495274373: "2/2 sub-unit(s) returned 200 but parsed 0
# rows"). Yale restructured epi.yale.edu: /downloads answers 404, the home page links a YEAR-SCOPED page
# (/2026/downloads) whose files live under /sites/default/files/<yyyy-mm>/, and the old
# downloads/epi2024results.csv URL now answers 200 with an HTML page. So the downloads pages are found from
# the home page's /<year>/downloads links (plus the old page, harmlessly, in case it returns).
_YEAR_PAGE_RE = re.compile(r'href=["\']((?:(?:https?:)?//epi\.yale\.edu)?/(\d{4})/downloads/?(?:[?#][^"\']*)?)["\']',
                           re.I)
# Floor, not the source of truth: these keep historical editions reachable even if
# the downloads page stops listing them. A floor URL that answers with an HTML page is RETIRED, not broken -
# its data is already held (the merge never shrinks).
#
# THE 2024 EDITION IS THE PUBLISHER'S ARCHIVE COPY (2026-09-29, review R1299). epi.yale.edu stopped serving
# epi2024results.csv; its own archive page says "EPI Archives are hosted on Dataverse", and the 2024 dataset is
# doi:10.7910/DVN/ZLAHG0 (version 2.0, CC BY-NC-SA 4.0). Its results file is the 2025-03-16 REVISION of the
# June 2024 release we had served: 1,263 values differ (the wastewater indicators, recomputed up the tree), and
# the 23 countries with a numeric code under 100 - dropped by the original ingest's len(code)==3 filter
# (R1289) - are in it. A Dataverse file id names immutable bytes, so the id is pinned with the md5 the dataset
# lists for it; a body with another md5 is not that file.
DATAVERSE_2024 = "https://dataverse.harvard.edu/api/access/datafile/14094607?format=original"
KNOWN_URLS = [
    (DATAVERSE_2024, 2024),
]
PINNED_MD5 = {DATAVERSE_2024: "688e7ee38f02d7698e3f299a40ef3fc0"}
_LINK_RE = re.compile(r'href=["\']([^"\']*epi(\d{4})results[^"\']*\.(?:csv|xlsx))',
                      re.I)
# THE COUNTRY VOCABULARY, from the current edition. Our 21,300+ published ids use EPI's numeric `code`
# (ISO 3166-1 numeric); the 2026 results workbook ships only alpha-3 `iso`. The map used to be learned from
# the 2024 CSV, which Yale no longer serves - and without a map the workbook would be parsed into a
# DISJOINT alpha-3 id space (see _parse_epi_csv). Every CSV in the 2026 indicator zip carries both columns
# (220 countries; checked 2026-09-29 to cover all 182 country codes in the served store, 0 disagreement
# between its members, PLW 585 and KNA 659 as below).
_VOCAB_RE = re.compile(r'href=["\']([^"\']*epi(\d{4})_indicators_[^"\']*\.zip)', re.I)


class NoVocabulary(Exception):
    """A results file keyed on alpha-3 with no code<->iso map: parsing it would fork the published ids."""


def _abs(href: str) -> str:
    if href.startswith("//"):
        return "https:" + href
    return href if href.startswith("http") else "https://epi.yale.edu" + (href if href.startswith("/") else "/" + href)


def _download_pages():
    """The downloads pages to scan: every /<year>/downloads page the home page links, then the old page."""
    pages = []
    try:
        r = requests.get(HOME, headers=UA, timeout=120, allow_redirects=True)
        if r.status_code == 200:
            pages = sorted({_abs(h) for h, _yr in _YEAR_PAGE_RE.findall(r.text)})
    except (requests.Timeout, requests.ConnectionError):
        pass
    return pages + [DOWNLOADS]


def _scan():
    """(results [(url, year)], vocabulary zips [(url, year)]) from every downloads page that answers 200."""
    results, vocab = {}, {}
    for page in _download_pages():
        try:
            r = requests.get(page, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            continue
        if r.status_code != 200:
            continue
        for href, yr in _LINK_RE.findall(r.text):
            results[_abs(href)] = int(yr)
        for href, yr in _VOCAB_RE.findall(r.text):
            vocab[_abs(href)] = int(yr)
    return (sorted(results.items(), key=lambda kv: kv[1]), sorted(vocab.items(), key=lambda kv: kv[1]))


def _is_html(resp) -> bool:
    ct = (resp.headers.get("content-type") or "").lower()
    return "text/html" in ct or resp.content[:64].lstrip().lower().startswith((b"<!doctype", b"<html"))


def _vocab_from_zip(data: bytes) -> dict:
    """{ISO3 -> numeric code} from the first member CSV that carries both `code` and `iso`; {} if none."""
    import zipfile                                           # noqa: PLC0415
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        return {}
    for m in z.namelist():
        if m.lower().endswith(".csv"):
            idx = _country_index(z.read(m))
            if idx:
                return idx
    return {}

# Countries EPI 2026 added that the 2024 edition never listed, so the vocabulary map
# derived from 2024 cannot translate them and they would be dropped (~746 real
# observations). The published `code` column is ISO 3166-1 NUMERIC — verified by
# spot-check: EPI's Afghanistan is 4, and ISO 3166-1 numeric for AFG is 004. These
# two were looked up in the ISO 3166-1 table, NOT typed from memory, and are written
# unpadded to match EPI's own formatting.
EXTRA_COUNTRY_CODES = {
    "PLW": "585",   # Palau
    "KNA": "659",   # Saint Kitts and Nevis
}


def _discover():
    """[(url, year)] for every results file the downloads pages currently list."""
    return _scan()[0]


def current_vintage(unit):
    """Vintage covers the SET of published releases, not one pinned file.

    Combining the discovered URL list with each file's ETag/Last-Modified means the
    signal moves both when Yale revises an existing edition AND when it publishes a
    new one at a new URL — the case that let EPI 2026 go unnoticed for three weeks.
    """
    found = _discover()
    urls = [u for u, _ in found] or [u for u, _ in KNOWN_URLS]
    parts = ["|".join(sorted(urls))]
    for u in urls:
        v = http_vintage(u)
        if v:
            parts.append(f"{u}={v}")
    return "; ".join(parts) or None


def _country_index(data: bytes):
    """{ISO3 -> the country code our published ids actually use}, read from a file
    that carries both. Returns {} when the file has only one vocabulary."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    num = next((h for h in headers if h.lower() in ("code",)), None)
    alpha = next((h for h in headers if h.lower() in ("iso", "iso3")), None)
    if not (num and alpha):
        return {}
    out = {}
    for row in reader:
        a, n = (row.get(alpha) or "").strip(), (row.get(num) or "").strip()
        if len(a) == 3 and n:
            out[a] = n
    return out


def _parse_epi_csv(data: bytes, default_year: int, iso_index=None):
    """Parse EPI results CSV — wide: country column + many indicator columns.
    The same row rules as jobs/ingest_yale_epi.parse_epi_csv (both now accept the unpadded numeric
    `code`, review R1289), plus the country-vocabulary translation and the no-vocabulary refusal below."""
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []

    iso3_col = next((h for h in headers if h.lower() in
                     ("iso", "iso3", "iso_code", "country_iso3", "code")), None)
    year_col = next((h for h in headers if h.lower() in ("year", "yr")), None)
    if not iso3_col:
        return [], [], []

    # WHICH COUNTRY VOCABULARY THE IDS USE. The 2024 CSV carries BOTH `code`
    # (ISO 3166-1 NUMERIC: Afghanistan = 4) and `iso` (alpha-3: AFG), and this picks
    # whichever appears first in the file — which is `code`. So every one of the
    # 21,300 published yale_epi ids is keyed on the NUMERIC code (EPI:AGR.new:4).
    # The 2026 workbook ships only `iso`, so parsing it naively produced
    # EPI:AGR.new:AFG — a completely disjoint id space. Merging that would not have
    # failed: it would have added 63,354 brand-new series while all 21,300 live ones
    # stayed frozen at 2024, and the source's newest observation would read 2026,
    # making the health gate green over a source that had stopped updating. Exactly
    # the failure this whole day has been about.
    #
    # So alpha-3 is translated back to the published numeric vocabulary, using a map
    # read from the edition that carries both rather than a table typed here.
    # (Switching the ids to alpha-3 would be an improvement and a RE-KEY of 21,300
    # live series — not a call to make inside a fetcher.)
    translate = {}
    if iso3_col.lower() in ("iso", "iso3", "iso_code", "country_iso3"):
        if not iso_index:
            # NO VOCABULARY, NO PARSE - for ANY file type (review R1287: the first guard checked only the
            # .xlsx extension, so an alpha-3-only CSV was still merged as EPI:<var>:AFG).
            raise NoVocabulary(f"the country column is alpha-3 `{iso3_col}` and no code<->iso map was read")
        translate = iso_index

    skip = {(iso3_col or "").lower(), (year_col or "").lower(),
            "country", "region", "continent", "rank", "tier", "country.name"}

    keys, dates, vals = [], [], []
    n_untranslated = 0
    for row in reader:
        iso3 = (row.get(iso3_col) or "").strip()
        if not iso3:
            continue
        if translate:
            mapped = translate.get(iso3)
            if not mapped:
                # A country the reference edition never listed. Emitting it under the
                # wrong vocabulary would silently fork that country's series, so skip
                # and count — a named gap beats a quiet duplicate id space.
                n_untranslated += 1
                continue
            iso3 = mapped
        elif iso3_col.lower() == "code":
            # the published NUMERIC vocabulary (ISO 3166-1 numeric, unpadded: Afghanistan = 4). The 3-char
            # test below is for alpha-3 only; applied here it silently dropped every code under 100
            # (found by review R1287's floor test)
            if not iso3.isdigit():
                continue
        elif len(iso3) != 3:
            continue

        if year_col and row.get(year_col):
            try:
                yr = int(float(row[year_col]))
            except (ValueError, TypeError):
                yr = default_year
        else:
            yr = default_year
        obs_d = dt.date(yr, 12, 31)

        for col, raw in row.items():
            if col is None or col.lower().strip() in skip or not col:
                continue
            if not raw or str(raw).strip() in ("", "NA", "N/A", "nan", "#N/A", "-"):
                continue
            try:
                v = float(str(raw).replace(",", ""))
                if v != v:
                    continue
                keys.append(f"EPI:{col.strip()}:{iso3}")
                dates.append(obs_d)
                vals.append(v)
            except (TypeError, ValueError):
                pass

    if n_untranslated:
        print(f"[yale_epi] {default_year}: {n_untranslated} country row(s) had no "
              f"entry in the reference vocabulary and were SKIPPED", flush=True)
    return keys, dates, vals


def _parse_epi_xlsx(data: bytes, default_year: int, iso_index=None):
    """Parse an EPI results workbook — same wide shape as the CSV, in a 'data' sheet.

    EPI 2026 ships xlsx rather than csv (README + data sheets, 178 rows x 420
    indicator columns). Rows are handed to the CSV parser rather than reimplementing
    the value/skip/ISO rules, so the two formats cannot drift apart in how they
    decide what is an observation.
    """
    import openpyxl                                          # lazy: see requirements

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb["data"] if "data" in wb.sheetnames else wb[wb.sheetnames[0]]
    rows = ws.iter_rows(values_only=True)
    try:
        header = ["" if c is None else str(c).strip() for c in next(rows)]
    except StopIteration:
        return [], [], []
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    for r in rows:
        w.writerow(["" if c is None else c for c in r])
    return _parse_epi_csv(buf.getvalue().encode("utf-8"), default_year, iso_index)


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "yale_epi.parquet")
    before = blob.row_count(path)
    tally = Tally()

    found, vocab_zips = _scan()
    if not found:
        # Loud, not silent. A downloads page that lists no results file means the
        # scrape broke or Yale restructured — either way the next release would be
        # missed, which is the whole failure being fixed here. Report it and still
        # fetch the known URLs so a scrape problem never costs data.
        tally.structural_unit("downloads page listed no epiYYYYresults file")
    urls, seen = [], set()
    for url, yr in list(found) + KNOWN_URLS:
        if url not in seen:
            seen.add(url)
            urls.append((url, yr))
    # A PINNED ARCHIVE IS NEVER "RETIRED" (review R1300). The retired-floor branch below says "its data is
    # already held" without measuring it; for Yale's dead URL that was true, but a DOI-backed, md5-pinned file
    # answering 404 or an HTML page (maintenance, a guestbook, a deaccession) is a BREAK - on a rebuild, or on a
    # store without the edition, that branch would report ok and silently serve 2026 alone (the R1289 class).
    floor_only = {u for u, _ in KNOWN_URLS} - {u for u, _ in found} - set(PINNED_MD5)

    # TWO PASSES, because the country vocabulary is defined by one edition and
    # needed by another: fetch everything first, learn ISO3 -> published code from
    # whichever file carries both columns, then parse. Parsing as we download would
    # have meant the 2026 workbook was decoded before the 2024 CSV taught us what
    # its countries are called in our ids.
    bodies = []
    for url, yr in urls:
        try:
            r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit()
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            tally.transient_unit()
            continue
        if url in floor_only and (r.status_code == 404 or (r.status_code == 200 and _is_html(r))):
            # A historic edition the site no longer serves (the 2024 CSV answers with an HTML page since the
            # 2026 redesign). RETIRED, not broken: its data is already in the store and the merge never
            # shrinks, so there is nothing to fetch - and failing the source over it would hide the edition
            # that IS current. Said once per run; a current (discovered) URL answering HTML is still a break.
            print(f"[yale_epi] {yr}: {url.rsplit('/', 1)[-1]} is no longer served (HTTP {r.status_code}, "
                  f"{'HTML page' if r.status_code == 200 else 'not found'}); its data is already held",
                  flush=True)
            continue
        if r.status_code != 200 or len(r.content) <= 500 or _is_html(r):
            # A wholesale 404 / tiny body / an HTML page where a data file was linked is a structural
            # break (Yale moved or renamed the file), not a quiet day.
            tally.structural_unit(f"{yr}: HTTP {r.status_code}, {len(r.content)} B"
                                  + (", an HTML page" if r.status_code == 200 and _is_html(r) else ""))
            continue
        want = PINNED_MD5.get(url)
        if want and hashlib.md5(r.content).hexdigest() != want:
            tally.structural_unit(f"{yr}: the pinned archive file answered with other bytes (md5 "
                                  f"{hashlib.md5(r.content).hexdigest()}, the publisher lists {want})")
            continue
        bodies.append((url, yr, r.content))

    # THE CURRENT EDITION'S VOCABULARY FIRST. A results CSV that carries both columns (the 2024 archive) only
    # SUPPLEMENTS the indicator zip's map: the 2024 file knows 180 countries and the 2026 zip 220, so letting the
    # older map key the newer workbook would drop every country it does not know (review R1287's rule, which
    # held only while no CSV was fetched). Listed zip that failed = transient, as before - never the smaller map.
    csv_index = {}
    for url, yr, body in bodies:
        if not url.lower().endswith(".xlsx"):
            for k, v in _country_index(body).items():
                csv_index.setdefault(k, v)
    iso_index = {}
    # NEWEST EDITION FIRST (review R1287: oldest-first let an older edition's map key a newer workbook).
    vocab_transient = False
    for url, yr in sorted(vocab_zips, key=lambda kv: -kv[1]):
        if iso_index:
            break
        try:
            r = requests.get(url, headers=UA, timeout=300, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            vocab_transient = True
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            vocab_transient = True
            continue
        if r.status_code == 200 and not _is_html(r):
            iso_index = _vocab_from_zip(r.content)
            if iso_index:
                print(f"[yale_epi] country vocabulary read from {url.rsplit('/', 1)[-1]}", flush=True)
    if iso_index:
        for k, v in csv_index.items():               # supplement: the zip's code wins where both know a country
            iso_index.setdefault(k, v)
    elif csv_index and not vocab_zips:
        iso_index = dict(csv_index)                  # no current zip listed at all: the CSV map is the only one
    if iso_index:
        # Supplement, never override: a code the reference edition supplies wins.
        for k, v in EXTRA_COUNTRY_CODES.items():
            iso_index.setdefault(k, v)
    if iso_index:
        print(f"[yale_epi] country vocabulary: {len(iso_index)} ISO3 -> published "
              f"code mappings", flush=True)

    all_keys, all_dates, all_vals = [], [], []
    for url, yr, body in bodies:
        try:
            if url.lower().endswith(".xlsx"):
                k, d, v = _parse_epi_xlsx(body, default_year=yr, iso_index=iso_index)
            else:
                k, d, v = _parse_epi_csv(body, default_year=yr, iso_index=iso_index)
        except NoVocabulary as e:
            # NO VOCABULARY, NO PARSE. Parsed without the map, an alpha-3 file yields EPI:<var>:AFG ids - a
            # second id space beside the published numeric one, with the live series frozen and the gate
            # reading green (see _parse_epi_csv). If the vocabulary zip only failed TRANSIENTLY that is
            # the publisher's bad hour (retried next run), not a break (review R1287).
            if vocab_transient:
                tally.transient_unit(f"{yr}: the vocabulary zip was unavailable ({e})")
            else:
                tally.structural_unit(f"{yr}: no country vocabulary (code<->iso) could be read, so the file "
                                      f"is not parsed - it would publish a forked alpha-3 id space ({e})")
            continue
        except Exception as e:                               # noqa: BLE001
            # A workbook we cannot open is a structural break on THAT release, named
            # so the log says which one rather than "yale_epi failed".
            print(f"[yale_epi] {url}: {type(e).__name__}: {e}", flush=True)
            tally.structural_unit(f"{yr}: unreadable ({type(e).__name__})")
            continue
        if not v:
            tally.structural_unit(f"{yr}: 200 but parsed 0 rows")  # schema break
            continue
        print(f"[yale_epi] {yr}: {len(v):,} obs from {url.rsplit('/', 1)[-1]}",
              flush=True)
        all_keys.extend(k)
        all_dates.extend(d)
        all_vals.extend(v)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })

    if tbl.num_rows == 0:
        # Nothing parsed across all URLs; Tally already recorded transient/structural.
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before))
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
