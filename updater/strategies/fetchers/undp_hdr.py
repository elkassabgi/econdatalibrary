"""S1 fetcher - UNDP Human Development Report composite indices (annual, 190+ countries).

License CC BY 3.0 IGO (DATABASE_LICENSES_VERBATIM.md: CLEARED, re-host with attribution). Single grouped parquet
clean_full/undp_hdr/undp_hdr.parquet, schema (series_key, obs_date, value); keys are {indicator}:{ISO3} (e.g.
hdi:AFG) with obs_date = Dec 31 of the reference year. UNDP ships one composite "complete time series" CSV per
HDR edition that carries the full 1990-present panel, so a per-obs delta is meaningless: each due run fetches the
chosen edition's whole CSV and MERGES it (dedup series_key+obs_date, new wins on revision, never-shrink).

WHICH EDITION (fixed 2026-11-25). Until this fix update() took the first URL that answered a HEAD from a fixed
list [HDR25, HDR23-24], so an HDR 2026 file under any name could never be fetched, and any non-200 HEAD (429, 403,
503) moved on to the older edition. The probe gated nothing: finalize() stamps new_vintage "date-tail" and this
module has not opted in to the probe stamp (strategies/base.py, R1154). UNDP's folder and file names change form
between editions (2023-24_HDR/HDR23-24_..., 2025_HDR/HDR25_...), so the edition is DISCOVERED on the downloads
page (DOWNLOADS_PAGE, whose server-rendered HTML carries the links; read 2026-11-25). discover() keeps the links
under https://hdr.undp.org/sites/default/files/ whose basename is exactly
HDR<NN>[-<NN>]_Composite_indices_complete_time_series.csv, and takes the newest by the LAST two-digit group
(HDR25 -> 2025, HDR23-24 -> 2024). It runs ONCE per run: current_vintage() stores its answer and update() takes it
(_take_selection), so probe and fetch use the same edition; update() discovers itself only when no probe ran.

ANY PAGE FAILURE IS LOUD, AND NEVER TRANSIENT FOR EVER. A timeout, a 403 challenge, a 503, a page with no matching
link (a layout change, or a page rendered by JavaScript), or a page that links only editions older than
PINNED_EDITION: the run fetches the pinned edition (HDR25, the newest the page linked on 2026-11-25) and records a
PARTIAL whose note starts with the reason. While the store's newest edition is HDR25, the pinned edition is the
one the store holds, so its revisions still reach users. ONCE A NEWER EDITION IS MERGED, a fallback to the pinned
HDR25 is REFUSED as older than the store (nothing merged; the run is still loud). Either way the unit stays in
ATTENTION and due every pass until discovery works again. A transient stop with no merge applies to the chosen
CSV's GET, not to the page. discover() prints one line per run (page status, bytes, links, choice, fallback).

YEARS ARE OBSERVATION YEARS OF THE hdi:<ISO3> FAMILY. The store also holds aggregates (hdi:ZZA.VHHD ...) and
country-NAME keys from the retired annex cold start (hdi:Afghanistan ..., ending 2022); neither may decide a
year. So "the newest year" of a table is the newest obs year over keys matching hdi:<three capitals>.
  * Refused, nothing merged: the edition's hdi year is older than the store's or than `since` (EQUAL passes: a
    re-fetch of the same edition is valid).
  * Gap PARTIAL (R1300): the edition's hdi year is below (UTC run year - 3). Measured releases: HDR23-24
    (Last-Modified 2024-03-13) has data to 2022, HDR25 (2025-05-05) to 2023, so before each spring release the
    newest correct data year is (run year - 3) and after it (run year - 2). Firing below (run year - 3) never
    fires falsely; it fires once a whole release has been missed.

HOW LOUD (each rule is pinned in tests/test_undp_hdr_newest_edition.py):
  * the chosen CSV: a transient (timeout, connection error, 429, 5xx) stops with nothing merged (partial); any
    other non-200 is REFUSED; never a fallback to an older edition;
  * the body must be a CSV: not text/html, not starting "<", a header line with at least three commas, over
    1,000 bytes; anything else is REFUSED;
  * a parse error is REFUSED, the error named; 0 rows parsed is structural (finalize); no hdi:<ISO3> series is
    REFUSED;
  * under 95% of the CURRENT stored keys (any family, with an observation in the store's hdi year) -> REFUSED.
A REFUSAL raises DefinitiveError with its reason FIRST (the digest shows 72 characters) and ends with the
"1/1 sub-unit(s) REFUSED" clause that health's stuck alarm reads to count the run as failed.

MERGE, NOT OVERWRITE: an overwrite would remove stored rows UNDP no longer ships, which is the owner's decision.
The merge reports which keys changed their value (report_changed_keys): a pass that revises values without adding
a row books a revision, so it reads ok and the CSV phase re-derives (R549), and Result.changed_keys hands the CSV
phase exactly the changed series ({} on a pass that changed nothing, also on a partial that is due every pass).

KNOWN LIMITS, NOT GUARDED: (1) the older-edition refusal compares observation years, because the store records no
edition stamp, so a same-year older edition can still merge over a newer one when only the older is reachable;
an edition-stamp sidecar would close it. (2) This fetcher refreshes composite keys only. The annex name-keyed
series (hdi/gdi/gii:<country name>; 630 in the catalogue copy of 2026-11-16, all ending 2022) are not refreshed
by anything - as before this fix: the old fetcher read only the composite CSV, and the old ingester returned when
a store existed.

Parse logic is jobs/ingest_undp_hdr.ingest_composite_csv, called with strict=True (a parse error raises) and a
year bound derived from the run clock.
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time
from urllib.parse import urljoin, urlparse

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import DefinitiveError
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "undp_hdr"
DEDUP = ("series_key", "obs_date")

DOWNLOADS_PAGE = "https://hdr.undp.org/data-center/documentation-and-downloads"
_FILES_PREFIX = "https://hdr.undp.org/sites/default/files/"
# The newest edition the downloads page linked when this was written (read 2026-11-25). Used ONLY when
# discovery fails, and then the run is PARTIAL. Never edited to follow UNDP: discovery does that.
PINNED_EDITION = (2025, "https://hdr.undp.org/sites/default/files/2025_HDR/"
                        "HDR25_Composite_indices_complete_time_series.csv")
_COMPOSITE = re.compile(r"HDR(\d{2}(?:-\d{2})?)_Composite_indices_complete_time_series\.csv")
_HREF = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)
_HDI_ISO3 = r"^hdi:[A-Z]{3}$"
_TRANSIENT_STATUS = (429, 500, 502, 503, 504)
_MIN_BYTES = 1000
_MIN_KEY_SHARE = 0.95
_SELECTION_TTL_S = 3600
_SELECTION = {}          # discover()'s answer, stored by current_vintage() for this run's update()


def _utc_now(clock=dt.datetime):
    """The run's clock, in UTC. The only place this module reads a clock; tests replace it."""
    return clock.now(dt.timezone.utc)


def _run_year(now) -> int:
    if now.tzinfo is None:
        raise ValueError("undp_hdr: the run clock must be timezone-aware (UTC)")
    return now.astimezone(dt.timezone.utc).year


def edition_year(url: str):
    """The edition year from an exact composite basename, by its LAST two-digit group: HDR25 -> 2025,
    HDR23-24 -> 2024. None for any other file."""
    m = _COMPOSITE.fullmatch(urlparse(url).path.rsplit("/", 1)[-1])
    return 2000 + int(m.group(1)[-2:]) if m else None


def composite_links(html: str, base: str = DOWNLOADS_PAGE) -> list:
    """[(edition_year, url)] newest first: links (relative ones resolved) under _FILES_PREFIX whose basename is
    exactly an HDR composite time-series CSV."""
    found = {}
    for href in _HREF.findall(html or ""):
        url = urljoin(base, href.strip())
        if not url.startswith(_FILES_PREFIX):
            continue
        y = edition_year(url)
        if y is not None:
            found.setdefault(y, url)
    return sorted(found.items(), key=lambda t: -t[0])


def discover():
    """(edition_year, url, fallback_reason or None). Never transient: any page failure (any requests
    exception, including ChunkedEncodingError and TooManyRedirects) falls back to the pinned edition with its
    reason. Prints ONE line every call - the page status, its bytes, the composite links found, the chosen URL
    and the fallback reason - so a dry run's log shows what the runner got from the page."""
    pin_y, pin_url = PINNED_EDITION
    status, nbytes, links, why = "no answer", 0, [], None
    try:
        r = requests.get(DOWNLOADS_PAGE, headers=UA, timeout=60, allow_redirects=True)
    except requests.RequestException as e:
        why = f"the downloads page did not answer ({type(e).__name__})"
    else:
        status, nbytes = f"HTTP {r.status_code}", len(r.content or b"")
        if r.status_code != 200:
            why = f"the downloads page answered HTTP {r.status_code}"
        else:
            links = composite_links(r.text)
            if not links:
                why = ("the downloads page holds no HDR composite time-series CSV link "
                       "(layout changed, or rendered by JavaScript?)")
            elif links[0][0] < pin_y:
                why = f"the downloads page links only editions up to {links[0][0]}, older than the pinned {pin_y}"
    if why is None:
        year, url, fallback = links[0][0], links[0][1], None
    else:
        year, url, fallback = pin_y, pin_url, f"discovery failed: {why}; fell back to the pinned {pin_y} edition"
    print(f"[{SOURCE}] discovery: page {status}, {nbytes:,} bytes, {len(links)} composite link(s); "
          f"chose {url}; fallback: {fallback or 'none'}", flush=True)
    return year, url, fallback


def store_path() -> str:
    """The single store file (the ingester's cold-start guard reads it too)."""
    return os.path.join(config.source_dir(SOURCE), "undp_hdr.parquet")


def _take_selection():
    """This run's discovery: the probe's answer if it is fresh (taken once), else a new discover()."""
    sel = _SELECTION.pop("sel", None)
    at = _SELECTION.pop("at", None)
    if sel is not None and at is not None and time.monotonic() - at < _SELECTION_TTL_S:
        return sel
    return discover()


def current_vintage(unit):
    """Cheap probe: discover(), then a HEAD of the chosen CSV. Its discovery is stored for this run's update().
    Token "<edition year>:<ETag or Last-Modified or Content-Length>"; None on a transient or a non-CSV answer.
    The token is NOT stored (finalize stamps "date-tail"; no probe-stamp opt-in), so it gates nothing."""
    sel = discover()
    _SELECTION.update(sel=sel, at=time.monotonic())
    year, url, _fallback = sel
    try:
        r = requests.head(url, headers=UA, timeout=60, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError):
        return None
    if r.status_code != 200 or (r.headers.get("Content-Type") or "").lower().startswith("text/html"):
        return None
    tag = r.headers.get("ETag") or r.headers.get("Last-Modified") or r.headers.get("Content-Length") or "200"
    return f"{year}:{tag}"


def _not_a_csv(r):
    """Why this 200 is not a composite CSV, or None when it is one."""
    body = r.content or b""
    ctype = (r.headers.get("Content-Type") or "").strip().lower()
    if ctype.startswith("text/html"):
        return f"Content-Type {ctype!r}"
    if len(body) <= _MIN_BYTES:
        return f"a body of only {len(body):,} bytes"
    head = body[:4096].decode("utf-8-sig", errors="replace").lstrip()
    if head.startswith("<"):
        return "an HTML/XML body"
    if head.split("\n", 1)[0].count(",") < 3:
        return "no comma-separated header line"
    return None


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


def _hdi_year(tbl):
    """The newest observation year over the hdi:<ISO3> family, or None when the table holds none."""
    fam = tbl.filter(pc.match_substring_regex(tbl.column("series_key"), _HDI_ISO3))
    if fam.num_rows == 0:
        return None
    newest = pc.max(fam.column("obs_date")).as_py()
    return None if newest is None else newest.year


def _stored(path):
    """(the store's hdi year, the CURRENT stored keys); (None, empty set) when there is no store.

    Current = keys of ANY family with an observation in the store's hdi year: the keys the last merged edition
    carried. A key an older edition dropped stays in the store (the merge never removes one) but is not current."""
    if not blob.exists(path):
        return None, set()
    t = blob.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return None, set()
    year = _hdi_year(t)
    if year is None:                                   # no hdi:<ISO3> at all: the newest year over every key
        newest = pc.max(t.column("obs_date")).as_py()
        year = None if newest is None else newest.year
        if year is None:
            return None, set()
        current = t.filter(pc.equal(pc.year(t.column("obs_date")), year))
        return None, set(current.column("series_key").to_pylist())
    current = t.filter(pc.equal(pc.year(t.column("obs_date")), year))
    return year, set(current.column("series_key").to_pylist())


def _year_of(since):
    if since is None:
        return None
    try:
        return int(str(since)[:4])
    except ValueError:
        return None


def _refuse(label: str, reason: str):
    """Raise a structural refusal with the REASON FIRST: the digest shows the first 72 characters, and
    finalize()'s "returned 200 but parsed 0 rows" would be false here. The closing "1/1 sub-unit(s) REFUSED"
    clause is the grammar health's stuck alarm reads (_STUCK_ALL_FAILED) to count the run as failed."""
    raise DefinitiveError(f"{SOURCE}: {reason}; nothing merged, existing data kept "
                          f"(1/1 sub-unit(s) REFUSED by the fetcher [{label}])")


def update(unit, since) -> Result:
    from jobs.ingest_undp_hdr import ingest_composite_csv  # the one composite parser

    path = store_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    before = blob.row_count(path)
    tally = Tally()
    run_year = _run_year(_utc_now())

    # 1. Which edition: the probe's discovery of this run (never transient; a failure is a pinned fallback).
    year, url, fallback = _take_selection()
    label = f"{year} edition"
    fname = url.rsplit("/", 1)[-1]

    # 2. Fetch exactly that CSV. A transient stops; any other failure is refused, never an older edition.
    # ANY requests exception (also ChunkedEncodingError / ContentDecodingError mid-download, which escaped to the
    # orchestrator as transient_fail "UNEXPECTED:" before) is a transient stop: nothing merged, store unchanged.
    try:
        r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
    except requests.RequestException as e:
        tally.transient_unit(f"{label}: {type(e).__name__}; nothing merged")
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code in _TRANSIENT_STATUS:
        tally.transient_unit(f"{label}: HTTP {r.status_code}; nothing merged")
        return finalize(tally, before, None, source=SOURCE)
    if r.status_code != 200:
        _refuse(label, f"the {year} composite CSV ({fname}) answers HTTP {r.status_code}")
    bad = _not_a_csv(r)
    if bad:
        _refuse(label, f"the {year} composite CSV ({fname}) is not a CSV: {bad}")

    # 3. Parse. A parse error is refused with its name; 0 rows is structural (finalize's sentence is true there).
    keys, dates, vals = [], [], []
    try:
        ingest_composite_csv(r.content, keys, dates, vals, strict=True, max_year=run_year + 1)
    except Exception as e:  # noqa: BLE001 - a real CSV body that will not parse is a break, named
        _refuse(label, f"the {year} composite CSV ({fname}) parse error {type(e).__name__}: {e}")
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })
    if tbl.num_rows == 0:
        tally.structural_unit(f"{label}: the composite CSV parsed 0 rows (layout changed?)")
        return finalize(tally, before, None, source=SOURCE)
    hdi_year = _hdi_year(tbl)
    if hdi_year is None:
        _refuse(label, f"the {year} composite CSV ({fname}) holds no hdi:<ISO3> series")

    # 4. Refuse an edition older than what we hold, and an id fork (hdi:<ISO3> years; equal passes).
    stored_year, current_keys = _stored(path)
    refuse = []
    if stored_year is not None and hdi_year < stored_year:
        refuse.append(f"is older than the store (its hdi year {hdi_year} < the store's {stored_year})")
    since_year = _year_of(since)
    if since_year is not None and hdi_year < since_year:
        refuse.append(f"is older than since (its hdi year {hdi_year} < since {since})")
    if current_keys:
        share = len(current_keys & set(keys)) / len(current_keys)
        if share < _MIN_KEY_SHARE:
            refuse.append(f"is an id fork (it holds {share:.1%} of the {len(current_keys):,} current stored keys; "
                          f"floor {_MIN_KEY_SHARE:.0%})")
    if refuse:
        _refuse(label, "; ".join(f"{label} {x}" for x in refuse))

    # 5. Merge; a revision with no new row is a revision; hand the CSV phase the merge-measured changed set.
    n, md, changed = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP, report_changed_keys=True)
    added = max(0, n - before)
    if added:
        tally.added_unit(added, label)
    elif changed:
        tally.revised_unit(label)
    else:
        tally.empty_unit(label)
    res = finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
    res.changed_keys = changed

    # 6. Partial reasons, FIRST in the note: discovery failed (pinned fallback), and the data gap (R1300).
    why = []
    if fallback:
        why.append(fallback)
    if hdi_year < run_year - 3:
        why.append(f"newest data gap: the {year} edition's hdi data ends {hdi_year}, below the UTC run year "
                   f"{run_year} - 3 = {run_year - 3}; a newer HDR edition is missing or not discoverable")
    if why:
        res.status = "partial"
        res.error = "; ".join(why) + f"; merged {year}; {res.error}"
    else:
        res.error = f"{res.error}; edition {year} ({fname})"
    return res
