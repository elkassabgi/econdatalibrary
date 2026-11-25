"""S1 fetcher - Fragile States Index (FSI), Fund for Peace (annual, 2006-present).

Licence CC BY-NC-SA 4.0 (DATABASE_LICENSES_VERBATIM.md:1237-1246: non-commercial, attribution to "The Fund for
Peace" with a link to its web site where possible, ShareAlike). Single grouped parquet
clean_full/fsi_fundforpeace/fsi_fundforpeace.parquet, schema (series_key, obs_date, value).
series_key = 'FSI_FP:{indicator}:{country}' (column and country names exactly as the file writes them; never
normalised, R22), obs_date = Dec-31 of the index year. Each annual release is a discrete static XLSX; FSI may
revise prior years, so each due run re-fetches ALL years and MERGES (dedup series_key+obs_date, new wins on
revision, never-shrink).

WHICH FILES (fixed 2026-11-25). The FSI "excel" page (EXCEL_PAGE) lists one XLSX per year; its server-rendered HTML
carries the links (read 2026-11-25: 36 xlsx anchors, each of 2006-2023 twice, one written with padding inside the
quotes, href=" https://... "; nothing newer). discover() keeps links (relative ones resolved, padding stripped)
under https://fragilestatesindex.org/wp-content/uploads/ whose basename is exactly fsi-<YYYY>[-download].xlsx
(case-insensitive). Two DIFFERENT links for one year -> REFUSED. It runs ONCE per run: current_vintage() stores
its answer and update() takes it. ANY page failure (any requests exception, a non-200, a link that will not parse,
no matching link) uses FALLBACK_URLS and makes the run PARTIAL with the reason FIRST - never a silent fallback (it
was silent before). discover() prints one line per run: page status, bytes, links kept, links left out, newest
year, fallback.

THE YEAR LABEL. When the file has a "Year" column it is the label: it must hold exactly one year over the rows that
carry an indicator value (ints, floats, datetimes or four-digit text; a row with values and an unreadable Year cell
counts against it), and the basename year must agree. When the file has NO Year column the basename year is the
label and the run says so in a note. Either way an upload-folder year (/uploads/<YYYY>/<MM>/) must be the label
year or the next one. A present Year column that disagrees or holds two years REFUSES the run. Rows with no numeric
indicator cell (a footer note, a blank line) are ignored. The old code took the FIRST 20dd in the URL, i.e. the
folder: an FSI-2024 file under a 2025 folder would have written 2025-12-31 rows for ever (never-shrink).

EVERY YEAR'S VERDICT IS DECIDED BEFORE THE MERGE. A structural year (a non-200 other than 404/429/5xx, a body that
is not an XLSX, a parse error, 0 rows, a year mismatch), two files with one label year, the newest LISTED year's
indicator columns differing from those of the listed year before it (checked only when both were read), or an id
fork -> DefinitiveError, NOTHING merged, with the reason FIRST (the digest shows 72 characters) and an
"N/N sub-unit(s) REFUSED" clause that health's stuck alarm reads as a failed run. Before this fix a structural year
raised in finalize() AFTER the merge, so the merged years never re-derived.

THE ID-FORK FLOOR (R2210): share = |current stored keys & keys of the NEWEST file read| / |current stored keys|,
where the current stored keys are the full FSI_FP:{col}:{country} keys with an observation in the store's newest
year. Under 95% refuses. It is measured on the newest file alone: across all years read, renamed countries would
still be found in the older files and the fork would pass.

PARTIAL, reason first (the good years are merged): discovery failed (fallback; a 404 under the fallback list is
partial through it); a transient year (finalize's own partial).

NOTES, reason first, with NO status demotion: a year the PAGE lists whose file answers 404 (named; it was a silent
empty unit before; our stored rows for it stand), the newest-year gap (dated with the run's UTC date), and a file
labelled from its name. None of these is a partial (R2209): partial is an ATTENTION status decided before health's
RED-DATA data-clock branch, and is_due counts the cadence from last_success, so a partial here would hide the
data-clock alarm and re-download on every pass after the due date. A pass that merged good years is never an
"empty window": finalize gets empty_window_floor = len(urls) + 1, so 404 years plus an unchanged merge cannot raise
after the merge.

GAP RULE (R1300). The 2018-2023 files were uploaded in months 4-7 of their index year (folders 2018/04, 2019/04,
2020/05, 2021/05, 2022/07, 2023/06), and the FSI 2024 DATA appeared as a Tableau view (fsi-2024-rankings, embedded
on /global-data/; its placeholder id is epoch-ms 2024-06-19) - month 6 of 2024. The item of 2025-02-18
(/uploads/2025/02/) is the 2024 REPORT PDF, not data. So a year Y counts as overdue from August of Y: the expected
newest year is the UTC run year from August, else the run year - 1. On 2026-11-25 the store ends 2023: 2024, 2025
and 2026 are overdue, and the note says no per-year XLSX exists after 2023 (FSI 2024 only as the Tableau view).

REVISIONS. The merge reports the keys whose value changed (report_changed_keys): a pass that revises values and
adds no row books revised_unit (ok, so the CSV phase re-derives, R549), and Result.changed_keys hands the CSV phase
exactly that set ({} when nothing changed).

"Change from Previous Year" is a signed difference, not an index value, and is skipped (_SKIP_COLS). That changes no
served row: the never-shrink merge keeps the 35 stored 'FSI_FP:Change from Previous Year:<country>' series (2019,
2020; catalogue copy of 2026-11-16), and no file after 2020 has the column. Those 35 series hold only the
non-negative half (values below 0 were dropped); their fate is an owner decision.
"""
from __future__ import annotations
import datetime as dt
import io
import os
import re
import time
from urllib.parse import urljoin, urlparse

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from ._vintage import content_hash, UA

SOURCE = "fsi_fundforpeace"
DEDUP = ("series_key", "obs_date")

# Canonical FSI download index - lists the current XLSX URL for every year.
EXCEL_PAGE = "https://fragilestatesindex.org/excel/"
_UPLOADS = "https://fragilestatesindex.org/wp-content/uploads/"
_FILE = re.compile(r"(?i)fsi-(20\d\d)(?:-download)?\.xlsx")
_FOLDER = re.compile(r"^(20\d\d)/\d\d/")
# An href value: double-quoted, single-quoted (padding inside the quotes is stripped), or unquoted.
_HREF = re.compile(r"""href\s*=\s*(?:"\s*([^"]*?)\s*"|'\s*([^']*?)\s*'|([^\s"'>]+))""", re.I)

# The per-year XLSX URLs the excel page listed (verified 2026-06 and again 2026-11-25). Used ONLY when discovery
# fails, and then the run is PARTIAL. Years 2006-2017 live under /uploads/data/, later years under year/month
# upload folders.
FALLBACK_URLS = [
    "https://fragilestatesindex.org/wp-content/uploads/2023/06/FSI-2023-DOWNLOAD.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/2022/07/fsi-2022-download.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/2021/05/fsi-2021.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/2020/05/fsi-2020.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/2019/04/fsi-2019.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/2018/04/fsi-2018.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2017.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2016.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2015.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2014.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2013.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2012.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2011.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2010.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2009.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2008.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2007.xlsx",
    "https://fragilestatesindex.org/wp-content/uploads/data/fsi-2006.xlsx",
]

# A browser-ish UA for the HTML index page (WordPress 403s some bot agents).
PAGE_UA = {"User-Agent": "Mozilla/5.0 (compatible; Econ-Fin Data Library admin@hfdatalibrary.com)"}

# Header columns that are not indicator values. "change from previous year" is a signed difference (2019, 2020).
_SKIP_COLS = {"country", "country name", "countryname", "name", "rank", "change", "trend", "year",
              "change from previous year"}
_TRANSIENT_STATUS = (429, 500, 502, 503, 504)
_MIN_BYTES = 5000
_MIN_KEY_SHARE = 0.95
_DUE_MONTH = 8          # year Y counts as overdue from August of Y (see GAP RULE)
_SELECTION_TTL_S = 3600
_SELECTION = {}         # discover()'s answer, stored by current_vintage() for this run's update()


def _utc_now(clock=dt.datetime):
    """The run's clock, in UTC. The only place this module reads a clock; tests replace it."""
    return clock.now(dt.timezone.utc)


def _as_utc(now):
    if now.tzinfo is None:
        raise ValueError("fsi_fundforpeace: the run clock must be timezone-aware (UTC)")
    return now.astimezone(dt.timezone.utc)


def name_year(url: str):
    """The year in an exact FSI basename (fsi-2022-download.xlsx -> 2022), else None."""
    m = _FILE.fullmatch(urlparse(url).path.rsplit("/", 1)[-1])
    return int(m.group(1)) if m else None


def folder_year(url: str):
    """The upload-folder year (/uploads/2025/02/... -> 2025), or None (e.g. /uploads/data/)."""
    if not url.startswith(_UPLOADS):
        return None
    m = _FOLDER.match(url[len(_UPLOADS):])
    return int(m.group(1)) if m else None


def hrefs(html: str) -> list:
    """Every href value on the page, padding inside the quotes stripped, in page order."""
    return [next(g for g in m.groups() if g is not None) for m in _HREF.finditer(html or "")]


def year_links(html: str, base: str = EXCEL_PAGE):
    """(kept, left_out, duplicates): kept = {name_year: url} for links under _UPLOADS with an exact FSI basename;
    left_out = other .xlsx/.csv links; duplicates = {year: sorted urls} when one year has two DIFFERENT links.
    Raises ValueError on a link urljoin cannot parse (e.g. "http://[broken/..."); discover() treats that as a
    page failure."""
    kept, left_out, dup = {}, [], {}
    for href in hrefs(html):
        url = urljoin(base, href.strip())
        if not url.lower().endswith((".xlsx", ".csv", ".xls")):
            continue
        y = name_year(url) if url.startswith(_UPLOADS) else None
        if y is None:
            if url not in left_out:
                left_out.append(url)
            continue
        if y in kept and kept[y] != url:
            dup.setdefault(y, {kept[y]}).add(url)
        kept.setdefault(y, url)
    return kept, left_out, {y: sorted(u) for y, u in dup.items()}


def discover():
    """{'urls': [newest first], 'fallback': reason or None, 'refuse': reason or None}. Never raises."""
    status, nbytes, kept, left_out, dup, why = "no answer", 0, {}, [], {}, None
    try:
        r = requests.get(EXCEL_PAGE, headers=PAGE_UA, timeout=60)
    except requests.RequestException as e:
        why = f"the excel page did not answer ({type(e).__name__})"
    else:
        status, nbytes = f"HTTP {r.status_code}", len(r.content or b"")
        if r.status_code != 200:
            why = f"the excel page answered HTTP {r.status_code}"
        else:
            try:
                kept, left_out, dup = year_links(r.text)
            except ValueError as e:
                kept, left_out, dup = {}, [], {}
                why = f"the excel page holds a link that will not parse ({type(e).__name__}: {str(e)[:60]})"
            else:
                if not kept:
                    why = "the excel page holds no fsi-<YYYY>[-download].xlsx link under /wp-content/uploads/"
    sel = {"urls": [kept[y] for y in sorted(kept, reverse=True)], "fallback": None, "refuse": None}
    if dup and not why:
        sel["refuse"] = "the excel page links two files for one year: " + "; ".join(
            f"{y}: {', '.join(u.rsplit('/', 1)[-1] for u in us)}" for y, us in sorted(dup.items()))
    elif why:
        sel["urls"] = list(FALLBACK_URLS)
        sel["fallback"] = f"discovery failed: {why}; used the {len(FALLBACK_URLS)} fallback URLs"
    newest = max((name_year(u) or 0) for u in sel["urls"]) if sel["urls"] else None
    print(f"[{SOURCE}] discovery: page {status}, {nbytes:,} bytes, {len(kept)} year link(s) kept, "
          f"{len(left_out)} left out; newest year {newest}; "
          f"fallback: {sel['fallback'] or 'none'}" + (f"; REFUSE: {sel['refuse']}" if sel["refuse"] else ""),
          flush=True)
    return sel


def _take_selection():
    """This run's discovery: the probe's answer if it is fresh (taken once), else a new discover()."""
    sel = _SELECTION.pop("sel", None)
    at = _SELECTION.pop("at", None)
    if sel is not None and at is not None and time.monotonic() - at < _SELECTION_TTL_S:
        return sel
    return discover()


def current_vintage(unit):
    """Cheap probe: discover() (stored for this run's update()); token = a content hash of the URL list,
    prefixed "fallback:" when discovery failed. None on a refusal. Not stored (finalize stamps date-tail; no
    probe-stamp opt-in), so it gates nothing."""
    sel = discover()
    _SELECTION.update(sel=sel, at=time.monotonic())
    if sel["refuse"]:
        return None
    tag = "fallback:" if sel["fallback"] else "urls:"
    return tag + content_hash("\n".join(sorted(sel["urls"])).encode("utf-8"))


def _year_cell(v):
    """A Year cell as an int, or None when it is not a year."""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.year
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return int(v) if float(v).is_integer() and 1900 <= v <= 2200 else None
    s = str(v).strip() if v is not None else ""
    return int(s) if re.fullmatch(r"(19|20|21)\d\d", s) else None


def _parse_xlsx(data: bytes):
    """Parse one FSI annual XLSX -> {'label': the Year column's one year, or None when the file has no Year
    column; 'rows': [(col, country, value)]; 'cols': set of indicator columns with a value}. Rows with no numeric
    indicator cell are ignored. Raises ValueError on a missing Country column, or on a PRESENT Year column that
    holds other than exactly one year over the rows with values (an unreadable Year cell on such a row counts)."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("the first sheet is empty")
    header = [str(c).strip() if c else "" for c in rows[0]]
    low = [h.lower() for h in header]
    ctry_idx = next((i for i, h in enumerate(low) if h in ("country", "country name", "countryname", "name")), None)
    if ctry_idx is None:
        raise ValueError("no Country column")
    yr_idx = next((i for i, h in enumerate(low) if h == "year"), None)
    skip_idx = {i for i, h in enumerate(low) if h in _SKIP_COLS}
    skip_idx.add(ctry_idx)

    years, bad_year, out, cols = set(), 0, [], set()
    for row in rows[1:]:
        if not row or ctry_idx >= len(row) or row[ctry_idx] is None:
            continue
        ctry = str(row[ctry_idx]).strip()
        if not ctry or ctry.lower() in ("country", "nan"):
            continue
        vals = []
        for ci, (col, cell) in enumerate(zip(header, row)):
            if ci in skip_idx or not col or cell is None:
                continue
            try:
                v = float(cell)
            except (TypeError, ValueError):
                continue
            if v != v or v < 0:  # NaN or negative -> not a valid index value
                continue
            vals.append((col, ctry, v))
        if not vals:
            continue            # a footer note or a blank line: no indicator value, so it says nothing
        if yr_idx is not None:
            y = _year_cell(row[yr_idx]) if yr_idx < len(row) else None
            if y is None:
                bad_year += 1
            else:
                years.add(y)
        out.extend(vals)
        cols.update(c for c, _k, _v in vals)
    if yr_idx is None:
        return {"label": None, "rows": out, "cols": cols}
    if len(years) != 1 or bad_year:
        raise ValueError(f"the Year column holds {sorted(years)} and {bad_year} unreadable cell(s) on rows with "
                         f"values; exactly one year is required")
    return {"label": years.pop(), "rows": out, "cols": cols}


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


def _stored(path):
    """(the store's newest year, the CURRENT stored keys = keys with an obs in that year); (None, set())."""
    if not blob.exists(path):
        return None, set()
    t = blob.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return None, set()
    newest = pc.max(t.column("obs_date")).as_py()
    if newest is None:
        return None, set()
    cur = t.filter(pc.equal(pc.year(t.column("obs_date")), newest.year))
    return newest.year, set(cur.column("series_key").to_pylist())


def _refuse(n: int, reason: str, labels):
    """Raise a refusal with the REASON FIRST and nothing merged. "n/n sub-unit(s) REFUSED" is the grammar
    health's stuck alarm reads (_STUCK_ALL_FAILED): every year was refused, because none was merged."""
    raise DefinitiveError(f"{SOURCE}: {reason}; nothing merged for any year, existing data kept "
                          f"({n}/{n} sub-unit(s) REFUSED by the fetcher [{', '.join(labels)}])")


def _years_text(years):
    ys = sorted(years)
    return " and ".join(str(y) for y in ys) if len(ys) <= 2 else f"{', '.join(map(str, ys[:-1]))} and {ys[-1]}"


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "fsi_fundforpeace.parquet")
    before = blob.row_count(path)
    tally = Tally()
    now = _as_utc(_utc_now())

    # 1. Which files: this run's one discovery. Two links for one year refuses before any fetch.
    sel = _take_selection()
    if sel["refuse"]:
        _refuse(1, sel["refuse"], ["excel page"])
    urls = sel["urls"]

    # 2. Every year's verdict, BEFORE any merge.
    good, structural, missing, no_year_col = [], [], [], []        # good: (label, url, parsed)
    for url in urls:
        ny = name_year(url)
        lab = f"{ny}" if ny else url.rsplit("/", 1)[-1]
        try:
            r = requests.get(url, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            tally.transient_unit(f"{lab}: {type(e).__name__}")
            continue
        except requests.RequestException as e:
            structural.append(f"{lab}: {type(e).__name__}")
            continue
        if r.status_code in _TRANSIENT_STATUS:
            tally.transient_unit(f"{lab}: HTTP {r.status_code}")
            continue
        if r.status_code == 404:
            tally.empty_unit(f"{lab}: 404")
            missing.append((lab, url))
            continue
        body = r.content or b""
        if r.status_code != 200 or len(body) < _MIN_BYTES or not body.startswith(b"PK"):
            structural.append(f"{lab}: HTTP {r.status_code}, {len(body):,} bytes, "
                              f"{'not an XLSX (zip) body' if not body.startswith(b'PK') else 'under 5,000 bytes'}")
            continue
        try:
            parsed = _parse_xlsx(body)
        except Exception as e:  # noqa: BLE001 - a real body that will not parse is a break, named
            structural.append(f"{lab}: {type(e).__name__}: {str(e)[:90]}")
            continue
        label, fy = parsed["label"], folder_year(url)
        if label is None:
            if ny is None:
                structural.append(f"{lab}: no Year column and no year in the file name")
                continue
            label = ny
            no_year_col.append(lab)
        elif ny is None or label != ny:
            structural.append(f"{lab}: the Year column says {label} but the file name says {ny}")
            continue
        if fy is not None and fy not in (label, label + 1):
            structural.append(f"{lab}: the year is {label} but the upload folder is {fy}")
            continue
        if not parsed["rows"]:
            structural.append(f"{lab}: a real XLSX parsed 0 rows")
            continue
        good.append((label, url, parsed))

    n_all = len(urls)
    labels_seen = [g[0] for g in good]
    dups = sorted({y for y in labels_seen if labels_seen.count(y) > 1})
    if dups:
        structural.append(f"two files carry the year {_years_text(dups)}")
    good.sort(key=lambda g: -g[0])
    # The column-set rule: only the newest LISTED year against the listed year before it, when both were read.
    listed = sorted({name_year(u) for u in urls if name_year(u)}, reverse=True)
    by_label = {g[0]: g for g in good}
    if len(listed) >= 2 and listed[0] in by_label and listed[1] in by_label:
        a, b = by_label[listed[0]][2]["cols"], by_label[listed[1]][2]["cols"]
        if a != b:
            structural.append(f"{listed[0]}: its indicator columns differ from {listed[1]}'s "
                              f"(added {sorted(a - b)[:4]}, gone {sorted(b - a)[:4]})")
    if structural:
        _refuse(n_all, "; ".join(structural), [s.split(":", 1)[0] for s in structural])

    if not good:
        # Nothing parsed: finalize gives partial when the years transient-failed, and raises when every year
        # was a 404 over a large window. Either way nothing is merged.
        res = finalize(tally, before, None, source=SOURCE)
        return _with_reasons(res, sel, missing, None, now, [])

    keys, dates, vals = [], [], []
    for label, _url, parsed in good:
        d = dt.date(label, 12, 31)
        for col, ctry, v in parsed["rows"]:
            keys.append(f"FSI_FP:{col}:{ctry}")
            dates.append(d)
            vals.append(v)
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })

    # 3. The id-fork floor (R2210): the CURRENT stored keys against the keys of the NEWEST file read.
    stored_year, current = _stored(path)
    if current:
        newest_label, _u, newest_parsed = good[0]
        newest_keys = {f"FSI_FP:{c}:{k}" for c, k, _v in newest_parsed["rows"]}
        share = len(current & newest_keys) / len(current)
        if share < _MIN_KEY_SHARE:
            _refuse(n_all, f"an id fork: the {newest_label} file holds {share:.1%} of the {len(current):,} stored "
                           f"keys current in {stored_year} (floor {_MIN_KEY_SHARE:.0%})", ["key floor"])

    # 4. Merge once; book it as one sub-unit; hand the CSV phase the merge-measured changed set.
    n, md, changed = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP, report_changed_keys=True)
    added = max(0, n - before)
    span = f"{len(good)} year(s) {good[-1][0]}-{good[0][0]}"
    if added:
        tally.added_unit(added, span)
    elif changed:
        tally.revised_unit(span)
    else:
        tally.empty_unit(span)
    # A pass that merged good years is not an "empty window": with the floor at its default (10), 11 years that
    # answer 404 plus one unchanged merge would make empty == attempted == 12 and finalize would raise AFTER the
    # merge, never naming the 404 years (the 12 files for 2006-2017 share one folder, /uploads/data/).
    res = finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl),
                   empty_window_floor=len(urls) + 1)
    res.changed_keys = changed
    newest = max([g[0] for g in good] + ([stored_year] if stored_year else []))
    listed_newest = max((name_year(u) or 0) for u in urls)
    return _with_reasons(res, sel, missing, newest, now, no_year_col, span, listed_newest)


def _with_reasons(res, sel, missing, newest, now, no_year_col, span=None, listed_newest=None):
    """Reasons FIRST in the note. A discovery fallback makes the run PARTIAL (a transient year already did, in
    finalize). A year the PAGE lists whose file answers 404, the newest-year gap and a name-labelled file are NOTES
    only, with no status demotion (R2209): our stored rows for that year stand, and a publisher's dead link must not
    make permanent ATTENTION and a re-download on every pass. A 404 under the fallback list is still PARTIAL, through
    the fallback itself."""
    demote, notes = [], []
    if sel["fallback"]:
        demote.append(sel["fallback"])
    if missing:
        missing = sorted(missing)                        # oldest first, whatever order the page listed them in
        notes.append(f"year(s) {', '.join(lab for lab, _u in missing)} listed but the file answers 404 "
                     f"({', '.join(u.rsplit('/', 1)[-1] for _l, u in missing)}); the stored rows for "
                     f"{'that year' if len(missing) == 1 else 'those years'} stand")
    if newest is not None:
        expected = now.year if now.month >= _DUE_MONTH else now.year - 1
        if newest < expected:
            overdue = list(range(newest + 1, expected + 1))
            day = now.date().isoformat()                 # the run's UTC date (now is UTC: _as_utc)
            seen = (f"the {listed_newest} file is listed but was not read on {day}"
                    if listed_newest and listed_newest > newest
                    else f"no per-year XLSX after {newest} was listed on {day}")
            notes.append(f"newest data gap: the newest FSI year held is {newest}; {_years_text(overdue)} "
                         f"{'is' if len(overdue) == 1 else 'are'} overdue (a year counts as overdue from August of "
                         f"that year); {seen}; FSI 2024 is published only as a Tableau view on /global-data/ "
                         f"(fsi-2024-rankings)")
    if no_year_col:
        notes.append(f"year(s) {', '.join(no_year_col)}: no Year column, labelled from the file name")
    if demote:
        res.status = "partial"
    if demote or notes:
        res.error = "; ".join(demote + notes) + f"; {res.error}"
    if span:
        res.error = f"{res.error}; merged {span}"
    return res
