"""S1 bulk fetcher — Stats NZ large-dataset CSVs (stats.govt.nz, CC BY 4.0, no key).

Stats NZ publishes one CSV per topic per release period at a predictable path:

    {NZ_BASE}/{topic}/{topic}-{period}/Download-data/{stem}-{period.lower()}.csv

WHY THIS FETCHER GENERATES PERIODS INSTEAD OF REUSING THE INGEST'S LIST. The first-pass
ingest (jobs/ingest_statsnz.py) discovers releases by probing a HARDCODED list of period
strings — "December-2024-quarter", "March-2025-quarter", "April-2025", … — newest first, and
taking the first that 200s. That is fine for a one-off backfill and useless for an updater:
the list ends in early 2025, so however often it ran it would keep re-finding the same newest
entry and never see a release published after the list was written. A source that refreshes
on schedule and can never advance is worse than one that is honestly frozen, because the
green run implies currency.

So periods are DERIVED from today's date, newest first, in the three shapes Stats NZ actually
uses (observed in the ingest's own list):
    quarterly  "December-2024-quarter"      (quarter-end month + year + '-quarter')
    monthly    "April-2025"                 (month + year)
    annual     "Year-ended-March-2024" | "2024"
We probe newest -> older and stop at the first that exists, exactly as the ingest does, but
over a window that moves with the clock instead of a frozen literal.

VINTAGE. Once the newest existing URL per dataset is known, the change signal is that URL's
own HTTP validator (ETag / Last-Modified / Content-Length) via _vintage.http_vintage, hashed
across all datasets. It moves when Stats NZ republishes a file OR when a newer period appears
(a new period changes the URL, which changes the hash). Both are real changes; neither is a
date-tail guess.

PARSING IS REUSED from jobs.ingest_statsnz (parse_statsnz_date, ingest_csv) so the fetcher and
the first-pass ingest emit byte-identical series_keys — the duplication invariant.

HONEST-STATUS: a discovery/listing outage -> TransientError (partial, retried, data kept). A
per-dataset download or parse failure -> transient_unit for that dataset only. A dataset that
parses to ZERO rows -> empty_unit and its vintage is NOT advanced, so a parser break resurfaces
next run instead of being sealed in.
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Deadline, Tally, finalize
from ._vintage import http_vintage
from jobs import ingest_statsnz as ig     # reuse the production parser

SOURCE = "stats_nz"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"
BUDGET_MIN = 12
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

_MONTHS = ["January", "February", "March", "April", "May", "June",
           "July", "August", "September", "October", "November", "December"]

# (topic, stem, prefix, period_kind) — topics/stems/prefixes copied from the ingest so the
# series_key prefixes stay identical; only the PERIOD discovery differs.
DATASETS = [
    ("Gross-domestic-product", "gross-domestic-product", "gdp_quarterly", "quarter"),
    ("Consumers-price-index", "consumers-price-index", "cpi", "quarter"),
    ("Labour-market-statistics", "labour-market-statistics", "labour", "quarter"),
    ("Balance-of-payments",
     "balance-of-payments-and-international-investment-position", "bop", "month"),
    ("Producers-price-index", "producers-price-index", "ppi", "quarter"),
    ("Retail-trade-survey", "retail-trade-survey", "retail", "quarter"),
    ("Business-financial-statistics", "business-financial-statistics",
     "biz_finance", "yearended"),
    ("Overseas-merchandise-trade", "overseas-merchandise-trade", "trade", "month"),
    ("Building-consents-issued", "building-consents-issued", "building", "month"),
    ("International-travel-and-migration", "international-travel-and-migration",
     "travel", "month"),
    ("National-accounts-income-and-expenditure",
     "national-accounts-income-and-expenditure", "gdp_annual", "yearended"),
    ("Estimated-resident-population-for-New-Zealand",
     "estimated-resident-population-for-new-zealand", "population", "year"),
]


# How far back to probe per period shape. Measured, not guessed: an 8-period window found
# only 2 of 12 datasets, because a MONTHLY window of 8 reaches back barely eight months and
# Stats NZ does not republish every topic that often. The window has to span the gap between
# releases, not the gap since the last one.
_BACK = {"quarter": 16, "month": 30, "yearended": 5, "year": 5}


def _periods(kind: str, today: dt.date, back: int = 0) -> list:
    """Candidate period strings, NEWEST FIRST, derived from today rather than hardcoded."""
    back = back or _BACK.get(kind, 12)
    out = []
    if kind == "quarter":
        q_end = [3, 6, 9, 12]
        y, m = today.year, today.month
        cur = max([x for x in q_end if x <= m], default=12)
        if cur == 12 and m < 3:
            y -= 1
        for _ in range(back):
            out.append(f"{_MONTHS[cur - 1]}-{y}-quarter")
            cur -= 3
            if cur < 1:
                cur += 12
                y -= 1
    elif kind == "month":
        y, m = today.year, today.month
        for _ in range(back):
            out.append(f"{_MONTHS[m - 1]}-{y}")
            m -= 1
            if m < 1:
                m += 12
                y -= 1
    elif kind == "yearended":
        for i in range(back):
            out.append(f"Year-ended-March-{today.year - i}")
    else:                                   # plain year
        for i in range(back):
            out.append(str(today.year - i))
    return out


def _discover(sess, today=None):
    """[(url, prefix)] — the NEWEST existing release per dataset. HEAD probes, newest first."""
    today = today or dt.date.today()
    found = []
    for topic, stem, prefix, kind in DATASETS:
        for period in _periods(kind, today):
            url = (f"{ig.NZ_BASE}/{topic}/{topic}-{period}/Download-data/"
                   f"{stem}-{period.lower()}.csv")
            try:
                r = sess.head(url, headers=UA, timeout=20, allow_redirects=True)
            except requests.RequestException:
                continue
            if r.status_code == 200:
                found.append((url, prefix))
                break
    return found


def current_vintage(unit) -> "str | None":
    """Hash of every discovered URL + its HTTP validator. Moves on republish OR a new period."""
    sess = requests.Session()
    try:
        found = _discover(sess)
    except Exception:                                        # noqa: BLE001
        return None
    if not found:
        return None
    h = hashlib.sha256()
    for url, prefix in sorted(found):
        try:
            v = http_vintage(url, session=sess)
        except Exception:                                    # noqa: BLE001
            v = ""
        h.update(f"{prefix}={url}|{v};".encode())
    return f"stats_nz:{h.hexdigest()[:16]}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = requests.Session()

    try:
        found = _discover(sess)
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"stats_nz: release discovery failed: {e!r}") from e
    if not found:
        raise TransientError("stats_nz: discovery found no releases at all")

    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    maxd = None
    dl = Deadline(minutes=BUDGET_MIN)

    for url, prefix in found:
        path = os.path.join(out_dir, f"{prefix}.parquet")
        try:
            cur_v = f"{url}|{http_vintage(url, session=sess)}"
        except Exception:                                    # noqa: BLE001
            cur_v = url
        if sidecar.get(prefix) == cur_v and blob.exists(path):
            continue                                         # already current — costs nothing
        if dl.spent():
            print(f"[stats_nz] budget {BUDGET_MIN} min spent — {prefix} deferred", flush=True)
            tally.transient_unit(prefix)
            continue

        raw = ig.try_download(url)
        if raw is None:
            tally.transient_unit(prefix)
            continue
        try:
            rows = ig.ingest_csv(raw, prefix)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(prefix)
            continue
        if not rows:
            # Do NOT advance the vintage: a parser break must resurface next run.
            tally.empty_unit()
            continue

        tbl = pa.table({
            "series_key": pa.array([str(r[0]) for r in rows], pa.string()),
            "obs_date": pa.array([r[1] for r in rows], pa.date32()),
            "value": pa.array([float(r[2]) for r in rows], pa.float64()),
        })
        before = blob.row_count(path) if blob.exists(path) else 0
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            tally.transient_unit(prefix)                     # one dataset must not sink the source
            continue
        published += n
        tally.added_unit(max(0, n - before), prefix)
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        sidecar[prefix] = cur_v                              # advance ONLY after a clean publish

    _save(out_dir, sidecar)
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    return finalize(tally, published, maxd or (since or None), source=SOURCE)
