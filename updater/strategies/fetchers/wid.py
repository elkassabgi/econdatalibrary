"""World Inequality Database (WID.world) — bulk distribution, per-country files.

LICENCE: CC BY-NC-SA 4.0, declared by WID's own `rel="license"` link on the chart at
wid.world/world/ (the site publishes no licence TEXT anywhere — /data/, /methodology/,
/website-credits/, the privacy policy and the bulk README were all checked rendered).
Re-hosted with written permission from WID.world dated 2026-07-27, which came with a
condition: "we invite you to keep the most updated data sources". This fetcher is how
that promise is kept — without it the re-hosted copy is a snapshot we agreed not to
serve. See DATABASE_LICENSES_VERBATIM.md for the verbatim evidence.

LAYOUT. WID ships one CSV per country/region at wid.world/bulk_download/
(424 of them, ~17 MB each, semicolon-separated). The store mirrors that shape one
parquet per country, so each file is fetched, parsed and merged INDEPENDENTLY:
memory stays bounded by the largest single country rather than by the ~7 GB total,
and a country that fails cannot cost the other 423. There is a
`WID_fulldataset_.zip` but it is a 7,116-byte placeholder last touched in 2020 — not
the full dataset its name promises.

KEYS. `WID:{variable}:{percentile}:{age}:{pop}:{country}`, verified against the
published store: 8,731 of 8,731 ids reproduced exactly for OA-PPP. Dates are
period-END (12-31), this source's existing convention.

BUDGET. A full refresh is ~7 GB, which does not fit a CI run. The vintage below moves
only when WID republishes, so the expensive path is rare; and within a run a wall-clock
budget stops cleanly and reports PARTIAL rather than being killed at the job ceiling,
so each run makes real progress and the next resumes with the countries still stale.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import os
import re
import time

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "wid"
DEDUP = ("series_key", "obs_date")
INDEX = "https://wid.world/bulk_download/"
# One country's CSV can exceed 20 MB; the whole set is ~7 GB. Stop cleanly well
# inside the job ceiling and report what is left rather than being SIGKILLed.
BUDGET_S = float(os.environ.get("AQUEDUCT_WID_BUDGET_MIN", "180")) * 60
_ROW_RE = re.compile(r'href="(WID_data_([A-Za-z0-9\-]+)\.csv)"'
                     r'.*?<td align="right">([\d\-]+\s[\d:]+)\s*</td>'
                     r'.*?<td align="right">\s*([\dKMG.]+)', re.S)


def _index_rows():
    """[(filename, country, last_modified, size)] from the bulk directory listing."""
    r = requests.get(INDEX, headers=UA, timeout=180)
    if r.status_code != 200:
        return []
    return _ROW_RE.findall(r.text)


def current_vintage(unit):
    """Digest of the whole listing — every filename, timestamp and size.

    Watching the LISTING rather than any single file means a newly ADDED country is
    itself a change signal, not just a revision to one we already track (ledger R78,
    learned when a pinned yale_epi URL hid an entire new EPI edition).
    """
    try:
        rows = _index_rows()
    except Exception:                                         # noqa: BLE001
        return None
    if not rows:
        return None
    h = hashlib.sha256()
    for fn, _c, mod, size in sorted(rows):
        h.update(f"{fn}|{mod}|{size}\n".encode())
    return f"wid-index:{len(rows)}:{h.hexdigest()[:16]}"


def _parse(text: str, country: str):
    rd = csv.DictReader(io.StringIO(text), delimiter=";")
    keys, dates, vals = [], [], []
    n_bad = 0
    for row in rd:
        v = (row.get("value") or "").strip()
        y = (row.get("year") or "").strip()
        if not v or not y[:4].isdigit():
            continue
        try:
            fv = float(v)
        except ValueError:
            n_bad += 1
            continue
        if fv != fv:                                          # NaN
            n_bad += 1
            continue
        keys.append("WID:%s:%s:%s:%s:%s" % (
            row.get("variable"), row.get("percentile"),
            row.get("age"), row.get("pop"), row.get("country") or country))
        dates.append(dt.date(int(y[:4]), 12, 31))             # period-END
        vals.append(fv)
    return keys, dates, vals, n_bad


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    tally = Tally()

    try:
        rows = _index_rows()
    except (requests.Timeout, requests.ConnectionError) as e:
        raise TransientError(f"wid: bulk index unreachable: {e!r}") from e
    if not rows:
        tally.structural_unit("wid: bulk index listed no WID_data_*.csv")
        return finalize(tally, 0, None, source=SOURCE)

    t0 = time.time()
    total_rows = 0
    newest = None
    deferred = 0
    for fn, country, _mod, _size in sorted(rows):
        if time.time() - t0 > BUDGET_S:
            # Deferral, not a verdict: the country is left untouched so the next run
            # picks it up. Counting it as a failure would be a false alarm, and
            # skipping it silently would be worse.
            deferred += 1
            continue
        path = os.path.join(out_dir, f"{country}.parquet")
        try:
            r = requests.get(INDEX + fn, headers=UA, timeout=600)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit(country)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            tally.transient_unit(country)
            continue
        if r.status_code != 200 or len(r.content) < 200:
            tally.structural_unit(f"{country}: HTTP {r.status_code}")
            continue

        keys, dates, vals, n_bad = _parse(
            r.content.decode("utf-8-sig", errors="replace"), country)
        if not keys:
            tally.structural_unit(f"{country}: parsed 0 usable rows")
            continue
        tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                        "obs_date": pa.array(dates, pa.date32()),
                        "value": pa.array(vals, pa.float64())})
        before = blob.row_count(path) if blob.exists(path) else 0
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        tally.added_unit(max(0, n - before), country)
        total_rows += n
        if md and (newest is None or md > newest):
            newest = md

    if deferred:
        print(f"[wid] {deferred} country file(s) DEFERRED to the next run "
              f"(budget {BUDGET_S / 60:.0f} min reached) — untouched, not failed",
              flush=True)
        tally.transient_unit(f"{deferred} countries deferred")

    return finalize(tally, total_rows, newest, source=SOURCE)
