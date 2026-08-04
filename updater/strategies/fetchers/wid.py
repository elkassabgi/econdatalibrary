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
budget stops cleanly and reports PARTIAL rather than being killed at the job ceiling.

RESUME. The budget is only useful if the next run continues where this one stopped,
and that does not happen for free: the loop walks sorted(rows) from the top every
time, so with nothing to skip it re-fetches the same early countries forever and the
end of the alphabet is unreachable at ANY budget. Proven, not assumed — with the
marker disabled, three runs fetched AA / AA,BB,CC / AA,BB,CC and stalled at 3 of 8
countries; with it, 1 then 4 then 7 of 8. `_country_vintage.json` records the
listing's own (last-modified|size) per country once its merge has landed, so a run
skips what is already at the published vintage and spends its budget on what is not.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import time

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Tally, cursors_from_table, finalize, merge_cursor_map
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


def _has_expected_header(text: str) -> bool:
    """True when the body IS the WID CSV, whether or not it carries data rows.

    Distinguishes "this entity is empty upstream" (header only) from "the publisher changed
    the format" (no recognisable header) — see the call site.
    """
    first = (text.splitlines() or [""])[0]
    cols = {c.strip().strip('"').lower() for c in first.split(";")}
    return {"variable", "year", "value"} <= cols


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


DONE_FILE = "_country_vintage.json"


def _load_done(out_dir):
    """{country: "last_modified|size"} for countries already merged at that vintage."""
    try:
        raw = blob.read_bytes(os.path.join(out_dir, DONE_FILE))
        return json.loads(raw) if raw else {}
    except Exception:                                         # noqa: BLE001
        return {}                                             # a lost marker re-fetches


def _save_done(out_dir, done):
    try:
        blob.write_bytes_atomic(os.path.join(out_dir, DONE_FILE),
                                json.dumps(done, sort_keys=True).encode())
    except Exception as e:                                    # noqa: BLE001
        # Never fail the run over the resume marker — losing it costs a re-fetch,
        # not correctness. But say so, or a silently unwritten marker looks exactly
        # like a working resume that mysteriously never advances.
        print(f"[wid] WARNING: could not persist resume marker: {e!r}", flush=True)


def update(unit, since) -> Result:
    cursors: dict[str, str] = {}
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
    skipped = 0
    done = _load_done(out_dir)
    dirty = False
    for fn, country, _mod, _size in sorted(rows):
        path = os.path.join(out_dir, f"{country}.parquet")

        # RESUME. Without this the loop restarts at sorted(rows)[0] every run and
        # re-fetches the same early countries forever: a run that exhausts its budget
        # would never advance past whatever it reached the first time, so the tail of
        # the alphabet could never be fetched at all. The deferral below promises the
        # next run "picks it up" — this is the only thing that makes that true.
        # The stamp is WID's own listing metadata for that file, so it moves exactly
        # when the country is republished. Checked against the parquet actually being
        # present, because a stamp alone would suppress the fetch after a store reset.
        # TWO STAMP FORMS, because the two cases have different evidence available.
        #   "mod|size"        a real country — skip only if its parquet is actually present,
        #                     so a store reset re-fetches rather than being suppressed.
        #   "mod|size|empty"  an entity WID publishes as a 47-byte header — there is no
        #                     parquet and never will be, so requiring one would re-fetch it
        #                     on every run forever (which is exactly what used to happen).
        # Either way the stamp is WID's own listing metadata, so it moves the moment the
        # entity is republished — the only moment an empty one could gain data.
        _stamp = done.get(country)
        if _stamp == f"{_mod}|{_size}|empty" or (
                _stamp == f"{_mod}|{_size}" and blob.exists(path)):
            skipped += 1
            continue

        if time.time() - t0 > BUDGET_S:
            # Deferral, not a verdict: the country is left untouched so the next run
            # picks it up. Counting it as a failure would be a false alarm, and
            # skipping it silently would be worse.
            deferred += 1
            continue
        try:
            r = requests.get(INDEX + fn, headers=UA, timeout=600)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit(country)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            tally.transient_unit(country)
            continue
        if r.status_code != 200:
            tally.structural_unit(f"{country}: HTTP {r.status_code}")
            continue

        text = r.content.decode("utf-8-sig", errors="replace")
        keys, dates, vals, n_bad = _parse(text, country)
        if not keys:
            # AN EMPTY UPSTREAM FILE IS NOT A SCHEMA BREAK. This branch used to fire on
            # `len(r.content) < 200` and report "HTTP 200" as a structural break, so six
            # entities WID publishes as HEADER-ONLY files were flagged as breakage on every
            # run: Al, ON, ON-MER, OO-MER, OP-MER, OQ-MER are each exactly 47 BYTES on the
            # bulk index, against 17-22 MB for a real country. 47 bytes is the CSV header
            # and nothing else.
            #
            # structural_unit is the loud signal that our PARSER no longer matches the
            # publisher; spending it on "this entity has no data" both cries wolf and hides
            # the real thing it exists to catch. So: a body that still parses as the expected
            # CSV (header present, the columns we key on) with zero data rows is EMPTY;
            # anything we cannot read as that CSV at all is structural.
            if _has_expected_header(text):
                tally.empty_unit(country)
                # STAMP THE EMPTY ONES TOO, or they are re-fetched forever AND become the only
                # thing a later run attempts.
                #
                # The success branch below records `done[country] = f"{mod}|{size}"` so a
                # country is not re-downloaded until WID republishes it. This branch recorded
                # NOTHING, so the 12 header-only entities were re-fetched on every run. Worse:
                # once every real country is stamped, these 12 are the entire work list, a run
                # attempts 12 sub-units of which 12 are empty, and finalize()'s empty-window
                # guard reads that as "the source went dark" and RAISES — on a source that is
                # perfectly healthy and whose upstream never failed.
                #
                # The `|empty` suffix lets the skip gate tell the two cases apart. A real
                # country must still have its parquet present to be skipped (a stamp alone
                # would suppress the fetch after a store reset), but an entity WID publishes
                # as 47 bytes of CSV header has no parquet and never will — demanding one
                # there would defeat the stamp entirely. blob.exists(path) is False for 11 of
                # these 12 today.
                #
                # WID's own (last-modified|size) still drives it, so each is re-checked exactly
                # when republished — the only moment it could gain data.
                done[country] = f"{_mod}|{_size}|empty"
                dirty = True
            else:
                tally.structural_unit(f"{country}: body is not the expected WID CSV")
            continue
        tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                        "obs_date": pa.array(dates, pa.date32()),
                        "value": pa.array(vals, pa.float64())})
        before = blob.row_count(path) if blob.exists(path) else 0
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        # Report WHICH series moved, or the orchestrator cannot re-derive their CSVs
        # (contract §5.7) and the published downloads silently drift away from the
        # parquet: right data in the store, stale values in every CSV a user fetches.
        # Cursors come from the NEW table rather than the merged file - on a source
        # this size, reporting every series would re-derive millions of unchanged CSVs.
        merge_cursor_map(cursors, cursors_from_table(tbl, cap=CURSOR_CAP), cap=CURSOR_CAP)
        tally.added_unit(max(0, n - before), country)
        total_rows += n
        if md and (newest is None or md > newest):
            newest = md

        # Only AFTER the merge landed. Stamping on fetch would mark a country done
        # that failed to parse, and the retry would never come.
        done[country] = f"{_mod}|{_size}"
        dirty = True
        if len(done) % 25 == 0:                               # bound loss if killed
            _save_done(out_dir, done)

    if dirty:
        _save_done(out_dir, done)
    if skipped:
        print(f"[wid] {skipped} country file(s) already at the published vintage — "
              f"not re-fetched", flush=True)
    if deferred:
        print(f"[wid] {deferred} country file(s) DEFERRED to the next run "
              f"(budget {BUDGET_S / 60:.0f} min reached) — untouched, not failed",
              flush=True)
        tally.deferred_unit(f"{deferred} countries deferred")

    return finalize(tally, total_rows, newest, source=SOURCE,
                    series_cursors=cursors or None)
