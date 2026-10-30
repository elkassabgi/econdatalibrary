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
The marker is written after EVERY country's merge or empty stamp (2026-10-30). It used to be
written when `len(done) % 25 == 0`. That counts entries, not new stamps, and on a re-issue every
country already has an entry. So during a re-issue the save inside the loop never ran, and a
pass killed mid-loop (the desktop's taskkill) lost every stamp it had written.

TWO-PHASE STAMP (2026-10-30, review AR-406 O1). A stamp keeps a country from being downloaded
again, so a stamp must never outlive the changed keys it stands for. A merge that changed served
values is stamped "mod|size|owed" in the loop. Only the end of an update that returns those keys
in its Result turns the marks final ("mod|size"). A pass that finds an owed mark (an earlier pass
was killed, or finalize raised) does not download the country. It reads the stored parquet and
reports every key in it; if that file cannot be read, the mark stays owed and the country is a
transient sub-unit, so the pass is partial and never no_change. If WID has republished an owed
country meanwhile, the pass that fetches it reports every key of the stored file, not only its own
diff. A merge that changed nothing is stamped final at once. Before each merge
the marker also records "merging|<old stamp>", so a kill between the merge's publish and its owed
stamp makes the pass that re-fetches the country report all of its keys (its re-merge alone would
report none). So the marker is written twice per merged country and once per header-only entity:
at most about 850 small writes in a full pass. Each is atomic (blob.write_bytes_atomic: a temp file
and os.replace locally, one PUT on R2); a kill can leave a stray "<marker>.<pid>.<id>.tmp" file
beside it locally. Stated residual: a kill after the end-of-update final save and before the
orchestrator's CSV step loses those keys.

CHANGE SIGNAL. The merge reports its changed keys (merge_and_write report_changed_keys=True), and
they are the Result's changed_keys, the set the CSV step derives. A revision with no added row is
`revised_unit`, never empty. Past CURSOR_CAP changed keys the set is reported unknown and
Result.cursor_cap_hit is set, so the orchestrator books full_rederive_owed.

WINDOW END (2026-10-30, PLAN R10.3 step 4, W3b). BUDGET_S counts from this fetcher's own start
and knows nothing of the pass it runs in. On the desktop, tools/run_local_heavy.ps1 clamps the run
budget to the CI window and kills the updater 10 minutes after it ends. So before each country
there is a second check. When the orchestrator has set a run ceiling (orchestrate._RUN_DEADLINE_TS),
a country is started only if the time left before that ceiling is at least the margin. The margin
is max(floor, 2 x the longest country this run has taken so far). The floor is
AQUEDUCT_WID_MARGIN_MIN, default 5 min. It is a CHOSEN value, not a measurement. The plan's
measured margin (W3: 2 x the longest country seen over days of the timing lines below) is meant
to replace it. Without a ceiling (AQUEDUCT_RUN_BUDGET_MIN <= 0, or a call outside run_once) nothing
changes. A CI run has a ceiling too, single-source dispatches included; there
AQUEDUCT_WID_BUDGET_MIN (60 in updater-daily.yml) normally stops wid first. A
country that is not started is deferred exactly like a budget deferral: no stamp, untouched,
`partial`.

TIMING. Each attempted country prints "[wid] country k/n start <C> at <UTC>Z (unit +Ns)" before its
download, and "[wid] country k/n done <C>: <outcome>; download Xs, parse Ys, merge Zs; total Ts"
on every outcome (the phases it reached). A country that a kill ends has a start line and no done
line. The update also prints the run ceiling in UTC, or that none is set.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import re
import sys
import time

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import (CURSOR_CAP, Tally, _max_by_key, cursors_from_table, finalize,
                      merge_cursor_map)
from ._vintage import UA

SOURCE = "wid"
DEDUP = ("series_key", "obs_date")
INDEX = "https://wid.world/bulk_download/"
# One country's CSV can exceed 20 MB; the whole set is ~7 GB. Stop cleanly well
# inside the job ceiling and report what is left rather than being SIGKILLed.
BUDGET_S = float(os.environ.get("AQUEDUCT_WID_BUDGET_MIN", "180")) * 60
# The floor of the window-end margin (see WINDOW END in the docstring). Chosen, not measured.
MARGIN_FLOOR_MIN_DEFAULT = 5.0
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


def _margin_floor_s() -> float:
    """AQUEDUCT_WID_MARGIN_MIN in seconds. Read at each update so a test or a run can set it. A value
    that is not a finite number >= 0 falls back to the default and says so: a bad value must not
    remove the margin."""
    raw = os.environ.get("AQUEDUCT_WID_MARGIN_MIN")
    if raw is None or raw == "":
        return MARGIN_FLOOR_MIN_DEFAULT * 60
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = float("nan")
    if not (math.isfinite(v) and v >= 0):
        print(f"[wid] AQUEDUCT_WID_MARGIN_MIN={raw!r} is not a finite number >= 0 - using "
              f"{MARGIN_FLOOR_MIN_DEFAULT:g} min", flush=True)
        return MARGIN_FLOOR_MIN_DEFAULT * 60
    return v * 60


def _window_end_ts():
    """The pass's run ceiling as an epoch timestamp, or None when no ceiling is set.

    orchestrate.run_once sets _RUN_DEADLINE_TS from AQUEDUCT_RUN_BUDGET_MIN, which the desktop's
    run_local_heavy.ps1 clamps to the time left before the CI window. The ps1 kills the updater 10
    minutes after it. Read through sys.modules, as _giant reads the orchestrator's flags, so this
    module does not import the orchestrator."""
    orch = sys.modules.get("updater.orchestrate")
    ts = getattr(orch, "_RUN_DEADLINE_TS", None) if orch is not None else None
    if isinstance(ts, bool) or not isinstance(ts, (int, float)) or not math.isfinite(ts):
        return None
    return float(ts)




# The three stamp forms in _country_vintage.json (all keyed on WID's own listing metadata):
#   "mod|size"        merged at that vintage and HANDED OVER: an earlier pass returned its changed keys.
#   "mod|size|owed"   merged at that vintage, changed served values, and the keys have NOT been handed
#                     over yet (the pass was killed, or finalize raised). Not fetched again; its keys are
#                     reported by the pass that finds the mark (see _carry).
#   "mod|size|empty"  WID publishes the entity as a header-only file: no parquet, never fetched again
#                     until it is republished.
#   "mod|size|empty|owed"  header-only now, but the stored parquet holds values an earlier merge
#                     revised and no Result has handed over. The gate does not match it, so the next
#                     pass fetches the 47-byte file again and carries the stored keys again.
# A stamp that owes keys ("|owed" at any vintage, or "merging|") and is NOT skipped by the gate (WID
# republished the country, or its parquet is gone) makes the fetching pass hand over EVERY key of the
# stored file: its merge's report holds only the diff against the store, and the earlier merge's
# revisions are already in the store (AR-406 F2).
# and one transient form, written BEFORE each merge:
#   "merging|<the stamp it had>"   a merge was about to publish. If a kill lands between the publish
#                     and the owed stamp, the country is fetched again (the gate does not match this
#                     form), and its re-merge then changes nothing - the data landed the first time - so
#                     the merge report alone would hand over NO key. This form makes that pass report
#                     every key of the country instead.
_OWED = "|owed"
_EMPTY = "|empty"
_MERGING = "merging|"


def _changed_fold(changed, src, cap=None) -> bool:
    """Fold a merge-measured {key: date} report into `changed`. True once the set passes `cap`.

    The bound is CURSOR_CAP, the same one the cursor channel has: every changed key is one
    _catalog_ids_for query and possibly one CSV, so an unbounded set is the risk CURSOR_CAP's
    docstring describes. Past it the caller reports the changed set as UNKNOWN (changed_keys None,
    cursor_cap_hit True), and the orchestrator books full_rederive_owed (ember does the same)."""
    if cap is None:
        cap = CURSOR_CAP               # the module's value at call time
    for k, v in src.items():
        if k not in changed:
            changed[k] = v
        elif v is not None and (changed[k] is None or str(v) > str(changed[k])):
            changed[k] = v
    return len(changed) > cap


def _carry(path):
    """{key: max obs_date} for EVERY series a country's stored parquet holds. RAISES when the file
    cannot be read: an owed mark must never be cleared on an empty answer.

    An owed mark does not record WHICH keys changed (that report died with the pass), so a carried
    country reports all of its series. That over-reports - a CSV whose bytes did not move is
    re-derived - and never under-reports."""
    tbl = blob.read_table(path, columns=["series_key", "obs_date"])
    if tbl.num_rows == 0:
        return {}
    return _max_by_key(tbl)


def update(unit, since) -> Result:
    cursors: dict[str, str] = {}
    changed: dict = {}             # the MERGE-MEASURED changed set (merge_and_write report)
    changed_over = False           # the changed set passed CURSOR_CAP: reported as unknown
    cursors_capped = False         # the per-series freshness cursors were cut at CURSOR_CAP
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
    identical = 0                  # merged, and the merge changed no served value
    carried: list = []             # countries whose owed mark this pass hands over
    owed_now: list = []            # countries this pass merged with a change (marked owed)
    done = _load_done(out_dir)
    if not isinstance(done, dict):
        done = {}
    dirty = False
    floor_s = _margin_floor_s()
    end_ts = _window_end_ts()      # read HERE, at update time, never at import
    if end_ts is None:
        print(f"[wid] no run ceiling set - the window-end check is off "
              f"(budget {BUDGET_S / 60:.0f} min only)", flush=True)
    else:
        print(f"[wid] run ceiling {dt.datetime.fromtimestamp(end_ts, dt.timezone.utc):%Y-%m-%dT%H:%M:%S}Z "
              f"({(end_ts - time.time()) / 60:.1f} min left); margin floor {floor_s / 60:g} min",
              flush=True)
    longest = 0.0                  # the longest country this pass has taken, start to done line
    capped_by = None               # "budget" or "window" once a country is not started
    cap_left_s = cap_margin_s = 0.0
    n_rows = len(rows)

    def _stop_now() -> bool:
        """The two stop checks, before each country (and each carried owed mark), never inside one.
        Once one fires it stays fired: elapsed time only grows, the time left only shrinks.
          budget  BUDGET_S from this fetcher's own start (AQUEDUCT_WID_BUDGET_MIN).
          window  the pass's run ceiling: start only with at least the margin left,
                  margin = max(floor, 2 x the longest country so far). See WINDOW END above."""
        nonlocal capped_by, cap_left_s, cap_margin_s
        if capped_by is None:
            now = time.time()
            margin_s = max(floor_s, 2.0 * longest)
            if now - t0 > BUDGET_S:
                capped_by = "budget"
            elif end_ts is not None and end_ts - now < margin_s:
                capped_by = "window"
                cap_left_s, cap_margin_s = end_ts - now, margin_s
        return capped_by is not None

    for k, (fn, country, _mod, _size) in enumerate(sorted(rows), 1):
        path = os.path.join(out_dir, f"{country}.parquet")

        # RESUME. Without this the loop restarts at sorted(rows)[0] every run and
        # re-fetches the same early countries forever: a run that exhausts its budget
        # would never advance past whatever it reached the first time, so the tail of
        # the alphabet could never be fetched at all. This skip is what lets a later
        # pass start where this one stopped.
        # The stamp is WID's own listing metadata for that file, so it moves exactly
        # when the country is republished. A merged country (final or owed form) is skipped
        # only if its parquet is actually present, so a store reset re-fetches rather than
        # being suppressed. The "|empty" form needs no parquet: an entity WID publishes as a
        # 47-byte header has none and never will, so requiring one would re-fetch it on every
        # run forever (which is exactly what used to happen).
        _stamp = done.get(country)
        _base = f"{_mod}|{_size}"
        # A stamp that still OWES keys, whatever vintage it names: "merging|..." or "...|owed".
        # When the gate below does not skip the country (WID republished it, or its parquet is
        # gone), the pass that fetches it must hand over every key of the stored file, not only
        # its own merge's diff (see the merge and the empty branch).
        owes_all = isinstance(_stamp, str) and (_stamp.startswith(_MERGING)
                                                or _stamp.endswith(_OWED))
        if _stamp == _base + _EMPTY or (_stamp in (_base, _base + _OWED) and blob.exists(path)):
            if _stamp == _base + _OWED:
                # TWO-PHASE STAMP, phase 2: merged by an earlier pass that never handed its keys
                # over. Not downloaded again; its keys go into THIS pass's changed set. Reading the
                # stored file takes time too, so it is under the same stop checks; a mark not
                # reached stays owed and the pass is partial.
                if _stop_now():
                    deferred += 1
                    continue
                try:
                    ch = _carry(path)
                except Exception as e:                        # noqa: BLE001
                    # Unreadable (a corrupt file, or a transient R2 GET): keep the mark, so a later
                    # pass retries it, and TALLY it, so this pass is partial. Without the tally
                    # (AR-406 F1, K5) the pass read no_change: last_success was set, wid was not due
                    # again for days, and the debt showed nowhere. The mark is never cleared
                    # without its keys in a Result.
                    print(f"[wid] {country}: owed mark kept - stored keys unreadable: {e!r}",
                          flush=True)
                    tally.transient_unit(f"{country}: owed keys unreadable")
                    continue
                # Not counted in `skipped`: a carried country is in the "carried" line, and its keys
                # are still owed, so it is not "already at the published vintage" in the log's sense.
                carried.append(country)
                changed_over = _changed_fold(changed, ch) or changed_over
                cursors_capped = merge_cursor_map(cursors, ch, cap=CURSOR_CAP) or cursors_capped
            else:
                skipped += 1
            continue

        if _stop_now():
            # Deferral, not a verdict: the country is left untouched (no stamp), so a later
            # pass fetches it. Counting it as a failure would be a false alarm, and
            # skipping it silently would be worse.
            deferred += 1
            continue

        c0 = time.time()
        print(f"[wid] country {k:,}/{n_rows:,} start {country} at "
              f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%S}Z (unit +{c0 - t0:.0f}s)",
              flush=True)
        outcome = "error"              # what the done line says if an exception leaves the country
        t_dl = t_parse = t_merge = None
        try:
            try:
                r = requests.get(INDEX + fn, headers=UA, timeout=600)
            except (requests.Timeout, requests.ConnectionError):
                tally.transient_unit(country)
                outcome = "transient (no response)"
                continue
            finally:
                t_dl = time.time() - c0
            if r.status_code in (429, 500, 502, 503, 504):
                tally.transient_unit(country)
                outcome = f"transient (HTTP {r.status_code})"
                continue
            if r.status_code != 200:
                tally.structural_unit(f"{country}: HTTP {r.status_code}")
                outcome = f"structural (HTTP {r.status_code})"
                continue

            p0 = time.time()
            text = r.content.decode("utf-8-sig", errors="replace")
            keys, dates, vals, n_bad = _parse(text, country)
            t_parse = time.time() - p0
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
                    if owes_all and blob.exists(path):
                        # Republished as header-only while an earlier merge's keys are still owed
                        # (an "|owed" or "merging|" stamp). The stored parquet keeps those values
                        # and nobody has derived them: hand them over here, and stamp
                        # "|empty|owed" so the debt survives until this Result settles it.
                        try:
                            ch_e = _carry(path)
                        except Exception as e:                # noqa: BLE001
                            print(f"[wid] {country}: owed mark kept - stored keys unreadable: "
                                  f"{e!r}", flush=True)
                            tally.transient_unit(f"{country}: owed keys unreadable")
                            outcome = "empty upstream, owed keys unreadable, mark kept"
                            continue
                        tally.empty_unit(country)
                        changed_over = _changed_fold(changed, ch_e) or changed_over
                        cursors_capped = (merge_cursor_map(cursors, ch_e, cap=CURSOR_CAP)
                                          or cursors_capped)
                        done[country] = _base + _EMPTY + (_OWED if ch_e else "")
                        if ch_e:
                            carried.append(country)
                        dirty = True
                        _save_done(out_dir, done)
                        outcome = (f"empty (header only), {len(ch_e):,} owed keys carried, "
                                   f"stamped {'empty owed' if ch_e else 'empty'}")
                        continue
                    tally.empty_unit(country)
                    # STAMP THE EMPTY ONES TOO, or they are re-fetched forever AND become the only
                    # thing a later run attempts: once every real country is stamped, these 12 are
                    # the entire work list, a run attempts 12 sub-units of which 12 are empty, and
                    # finalize()'s empty-window guard reads that as "the source went dark" and
                    # RAISES - on a source that is perfectly healthy. WID's own
                    # (last-modified|size) still drives the stamp, so each is re-checked exactly
                    # when republished - the only moment it could gain data.
                    done[country] = _base + _EMPTY
                    dirty = True
                    _save_done(out_dir, done)                 # per country: see RESUME
                    outcome = "empty (header only), stamped"
                else:
                    tally.structural_unit(f"{country}: body is not the expected WID CSV")
                    outcome = "structural (not the expected WID CSV)"
                continue
            tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                            "obs_date": pa.array(dates, pa.date32()),
                            "value": pa.array(vals, pa.float64())})
            m0 = time.time()
            before = blob.row_count(path) if blob.exists(path) else 0
            # MERGE-MEASURED CHANGE (2026-10-30). `n - before` counts only rows ADDED, so a pure
            # revision - WID's usual re-issue (R764: 66 of 148 sampled series revised, 0 new dates)
            # - read as EMPTY: with more than 10 countries that tripped finalize()'s empty-window
            # guard into a false "structural break" AFTER the marker had stamped every country, and
            # the orchestrator books that as `partial` with no Result: no keys, no CSV step, and
            # the following pass skips them all. The merge's own report sees a revision; the cap
            # is this table's size so the report can never refuse.
            # The "merging|" form goes to the marker BEFORE the merge publishes (see the stamp forms
            # above): a kill between the publish and the owed stamp must not leave data that no
            # marker and no Result speaks for.
            was_merging = isinstance(_stamp, str) and _stamp.startswith(_MERGING)
            done[country] = _stamp if was_merging else _MERGING + str(_stamp or "")
            _save_done(out_dir, done)
            n, md, ch = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP,
                                              report_changed_keys=True,
                                              changed_keys_cap=max(tbl.num_rows, 1))
            if owes_all:
                # The stored file holds values no Result has handed over: an earlier pass died
                # inside this country's merge ("merging|"), or merged it and never settled its owed
                # mark ("|owed") before WID republished it (AR-406 F2, K7). Today's report sees
                # only the diff against the store, so it would drop every key the earlier merge
                # revised and this one did not. Hand over every key the file holds.
                ch = {**_carry(path), **ch}
            t_merge = time.time() - m0
            # Cursors (per-series freshness) still come from the NEW table, as before.
            cursors_capped = merge_cursor_map(cursors, cursors_from_table(tbl, cap=CURSOR_CAP),
                                              cap=CURSOR_CAP) or cursors_capped
            changed_over = _changed_fold(changed, ch) or changed_over
            added = max(0, n - before)
            if added:
                tally.added_unit(added, country)
            elif ch:
                tally.revised_unit(country)    # changed served values without adding a row
            else:
                # Merged and identical. Not dark: it parsed rows. Counted empty as before, and
                # excluded from the empty-window guard's floor below for that reason.
                tally.added_unit(0, country)
                identical += 1
            total_rows += n
            if md and (newest is None or md > newest):
                newest = md

            # TWO-PHASE STAMP, phase 1. Only AFTER the merge landed (stamping on fetch would mark
            # a country done that failed to parse, and the retry would never come). A merge that
            # changed served values is stamped OWED: the stamp keeps the country from being
            # downloaded again, and the owed form keeps its keys reachable until a pass returns
            # them. A merge that changed nothing has nothing to hand over and is stamped final.
            done[country] = _base + (_OWED if ch else "")
            if ch:
                owed_now.append(country)
            dirty = True
            # PER COUNTRY, so a kill keeps every stamp written before it. This used to be
            # `if len(done) % 25 == 0`, which never fires on a re-issue (see RESUME).
            _save_done(out_dir, done)
            outcome = (f"{'changed' if ch else 'identical'}, {len(keys):,} rows parsed, "
                       f"{len(ch):,} keys changed, +{added:,} rows, "
                       f"{'stamped owed' if ch else 'stamped'}")
        finally:
            took = time.time() - c0
            longest = max(longest, took)
            parts = ", ".join(f"{lbl} {v:.1f}s" for lbl, v in
                              (("download", t_dl), ("parse", t_parse), ("merge", t_merge))
                              if v is not None)
            print(f"[wid] country {k:,}/{n_rows:,} done {country}: {outcome}; "
                  f"{parts}; total {took:.1f}s", flush=True)

    if dirty:
        _save_done(out_dir, done)
    if skipped:
        print(f"[wid] {skipped} country file(s) already at the published vintage — "
              f"not re-fetched", flush=True)
    if carried:
        print(f"[wid] {len(carried)} country file(s) merged by an earlier pass whose keys were "
              f"never handed over (owed mark) - their keys are reported now", flush=True)
    if deferred:
        if capped_by == "window":
            why = (f"{cap_left_s / 60:.1f} min left before the run ceiling, under the "
                   f"{cap_margin_s / 60:.1f}-min margin")
        else:
            why = f"budget {BUDGET_S / 60:.0f} min reached"
        print(f"[wid] {deferred} country file(s) DEFERRED to a later pass "
              f"({why}) — untouched, not failed", flush=True)
        tally.deferred_unit(f"{deferred} countries deferred ({capped_by})")

    # finalize() may RAISE (a structural unit). The owed marks are already saved above, so a raise
    # leaves every merged-and-changed country owed, and a later pass hands its keys over.
    # Identical merges are not "the source went dark": they raise the empty-window floor.
    res = finalize(tally, total_rows, newest, source=SOURCE, series_cursors=cursors or None,
                   empty_window_floor=10 + identical)
    # WHAT THE CSV STEP DERIVES. The orchestrator derives res.changed_keys when it is a dict (the
    # merge-measured set) and falls back to res.series_cursors only when it is None. So the cap that
    # matters is the changed set's: under CURSOR_CAP it is complete and is the derive set; past it
    # the set is reported UNKNOWN and Result.cursor_cap_hit is set, which the orchestrator books as
    # full_rederive_owed (orchestrate.py, the cursor_cap_hit branch of the CSV step). Before this,
    # merge_cursor_map's True was thrown away, the cursors were the derive set, and 50,000 of
    # 2,465,197 series (about six countries) was all a re-issue could ever derive (R764).
    if cursors_capped:
        print(f"[wid] per-series cursors capped at {CURSOR_CAP:,} (freshness only - the derive "
              f"set is the merge-measured changed set)", flush=True)
    if changed_over:
        print(f"[wid] changed keys passed {CURSOR_CAP:,} this pass - reporting the changed set as "
              f"UNKNOWN (cursor_cap_hit); a full re-derive is owed", flush=True)
        res.changed_keys = None
        res.cursor_cap_hit = True
    else:
        res.changed_keys = changed
    if carried and res.status == "no_change":
        # Nothing new this pass, but owed keys are handed over: the CSV step must run, and only
        # ok/partial reach it (orchestrate._should_derive_csvs).
        res.status = "ok"
        res.error = (f"{res.error}; {len(carried)} country file(s) carried from an earlier pass's "
                     f"owed mark")

    # TWO-PHASE STAMP, the end: this Result carries every owed country's keys (or, past the cap,
    # cursor_cap_hit, which the orchestrator books as full_rederive_owed), so the marks become final.
    # A kill between this save and the orchestrator's CSV step is NOT covered (stated residual).
    settled = [c for c in carried + owed_now if str(done.get(c, "")).endswith(_OWED)]
    if settled:
        for c in settled:
            done[c] = done[c][:-len(_OWED)]
        _save_done(out_dir, done)
    return res
