"""S2 fetcher — Bank of England IADB (Interactive statistical DataBase): FX rates, yields, monetary
and banking statistics. OGL-UK-3.0, no key.

Storage (matches the ingester): ONE parquet per 3-character series-code PREFIX (XUD, IUM, CFM, RPM…),
~38 files, each dense with many series. Schema (series_key, obs_date, value); series_key = the BoE code
(e.g. XUDLUSS). The IADB CSV export returns a wide table for up to ~50 codes at a time and honours a
server-side Datefrom/Dateto — a REAL date filter — so this is a true date-tail:

  per prefix file → read its distinct codes + stored max obs_date (sane, guarded vs corrupt far-future),
  request the codes in batches of 50 over [stored_max - LOOKBACK .. today] only, parse the wide CSV,
  merge (dedup series_key+obs_date, never-shrink) back into that prefix's parquet.

CI-safe: the code universe is the distinct series_key already in each parquet (read via blob), not the
local enumerate sidecar — so refreshing existing series needs no raw-path file (R36). Genuinely-new BoE
codes are a re-enumeration concern handled separately, not here.

HONEST-STATUS: a batch whose CSV fails after retries (5xx / HTML error body / timeout) -> transient_unit
(kept, retried); a batch that parses to no obs in the window -> empty_unit; parsed obs -> added_unit.
Nothing is silently reported no_change on a transport failure.
"""
from __future__ import annotations
import datetime as dt
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after,
                      save_rotation)
from ._common import cancellable_pool

SOURCE = "boe"
CSV_URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
# The IADB CSV export takes the codes in the QUERY STRING, so the real cap is URL LENGTH,
# not a code count: a SeriesCodes value of ~1,399 chars succeeds and ~1,599 chars returns 404
# (measured 2026-07-24). Codes are NOT uniform width — XUD*/CFM* are 7 chars but VPQB4S9KY is
# 9 and RPMB8ZZOTHE is 11 — so a fixed batch COUNT that works on one prefix silently 404s on
# another. Batch by character budget instead, with headroom under the observed limit.
MAX_CODES_CHARS = 1350
LOOKBACK_DAYS = 120          # re-pull a trailing window to absorb BoE back-revisions
EPOCH = dt.date(1963, 1, 1)  # the IADB epoch; dates before this are rejected
MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTHS = {m: i for i, m in enumerate(MON, 1)}


def _fmt(d: dt.date) -> str:
    return f"{d.day:02d}/{MON[d.month - 1]}/{d.year}"


def _parse_date(s):
    s = s.strip().strip('"')
    if not s:
        return None
    parts = s.split()
    try:
        if len(parts) == 3:
            return dt.date(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
        if len(parts) == 2:
            return dt.date(int(parts[1]), MONTHS[parts[0]], 1)
        if len(parts) == 1 and parts[0].isdigit():
            return dt.date(int(parts[0]), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


def _parse_value(c):
    c = c.strip().strip('"')
    if not c:
        return None
    try:
        return float(c)
    except ValueError:
        return None


def _split_csv(line):
    if '"' not in line:
        return line.split(",")
    out, cur, q = [], [], False
    for ch in line:
        if ch == '"':
            q = not q
        elif ch == "," and not q:
            out.append("".join(cur)); cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return out


def _parse_csv(text):
    """Parse the IADB wide CSV → list[(code, obs_date, value)]. Raises ValueError on a bad header."""
    lines = text.splitlines()
    n = len(lines)
    if not lines or not lines[0].startswith("SERIES"):
        raise ValueError("unexpected CSV header")
    i = 1
    while i < n and lines[i].strip() != "":     # skip the SERIES/DESCRIPTION block
        i += 1
    while i < n and not lines[i].startswith("DATE,"):
        i += 1
    if i >= n:
        return []
    cols = _split_csv(lines[i])[1:]
    i += 1
    obs = []
    while i < n:
        row = lines[i]; i += 1
        if not row.strip():
            continue
        cells = _split_csv(row)
        od = _parse_date(cells[0])
        if od is None:
            continue
        for j, code in enumerate(cols, start=1):
            if j >= len(cells):
                break
            v = _parse_value(cells[j])
            if v is None:
                continue
            obs.append((code, od, v))
    return obs


def _fetch_csv(sess, codes, datefrom, dateto, tries=5):
    """Wide CSV for a batch of codes over [datefrom..dateto]. Exhausted retries -> TransientError."""
    params = {"csv.x": "yes", "SeriesCodes": ",".join(codes), "Datefrom": datefrom,
              "Dateto": dateto, "CSVF": "TT", "UsingCodes": "Y", "VPD": "Y", "VFD": "N"}
    for i in range(tries):
        try:
            r = sess.get(CSV_URL, params=params, timeout=300)
        except (requests.Timeout, requests.ConnectionError) as e:
            if i == tries - 1:
                raise TransientError(f"boe: {e}")
            time.sleep(2 * (i + 1) + 1); continue
        if r.status_code == 200 and r.text.lstrip().startswith("SERIES"):
            return r.text
        # a 200 with an HTML error/empty body, or a 5xx/429 -> retry
        if r.status_code in (200, 429, 500, 502, 503, 504):
            if i == tries - 1:
                raise TransientError(f"boe: non-CSV/{r.status_code} body after retries")
            time.sleep(2 * (i + 1) + 1); continue
        raise TransientError(f"boe HTTP {r.status_code}")
    raise TransientError("boe: retry budget exhausted")


def _codes_and_max(path):
    """Distinct BoE codes + sane max obs_date (<= today) for one prefix parquet."""
    if not blob.exists(path):
        return [], None
    t = blob.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return [], None
    codes = sorted({c for c in t.column("series_key").to_pylist() if c})
    md = pc.max(t.column("obs_date")).as_py()
    if isinstance(md, dt.date) and md > dt.date.today():
        # ignore a corrupt far-future stamp when choosing the refetch window
        od = t.column("obs_date").to_pylist()
        today = dt.date.today()
        sane = [d for d in od if isinstance(d, dt.date) and d <= today]
        md = max(sane) if sane else None
    return codes, (md if isinstance(md, dt.date) else None)


def _chunk_by_len(codes, max_chars=MAX_CODES_CHARS):
    """Group codes into batches whose comma-joined SeriesCodes value stays under `max_chars`.

    Length-aware (not fixed-count) because BoE's limit is on the URL, and code widths vary by
    prefix (7-11 chars) — a fixed count tuned on 7-char codes would 404 on the wider ones.
    A single code longer than the budget still goes out alone rather than being dropped.
    """
    batch, n = [], 0
    for c in codes:
        add = len(c) + (1 if batch else 0)      # +1 for the joining comma
        if batch and n + add > max_chars:
            yield batch
            batch, n = [c], len(c)
        else:
            batch.append(c)
            n += add
    if batch:
        yield batch


MAX_WORKERS = 5   # BoE tolerates ~6 concurrent (the ingester's proven level); stay under
# Prefixes fetched+merged per deadline check. The wave is what bounds the run: every
# task in a wave is submitted at once, so a smaller wave means a tighter budget and
# more merges, a larger one means fewer merges but a coarser stop.
PREFIX_WAVE = int(os.environ.get("BOE_PREFIX_WAVE", "6"))


def _fetch_batch_task(batch, datefrom, dateto):
    """Thread task: fetch+parse ONE code-batch over the window. Own session (requests.Session
    is not safe to share across threads). Returns ('added', obs) | ('empty', []) | ('transient', [])."""
    sess = requests.Session()
    sess.headers.update(UA)
    try:
        text = _fetch_csv(sess, batch, datefrom, dateto)
    except TransientError:
        return "transient", []
    try:
        obs = _parse_csv(text)
    except ValueError:
        return "transient", []          # a malformed body is a transport hiccup, not real no-data
    return ("added", obs) if obs else ("empty", [])


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    prefixes = blob.list_parquets(out_dir)
    tally = Tally()
    dateto = _fmt(dt.date.today())
    grand_total = 0
    grand_max: dt.date | None = None
    cursors: dict[str, str] = {}

    # FETCH IN BOUNDED, ROTATING WAVES OF PREFIXES.
    #
    # This used to submit all ~613 CSV fetches at once, collect every result, and only then
    # merge. boe takes 105 MINUTES (measured, cloud state 2026-07-24) against the
    # orchestrator's 45-minute per-unit cap, which landed 2026-08-01 — after boe's last run.
    # So on its next run it would be killed during the fetch phase, before the first merge,
    # and store NOTHING: not a truncated run but a discarded one, on 3,860,998 obs, looking
    # exactly like a busy healthy run in the log (R243).
    #
    # It cannot finish inside the cap at any submission order — it needs ~3 ticks — so the
    # fix has to be a budget AND a bookmark. Prefixes are the natural unit: merges, cursors
    # and the empty-window floor are all per-prefix, so a wave that completes is coherent on
    # its own and a wave that never starts costs nothing but a deferral.
    budget_min = float(os.environ.get("BOE_BUDGET_MIN", "35"))
    dl = Deadline(minutes=budget_min)
    ordered = rotate_after(list(prefixes), load_rotation(out_dir))
    stopped_early = False
    last_pf = ""
    done_pf = 0
    today = dt.date.today()

    for wave_start in range(0, len(ordered), PREFIX_WAVE):
        if dl.spent():
            stopped_early = True
            print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — {done_pf}/{len(ordered)} prefixes done, "
                  f"{len(ordered) - done_pf} deferred to the next tick "
                  f"(resuming after {last_pf!r})", flush=True)
            break
        wave = ordered[wave_start:wave_start + PREFIX_WAVE]

        # Build this wave's tasks (cheap serial reads); the CSV fetches are the parallel part.
        prefix_obs: dict[str, list] = {}
        tasks = []                        # (prefix, batch_codes, datefrom)
        for pf in wave:
            path = os.path.join(out_dir, pf)
            codes, smax = _codes_and_max(path)
            if not codes:
                grand_total += blob.row_count(path)
                tally.empty_unit()
                continue
            start = max(EPOCH, (smax - dt.timedelta(days=LOOKBACK_DAYS)) if smax else EPOCH)
            prefix_obs[pf] = []
            for batch in _chunk_by_len(codes):
                tasks.append((pf, batch, _fmt(start)))

        if tasks:
            with cancellable_pool(MAX_WORKERS) as ex:
                futs = {ex.submit(_fetch_batch_task, b, df, dateto): pf
                        for (pf, b, df) in tasks}
                for fut in as_completed(futs):
                    pf = futs[fut]
                    outcome, obs = fut.result()
                    if outcome == "transient":
                        tally.transient_unit()
                    elif outcome == "empty":
                        tally.empty_unit()
                    else:
                        tally.added_unit(len(obs))
                        prefix_obs[pf].extend(obs)

        # Merge this wave before starting the next one (serial — atomic, one file at a time).
        # Merging per wave rather than once at the end is the whole point: an interruption
        # after this line has already published everything the wave fetched.
        for pf, obs in prefix_obs.items():
            path = os.path.join(out_dir, pf)
            if not obs:
                grand_total += blob.row_count(path)
                continue
            keys = [o[0] for o in obs]
            dates = [o[1] for o in obs]
            vals = [o[2] for o in obs]
            tbl = pa.table({
                "series_key": pa.array(keys, pa.string()),
                "obs_date": pa.array(dates, pa.date32()),
                "value": pa.array(vals, pa.float64()),
            })
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
            grand_total += n
            for k, d in zip(keys, dates):
                iso = d.isoformat()
                if k not in cursors or iso > cursors[k]:
                    cursors[k] = iso
                if isinstance(d, dt.date) and d <= today and (grand_max is None or d > grand_max):
                    grand_max = d

        done_pf += len(wave)
        last_pf = wave[-1]

    # Deferred prefixes were not touched, but their rows are still IN the store. Counting
    # only what this tick walked would report the source as having shrunk to a fraction of
    # its size, and merge's never-shrink guard exists precisely because that reads as data
    # loss.
    for pf in ordered[done_pf:]:
        grand_total += blob.row_count(os.path.join(out_dir, pf))

    if last_pf:
        save_rotation(out_dir, last_pf)

    last_obs = grand_max.isoformat() if grand_max else (since or None)
    # The floor must be measured against the prefixes this tick actually ATTEMPTED; against
    # the full list a bounded pass would look like a wholesale outage every time.
    floor = (done_pf + 1) if stopped_early else (len(prefixes) + 1)
    return finalize(tally, grand_total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=max(floor, 1))
