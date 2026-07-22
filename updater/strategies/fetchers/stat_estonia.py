"""S3 (sdmx_delta) fetcher — Statistics Estonia (andmed.stat.ee PxWeb v1). No key.

Statistics Estonia is a pure **PxWeb** source (no SDMX 2.1, so no ?updatedAfter /
?startPeriod — the date-tail is expressed as a POST whose TIME dimension is restricted
to the period codes that come AFTER the stored max). The strategy is `sdmx_delta`: for
each table we read the already-published max(obs_date) and request only newer periods.

LAYOUT (set by jobs/ingest_stat_estonia.py): ONE parquet per top-level SUBJECT area
(the first segment of a table's PxWeb path) under clean_full/stat_estonia/<subject>.parquet
with the uniform schema
    series_key : "EE:<path-with-':'-separators>[:<dim>=<code>...]"  (string)
    obs_date   : date32  (parsed from the table's FIRST time dimension; see below)
    value      : float64
Dedup key is (series_key, obs_date) for every subject file (verified uniform on disk).

SUB-UNIT = one PxWeb table (the catalog's `type == "t"` leaves). There are ~4978 of
them across 7 subjects. We reuse the ingester's catalog (data/clean_full/stat_estonia/
_catalog.json via crawl_catalog) and its PARSE logic verbatim — parse_jsonstat2,
is_time_dim, parse_date, MAX_CELLS, the EE:-prefix and the over-MAX_CELLS aggregation
rule — so the keys/dates we write are byte-identical to what the bulk ingester wrote.
We do NOT re-discover the catalog or re-implement the parser.

TIME-DIM SUBTLETY (must match the parser exactly): a PxWeb table can carry TWO
time-ish variables (e.g. Aasta=year AND Kuu=month). parse_jsonstat2 keys obs_date on
the axis picked by the shared value-first resolver (core/pxweb.py: authoritative
`time: true` code, else highest date-parse-rate, else literal name) and pushes every
OTHER dimension — including a second time-ish one like Kuu — into the series_key. So
the date-tail must restrict ONLY that resolved dimension to newer period codes and
request the other variables exactly as a full pull would, or whole month-keys would
be dropped. _build_query resolves the axis with the SAME inputs the parser uses.

HONEST STATUS (Tally + finalize): each table is a sub-unit. We do our OWN HTTP (rather
than the ingester's error-swallowing get_json/post_json) so we can tell apart:
  - transient  : timeout / 5xx / 429 / connection drop / non-JSON-200-under-load
                 -> transient_unit()  => the WHOLE run is 'partial' (orchestrator does
                    NOT stamp last_success; the unit re-runs next tick). Existing data
                    is left untouched.
  - structural : a 200 with a real time-series envelope (a parseable JSON-stat2 body
                 that DOES contain a time dimension) that nonetheless parsed 0 usable
                 observations on a FULL pull of a table that previously had data
                 -> structural_unit() => finalize raises DefinitiveError.
  - empty      : 404/400 (table gone from the tree / range empty), a table with no time
                 dimension at all (one-off census snapshots), or a date-tail that
                 legitimately returned nothing newer -> empty_unit().
A new period merged -> added_unit(n). merge.merge_and_write enforces never-shrink, so
existing data is preserved in every failure branch (this is about correct STATUS).

series_cursors map {table-prefix "EE:...": 'YYYY-MM-DD'} carry per-table freshness so a
frozen single table can't hide behind the subject-level max.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import random
import re
import time
from collections import defaultdict

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, sane_since, structural_on_zero_rows

import sys
# The shared value-first PxWeb time-axis resolver lives in this repo's core/ package
# (core/pxweb.py). Derive the repo root from __file__ — updater/strategies/fetchers/ is
# four levels below it — so `from core import pxweb` resolves to THIS checkout's copy both
# when the updater imports this fetcher as a package and if it is loaded standalone. No
# hardcoded ROOT: only the worktree carries core/pxweb.py on this branch (same __file__
# convention as jobs/ingest_hagstofa.py and tools/pxweb_regression.py).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from core import pxweb as _pxweb

SOURCE = "stat_estonia"
DEDUP = ("series_key", "obs_date")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
RATE = 0.25            # polite spacing between table requests
MAX_ATTEMPTS = 5
META_TIMEOUT = 60
DATA_TIMEOUT = 120

# Statistics Estonia legitimately publishes FORWARD-LOOKING period codes that are NOT
# "the latest observation": population-PROJECTION tables carry obs_date out to 2085, and
# some discontinued/open-ended PxWeb dimensions use the sentinel year 9999 ("not
# applicable"). Those are real values we must keep on disk and report honestly in the
# per-table series_cursors (a 2085 projection cursor is genuinely fresh, not stale). But
# the UNIT-LEVEL last_obs rollup is a FRESHNESS signal (health.py turns it into obs_age),
# so a 9999/2085 value would make the source look perpetually fresh. We therefore cap the
# unit-level rollup at a sane real-observation horizon (today + ~13 months); anything
# beyond that can only be a projection/sentinel, never an actual latest observation. The
# stored data and the per-table cursors are untouched by this cap.
OBS_HORIZON = dt.date.today() + dt.timedelta(days=400)


def _bump_unit_max(maxd: dt.date | None, d: dt.date) -> dt.date | None:
    """Advance the unit-level freshness rollup, ignoring projection/sentinel far-future
    dates (kept verbatim on disk and in per-table cursors, just not used as 'freshness')."""
    if d > OBS_HORIZON:
        return maxd
    return d if (maxd is None or d > maxd) else maxd


def _cursor_value(d: dt.date) -> str:
    """ISO string to record in series_cursors for a per-table max(obs_date).

    health.assess() computes a source's DATA-recency frontier as max(cursors.values())
    (health.py: newest_obs = max(obs_vals)). A corrupt/sentinel far-future obs_date
    (Statistics Estonia's discontinued-table tree stores year-9999 cells; population
    PROJECTION tables run out to 2085) would therefore become the reported frontier and
    make a genuinely frozen source look perpetually fresh — masking RED-DATA. We cap any
    cursor beyond the sane real-observation horizon to today so the table still reports a
    cursor (it stays TRACKED, not dropped) but can never inflate the freshness max. The
    on-disk data and the merge are untouched — this only affects the reported cursor."""
    if d > OBS_HORIZON:
        return dt.date.today().isoformat()
    return d.isoformat()


# Allow only PxWeb subject tokens that are safe as a single path component. The parquet
# write target is os.path.join(out_dir, f"{subj}.parquet") and merge_and_write ->
# blob.write_table_atomic does os.makedirs(dirname) + os.replace, with NO containment
# check that the path stays under out_dir. `subj` derives from an upstream-supplied
# catalog `path` (PxWeb item ids), so a poisoned/MITM'd/malformed catalog row could
# otherwise carry '..', a path separator, or a drive letter and write outside the source
# dir. Reject anything that isn't a plain [A-Za-z0-9_.-] token.
_SAFE_SUBJ = re.compile(r"^[A-Za-z0-9_.-]+$")


def _safe_subject(subj: str) -> str | None:
    """Return subj if it is a single safe filename component, else None (skip + log)."""
    if not subj or subj in (".", ".."):
        return None
    if "/" in subj or "\\" in subj or os.sep in subj or (os.altsep and os.altsep in subj):
        return None
    if not _SAFE_SUBJ.match(subj):
        return None
    return subj


# --------------------------------------------------------------------------- #
# Reuse the ingester verbatim (catalog enumeration + parse logic), no re-discovery.
# --------------------------------------------------------------------------- #
def _ingester():
    path = os.path.join(config.JOBS_DIR, "ingest_stat_estonia.py")
    if not os.path.exists(path):
        raise DefinitiveError(f"{SOURCE}: ingester missing at {path}")
    spec = importlib.util.spec_from_file_location("ingest_stat_estonia", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------- #
# HTTP with honest transient/definitive classification (parse logic still reused).
# --------------------------------------------------------------------------- #
def _get_meta(sess, url):
    """GET a table's PxWeb metadata.

    Returns: dict (the metadata) | None (404/400 -> table gone / empty).
    Raises:  TransientError (timeout/5xx/429/conn/non-JSON-200 after the budget).
    """
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, headers=UA, timeout=META_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} GET {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20) + random.uniform(0, 0.8)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"{SOURCE} GET {url[-60:]}: {last}")
                time.sleep(min(2 ** a, 20) + random.uniform(0, 0.8)); continue
        if r.status_code in (400, 404):
            return None  # table removed from the tree / not available -> empty
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} GET {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20) + random.uniform(0, 0.8)); continue
        raise DefinitiveError(f"{SOURCE} GET {url[-60:]}: HTTP {r.status_code}")
    raise TransientError(f"{SOURCE} GET {url[-60:]}: {last}")


def _post_data(sess, url, body):
    """POST a PxWeb data query (json-stat2).

    Returns: dict (json-stat2 payload) | None (400/403 -> query rejected / empty).
    Raises:  TransientError (timeout/5xx/429/conn/non-JSON-200 after the budget).
    """
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.post(url, json=body, headers=UA, timeout=DATA_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} POST {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20) + random.uniform(0, 0.8)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"{SOURCE} POST {url[-60:]}: {last}")
                time.sleep(min(2 ** a, 20) + random.uniform(0, 0.8)); continue
        if r.status_code in (400, 403):
            return None  # query rejected (e.g. selection no longer valid) -> empty
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} POST {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20) + random.uniform(0, 0.8)); continue
        raise DefinitiveError(f"{SOURCE} POST {url[-60:]}: HTTP {r.status_code}")
    raise TransientError(f"{SOURCE} POST {url[-60:]}: {last}")


# --------------------------------------------------------------------------- #
# Per-subject per-table max(obs_date), keyed by the table's EE:-prefix.
# --------------------------------------------------------------------------- #
def _table_prefix(path: str) -> str:
    """The EE:-prefix the ingester writes for a table path (path separators -> ':')."""
    return "EE:" + path.replace("/", ":")


def _max_by_table(parquet_path: str) -> dict[str, dt.date]:
    """Map {table-prefix 'EE:...'} -> max(obs_date) for every table in a subject file.

    A table's key is "<prefix>" or "<prefix>:<dim>=<code>...", so the prefix up to and
    including the ".PX" leaf identifies the table. We split each series_key at ".PX"
    (case-insensitive on the extension actually stored) to recover the table prefix.
    """
    out: dict[str, dt.date] = {}
    if not blob.exists(parquet_path):
        return out
    t = pq.read_table(parquet_path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return out
    keys = t.column("series_key").to_pylist()
    dates = t.column("obs_date").to_pylist()
    for sk, d in zip(keys, dates):
        if d is None or sk is None:
            continue
        if isinstance(d, dt.datetime):
            d = d.date()
        # table prefix = everything up to and including the ".PX"/".px" leaf token.
        low = sk.lower()
        cut = low.find(".px")
        pref = sk[: cut + 3] if cut != -1 else sk.split(":")[0]
        prev = out.get(pref)
        if prev is None or d > prev:
            out[pref] = d
    return out


# --------------------------------------------------------------------------- #
# Build the date-tail query for one table (only periods AFTER stored max).
# --------------------------------------------------------------------------- #
def _build_query(ing, variables, stored_max: dt.date | None):
    """Construct the PxWeb query restricting THE time dimension to newer codes.

    Returns (query, time_code, n_new_periods) or (None, None, 0) if there is nothing
    new to ask for (the time dim has no code parsing to a date > stored_max).

    THE time dimension is picked with the shared value-first resolver
    (core/pxweb.py) fed the SAME inputs ing.parse_jsonstat2 resolves with — the
    authoritative `time: true` code, else highest date-parse-rate, else literal
    name, using ing.parse_date's grammar — so the axis restricted here is EXACTLY
    the axis the parser keys obs_date on. The OLD selection took the FIRST
    is_time_dim() match in variable order, which in a Kuu+Aasta (month+year) cube
    picked the month axis: its codes ('01'..'12') parse to no date, so
    "codes > stored_max" selected NOTHING and the table was reported permanently
    current — a silent freeze — while the parser keyed obs_date on Aasta.

    Mirrors the ingester's selection exactly:
      * THE resolved time dimension is restricted to newer codes;
      * every OTHER variable is requested in FULL when total cells <= MAX_CELLS; over
        MAX_CELLS, a variable the ingester's own is_time test keeps full (the flagged
        code when `time: true` is present, else ing.is_time_dim — e.g. the demoted
        month axis, whose stored keys cover every month) stays FULL, and the rest
        aggregate to a single TOTAL/first code (identical to
        jobs/ingest_stat_estonia.query_table).
    """
    # THE time axis, exactly as ing.parse_jsonstat2 will pick it on the response.
    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    time_idx = _pxweb.resolve_time_dim(
        [v.get("code", "") for v in variables],
        [[str(c) for c in (v.get("values") or [])] for v in variables],
        meta_time_code=meta_time_code, parse_fn=ing.parse_date)
    if time_idx is None:
        return None, None, 0  # no time dimension -> nothing date-tailable (census snapshot)

    tvar = variables[time_idx]
    all_time = tvar.get("values", [])
    if stored_max is None:
        new_codes = list(all_time)  # first-time / unmapped table -> full pull (merge dedups)
    else:
        new_codes = [c for c in all_time
                     if (pd := ing.parse_date(c)) is not None and pd > stored_max]
    if not new_codes:
        return None, tvar.get("code"), 0

    # decide full vs aggregated selection for the NON-time variables, using the ingester's
    # MAX_CELLS rule but measured against the RESTRICTED time-code count (the real query).
    total_cells = len(new_codes)
    for i, v in enumerate(variables):
        if i == time_idx:
            continue
        total_cells *= max(len(v.get("values", [])), 1)

    query = []
    for i, v in enumerate(variables):
        code = v.get("code", "")
        vals = v.get("values", [])
        if i == time_idx:
            query.append({"code": code, "selection": {"filter": "item", "values": new_codes}})
            continue
        if not vals:
            continue
        if total_cells <= ing.MAX_CELLS:
            query.append({"code": code, "selection": {"filter": "item", "values": vals}})
        elif ((code == meta_time_code) if meta_time_code is not None
              else ing.is_time_dim(code, vals)):
            # Ingester keep-full PARITY (jobs/ingest_stat_estonia.query_table): over
            # MAX_CELLS the ingester keeps every variable its own is_time test flags at
            # the FULL value list — e.g. the demoted month axis — so the stored keys
            # cover all its values; collapsing it here would tail only a sliver of them.
            query.append({"code": code, "selection": {"filter": "item", "values": vals}})
        else:
            agg = [x for x in vals if str(x).upper() in ("0", "000", "TOTAL", "T", "ALL", "KOKKU")]
            sel = agg[:1] if agg else vals[:1]
            query.append({"code": code, "selection": {"filter": "item", "values": sel}})
    return query, tvar.get("code"), len(new_codes)


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    ing = _ingester()
    base = ing.BASE
    out_dir = config.source_dir(SOURCE)
    if not os.path.isdir(out_dir):
        raise DefinitiveError(f"{SOURCE} source dir missing: {out_dir}")

    tables = ing.crawl_catalog()          # cached _catalog.json; no re-discovery
    if not tables:
        raise DefinitiveError(f"{SOURCE}: empty catalog (crawl returned 0 tables)")

    # group sub-units by SUBJECT (first path segment == parquet filename). The subject
    # token becomes a filename, so reject any catalog row whose subject is not a safe
    # single path component (path traversal / separator / drive injection from a poisoned
    # or malformed upstream catalog) — skip it rather than write outside the source dir.
    by_subject: dict[str, list] = defaultdict(list)
    n_skipped_unsafe = 0
    for t in tables:
        raw_subj = t["path"].split("/")[0] if t.get("path") else "root"
        subj = _safe_subject(raw_subj)
        if subj is None:
            n_skipped_unsafe += 1
            continue
        by_subject[subj].append(t)
    if n_skipped_unsafe:
        print(f"{SOURCE}: skipped {n_skipped_unsafe} catalog row(s) with unsafe subject token",
              flush=True)

    sess = requests.Session()
    sess.headers.update(dict(UA, Connection="close"))
    tally = Tally()
    total = 0
    maxd: dt.date | None = None
    cursors: dict[str, str] = {}   # table-prefix -> max obs_date (per-table freshness)

    n_subunits = sum(len(v) for v in by_subject.values())  # safe sub-units actually attempted

    for subj in sorted(by_subject):
        subj_tables = by_subject[subj]
        path = os.path.join(out_dir, f"{subj}.parquet")
        before = blob.row_count(path)
        stored = _max_by_table(path)     # per-table max within this subject file

        # seed cursors from the on-disk frontier so untouched tables still report a cursor
        # (a frozen table can't hide behind the subject-level max). A corrupt/sentinel
        # far-future obs_date (year-9999 in the discontinued-tables tree) or a legitimate
        # 2085 PROJECTION must NOT become the reported freshness frontier — health.assess()
        # takes max(cursors.values()), so an uncapped far-future cursor would mask a real
        # freeze (RED-DATA). _cursor_value caps any beyond-horizon date to today; on-disk
        # data is untouched. The unit-level rollup also ignores those far-future codes.
        for pref, d in stored.items():
            cursors[pref] = _cursor_value(d)
            maxd = _bump_unit_max(maxd, d)

        # accumulate this subject's NEW rows across all its tables, then merge once.
        keys: list[str] = []
        dates: list[dt.date] = []
        vals: list[float] = []
        seen: set[tuple] = set()

        for t in subj_tables:
            tpath = t["path"]
            prefix = _table_prefix(tpath)
            # Guard the FETCH boundary against a CORRUPT far-future stored max (year-9999
            # sentinel / 2085 projection). _build_query selects only period codes
            # strictly > stored_max; if stored_max is year 9999 NOTHING is ever newer and
            # the table freezes forever. sane_since() returns None for a beyond-horizon
            # stored_max -> fall back to a FULL pull (stored_max=None semantics: request
            # all periods, merge dedups) so a discontinued/sentinel table can still pick
            # up any real new period. A genuine recent stored_max is returned unchanged.
            raw_stored_max = stored.get(prefix)
            stored_max = sane_since(raw_stored_max)
            url = f"{base}/{tpath}"        # no trailing slash (ingester note)

            # 1) metadata
            try:
                meta = _get_meta(sess, url)
            except TransientError:
                tally.transient_unit()
                time.sleep(RATE)
                continue
            if not meta or not isinstance(meta, dict) or not meta.get("variables"):
                tally.empty_unit()         # 404/400 or no variables -> legitimately empty
                time.sleep(RATE)
                continue

            # 2) build the date-tail query
            query, _tcode, n_new = _build_query(ing, meta["variables"], stored_max)
            if not query:
                tally.empty_unit()         # no time dim, or nothing newer than stored max
                time.sleep(RATE)
                continue

            # 3) data POST
            body = {"query": query, "response": {"format": "json-stat2"}}
            try:
                resp = _post_data(sess, url, body)
            except TransientError:
                tally.transient_unit()
                time.sleep(RATE)
                continue
            if not resp or not isinstance(resp, dict):
                tally.empty_unit()         # 400/403 query rejected -> empty
                time.sleep(RATE)
                continue

            # 4) parse with the ingester's parser (identical keys/dates as bulk ingest).
            # Thread the AUTHORITATIVE PxWeb `time: true` flag so the shared value-first
            # resolver locks onto it; None (Estonia omits it on some tables) -> value-first.
            meta_time_code = next((v.get("code") for v in meta["variables"] if v.get("time") is True), None)
            rows = ing.parse_jsonstat2(resp, prefix, meta_time_code)

            if not rows:
                # 200 with a real body but 0 parsed rows. Classify via the shared PxWeb
                # rule (identical to statfin/hagstofa; MISTAKES R25): a STRUCTURAL break is
                # only the loss of data we ALREADY serve — a table with a SANE on-disk
                # boundary (stored_max is not None) whose real json-stat2 envelope still
                # carries >=1 non-null value yet parsed to nothing. A never-landed table, a
                # corrupt-boundary table demoted to a full pull (stored_max None), or a
                # newer period slot published ahead of its (all-null) data is benign empty.
                # NB: the OLD gate was INVERTED — it fired on never-landed tables and stayed
                # silent when a populated table went dark (the real break). Fixed here.
                if structural_on_zero_rows(stored_max, resp):
                    tally.structural_unit()
                else:
                    tally.empty_unit()
                time.sleep(RATE)
                continue

            n_added_table = 0
            t_max: dt.date | None = None
            for key, d, v in rows:
                if isinstance(d, dt.datetime):
                    d = d.date()
                # only keep periods strictly newer than what's stored (date-tail);
                # when unmapped (stored_max None) keep all and let merge dedup.
                if stored_max is not None and d <= stored_max:
                    continue
                tok = (key, d)
                if tok in seen:
                    continue
                seen.add(tok)
                keys.append(key); dates.append(d); vals.append(v)
                n_added_table += 1
                if t_max is None or d > t_max:
                    t_max = d

            if n_added_table > 0:
                tally.added_unit(n_added_table)
                if t_max is not None:
                    # cap a far-future (sentinel/projection) cursor so it can't mask
                    # RED-DATA via health's max(cursors); on-disk data is untouched.
                    cv = _cursor_value(t_max)
                    prev = cursors.get(prefix)
                    if prev is None or cv > prev:
                        cursors[prefix] = cv
                    maxd = _bump_unit_max(maxd, t_max)
            else:
                tally.empty_unit()         # newer codes existed but all <= stored after parse
            time.sleep(RATE)

        # 5) merge this subject's accumulated new rows (one publish per subject file)
        if vals:
            new_tbl = pa.table({
                "series_key": pa.array(keys, pa.string()),
                "obs_date":   pa.array(dates, pa.date32()),
                "value":      pa.array(vals, pa.float64()),
            })
            n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
            total += n
            if md:
                # md is the whole-file max (may be a projection/2085 or 9999 sentinel);
                # the per-table cursors above already carry exact freshness, so only feed
                # the capped value into the unit-level rollup.
                maxd = _bump_unit_max(maxd, dt.date.fromisoformat(md))
        else:
            total += before

    last_obs = maxd.isoformat() if maxd is not None else (since or None)
    # empty_window_floor = (#sub-units) - 1 per the contract: a real wholesale outage
    # (every table empty/404) trips the structural floor; a healthy run where most
    # tables are simply "nothing newer" is legitimate no_change and must NOT.
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=n_subunits - 1)
