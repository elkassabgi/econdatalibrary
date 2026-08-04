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
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after, sane_since,
                      save_rotation, structural_on_zero_rows)

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
# Second, finer bookmark: "<subject>|<table path>". _rotation.json alone rotates SUBJECTS, which
# is useless when one subject cannot finish inside the orchestrator's 45-minute cap — the sweep
# is killed mid-subject and the next visit re-walks that subject's head. This one records where
# inside a subject to resume. Separate file so the two grains cannot overwrite each other.
_TBL_ROTATION = "_rotation_table.json"
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
    t = blob.read_table(parquet_path, columns=["series_key", "obs_date"])
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
# COLD TABLES: the archive must not eat the budget the live tables need.
# --------------------------------------------------------------------------- #
# MEASURED 2026-08-04, catalogue (4,978 tables) + local store (3,447 with rows):
#
#   Lepetatud_tabelid ("discontinued tables")  2,832 catalogued  56.9% of the source
#   the other six subjects                     2,146             43.1%
#
# and at ~60 tables/min under an 18-minute budget a full pass is ~4.6 ticks, of which the
# archive alone is ~2.6. The first successful capped run spent 100% of its 1,079 tables
# inside Lepetatud_tabelid and never reached a live subject at all.
#
# TWO SIGNALS, because ONE IS NOT ENOUGH — this is the part I got wrong before measuring:
#
#  * Freshness alone is a poor discriminator. A 3-year cutoff calls 94.8% of the archive
#    cold, but ALSO 481 of 1,578 (30.5%) stored tables in the LIVE subjects — finished
#    surveys that simply live in an active tree (KO11..KO19 end 2020, SHL0xx end 2015).
#    Those are genuinely finished and deferring them is right, but it means "stale" and
#    "archived" are different questions.
#  * The archive label alone is not enough either: 963 of the 2,832 archive tables have NO
#    stored rows, so a freshness-only rule leaves a third of the archive permanently hot.
#
# So: cold if the publisher files it under the archive tree, OR its OBSERVED frontier is
# older than the cutoff. Observed, not raw — a 2085 projection is not freshness (R327), and
# a table whose only rows are future-dated is left HOT rather than judged.
#
# COLD IS A CADENCE, NEVER A SKIP. Cold tables are visited on a bounded slice per pass with
# their own per-subject bookmark, so every one is reached within ceil(n_cold/slice) passes.
# A table that is simply never revisited is R190 — a silent truncation that reports itself
# as a healthy `partial` for ever — which is the exact failure this fetcher already carries
# two bookmarks to prevent.
_DISCONTINUED_SUBJECT = "Lepetatud_tabelid"     # publisher's own archive tree
_COLD_ROTATION_FMT = "_rotation_cold_{}.json"   # per SUBJECT: one shared file would let each
                                                # subject clobber the next one's bookmark and
                                                # re-walk the same prefix for ever (R190).


def _cold_after_days() -> float:
    return float(os.environ.get("STAT_ESTONIA_COLD_DAYS", "1095"))    # 3 years


def _cold_slice() -> int:
    return int(os.environ.get("STAT_ESTONIA_COLD_SLICE", "150"))


def _is_cold(subject: str, stored_max, today: dt.date, cutoff_days: float) -> bool:
    """Is this table's data finished, so far as we can tell from what we hold?

    NEVER DEFER AN UNKNOWN. A table outside the archive tree with no stored rows might be
    brand new, so it stays hot; only the publisher's own archive label can make an
    unknown cold.
    """
    if subject == _DISCONTINUED_SUBJECT:
        return True
    if stored_max is None:
        return False
    d = stored_max if isinstance(stored_max, dt.date) else None
    if d is None:
        try:
            d = dt.date.fromisoformat(str(stored_max)[:10])
        except Exception:                                    # noqa: BLE001
            return False
    if d > today:
        return False          # a projection is not staleness — leave it hot (R327)
    return (today - d).days > cutoff_days


def _cold_plan(tables, subject, stored, bookmark, today, cutoff_days, slice_n):
    """(cold_paths_set, due_paths_set) for one subject's pass.

    Pure on purpose: the scheduling rule is the part that can silently truncate, so it is
    testable without a network, a store or a clock.

    `bookmark` is the last cold table ACTUALLY VISITED last pass (not the last one planned)
    — see the caller. Rotating on a planned-but-unreached table is how a budget cut turns
    a cadence into a skip.
    """
    cold, order = set(), []
    for t in tables:
        p = t["path"]
        if _is_cold(subject, stored.get(_table_prefix(p)), today, cutoff_days):
            cold.add(p)
            order.append(p)
    if not order:
        return cold, set()
    if bookmark and bookmark in order:
        order = rotate_after(order, bookmark)
    return cold, set(order[:max(0, slice_n)])


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
    # No isdir guard: sub-units come from the upstream catalog (below), and every
    # store touch is blob-routed — the local dir legitimately does not exist on a
    # CI runner under AQUEDUCT_BACKEND=r2.

    # Load the crawled catalog BLOB-FIRST: in CI (backend=r2) the ingester's local
    # _catalog.json cache does not exist on the runner, so ing.crawl_catalog() would
    # re-crawl all ~4,978 tables live (measured 1h41m/run). Read the R2 cache instead and
    # fall back to a fresh crawl only if it is absent everywhere (ledger R36, hagstofa pattern).
    import json as _json
    tables = None
    _craw = blob.read_bytes(os.path.join(out_dir, "_catalog.json"))
    if _craw is not None:
        try:
            _t = _json.loads(_craw.decode("utf-8"))
            if isinstance(_t, list) and _t:
                tables = _t
        except ValueError:
            tables = None
    if tables is None:
        tables = ing.crawl_catalog()      # cached _catalog.json absent -> fresh crawl
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

    # BOUND ITSELF BELOW THE ORCHESTRATOR'S CAP, AND ROTATE. In the 2026-08-02 cloud run
    # this source was killed by the 45-minute hard timeout, as were statfin and
    # worldbank_wdi — ~135 of the run's ~262 minutes spent on three sources that were then
    # interrupted, while only 20 sources in total were attempted all day. Yielding and
    # being killed are not the same event; only one of them runs cleanup.
    #
    # And the sweep was `sorted(by_subject)` — a FIXED order, so the kill always landed in
    # the same place and the tail subjects were never reached, however many runs passed
    # (R190: a bound over a fixed order is a truncation, not a budget). Per-subject merges
    # already land inside the loop, so stopping keeps what was done; the bookmark is what
    # makes the remainder actually arrive.
    # 30 -> 18, MEASURED not guessed. The bound above was added precisely to stop the 45-minute
    # kill, and on 2026-08-03 stat_estonia was killed by it AGAIN — "exceeded its 45-minute hard
    # limit and was interrupted", with no budget message printed at all, so the deadline never
    # got to fire. That is the shape of the problem: dl.spent() is only consulted BETWEEN
    # subjects, so the real ceiling is budget + the longest single subject, and a 30-minute
    # budget leaves only 15 minutes of headroom for a subject that evidently needs more.
    #
    # census showed the same arithmetic from the other side the same day: a 20-minute budget was
    # reported "spent after 35.6 min". A cooperative deadline over coarse units does not bound
    # wall-clock; it bounds when you next LOOK at the clock.
    #
    # 18 leaves 27 minutes for one in-flight subject. If the kill recurs, the next move is not a
    # smaller number — it is checking the deadline inside the per-subject table loop, because at
    # that point one subject alone exceeds the cap and no budget can help.
    #
    # THAT MOVE WAS MADE (272faee5 + ad6360b0) AND IS NOW MEASURED, 2026-08-04. First execution of
    # the current code — the three 45-minute kills on record all predate it, so until this run
    # nothing had ever exercised it (R339):
    #
    #     budget of 18 min spent after 18.0 min INSIDE subject 'Lepetatud_tabelid' —
    #     completed 1079 of 2832 table(s); resuming after '.../HT295.PX' next tick
    #     [orchestrator] <<< stat_estonia/_all took 1,093s     -> partial
    #
    # 1,093s against the 2,700s hard limit, and `partial` instead of `transient_fail obs=0`. The
    # in-loop deadline bounds the clock and the table bookmark makes the remainder arrive.
    #
    # WHAT IS STILL WRONG IS NOT THE CAP, IT IS WHERE THE BUDGET GOES. 100% of that pass was spent
    # inside `Lepetatud_tabelid` — Estonian for DISCONTINUED TABLES — at ~60 tables/min, and that
    # one subject holds 2,832 of them, i.e. ~2.6 full ticks before any live subject is reached.
    # A source that only ever walks its archive is bounded, honest, and still not updating. That
    # is the per-table long-cadence gate (queue #92), and this run is its measurement.
    budget_min = float(os.environ.get("STAT_ESTONIA_BUDGET_MIN", "18"))
    dl = Deadline(minutes=budget_min)
    subjects = rotate_after(sorted(by_subject), load_rotation(out_dir))
    stopped_early = False
    last_subj = ""
    # Set when the budget expired INSIDE a subject's table loop, so the end-of-function
    # save_rotation below must not overwrite the wound-back subject bookmark. Function-scope on
    # purpose: the per-subject flag cannot be seen after the loop, and the save happens there.
    capped_inside_subject = False

    # Sub-units actually ATTEMPTED — recomputed after the sweep, because the contract floor
    # must be measured against what this tick visited, not the whole catalogue. Counting all
    # of them after a budget stop would make a clean partial pass look like a wholesale
    # outage and trip the structural floor.
    n_subunits = sum(len(v) for v in by_subject.values())

    # TABLE-LEVEL RESUME POINT, for the subject that alone cannot fit the cap.
    #
    # The subject loop's deadline check bounds when we next START a subject, not how long one
    # takes — so a single oversized subject runs past the orchestrator's 45-minute hard limit and
    # is KILLED. Measured on run 30799503843: stat_estonia took exactly 2,700s and printed
    # "exceeded its 45-minute hard limit", with no budget message, on an 18-minute budget. The
    # per-subject bookmark did survive that kill (R273's fix works — _rotation.json read
    # {"after": "Lepetatud_tabelid"}), but surviving a kill is not the same as not being killed.
    #
    # A second bookmark, at TABLE grain, is what makes an interrupted subject resumable. Without
    # it, breaking out of the table loop would be R190 one level down: the subject bookmark has
    # already advanced past this subject, so the next visit re-walks its head, stops at the same
    # table, and its TAIL IS NEVER FETCHED — a silent truncation reported as an honest `partial`.
    tbl_rot = load_rotation(out_dir, _TBL_ROTATION)
    for subj in subjects:
        if dl.spent():
            stopped_early = True
            n_left = len(subjects) - subjects.index(subj)
            # Tallied for the same reason as the inner check: an untallied deferral leaves
            # finalize() with nothing added and nothing failed, so it returns `no_change` and
            # stamps a vintage claiming a coverage this tick never reached (R303).
            tally.deferred_unit(f"{n_left} subject(s) after {last_subj or '(start)'}")
            print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — stopped after subject {last_subj!r}, "
                  f"{n_left} of {len(subjects)} subject(s) "
                  f"deferred to the next tick", flush=True)
            break
        prev_subj = last_subj
        last_subj = subj
        # SAVED HERE, NOT ONLY AT THE END OF THE FUNCTION. The end-of-function save is exactly
        # what a 45-minute kill destroys — the orchestrator interrupts the source rather than
        # breaking its loop, so nothing after the sweep runs. That is why this source has never
        # written a _rotation.json while worldbank_wdi and hagstofa each wrote their first one
        # the moment they stopped being killed (R273, confirmed 2026-08-03). Relying on "we will
        # not be killed" to persist the state whose whole purpose is surviving a kill is circular.
        # One small write per subject removes the dependency.
        save_rotation(out_dir, subj)
        subj_tables = by_subject[subj]
        # Resume mid-subject if the LAST run ran out of budget inside this one. The bookmark is
        # "<subject>|<table path>"; it only applies to its own subject, so any other subject
        # starts at its first table as usual.
        if tbl_rot.startswith(f"{subj}|"):
            resume_after = tbl_rot.split("|", 1)[1]
            paths = [t["path"] for t in subj_tables]
            if resume_after in paths:
                subj_tables = rotate_after(subj_tables, resume_after,
                                           key=lambda t: t["path"])
                print(f"[{SOURCE}] {subj}: resuming after table {resume_after!r} "
                      f"({len(subj_tables)} table(s) this pass)", flush=True)
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

        # COLD PLAN for this subject. `subj` already passed _safe_subject when by_subject was
        # built, so it is a safe filename component here.
        _cold_file = _COLD_ROTATION_FMT.format(subj)
        _cold_book = load_rotation(out_dir, _cold_file)
        cold_set, cold_due = _cold_plan(subj_tables, subj, stored, _cold_book,
                                        dt.date.today(), _cold_after_days(), _cold_slice())
        if cold_set:
            print(f"[{SOURCE}] {subj}: {len(cold_set)} of {len(subj_tables)} table(s) cold "
                  f"(archive or frontier older than {_cold_after_days():.0f}d); "
                  f"{len(cold_due)} due this pass, resuming after {_cold_book.split('|')[-1] or '(start)'!r}",
                  flush=True)

        # accumulate this subject's NEW rows across all its tables, then merge once.
        keys: list[str] = []
        dates: list[dt.date] = []
        vals: list[float] = []
        seen: set[tuple] = set()

        last_tbl = ""
        # The last COLD table actually reached this pass. The cold bookmark advances over
        # this and never over the planned slice: if the budget cuts the pass short, the cold
        # tables we never got to must still be first in line next time, or the cadence
        # silently becomes a skip (R190).
        last_cold_visited = ""
        n_cold_deferred = 0
        n_cold_visited = 0
        hit_cap_inside = False
        for t in subj_tables:
            # THE DEADLINE, CHECKED PER TABLE. The subject-level check above bounds when the NEXT
            # subject starts; it cannot bound a subject already running, which is how an 18-minute
            # budget produced a 45-minute kill. Checking here is what actually stops the clock.
            #
            # Rows accumulated so far are NOT discarded — the merge below still runs, so a capped
            # pass publishes what it fetched. The table bookmark records the last table COMPLETED
            # (not the one about to start, which would skip it), and the SUBJECT bookmark is wound
            # back to the previous subject so the next tick re-enters this one and continues from
            # the bookmark instead of stepping over its unfinished tail.
            if dl.spent():
                hit_cap_inside = True
                capped_inside_subject = True
                stopped_early = True
                save_rotation(out_dir, f"{subj}|{last_tbl}", _TBL_ROTATION)
                save_rotation(out_dir, prev_subj)
                # TALLY THE DEFERRAL, or the run lies about itself. Without this the tally is
                # empty, finalize() sees nothing added and nothing failed, and returns
                # `no_change` — stamping a vintage that claims a completeness this tick did not
                # reach. Observed on a forced 3-second budget: 2,832 tables deferred and the run
                # reported "no new rows" (R303's shape, in a source that had no deferred slot).
                # deferred_unit does not touch `attempted`, so the denominator stays honest.
                n_deferred = len(subj_tables) - subj_tables.index(t)
                tally.deferred_unit(f"{subj}: {n_deferred} table(s) after {last_tbl or '(start)'}")
                print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                      f"{dl.elapsed_min():.1f} min INSIDE subject {subj!r} — completed "
                      f"{subj_tables.index(t)} of {len(subj_tables)} table(s); resuming after "
                      f"{last_tbl!r} next tick (subject bookmark held at {prev_subj!r})",
                      flush=True)
                break
            tpath = t["path"]
            # COLD AND NOT DUE -> defer, cheaply, with NO network. Counted, never silent:
            # an untallied deferral leaves finalize() with nothing added and nothing failed,
            # so it returns `no_change` and stamps a vintage claiming a coverage this tick
            # never reached (R303). Unlabelled on purpose — 2,682 labels would bury the real
            # offenders in the run note, and the aggregate is printed below instead.
            if tpath in cold_set and tpath not in cold_due:
                tally.deferred_unit()
                n_cold_deferred += 1
                continue
            if tpath in cold_set:
                last_cold_visited = tpath
                n_cold_visited += 1
            prefix = _table_prefix(tpath)
            # "Last table VISITED", recorded before the work rather than after it. Every branch
            # below can `continue` (transient, empty, rejected query), and a bookmark that only
            # advanced on success would re-walk a run of failing tables forever — the same
            # truncation this bookmark exists to prevent. A visited-but-failed table is retried
            # on the next wrap-around, not skipped.
            last_tbl = tpath
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
                tally.transient_unit(tpath)
                time.sleep(RATE)
                continue
            if not meta or not isinstance(meta, dict) or not meta.get("variables"):
                tally.empty_unit(tpath)    # 404/400 or no variables -> legitimately empty
                time.sleep(RATE)
                continue

            # 2) build the date-tail query
            query, _tcode, n_new = _build_query(ing, meta["variables"], stored_max)
            if not query:
                tally.empty_unit(tpath)    # no time dim, or nothing newer than stored max
                time.sleep(RATE)
                continue

            # 3) data POST
            body = {"query": query, "response": {"format": "json-stat2"}}
            try:
                resp = _post_data(sess, url, body)
            except TransientError:
                tally.transient_unit(tpath)
                time.sleep(RATE)
                continue
            if not resp or not isinstance(resp, dict):
                tally.empty_unit(tpath)    # 400/403 query rejected -> empty
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
                    tally.structural_unit(tpath)
                else:
                    tally.empty_unit(tpath)
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

        # COLD BOOKMARK — advanced over what was REACHED, and placed here so it is written on
        # BOTH exits: a complete subject falls through, and a capped one breaks out of the table
        # loop into the merge above and arrives here too. Writing it inside the deadline branch
        # instead would leave a complete pass never advancing, i.e. the same 150 archive tables
        # for ever.
        #
        # Left UNTOUCHED when nothing cold was reached (budget died first), so the slice that
        # was due stays due. Advancing on a planned-but-unvisited table is exactly how a
        # cadence decays into a skip.
        if last_cold_visited:
            save_rotation(out_dir, last_cold_visited, _cold_file)
        if n_cold_deferred:
            # VISITED, not planned. On a capped pass these differ, and reporting the plan as
            # though it were the outcome is how a bound starts looking like coverage.
            print(f"[{SOURCE}] {subj}: deferred {n_cold_deferred} cold table(s) to a later pass; "
                  f"visited {n_cold_visited} of {len(cold_due)} due, "
                  f"last {last_cold_visited or '(none reached)'!r}", flush=True)

        if hit_cap_inside:
            # The merge above published this subject's partial haul; stop the sweep. Both
            # bookmarks are already written (table = where to resume, subject = wound back so
            # this subject is re-entered rather than stepped over).
            break
        if tbl_rot.startswith(f"{subj}|"):
            # Subject finished. Retire its table bookmark so a later visit starts at its first
            # table instead of resuming from a point that no longer means anything.
            save_rotation(out_dir, "", _TBL_ROTATION)
            tbl_rot = ""

    # Save the bookmark even after a COMPLETE pass: it is then the last subject in order and
    # the next run wraps to the top through this same path, so no branch can silently stop
    # the rotation.
    # NOT when the budget expired INSIDE a subject. This line silently UNDID the whole
    # table-grain fix: the inner deadline winds the subject bookmark back to prev_subj so the
    # next tick re-enters the unfinished subject, and then this save overwrote it with
    # last_subj — the interrupted one — so the next run resumed AFTER it and its tail was
    # skipped exactly as before. Caught by running the fetcher with a 3-second budget, not by
    # reading it: both writes look correct in isolation and only the order reveals the bug.
    if last_subj and not capped_inside_subject:
        save_rotation(out_dir, last_subj)
    if stopped_early:
        visited = set(subjects[:subjects.index(last_subj) + 1])
        n_subunits = sum(len(by_subject[s]) for s in visited if s in by_subject)

    last_obs = maxd.isoformat() if maxd is not None else (since or None)
    # empty_window_floor = (#sub-units) - 1 per the contract: a real wholesale outage
    # (every table empty/404) trips the structural floor; a healthy run where most
    # tables are simply "nothing newer" is legitimate no_change and must NOT.
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=max(n_subunits - 1, 1))
