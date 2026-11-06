"""S3 (sdmx_delta) fetcher — Statistics Estonia (andmed.stat.ee PxWeb v1). No key.

Statistics Estonia is a pure **PxWeb** source (no SDMX 2.1, so no ?updatedAfter /
?startPeriod — the date-tail is expressed as a POST whose TIME dimension is restricted
to the period codes from the stored max on). The strategy is `sdmx_delta`: for each
table we read the already-published max(obs_date) and request the newer periods AND - for a hot
table of the D1 shape (a YEAR axis with a quarter/month variable; see BOUNDARY_ENV) - the boundary
period itself, plus - only when the revision window is armed (OFF by default, REVISION_ENV) - the
last N calendar years up to and including the stored max (N=5: 2022-2026 for a 2026 max; any table
shape). Every fetched row is
compared with the stored row on (series_key, obs_date), and only changed and added rows are
merged; a stored row the fetch did not return is HELD (kept). Nothing is ever deleted.

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
be dropped. _plan_table resolves the axis with the SAME inputs the parser uses.

STORE REPAIR OF THE MIXED-GRAIN TABLES (D2) IS A SEPARATE, REVIEWED STEP - NOT RUN BY THIS
MODULE. TO0053's history (2000..2026-04) is one aggregate series, its 2026-05+ tail holds the
315 detail series, and 97,802 history rows are unreachable (measured 2026-11-06). The tail
cannot repair it: its added history would be new rows under KNOWN series below the stored max,
which the revision growth bound withholds, and a new series is withheld by the new-series
bound. The repair, per table, by an operator: plan with _plan_table(ing, variables,
stored_max=None) (full detail, time-chunked under MAX_CELLS), fetch every chunk, compare with
_compare_subject, merge changed + added rows only after a reviewer has read the counts, then
read the table back against the publisher. Candidates by store signature (F: copies, one
early key then detail): EH045, EH046, EH047, EM041, PM09, PM0281, RR302, TO0053, TO0083,
TU131, RVR01 - the over-MAX_CELLS condition itself needs each table's metadata.


HONEST STATUS (Tally + finalize): each table is a sub-unit. We do our OWN HTTP (rather
than the ingester's error-swallowing get_json/post_json) so we can tell apart:
  - transient  : timeout / 5xx / 429 / connection drop / non-JSON-200-under-load
                 -> transient_unit()  => the WHOLE run is 'partial' (orchestrator does
                    NOT stamp last_success; a later pass selects the unit again, per
                    base.Strategy.is_due). Existing data
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
import json
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
from ..base import MERGE_NOTE, Result
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

# --------------------------------------------------------------------------- #
# WHAT A TABLE VISIT ASKS FOR (fix/stat-estonia-revisions, measured 2026-11-06)
# --------------------------------------------------------------------------- #
# Until this change the tail asked ONLY for period codes strictly newer than the table's stored max
# and dropped every parsed row at or below it. Measured against the publisher on 11 tables
# (E: store copy, re-derived by review AR-452):
#   D1  a table whose time axis is a YEAR and whose key carries the quarter or month (RAA0012,
#       RAA0042, IA002) stores "2026" once and never asks for it again, so 2026 Q2-Q4 / months 6-12
#       are lost for good: 11 + 173 + 70 rows. 353 of the 3,464 stored tables have that shape (store
#       copies, every date 12-31 and a Kvartal/Kuu/Poolaasta/Nadal key part; an undercount - month
#       dimensions under other names are missed), 106 of them show the frozen symptom now.
#   D0  published revisions of past periods are never fetched: 5,887 rows in 4 of the 11 tables,
#       national accounts back to 1995, industrial production 2025-09..2026-07.
# THE BOUNDARY (D1 fix) - ON by default, for D1-SHAPE tables only: a hot table whose time axis is a
# YEAR and which carries a sub-period variable (Kvartal, Kuu, Registreerimiskuu, ...; _is_subperiod_name)
# re-requests the period of its stored max, and every row of that period is compared with the store and
# merged (revised values overwrite on (series_key, obs_date); nothing is deleted).
# WHY D1 ONLY (review AR-453 C1): the boundary costs one POST per table on EVERY visit, and 1,113 stored
# tables are hot; at ~0.75 s a POST that is ~14 minutes of an 18-minute pass. Only on a D1 table does
# the boundary carry data nothing else can reach - later quarters/months arrive under the year code
# already stored, which the strictly-newer request never asks again. On any other table the boundary
# only re-reads the last period for a revision, which is the revision window's job (D0). A rule on
# "stored max in the current year" would not narrow it: most hot tables end this year.
# Cold tables (archive / finished) keep the strictly-newer request (review CHALLENGE M1).
# STAT_ESTONIA_BOUNDARY_REFETCH: unset/"1" = D1 tables (default), "0" = off (the operator valve),
# "all" = every hot table (the first build's rule, for an operator who wants last-period revisions).
# TRIGGER (PR #243): if a pass attempts fewer than 900 tables, set it to 0 and re-plan; the
# "requests this pass" log line is the instrument.
# THE REVISION WINDOW (D0 fix) - OFF by default, its own switch (census #239's pattern): see
# revision_activated_on().
BOUNDARY_ENV = "STAT_ESTONIA_BOUNDARY_REFETCH"
REVISION_ENV = "STAT_ESTONIA_REVISION_FROM"
# Set by a reviewed one-line commit. None = off. STAT_ESTONIA_REVISION_FROM=YYYY-MM-DD arms it on a
# workstation instead.
OPERATOR_ACTIVATED_ON: dt.date | None = None
# CHOSEN, not measured. The floor is 1 January of (stored max year - YEARS + 1): for a 2026 max and
# YEARS=5 that is 2022-01-01, so the window is 2022-2026, the current year included, and 2021 is
# OUTSIDE it. Replayed on the measurement's saved answers (review AR-453): the default window recovers
# 5,592 of the 5,887 measured revised rows; the 295 it misses are all RAA0012's (225 of its 520 are
# inside). RAA0063, RAA0042 and TO0053's revisions are all inside.
REVISION_YEARS_DEFAULT = 5
REVISION_EVERY_DAYS_DEFAULT = 90     # a table is re-pulled at most this often (slow cadence)
REVISION_RETRY_DAYS = 7              # ... or this soon after a transient attempt
REVISION_BUDGET_MIN_DEFAULT = 4.0    # per-pass slice INSIDE the 18-minute budget (gates a table's start)
REVISION_FILE = "_revision.json"     # {table path: {"at": ISO date, "outcome": str}}, blob-routed
# GROWTH BOUNDS. A revised period may ADD rows; a grain change (the MAX_CELLS collapse, D2) adds MANY.
#   * new series: added rows whose series_key the table has NEVER stored. More than
#     max(GROWTH_MIN_ROWS, GROWTH_PCT % of the table's stored rows) is WITHHELD, on every path, the
#     boundary included (CHALLENGE B1: the bulk ingest stored history AGGREGATED wherever the full
#     history was over MAX_CELLS, so a detail request at the boundary would mint a mixed-grain table).
#     The stored grain is kept until the reviewed store repair (see _plan_table).
#   * revision rows: added rows dated BEFORE the stored max under series the table already holds -
#     more than max(GROWTH_MIN_ROWS, GROWTH_PCT % of the stored rows in the window) is withheld.
#   CHANGED rows (same key and date, new value) always merge (CHALLENGE M3).
GROWTH_MIN_ROWS = 50
GROWTH_PCT = 1.0
# The merge note is stored unclipped on the success path: bound it.
NOTE_MAX_CHARS = 700


def boundary_mode(environ=None) -> str:
    """"d1" (default: D1-shape hot tables), "all" (every hot table) or "off" (BOUNDARY_ENV=0)."""
    env = os.environ if environ is None else environ
    raw = (env.get(BOUNDARY_ENV) or "1").strip().lower()
    return "off" if raw == "0" else "all" if raw == "all" else "d1"


def boundary_refetch_on(environ=None) -> bool:
    return boundary_mode(environ) != "off"


# A SUB-PERIOD VARIABLE: its code (the series-key part name) is, or ENDS in, a quarter / half-year /
# month / week word. Word-final, so 'Registreerimiskuu', 'Sünnikuu', 'Surmakuu' (month of registration,
# birth, death - review CHALLENGE m2: 54 year-only tables the exact list missed, 4 frozen) count, and
# 'Leibkonnaliikme kuu netosissetulek' (monthly income) or 'Aasta algul ja lõpul' do not. A false
# positive costs one POST per visit and nothing else; a false negative is a frozen D1 table.
_SUBPERIOD_STEMS = ("kvartal", "kuu", "poolaasta", "nadal", "quarter", "month", "half-year", "week")


def _fold(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", str(s).lower())
                   if not unicodedata.combining(c))


def _is_subperiod_name(code: str) -> bool:
    words = _fold(code).split()
    if not words:
        return False
    last = words[-1]
    return last in _SUBPERIOD_STEMS or last.endswith("kuu") or last.endswith("kvartal")


def _is_d1_shape(dates, var_codes) -> bool:
    """A YEAR axis (every parsed period is 31 Dec) and a sub-period variable among the others. The
    same rule classifies a stored table from its keys (dates + key-part names) and a visited table from
    its metadata (parsed time codes + variable codes), so the count in PR #243 is the fetcher's rule."""
    ds = [d for d in dates if d is not None]
    return bool(ds) and all(d.month == 12 and d.day == 31 for d in ds) and any(
        _is_subperiod_name(c) for c in var_codes)


def revision_activated_on(environ=None):
    """The date the revision window is armed from, or None (off)."""
    env = os.environ if environ is None else environ
    raw = (env.get(REVISION_ENV) or "").strip()
    if raw:
        try:
            return dt.date.fromisoformat(raw)
        except ValueError:
            print(f"[{SOURCE}] WARNING: {REVISION_ENV}={raw!r} is not YYYY-MM-DD - revision window OFF",
                  flush=True)
            return None
    return OPERATOR_ACTIVATED_ON


def _env_num(name, default):
    try:
        v = float(os.environ.get(name, "") or default)
        return v if v >= 0 else default
    except ValueError:
        return default


def _revision_due(entry, today: dt.date) -> bool:
    """Is a table due for its revision re-pull? Every ATTEMPT is stamped, whatever its outcome, so a
    table that is held or refused waits its cadence instead of pinning the head of the slice (R190)."""
    if not isinstance(entry, dict):
        return True
    try:
        at = dt.date.fromisoformat(str(entry.get("at"))[:10])
    except ValueError:
        return True
    wait = (REVISION_RETRY_DAYS if entry.get("outcome") == "transient"
            else _env_num("STAT_ESTONIA_REVISION_EVERY_DAYS", REVISION_EVERY_DAYS_DEFAULT))
    return (today - at).days >= wait


def _load_revision_state(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, REVISION_FILE))
    if raw is None:
        return {}
    try:
        d = json.loads(raw.decode("utf-8"))
        return d if isinstance(d, dict) else {}
    except ValueError:
        print(f"[{SOURCE}] {REVISION_FILE} unreadable - every table counts as due", flush=True)
        return {}


def _save_revision_state(out_dir, state: dict) -> bool:
    try:
        blob.write_bytes_atomic(os.path.join(out_dir, REVISION_FILE),
                                json.dumps(state, indent=0, sort_keys=True).encode("utf-8"))
        return True
    except Exception as e:                                   # noqa: BLE001
        print(f"[{SOURCE}] {REVISION_FILE} not saved ({e!r}) - the stamped tables count as due again",
              flush=True)
        return False


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


def _said(label: str) -> str:
    """Print a failed sub-unit to the run log and return its label for the tally (census's shape):
    a refusal must be readable in the log, not only in the state note."""
    print(f"[{SOURCE}] SUB-UNIT FAILED - {label}", flush=True)
    return label


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
    return _table_stats(parquet_path)[0]


def _table_stats(parquet_path: str):
    """({table prefix: max obs_date}, {table prefix: stored row count}) in one read of the file.
    The row count sizes the growth bounds (GROWTH_*)."""
    out: dict[str, dt.date] = {}
    rows: dict[str, int] = defaultdict(int)
    if not blob.exists(parquet_path):
        return out, rows
    t = blob.read_table(parquet_path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return out, rows
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
        rows[pref] += 1
        prev = out.get(pref)
        if prev is None or d > prev:
            out[pref] = d
    return out, rows


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
# Plan one table visit: which period codes, at which grain, in which POSTs.
# --------------------------------------------------------------------------- #
def _selection(ing, variables, time_idx, meta_time_code, *, force_aggregate=False):
    """({variable index: selected codes}, cells per period, grain) for every NON-time variable.

    THE GRAIN IS A PROPERTY OF THE TABLE, NOT OF HOW MANY PERIODS ARE PENDING. The old rule measured
    MAX_CELLS against the RESTRICTED time list, so one table was pulled in full detail when one period
    was pending and aggregated when many were: TO0053's history is one aggregate series while its
    tail holds 315 detail series (D2). Now: if ONE period in full detail fits under MAX_CELLS, every
    request is full detail and the time list is CHUNKED (see _plan_table); otherwise the ingester's
    over-cap selection (jobs/ingest_stat_estonia.query_table) is used unchanged - every variable its
    own is_time test keeps (the flagged code when `time: true` is present, else ing.is_time_dim) stays
    full, the rest collapse to one TOTAL/first code.

    The aggregate key is one member of the detail key set (the same codes), so the two grains never
    write conflicting values for one cell. What a grain change CAN do is add series a table has never
    held; update() withholds that (GROWTH_*), so a table's stored grain is kept.
    PARITY with the ingester (two parsers, R333): whenever the ingester's whole-history query fits under
    MAX_CELLS, both choose full detail; whenever one period is over MAX_CELLS, both choose the same
    aggregate selection (tests pin it). They differ only for per-period <= MAX_CELLS < whole history,
    where the ingester aggregated - exactly the D2 tables. The ingester is NOT changed here: a store
    repair of those tables must go through this module's chunked path (_plan_table with
    stored_max=None), never through a re-ingest.
    """
    per_full = 1
    for i, v in enumerate(variables):
        if i != time_idx:
            per_full *= max(len(v.get("values") or []), 1)
    sels: dict[int, list] = {}
    if per_full <= ing.MAX_CELLS and not force_aggregate:
        for i, v in enumerate(variables):
            vals = v.get("values") or []
            if i != time_idx and vals:
                sels[i] = list(vals)
        return sels, per_full, "full"
    per = 1
    for i, v in enumerate(variables):
        code = v.get("code", "")
        vals = v.get("values") or []
        if i == time_idx or not vals:
            continue
        keep_full = ((code == meta_time_code) if meta_time_code is not None
                     else ing.is_time_dim(code, vals))
        if keep_full:
            sel = list(vals)
        else:
            agg = [x for x in vals if str(x).upper() in ("0", "000", "TOTAL", "T", "ALL", "KOKKU")]
            sel = agg[:1] if agg else vals[:1]
        sels[i] = sel
        per *= len(sel)
    return sels, per, "aggregate"


def _plan_table(ing, variables, stored_max: dt.date | None, *, boundary: bool | str = True,
                rev_floor: dt.date | None = None, full_pull_grain: str = "ingester"):
    """The POSTs one table visit sends, or None when the table has no time dimension.

    Returns {"tcode", "queries": [query, ...], "dates": set | None, "n_new", "n_boundary", "n_rev",
    "grain", "per_period"}. `dates` is the set of obs_dates the request ASKS for; update() keeps only
    rows dated in it and compares them with the store. None = a full pull (no stored max): keep every
    parsed row, as before.

    THE CODES ASKED (stored_max not None):
      * new      - codes dated after the stored max (the old tail);
      * boundary - codes dated ON the stored max: when `boundary` is "all", or when it is truthy and
                   the table has the D1 shape (_is_d1_shape: the year code of a table that keys its
                   quarter or month). False / "off" asks no boundary.
      * revision - codes dated in [rev_floor, stored_max), when the revision window gives a floor (D0);
                   an armed window also asks the boundary, on any table shape.
    THE time axis is picked with the shared value-first resolver (core/pxweb.py) fed the SAME inputs
    ing.parse_jsonstat2 resolves with, so the axis restricted here is EXACTLY the axis the parser keys
    obs_date on (the old first-is_time_dim pick froze Kuu+Aasta cubes).

    CHUNKS. Each POST carries at most MAX_CELLS cells: the codes are SORTED ASCENDING by date and cut
    into runs of floor(MAX_CELLS / cells per period). The caller treats a table as all-or-nothing (a
    failed chunk merges none of the table), so a newer chunk can never land over an older hole and move
    the stored max past it.

    A FULL PULL (stored_max None) keeps the INGESTER's request by default (full_pull_grain="ingester"):
    one POST, full detail when the whole history fits MAX_CELLS, else the aggregate selection - exactly
    what main sent. ~1,500 visited tables hold no rows (most answer nothing), and a chunked full-detail
    pull of each on every visit would multiply their POSTs for no data (review VERIFY V-M1).
    full_pull_grain="table" gives the per-table grain in chunks: the D2 store repair's path.
    """
    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    time_idx = _pxweb.resolve_time_dim(
        [v.get("code", "") for v in variables],
        [[str(c) for c in (v.get("values") or [])] for v in variables],
        meta_time_code=meta_time_code, parse_fn=ing.parse_date)
    if time_idx is None:
        return None              # no time dimension -> nothing date-tailable (census snapshot)
    tvar = variables[time_idx]
    all_time = list(tvar.get("values") or [])
    d1 = _is_d1_shape([ing.parse_date(c) for c in all_time],
                      [v.get("code", "") for i, v in enumerate(variables) if i != time_idx])
    n_new = n_bnd = n_rev = 0
    if stored_max is None:
        # first-time / unmapped / corrupt-boundary table -> full pull (merge dedups). Publisher order is
        # kept: positional codes ('0','1',..) parse only through their labels, so they cannot be sorted.
        codes, dates = all_time, None
        n_new = len(codes)
    else:
        dated = [(c, d) for c in all_time if (d := ing.parse_date(c)) is not None]
        new = [(c, d) for c, d in dated if d > stored_max]
        # An ARMED revision window re-pulls through the stored max, whatever the table's shape: without
        # this a non-D1 table's last period is never re-read (the AR-453 replay lost TO0053's 117
        # revised rows at its stored month when the boundary was narrowed to D1).
        ask_bnd = (boundary == "all" or rev_floor is not None
                   or (bool(boundary) and boundary != "off" and d1))
        bnd = [(c, d) for c, d in dated if d == stored_max] if ask_bnd else []
        rev = ([(c, d) for c, d in dated if rev_floor <= d < stored_max]
               if rev_floor is not None else [])
        n_new, n_bnd, n_rev = len(new), len(bnd), len(rev)
        picked = sorted(rev + bnd + new, key=lambda cd: cd[1])
        codes = [c for c, _ in picked]
        dates = {d for _, d in picked}
    plan = {"tcode": tvar.get("code"), "queries": [], "dates": dates, "n_new": n_new,
            "n_boundary": n_bnd, "n_rev": n_rev, "grain": None, "per_period": 0,
            "d1": d1, "tail_grain_changed": False}
    if not codes:
        return plan
    sels, per, grain = _selection(ing, variables, time_idx, meta_time_code)
    step = max(1, ing.MAX_CELLS // max(per, 1))
    if stored_max is None and full_pull_grain == "ingester":
        # jobs/ingest_stat_estonia.query_table, verbatim in effect: whole-history cells decide.
        if grain == "full" and len(codes) * per > ing.MAX_CELLS:
            sels, per, grain = _selection(ing, variables, time_idx, meta_time_code, force_aggregate=True)
        step = len(codes)                                  # one POST, as before
    elif n_new and grain == "full":
        # MAIN'S GRAIN FOR THE SAME TAIL (review AR-453 C2): main measured MAX_CELLS against the
        # restricted (new-only) list, so it aggregated a tail of many periods. Where this request is
        # full detail and main's would have been aggregate, the tail's rows under keys the table never
        # held are a grain change and stay under the new-series bound; anywhere else the tail merges
        # exactly as on main (a genuine new category included).
        plan["tail_grain_changed"] = n_new * per > ing.MAX_CELLS
    plan["grain"], plan["per_period"] = grain, per
    for j in range(0, len(codes), step):
        part = codes[j:j + step]
        query = []
        for i, v in enumerate(variables):
            if i == time_idx:
                query.append({"code": v.get("code", ""),
                              "selection": {"filter": "item", "values": part}})
            elif i in sels:
                query.append({"code": v.get("code", ""),
                              "selection": {"filter": "item", "values": sels[i]}})
        plan["queries"].append(query)
    return plan


# --------------------------------------------------------------------------- #
# Compare a subject's fetched rows with the store, whole rows on (series_key, obs_date).
# --------------------------------------------------------------------------- #
_PREFIX_RE = r"^(?P<p>.*?\.[pP][xX])(?::|$)"


def _prefixes(keys):
    """The table prefix of each series_key (vectorised; as _max_by_table: up to and including '.px')."""
    st = pc.extract_regex(keys, _PREFIX_RE)
    return pc.struct_field(st, [0])


def _n_dup(t) -> int:
    """Rows beyond one per (series_key, obs_date). Sort + adjacent compare, never a group_by."""
    if t.num_rows < 2:
        return 0
    s = t.select(["series_key", "obs_date"]).sort_by([("series_key", "ascending"),
                                                      ("obs_date", "ascending")])
    k = s.column("series_key").combine_chunks()
    d = s.column("obs_date").combine_chunks()
    n = s.num_rows
    same = pc.and_(pc.equal(k.slice(0, n - 1), k.slice(1, n - 1)),
                   pc.equal(d.slice(0, n - 1), d.slice(1, n - 1)))
    return int(pc.sum(pc.cast(pc.fill_null(same, False), pa.int64())).as_py() or 0)


def _compare_subject(path, fetched, stored_rows):
    """What merging this subject's fetched rows would do, per table, and the rows to merge.

    fetched: {table prefix: {"rows": [(key, date, value)], "dates": set|None, "stored_max": date|None,
              "dup_fetched": int}}
    stored_rows: {table prefix: stored row count} (sizes the growth bounds).

    Per table: changed (stored key+date, other value: overwritten), added (key+date the store lacks),
    held (stored rows at the asked dates the fetch did not return: KEPT, merge never deletes),
    withheld_new_series / withheld_revision (added rows a growth bound keeps OUT of the merge, named;
    the new-series bound covers rows dated AT or BEFORE the stored max - the boundary and the revision
    window - and the new tail only when fetched[t]["tail_grain_changed"]; a same-grain tail merges as
    on main, review AR-453 C2),
    refused (the key is not unique in the fetch or in the stored window: nothing of the table merges).
    Returns (stats, merge_table|None). One pass over the stored file in batches, vectorised (R244).
    """
    from ... import merge as _m
    tbls = sorted(fetched)
    fk, fd, fv, ft = [], [], [], []
    for t in tbls:
        for k, d, v in fetched[t]["rows"]:
            fk.append(k); fd.append(d); fv.append(v); ft.append(t)
    F = pa.table({"series_key": pa.array(fk, pa.string()), "obs_date": pa.array(fd, pa.date32()),
                  "value": pa.array(fv, pa.float64()), "tbl": pa.array(ft, pa.string())})
    touched = pa.array(tbls, pa.string())
    parts, uniq = [], []
    if blob.exists(path):
        for b in blob.iter_batches(path, columns=["series_key", "obs_date", "value"]):
            bt = pa.Table.from_batches([b])
            if bt.schema.field("series_key").type != pa.string():
                # merge.py promotes series_key to large_string past 1 GiB and the type persists in the
                # file; Arrow refuses to join string with large_string (VERIFY minor 5). One batch's
                # keys are far below 2 GiB, so the cast back is safe here.
                bt = bt.set_column(bt.schema.get_field_index("series_key"), "series_key",
                                   pc.cast(bt.column("series_key"), pa.string()))
            pref = _prefixes(bt.column("series_key"))
            mask = pc.fill_null(pc.is_in(pref, value_set=touched), False)
            if not pc.any(mask).as_py():
                continue
            sub = bt.filter(mask).append_column("tbl", pc.filter(pref, mask))
            uniq.append(pc.unique(sub.column("series_key")))
            parts.append(sub)
    if parts:
        S = pa.concat_tables(parts)
        stored_keys = pc.unique(pa.chunked_array(uniq).combine_chunks())
    else:
        S = pa.table({"series_key": pa.array([], pa.string()), "obs_date": pa.array([], pa.date32()),
                      "value": pa.array([], pa.float64()), "tbl": pa.array([], pa.string())})
        stored_keys = pa.array([], pa.string())
    # the stored WINDOW: rows of each table at the dates its request asked (all dates for a full pull)
    allowed_t, allowed_d, full_tbls = [], [], []
    for t in tbls:
        ds = fetched[t]["dates"]
        if ds is None:
            full_tbls.append(t)
        else:
            for d in ds:
                allowed_t.append(t); allowed_d.append(d)
    W_parts = []
    if allowed_t and S.num_rows:
        A = pa.table({"tbl": pa.array(allowed_t, pa.string()), "obs_date": pa.array(allowed_d, pa.date32())})
        W_parts.append(S.join(A, keys=["tbl", "obs_date"], join_type="inner"))
    if full_tbls and S.num_rows:
        W_parts.append(S.filter(pc.is_in(S.column("tbl"), value_set=pa.array(full_tbls, pa.string()))))
    W = (pa.concat_tables([w.select(["series_key", "obs_date", "value", "tbl"]) for w in W_parts])
         if W_parts else S.slice(0, 0))
    Wm = W.select(["series_key", "obs_date", "value"]).rename_columns(
        ["series_key", "obs_date", "old"]).append_column("__s", pa.array([True] * W.num_rows, pa.bool_()))
    J = F.join(Wm, keys=["series_key", "obs_date"], join_type="left outer")
    matched = pc.fill_null(J.column("__s"), False)
    same = _m._same_vals(J.column("value"), J.column("old"))
    changed_m = pc.and_(matched, pc.invert(same))
    added_m = pc.invert(matched)
    is_new_series = pc.invert(pc.fill_null(pc.is_in(J.column("series_key"), value_set=stored_keys), False))

    def _count_by(mask):
        c = defaultdict(int)
        for t in pc.filter(J.column("tbl"), mask).to_pylist():
            c[t] += 1
        return c
    n_changed = _count_by(changed_m)
    n_added = _count_by(added_m)
    n_matched = _count_by(matched)
    n_window = defaultdict(int)
    for t in W.column("tbl").to_pylist():
        n_window[t] += 1
    # revision rows: added under KNOWN series, dated before the table's stored max
    smax = {t: fetched[t]["stored_max"] for t in tbls}
    below = []
    for t, d in zip(J.column("tbl").to_pylist(), J.column("obs_date").to_pylist()):
        sm = smax.get(t)
        below.append(sm is not None and d < sm)
    below = pa.array(below, pa.bool_())
    n_added_rev = _count_by(pc.and_(pc.and_(added_m, pc.invert(is_new_series)), below))
    # rows the NEW-SERIES bound applies to: at or before the stored max (boundary, revision window), or
    # anywhere in a table whose new tail changed grain against main's request
    at_or_below = []
    for t, d in zip(J.column("tbl").to_pylist(), J.column("obs_date").to_pylist()):
        sm = smax.get(t)
        at_or_below.append(bool(fetched[t].get("tail_grain_changed")) or (sm is not None and d <= sm))
    bounded = pc.and_(pc.and_(added_m, is_new_series), pa.array(at_or_below, pa.bool_()))
    n_added_new = _count_by(bounded)

    stats, refuse, hold_new, hold_rev = {}, set(), set(), set()
    for t in tbls:
        dup_s = _n_dup(W.filter(pc.equal(W.column("tbl"), t))) if n_window[t] else 0
        st = {"changed": n_changed[t], "added": n_added[t], "held": n_window[t] - n_matched[t],
              "dup_fetched": fetched[t]["dup_fetched"], "dup_stored": dup_s,
              "withheld_new_series": 0, "withheld_revision": 0, "refused": False}
        if st["dup_fetched"] or dup_s:
            st["refused"] = True
            refuse.add(t)
        else:
            base_rows = stored_rows.get(t, 0)
            if base_rows and n_added_new[t] > max(GROWTH_MIN_ROWS, base_rows * GROWTH_PCT / 100):
                hold_new.add(t)
                st["withheld_new_series"] = n_added_new[t]
            if n_added_rev[t] > max(GROWTH_MIN_ROWS, n_window[t] * GROWTH_PCT / 100):
                hold_rev.add(t)
                st["withheld_revision"] = n_added_rev[t]
        stats[t] = st
    # the rows to merge: changed + added, minus refused tables and withheld rows
    tblcol = J.column("tbl")
    keep = pc.or_(changed_m, added_m)
    if refuse:
        keep = pc.and_(keep, pc.invert(pc.is_in(tblcol, value_set=pa.array(sorted(refuse), pa.string()))))
    if hold_new:
        in_h = pc.is_in(tblcol, value_set=pa.array(sorted(hold_new), pa.string()))
        keep = pc.and_(keep, pc.invert(pc.and_(in_h, bounded)))
    if hold_rev:
        in_h = pc.is_in(tblcol, value_set=pa.array(sorted(hold_rev), pa.string()))
        keep = pc.and_(keep, pc.invert(pc.and_(pc.and_(pc.and_(in_h, added_m),
                                                       pc.invert(is_new_series)), below)))
    M = J.filter(keep)
    for t in tbls:
        mt = M.filter(pc.equal(M.column("tbl"), t))
        stats[t]["merged_changed"] = int(pc.sum(pc.cast(pc.fill_null(mt.column("__s"), False),
                                                        pa.int64())).as_py() or 0) if mt.num_rows else 0
        stats[t]["merged_added"] = mt.num_rows - stats[t]["merged_changed"]
        mx = pc.max(mt.column("obs_date")).as_py() if mt.num_rows else None
        stats[t]["merged_max"] = mx
    if M.num_rows == 0:
        return stats, None
    return stats, M.select(["series_key", "obs_date", "value"])


def _merge_note(stats: dict, extra: str = "") -> str:
    """The run note's merge tail. No '; ' inside (health drops a note SEGMENT by prefix), bounded."""
    if not stats and not extra:
        return ""
    tot = defaultdict(int)
    for st in stats.values():
        for k in ("changed", "added", "held", "withheld_new_series", "withheld_revision"):
            tot[k] += st[k]
        tot["refused"] += int(st["refused"])
    head = (f"{MERGE_NOTE} {len(stats)} table(s) compared - changed {tot['changed']} added "
            f"{tot['added']} held {tot['held']} (held = stored rows not returned, kept)")
    if tot["withheld_new_series"] or tot["withheld_revision"]:
        head += (f", withheld {tot['withheld_new_series']} new-series + "
                 f"{tot['withheld_revision']} revision row(s)")
    if tot["refused"]:
        head += f", {tot['refused']} table(s) refused"
    if extra:
        head += f", {extra}"
    busy = sorted((t for t, st in stats.items()
                   if st["changed"] or st["added"] or st["held"] or st["refused"]
                   or st["withheld_new_series"] or st["withheld_revision"]),
                  key=lambda t: -(stats[t]["changed"] + stats[t]["added"] + stats[t]["held"]))
    items = []
    for t in busy:
        st = stats[t]
        leaf = t.rsplit(":", 1)[-1]
        tag = f"{leaf} {st['changed']}/{st['added']}/{st['held']}"
        if st["refused"]:
            tag += " refused"
        elif st["withheld_new_series"] or st["withheld_revision"]:
            tag += " withheld"
        items.append(tag)
    out = head
    if items:
        shown, used = [], len(head) + 30
        for it in items:
            if used + len(it) + 2 > NOTE_MAX_CHARS and shown:
                break
            shown.append(it)
            used += len(it) + 2
        more = len(items) - len(shown)
        out += f" [changed/added/held: {', '.join(shown)}" + (f", +{more} more]" if more else "]")
    return out.replace("; ", ", ")


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
    # THE TABLES THIS PASS CHANGED, MEASURED BY THE MERGE - the CSV phase's changed set. The cursors
    # above are SEEDED from every stored table of every visited subject (so a frozen table cannot
    # hide), and the orchestrator used to map all of them: under the r2 backend a runner holds only
    # the subject files this pass WROTE, so every other table's derive failed with ResolveError
    # ("csv_derive failed 173/911 series" on 2026-09-18; 2,089 stat_estonia ids stuck in
    # csv_retry_queue, all ResolveError on archive and census tables). orchestrate._catalog_ids_for
    # assumes a mapped id's file is one this run wrote; this keeps that true. {table prefix: date}.
    changed: dict[str, str] = {}

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

    # THE BOUNDARY AND THE REVISION WINDOW (see BOUNDARY_ENV / REVISION_ENV at the top).
    today = dt.date.today()
    b_mode = boundary_mode()
    boundary_on = b_mode != "off"
    act = revision_activated_on()
    revision_armed = act is not None and act <= today
    # OFF means OFF: no sidecar read, no sidecar write, no period before the boundary asked.
    rev_state = _load_revision_state(out_dir) if revision_armed else {}
    rev_years = int(_env_num("STAT_ESTONIA_REVISION_YEARS", REVISION_YEARS_DEFAULT))
    rev_slice_s = _env_num("STAT_ESTONIA_REVISION_BUDGET_MIN", REVISION_BUDGET_MIN_DEFAULT) * 60
    rev_spent_s = 0.0
    rev_done = rev_left = 0
    rev_stamped: set = set()       # table paths whose revision outcome was stamped THIS pass
    rev_cold_paths: set = set()    # cold table paths of the subjects this pass reached
    rev_stale: list = []           # standing holds dropped this pass (gone or cold), named in the note
    note_stats: dict = {}          # {table prefix: compare stats} over the whole pass, for the note
    # THE REQUEST COST OF THIS PASS, printed at the end (VERIFY V-M1: the boundary's cost is not yet
    # measured; this line is the instrument, read on the first runs after the merge).
    req_posts = req_chunks = req_d1 = req_bnd = 0
    req_tables: dict = defaultdict(int)          # {grain: tables planned with >= 1 POST}
    if revision_armed:
        print(f"[{SOURCE}] revision window ARMED (from {act}): last {rev_years} year(s) per hot table, "
              f"each at most every {_env_num('STAT_ESTONIA_REVISION_EVERY_DAYS', REVISION_EVERY_DAYS_DEFAULT):.0f} "
              f"day(s), slice {rev_slice_s / 60:.1f} min per pass", flush=True)

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
                  f"deferred to a later pass", flush=True)
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
        # per-table max (and row count, for the growth bounds) within this subject file
        stored, stored_rows = _table_stats(path)

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
        rev_cold_paths |= cold_set     # every cold table of a reached subject (standing holds, R3-2)
        if cold_set:
            print(f"[{SOURCE}] {subj}: {len(cold_set)} of {len(subj_tables)} table(s) cold "
                  f"(archive or frontier older than {_cold_after_days():.0f}d); "
                  f"{len(cold_due)} due this pass, resuming after {_cold_book.split('|')[-1] or '(start)'!r}",
                  flush=True)

        # this subject's FETCHED rows per table; compared with the store and merged ONCE, below.
        fetched: dict[str, dict] = {}
        rev_tables: set = set()        # tables whose request carried revision codes this pass
        rev_dirty = False

        # Start at the resume point, so a deadline that fires in the FIRST table of a resumed subject
        # writes the same bookmark back instead of "<subject>|" (which would re-walk its head).
        last_tbl = (tbl_rot.split("|", 1)[1] if tbl_rot.startswith(f"{subj}|")
                    and tbl_rot.split("|", 1)[1] in {t_["path"] for t_ in subj_tables} else "")
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
            # back to the previous subject so the next pass re-enters this one and continues from
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
                      f"{last_tbl!r} in a later pass (subject bookmark held at {prev_subj!r})",
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
            is_cold = tpath in cold_set
            prev_tbl, prev_cold = last_tbl, (last_cold_visited, n_cold_visited)
            if is_cold:
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
            # sentinel / 2085 projection). The plan asks only periods dated at or after the
            # stored max; if stored_max is year 9999 NOTHING is ever asked and the table freezes
            # forever. sane_since() returns None for a beyond-horizon
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

            # 2) plan the request: new periods, the boundary (hot tables), the revision window
            t0 = time.time()
            rev_floor = None
            if (revision_armed and not is_cold and stored_max is not None
                    and _revision_due(rev_state.get(tpath), today)):
                if rev_spent_s < rev_slice_s:
                    rev_floor = dt.date(stored_max.year - rev_years + 1, 1, 1)
                else:
                    rev_left += 1          # due, slice spent: a later pass (named in the merge note)
            plan = _plan_table(ing, meta["variables"], stored_max,
                               boundary=b_mode if (boundary_on and not is_cold) else "off",
                               rev_floor=rev_floor)
            if plan is None or not plan["queries"]:
                tally.empty_unit(tpath)    # no time dim, or nothing at/after the stored max
                time.sleep(RATE)
                continue
            is_rev = rev_floor is not None and plan["n_rev"] > 0
            req_tables[plan["grain"]] += 1
            req_d1 += int(bool(plan["d1"]))
            req_bnd += int(plan["n_boundary"] > 0)
            req_chunks += len(plan["queries"])

            # 3) data POSTs, one per chunk, ALL OR NOTHING for the table (see _plan_table)
            meta_time_code = next((v.get("code") for v in meta["variables"] if v.get("time") is True), None)
            rows: list = []
            resps: list = []
            outcome = None
            for qi, query in enumerate(plan["queries"]):
                if qi and dl.spent():
                    outcome = "deadline"
                    break
                body = {"query": query, "response": {"format": "json-stat2"}}
                req_posts += 1
                try:
                    resp = _post_data(sess, url, body)
                except TransientError:
                    outcome = "transient"
                    break
                if not resp or not isinstance(resp, dict):
                    outcome = "rejected"
                    break
                resps.append(resp)
                # 4) parse with the ingester's parser (identical keys/dates as bulk ingest).
                # Thread the AUTHORITATIVE PxWeb `time: true` flag so the shared value-first
                # resolver locks onto it; None (Estonia omits it on some tables) -> value-first.
                rows.extend(ing.parse_jsonstat2(resp, prefix, meta_time_code))
                if qi + 1 < len(plan["queries"]):
                    time.sleep(RATE)
            if is_rev:
                rev_spent_s += time.time() - t0     # every attempt uses the slice, failed ones too
            if outcome == "deadline":
                # Stopped BETWEEN chunks: nothing of this table merges, and BOTH bookmarks are written
                # exactly as the per-table cap above writes them, but on the table BEFORE this one, so
                # the next pass re-enters this subject and starts AT this table - also when it is the
                # subject's last table (a `continue` would then fall off the loop and the subject
                # bookmark, saved on entry, would step over it).
                last_tbl = prev_tbl
                last_cold_visited, n_cold_visited = prev_cold
                hit_cap_inside = True
                capped_inside_subject = True
                stopped_early = True
                save_rotation(out_dir, f"{subj}|{last_tbl}", _TBL_ROTATION)
                save_rotation(out_dir, prev_subj)
                n_deferred = len(subj_tables) - subj_tables.index(t)
                tally.deferred_unit(f"{tpath}: stopped between its {len(plan['queries'])} request "
                                    f"chunks, {n_deferred} table(s) from it")
                print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                      f"{dl.elapsed_min():.1f} min BETWEEN the request chunks of {tpath!r} - nothing of "
                      f"it merged; resuming at it in a later pass", flush=True)
                break
            if outcome == "transient":
                tally.transient_unit(tpath)
                if is_rev:
                    rev_state[tpath] = {"at": today.isoformat(), "outcome": "transient"}
                    rev_dirty = True
                time.sleep(RATE)
                continue
            if outcome == "rejected":
                tally.empty_unit(tpath)    # 400/403 query rejected -> empty
                print(f"[{SOURCE}] {tpath}: request chunk {len(resps) + 1} of {len(plan['queries'])} "
                      f"rejected (400/403) - nothing of the table merged", flush=True)
                if is_rev:
                    rev_state[tpath] = {"at": today.isoformat(), "outcome": "rejected"}
                    rev_dirty = True
                time.sleep(RATE)
                continue

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
                # A request that asked NO new period (boundary / revision only) is benign too:
                # until this change such a table never POSTed, and a structural count vetoes
                # the WHOLE source, so it is named in the log instead (CHALLENGE: holds).
                if plan["n_new"] and any(structural_on_zero_rows(stored_max, r) for r in resps):
                    tally.structural_unit(tpath)
                else:
                    if stored_max is not None and not plan["n_new"]:
                        print(f"[{SOURCE}] {tpath}: boundary/revision request parsed 0 rows "
                              f"({len(resps)} body(ies)) - booked empty, stored rows kept", flush=True)
                    tally.empty_unit(tpath)
                if is_rev:
                    rev_state[tpath] = {"at": today.isoformat(), "outcome": "no rows"}
                    rev_dirty = True
                time.sleep(RATE)
                continue

            # keep the rows dated in what was ASKED; count repeats BEFORE dropping them (a repeat
            # means (series_key, obs_date) is not a key here - e.g. H1 and Q1 both parse to 1 Jan)
            dset = plan["dates"]
            seen_n: dict = defaultdict(int)
            kept: list = []
            n_outside = 0
            for key, d, v in rows:
                if isinstance(d, dt.datetime):
                    d = d.date()
                if dset is not None and d not in dset:
                    n_outside += 1
                    continue
                tok = (key, d)
                seen_n[tok] += 1
                if seen_n[tok] > 1:
                    continue
                kept.append((key, d, v))
            if not kept:
                tally.empty_unit(tpath)    # parsed rows, none dated in what was asked
                print(f"[{SOURCE}] {tpath}: {len(rows)} parsed row(s), none dated in what was asked",
                      flush=True)
                if is_rev:
                    rev_state[tpath] = {"at": today.isoformat(), "outcome": "no rows"}
                    rev_dirty = True
                time.sleep(RATE)
                continue
            fetched[prefix] = {"rows": kept, "dates": dset, "stored_max": stored_max,
                               "dup_fetched": sum(c - 1 for c in seen_n.values()),
                               "path": tpath, "n_outside": n_outside, "grain": plan["grain"],
                               "chunks": len(plan["queries"]),
                               "tail_grain_changed": plan["tail_grain_changed"]}
            if is_rev:
                rev_tables.add(prefix)
            time.sleep(RATE)

        # 5) compare this subject's fetched rows with the store (whole rows on the dedup key) and
        # merge ONLY the changed and added ones (one publish per subject file, none when nothing
        # changed - a re-fetched identical boundary must not rewrite the subject parquet).
        if fetched:
            stats = mtbl = None
            try:
                stats, mtbl = _compare_subject(path, fetched, stored_rows)
            except Exception as e:                           # noqa: BLE001
                for p_ in sorted(fetched):
                    tally.transient_unit(_said(f"{fetched[p_]['path']}: store compare failed "
                                               f"({type(e).__name__}: {str(e)[:100]}); nothing merged"))
            if stats is None:
                total += before
                for p_ in rev_tables:
                    rev_state[fetched[p_]["path"]] = {"at": today.isoformat(), "outcome": "transient"}
                    rev_dirty = True
            else:
                for p_ in sorted(stats):
                    st, f_ = stats[p_], fetched[p_]
                    print(f"[{SOURCE}] {f_['path']}: grain={f_['grain']} chunks={f_['chunks']} "
                          f"changed={st['changed']} added={st['added']} held={st['held']} (held = "
                          f"stored rows not returned, kept) withheld_new_series={st['withheld_new_series']} "
                          f"withheld_revision={st['withheld_revision']} dup_fetched={st['dup_fetched']} "
                          f"dup_stored={st['dup_stored']} outside_asked={f_['n_outside']}"
                          + (" REFUSED (key not unique)" if st["refused"] else ""), flush=True)
                merged_ok = True
                if mtbl is not None:
                    exp_added = sum(st["merged_added"] for st in stats.values())
                    # NO ROW IS EVER DELETED - refused BEFORE the write, not detected after it. The
                    # merge dedups the WHOLE file, so a duplicate stored key anywhere would collapse
                    # (delete) a row while the total still grows; min_ratio=1.0 cannot see that (a test
                    # caught it). The merge must end with exactly stored + added rows, so the ratio is
                    # set to refuse anything below that (-0.5: integer rows, no float edge).
                    floor_ratio = ((before + exp_added - 0.5) / before) if before else 0.97
                    try:
                        n, md, ch = merge.merge_and_write(path, mtbl, mode="merge", dedup_keys=DEDUP,
                                                          min_ratio=floor_ratio, report_changed_keys=True,
                                                          changed_keys_cap=max(mtbl.num_rows, 1))
                    except (DefinitiveError, ValueError) as e:
                        merged_ok = False
                        total += before
                        for p_ in sorted(stats):
                            if stats[p_]["merged_added"] or stats[p_]["merged_changed"]:
                                tally.transient_unit(_said(
                                    f"{fetched[p_]['path']}: merge refused ({str(e)[:120]}); "
                                    f"stored rows kept"))
                    else:
                        if n != before + exp_added:
                            tally.transient_unit(_said(
                                f"{subj}: merge DEFECT - {before:,} stored + {exp_added:,} added "
                                f"!= {n:,} after"))
                        exp_keys = set(mtbl.column("series_key").to_pylist())
                        if set(ch) != exp_keys:
                            tally.transient_unit(_said(
                                f"{subj}: merge DEFECT - the merge's changed-key report "
                                f"({len(ch):,}) != the compare's changed+added keys ({len(exp_keys):,})"))
                        for k, d in ch.items():
                            low = (k or "").lower()
                            cut = low.find(".px")
                            # the table prefix, as _max_by_table; a key with no '.px' keeps its WHOLE
                            # key, so it lands in the orchestrator's unmapped set and is seen, never
                            # silently dropped (R1129)
                            pref = k[: cut + 3] if cut != -1 else k
                            if pref and (pref not in changed or (d and str(d) > str(changed[pref]))):
                                changed[pref] = d
                        del ch
                        total += n
                        if md:
                            # md is the whole-file max (may be a projection/2085 or 9999 sentinel);
                            # the per-table cursors carry exact freshness, so only feed the capped
                            # value into the unit-level rollup.
                            maxd = _bump_unit_max(maxd, dt.date.fromisoformat(md))
                else:
                    total += before
                # THE TALLY COMES FROM THE COMPARE (not from rows fetched): added rows -> added,
                # values revised with no row added -> revised (so the pass is ok/partial and the CSV
                # phase runs, R1125), else empty. Cursors move only over what MERGED.
                for p_ in sorted(stats):
                    st = stats[p_]
                    tp = fetched[p_]["path"]
                    if st["refused"]:
                        tally.transient_unit(_said(
                            f"{tp}: merge refused - (series_key, obs_date) not unique "
                            f"({st['dup_fetched']} repeated fetched, {st['dup_stored']} repeated stored); "
                            f"stored rows kept"))
                    elif not merged_ok and (st["merged_added"] or st["merged_changed"]):
                        pass                                 # booked transient above
                    elif st["withheld_new_series"] or st["withheld_revision"]:
                        # WITHHELD ROWS NEED A PERSON (review AR-453 C2): booked TRANSIENT and named, so
                        # the source reads ATTENTION in the gate and the digest. A boundary withhold
                        # recurs on every visit; a revision withhold is re-booked from its sidecar stamp
                        # on every armed pass (the standing hold before finalize, R2-1), so either way
                        # it stays ATTENTION until someone acts. The merge note alone is a non-failure
                        # tail that health strips. What did
                        # merge from this table (changed values, added rows outside the bound) is in the
                        # store and the row total; its added rows still count in tally.added.
                        tally.added += st["merged_added"]
                        tally.transient_unit(_said(
                            f"{tp}: WITHHELD {st['withheld_new_series']} row(s) under series the table "
                            f"never held + {st['withheld_revision']} revision row(s) over the growth "
                            f"bound - needs review ({st['merged_changed']} changed / "
                            f"{st['merged_added']} added merged)"))
                    elif st["merged_added"]:
                        tally.added_unit(st["merged_added"])
                    elif st["merged_changed"]:
                        tally.revised_unit(tp)
                    else:
                        tally.empty_unit()
                    if merged_ok and st["merged_max"] is not None:
                        cv = _cursor_value(st["merged_max"])
                        prev = cursors.get(p_)
                        if prev is None or cv > prev:
                            cursors[p_] = cv
                        maxd = _bump_unit_max(maxd, st["merged_max"])
                    if p_ in rev_tables:
                        oc = ("refused" if st["refused"]
                              else "transient" if not merged_ok
                              else "withheld" if (st["withheld_new_series"] or st["withheld_revision"])
                              else "merged" if (st["merged_added"] or st["merged_changed"])
                              else "no change")
                        rev_state[tp] = {"at": today.isoformat(), "outcome": oc}
                        if oc == "withheld":
                            # the counts travel with the stamp, so the STANDING hold below can name
                            # them on the passes that do not re-pull the table
                            rev_state[tp].update(withheld_new_series=st["withheld_new_series"],
                                                 withheld_revision=st["withheld_revision"])
                        rev_stamped.add(tp)
                        rev_dirty = True
                        rev_done += 1
                note_stats.update(stats)
        else:
            total += before
        if revision_armed and rev_dirty:
            # per subject, so a kill loses at most the subject whose rows did not merge either (R273)
            _save_revision_state(out_dir, rev_state)

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
    # next pass re-enters the unfinished subject, and then this save overwrote it with
    # last_subj — the interrupted one — so the next run resumed AFTER it and its tail was
    # skipped exactly as before. Caught by running the fetcher with a 3-second budget, not by
    # reading it: both writes look correct in isolation and only the order reveals the bug.
    if last_subj and not capped_inside_subject:
        save_rotation(out_dir, last_subj)
    if stopped_early:
        visited = set(subjects[:subjects.index(last_subj) + 1])
        n_subunits = sum(len(by_subject[s]) for s in visited if s in by_subject)

    # A STANDING REVISION HOLD (review AR-453 R2-1). A revision withhold is stamped "withheld" and the
    # table is not re-pulled for REVISION_EVERY_DAYS (90), so without this the source would read ROTATING
    # again from the next pass. While the sidecar says "withheld", every ARMED pass re-books the table's
    # named WITHHELD transient - no request is sent - until the stamp changes: a person repairs the table
    # or edits/removes its sidecar entry, or the next due re-pull merges. Disarming the window stands the
    # holds down with it (off means off: the sidecar is not read), and re-arming brings them back.
    # A STALE HOLD IS DROPPED (review AR-453 R3-2): a stamp for a table that is no longer in this pass's
    # table list (it left the catalogue) or is cold can never be re-pulled, so it would hold the source in
    # ATTENTION for ever. Its sidecar entry is removed and the table is named ONCE in the merge note as
    # "stale hold dropped". A table in a subject this pass did not reach keeps its hold (its coldness is
    # not known this pass). A standing hold is booked transient but NOT attempted: no request was made.
    if revision_armed:
        cat_paths = {t["path"] for s_ in by_subject for t in by_subject[s_]}
        for tp_, e_ in sorted(rev_state.items()):
            if not (isinstance(e_, dict) and e_.get("outcome") == "withheld" and tp_ not in rev_stamped):
                continue
            if tp_ not in cat_paths or tp_ in rev_cold_paths:
                rev_stale.append(f"{tp_} ({'cold' if tp_ in cat_paths else 'not in the catalogue'})")
                del rev_state[tp_]
                print(f"[{SOURCE}] stale hold dropped: {rev_stale[-1]}", flush=True)
                continue
            tally.transient += 1
            tally.transient_ids.append(_said(
                f"{tp_}: WITHHELD since {e_.get('at')} - {e_.get('withheld_new_series', '?')} row(s) "
                f"under series the table never held + {e_.get('withheld_revision', '?')} revision "
                f"row(s) over the growth bound - needs review (standing hold, not re-pulled this "
                f"pass)"))
        if rev_stale:
            _save_revision_state(out_dir, rev_state)
    print(f"[{SOURCE}] requests this pass: {req_posts} POST(s) over {req_chunks} planned chunk(s); "
          f"tables by grain: full={req_tables['full']} aggregate={req_tables['aggregate']}; "
          f"boundary {b_mode} (asked on {req_bnd} table(s), {req_d1} visited table(s) D1-shape), revision {'ARMED' if revision_armed else 'off'}; "
          f"{tally.attempted} sub-unit(s) attempted", flush=True)
    last_obs = maxd.isoformat() if maxd is not None else (since or None)
    # empty_window_floor = (#sub-units) - 1 per the contract: a real wholesale outage
    # (every table empty/404) trips the structural floor; a healthy run where most
    # tables are simply "nothing newer" is legitimate no_change and must NOT.
    res = finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                   empty_window_floor=max(n_subunits - 1, 1))
    res.changed_keys = changed        # complete: bounded by the ~4,978 tables, never capped
    # THE MERGE NOTE (a non-failure tail, base.MERGE_NOTE): what the merge did, counted from the
    # whole-row compare - changed / added / held per table. Refusals and defects are ALSO booked
    # transient above, so a note can never be the only trace of a failure.
    extra = ""
    if revision_armed:
        extra = (f"revision window {rev_done} table(s) re-pulled, {rev_left} due table(s) left "
                 f"for a later pass")
        if rev_stale:
            extra += f", stale hold dropped: {', '.join(rev_stale)}"
    note = _merge_note(note_stats, extra)
    if note:
        res.error = "; ".join(x for x in (res.error, note) if x)
    return res
