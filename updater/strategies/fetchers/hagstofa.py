"""S3 (sdmx_delta) fetcher — Statistics Iceland (Hagstofa Íslands), PxWeb. No key.

License: CC BY 4.0.  Source: https://px.hagstofa.is/pxen/api/v1/en (PxWeb v1, JSON-stat2).

LAYOUT (set by jobs/ingest_hagstofa.py): ONE parquet per DATABASE under
clean_full/hagstofa/<db>.parquet (db in Atvinnuvegir / Efnahagur / Ibuar /
Samfelag / Umhverfi). Schema is (series_key, obs_date, value):
  series_key : "ICE:<db>:<path-with-/-as-:>:<dim>=<code>[:<dim>=<code>...]" where the
               leading "ICE:<db>:<path>" segment (through the ".px" leaf) identifies
               the source TABLE and the trailing "<dim>=<code>" tokens identify the
               non-time cell within that table. This is exactly what parse_jsonstat2
               (reused verbatim from the ingester) emits, so re-fetched rows collide
               with their on-disk twins and merge dedup overwrites revisions in place.
  obs_date   : date32, parsed from the PxWeb time dimension code (parse_date).
  value      : float64.
DEDUP KEY = (series_key, obs_date) — the exact key the ingester wrote.

SUB-UNIT = one PxWeb TABLE (a catalog entry). The catalog (data/clean_full/hagstofa/
_catalog.json) — written by the ingester's crawl — is REUSED verbatim (db / path / id),
never re-discovered. There are ~1900 tables across 5 databases.

DATE-TAIL (cheap incremental): for each table we read its on-disk max(obs_date)
(from the rows whose series_key starts with that table's prefix), GET the live PxWeb
metadata to find the time variable + its sorted time codes, and POST a query that
restricts the time dimension to ONLY the codes whose parsed date is >= the stored
max (boundary INCLUSIVE so an in-place revision of the latest period is captured;
merge dedups the overlap). Non-time dimensions are selected exactly as the ingester
did (all values when the full cube fits MAX_CELLS, else the aggregate/first slice).
A table with NO on-disk history is fetched in FULL (all time codes) — a first landing.

HONEST STATUS (Tally + finalize):
  Per table we record:
    added_unit(n)    rows successfully parsed (n>0 new, n==0 net-empty-but-flowed)
    empty_unit()     a legitimately quiet tail (no time codes newer than the boundary)
                     or a table the catalog lists but PxWeb now 404/400s with no history
    transient_unit() timeout / 5xx / 429 / network drop / non-JSON 200 -> KEEP GOING
    structural_unit() a 200 with a real metadata envelope (variables present) but the
                     time dimension is gone, OR a FULL (no-date-filter) fetch of a
                     table that HAS on-disk history parsed 0 rows from a real body
  finalize() then returns 'ok'/'no_change' only when nothing transient/structural-
  failed; 'partial' on any transient (orchestrator does NOT stamp success -> re-run);
  DefinitiveError on a structural break or a large all-empty window. Existing data is
  ALWAYS preserved — every write goes through merge.merge_and_write (never-shrink).

ONE entry point: update(unit, since) -> Result. detect_change is the strategy's job.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import json
import os
import time
from collections import defaultdict

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after,
                      sane_since, save_rotation)

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

SOURCE = "hagstofa"
DEDUP = ("series_key", "obs_date")
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.25            # polite gap between HTTP calls
MAX_CELLS = 100_000    # PxWeb per-request cell cap (same as the ingester)
TIMEOUT = 90
MAX_ATTEMPTS = 4
TRAIL_YEARS = 5        # trailing-window fallback when a boundary is corrupt/far-future


# --------------------------------------------------------------------------- #
# Reuse the ingester's enumeration + parse logic VERBATIM (no re-discovery).
# Loaded by path so we don't depend on jobs/ being importable as a package.
# --------------------------------------------------------------------------- #
def _load_ingester():
    path = os.path.join(config.JOBS_DIR, "ingest_hagstofa.py")
    if not os.path.exists(path):
        raise DefinitiveError(f"hagstofa ingester missing: {path}")
    spec = importlib.util.spec_from_file_location("_ingest_hagstofa", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_ING = _load_ingester()
BASE = _ING.BASE                      # https://px.hagstofa.is/pxen/api/v1/en
parse_jsonstat2 = _ING.parse_jsonstat2
parse_date = _ING.parse_date
is_time_dim = _ING.is_time_dim


def _catalog_path() -> str:
    return os.path.join(config.source_dir(SOURCE), "_catalog.json")


def _load_catalog() -> list[dict]:
    """Reuse the ingester's crawled catalog (db / path / id / text). The cache read is
    blob-routed so CI (AQUEDUCT_BACKEND=r2) uses the R2 copy instead of re-crawling all
    1906 tables every run (ledger R36); if the cache is absent everywhere, fall back to a
    fresh crawl via the ingester (slow, but correct)."""
    raw = blob.read_bytes(_catalog_path())
    if raw is not None:
        try:
            cat = json.loads(raw.decode("utf-8"))
            if isinstance(cat, list) and cat:
                return cat
        except ValueError:
            pass
    # No cached catalog -> let the ingester crawl (it writes the cache too).
    cat = _ING.crawl_catalog()
    if not cat:
        raise DefinitiveError(f"hagstofa: catalog empty and crawl returned nothing")
    return cat


def _table_prefix(db: str, path: str) -> str:
    """ICE:<db>:<path-with-/-as-:> — the leading segment of every series_key for a table."""
    return f"ICE:{db}:{path.replace('/', ':')}"


def _per_table_max(path: str) -> dict[str, dt.date]:
    """For a db parquet, max(obs_date) per TABLE prefix.

    A series_key is 'ICE:<db>:<path>.px:<dim>=...'. The table prefix is the substring
    through the '.px' segment. We bucket every row's max obs_date under that prefix so a
    table's date-tail boundary is its OWN latest period, not the whole db's.
    """
    out: dict[str, dt.date] = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    keys = t.column("series_key").to_pylist()
    dates = t.column("obs_date").to_pylist()
    for k, o in zip(keys, dates):
        if o is None or not k:
            continue
        # table prefix = up to and including the '.px' segment
        parts = k.split(":")
        pref = None
        for i, p in enumerate(parts):
            if p.endswith(".px"):
                pref = ":".join(parts[: i + 1])
                break
        if pref is None:
            continue
        if isinstance(o, dt.datetime):
            o = o.date()
        prev = out.get(pref)
        if prev is None or o > prev:
            out[pref] = o
    return out


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(dict(UA, Connection="close"))
    return s


def _get_meta(sess, url):
    """GET PxWeb table metadata. 200 -> dict; 404/400 -> None (table retired/empty);
    timeout/5xx/429/network/non-JSON -> TransientError after the retry budget."""
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"hagstofa GET {url}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                last = "bad json"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"hagstofa GET {url}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 404):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"hagstofa GET {url}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"hagstofa GET {url}: HTTP {r.status_code}")
    raise TransientError(f"hagstofa GET {url}: {last}")


def _post_data(sess, url, body):
    """POST a PxWeb query. 200 -> dict; 400/403 -> None (rejected query / no cells);
    timeout/5xx/429/network/non-JSON -> TransientError after the retry budget."""
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.post(url, json=body, timeout=TIMEOUT + 30)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"hagstofa POST {url}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                last = "bad json"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"hagstofa POST {url}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 403):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"hagstofa POST {url}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"hagstofa POST {url}: HTTP {r.status_code}")
    raise TransientError(f"hagstofa POST {url}: {last}")


def _time_var(variables):
    """THE PxWeb time variable, resolved exactly as parse_jsonstat2 keys obs_date:
    the shared value-first resolver (core/pxweb.py) fed the same authoritative
    `time: true` code and parse_date grammar the parser is given — authoritative
    flag, else highest date-parse-rate, else literal name. The OLD fallback took
    the FIRST is_time_dim() match in variable order, which in a Mánuður+Ár
    (month+year) cube picked the month axis (index-like codes, no dates): its
    unparseable codes were all kept by _newer_time_codes, so the "tail"
    degenerated into a full-cube request (small cubes) or a first-year-only
    slice (over-budget cubes) — never the real year tail the parser keys —
    silently freezing the table. Resolving the parser's own axis kills that
    class. Returns None when the cube has no date axis at all."""
    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    # dim_labels (valueTexts) supplied so the resolver can judge a name-matched axis on
    # its LABELS when the codes are positional — hagstofa ships unflagged `Ár`/`Year`
    # axes coded '0','1','2'… with the period only in valueTexts, and refusing them on
    # codes alone booked 33 live tables (deaths to 2025, elections 2024) as structural
    # breaks on every run. Same-axis-only by construction; see resolve_time_dim.
    idx = _pxweb.resolve_time_dim(
        [v.get("code", "") for v in variables],
        [[str(c) for c in (v.get("values") or [])] for v in variables],
        meta_time_code=meta_time_code, parse_fn=parse_date,
        dim_labels=[[str(x) for x in (v.get("valueTexts") or [])] for v in variables])
    return variables[idx] if idx is not None else None


def _newer_time_codes(tvar, since_date: dt.date | None) -> list[str]:
    """Time codes whose parsed date >= since_date (boundary inclusive, for revisions).
    since_date None -> ALL codes (first landing / full fetch).

    The on-disk boundary is first passed through _common.sane_since: PxWeb time-dim
    heuristics can mis-store a corrupt far-future sentinel (year 9999/6000) as a table's
    max obs_date. Filtering `>= 9999` would select NOTHING and freeze the table forever,
    so when the boundary is corrupt we DROP the delta filter and request a TRAILING
    window (last TRAIL_YEARS) instead. merge dedups the overlap; the real fetched max
    then replaces the corrupt seed."""
    vals = tvar.get("values", [])
    if since_date is None:
        return list(vals)
    safe_since = sane_since(since_date)
    if safe_since is None:
        # corrupt/implausible boundary -> trailing-window backfill (not a frozen delta)
        floor = dt.date(dt.date.today().year - TRAIL_YEARS, 1, 1)
    else:
        floor = safe_since
    out = []
    for code in vals:
        d = parse_date(str(code))
        if d is None or d >= floor:
            # keep unparseable codes too (don't silently drop a period we can't date)
            out.append(code)
    return out


def _build_query(variables, tvar, time_codes):
    """Non-time selection mirrors the ingester (all values if the restricted cube fits
    MAX_CELLS; over MAX_CELLS a variable the ingester's own is_time test keeps full —
    the flagged code when `time: true` is present, else is_time_dim, e.g. the demoted
    month axis of a month+year cube whose stored keys cover every month — stays FULL,
    and the rest take the aggregate/first slice). Time dim is restricted to the
    supplied (newer) codes."""
    # restricted cube size with the chosen time codes
    total_cells = max(len(time_codes), 1)
    for v in variables:
        if v.get("code") == tvar.get("code"):
            continue
        total_cells *= max(len(v.get("values", [])), 1)

    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    query = []
    for v in variables:
        code = v.get("code", "")
        vals = v.get("values", [])
        if code == tvar.get("code"):
            query.append({"code": code, "selection": {"filter": "item", "values": time_codes}})
            continue
        if not vals:
            continue
        if total_cells <= MAX_CELLS:
            query.append({"code": code, "selection": {"filter": "item", "values": vals}})
        elif ((code == meta_time_code) if meta_time_code is not None
              else is_time_dim(code, vals)):
            # Ingester keep-full PARITY (jobs/ingest_hagstofa.py query builder): over
            # MAX_CELLS the ingester keeps every variable its own is_time test flags at
            # the FULL value list, so the stored series_keys cover all its values;
            # collapsing it here would tail only a sliver of those stored series.
            query.append({"code": code, "selection": {"filter": "item", "values": vals}})
        else:
            agg = [x for x in vals if str(x).upper() in ("0", "000", "TOTAL", "T", "ALL", "HEILD")]
            selected = agg[:1] if agg else vals[:1]
            query.append({"code": code, "selection": {"filter": "item", "values": selected}})
    return query


def _fetch_table(sess, db, path, prefix, since_date):
    """Date-tail fetch one table. Returns (rows, outcome) where outcome is one of:
      'data'       -> rows is a list of (series_key, obs_date, value)
      'quiet'      -> nothing newer than the boundary (legitimately empty tail)
      'empty'      -> table 404/400 (retired); metadata had no usable variables; OR a
                      table the ingester never stored (since_date is None) that has no
                      parseable time dimension — i.e. legitimately NOT in the dataset
                      (the ingester's parse_jsonstat2 emits nothing for such tables, so
                      a missing on-disk prefix is expected, NOT a break).
      'structural' -> a table that HAS on-disk history (since_date is not None) whose
                      time dimension is now GONE, or whose 200 body no longer parses to
                      any rows — a real schema/structural regression of stored data.
    Raises TransientError on timeout/5xx/429/network (propagated to caller as partial).

    Structural classification is gated on since_date is not None: only the LOSS of a
    table we already store counts as a break. A full fetch (since_date None) of a
    never-stored, time-less table is just "not applicable" -> empty.
    """
    url = f"{BASE}/{db}/{path}/"
    meta = _get_meta(sess, url)
    time.sleep(RATE)
    if meta is None or not isinstance(meta, dict):
        # A table with ON-DISK history that now 404/400s lost its endpoint -> structural;
        # a never-stored table that 404/400s is simply absent -> empty.
        return [], ("structural" if since_date is not None else "empty")
    variables = meta.get("variables", [])
    if not variables:
        return [], ("structural" if since_date is not None else "empty")

    tvar = _time_var(variables)
    if tvar is None:
        # Time dimension gone. Structural ONLY if we already store this table; a
        # never-stored time-less table (e.g. a geography/topic lookup) is not part of
        # the dataset and the ingester correctly emitted nothing for it.
        if since_date is not None:
            # ARCHIVAL discriminator (2026-08-05). Seven stored tables (KOS03190/a,
            # CEN01560 + 4 more manntal/2011) are single-EVENT cross-tabs — probed
            # live: KOS03190 is 'Participation by sex, age and municipality 2018'
            # with Municipality/Age/Sex and NO time variable — whose stored history
            # predates an upstream restructure. They can never parse again, so
            # 'structural' re-fired on every sweep and hagstofa could never go green
            # (the ons_uk not-a-time-series class meeting the R244 always-red gate).
            # A stored max already >=2 years old is a frozen archive: kept, logged,
            # 'quiet'. A RECENT stored max still classifies structural — a live
            # table losing its time dimension is a real break.
            try:
                age_days = (dt.date.today()
                            - dt.date.fromisoformat(str(since_date)[:10])).days
            except ValueError:
                age_days = 0
            if age_days >= 730:
                print(f"[hagstofa] {path}: no time dimension upstream and stored "
                      f"data ends {since_date} — archival event table, kept frozen",
                      flush=True)
                return [], "quiet"
            return [], "structural"
        return [], "empty"

    time_codes = _newer_time_codes(tvar, since_date)
    if since_date is not None and not time_codes:
        return [], "quiet"          # current through the latest period; nothing to ask
    if not time_codes:
        return [], "empty"          # full fetch but the time dim has no values at all

    body = {"query": _build_query(variables, tvar, time_codes),
            "response": {"format": "json-stat2"}}
    resp = _post_data(sess, url, body)
    time.sleep(RATE)
    if resp is None:
        # PxWeb rejected the query (400/403). On an incremental tail this is benign
        # (treat as quiet); on a full fetch of a table with no history it's empty.
        return [], ("quiet" if since_date is not None else "empty")

    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    rows = parse_jsonstat2(resp, prefix, meta_time_code)
    if not rows:
        # 200 but parse yielded nothing.
        body_has_values = bool(resp.get("value")) if isinstance(resp, dict) else False
        if since_date is not None and body_has_values:
            # A table we ALREADY store returned a real value array we can no longer
            # parse to rows -> the structure parse_jsonstat2 expects is gone -> break.
            return [], "structural"
        # Incremental tail with no usable rows in the window (quiet), or a never-stored
        # table whose body we can't parse (not in the dataset) -> empty.
        return [], ("quiet" if since_date is not None else "empty")
    return rows, "data"


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:  # noqa: ARG001  (since handled per-table via on-disk max)
    from ..base import Result  # local import keeps the module importable standalone

    out_dir = config.source_dir(SOURCE)
    # No isdir guard: the table set comes from the (blob-routed) catalog and every store
    # touch is blob-routed, so the local dir legitimately does not exist on a CI runner
    # under AQUEDUCT_BACKEND=r2 (ledger R36).

    catalog = _load_catalog()
    # Optional bounded subset for the LIVE one-shot test only (production passes nothing).
    # The fetcher itself always iterates the FULL catalog; the limit is opt-in via cfg.
    limit = None
    try:
        limit = (unit.config or {}).get("_test_limit")
    except AttributeError:
        limit = None

    by_db: dict[str, list] = defaultdict(list)
    for t in catalog:
        by_db[t["db"]].append(t)

    # Per-db, per-table on-disk max obs_date (date-tail boundaries).
    db_table_max: dict[str, dict[str, dt.date]] = {}
    for db in by_db:
        db_table_max[db] = _per_table_max(os.path.join(out_dir, f"{db}.parquet"))

    sess = _session()
    tally = Tally()
    cursors: dict[str, str] = {}     # table prefix -> max obs_date (per-table freshness)
    maxd: dt.date | None = None
    total = 0

    # BOUND BELOW THE ORCHESTRATOR'S 45-MINUTE CAP, AND ROTATE.
    # hagstofa's measured cloud runs are 53.0 min median / 72.4 max — over the cap on every
    # run, and the cap landed 2026-08-01 (36130d02) after its last run. The merge is INSIDE
    # this loop, so a kill truncates rather than discards (unlike bcb/unhcr, which merged
    # after their loops) — but `sorted(by_db)` is a FIXED order, so the kill lands in the
    # same place every time and the tail dbs are never reached at all, however many runs
    # pass. A bound over a fixed order is a truncation, not a budget (R190).
    #
    # 30 minutes also gives the shared daily run its time back: all 106 live cloud sources
    # cost a median 1,211 min against a 240-min budget, and only 20 were attempted on
    # 2026-08-02 because a handful ran to the cap.
    budget_min = float(os.environ.get("HAGSTOFA_BUDGET_MIN", "30"))
    dl = Deadline(minutes=budget_min)
    dbs = rotate_after(sorted(by_db), load_rotation(out_dir))
    last_db = ""

    for db in dbs:
        if dl.spent():
            print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — stopped after db {last_db!r}, "
                  f"{len(dbs) - dbs.index(db)} of {len(dbs)} db(s) deferred to the next "
                  f"tick", flush=True)
            break
        last_db = db
        path = os.path.join(out_dir, f"{db}.parquet")
        before = blob.row_count(path)
        tmax = db_table_max.get(db, {})
        tables = by_db[db]

        # seed cursors with the on-disk frontier so an untouched table still reports
        # its real cursor (a frozen table can't hide behind the db-level max). SKIP a
        # corrupt far-future seed (year 9999/6000 PxWeb time-dim artifact): writing it
        # would mask RED-DATA staleness in health.py via max(cursors). The corrupt rows
        # stay on disk (never-shrink); the trailing-window re-fetch supplies the real max.
        for pref, mx in tmax.items():
            if sane_since(mx) is not None:
                cursors[pref] = mx.isoformat()

        # accumulate this db's new rows, merge ONCE.
        keys: list[str] = []
        dates: list[dt.date] = []
        vals: list[float] = []

        processed = 0
        for t in tables:
            if limit is not None and processed >= limit:
                break
            processed += 1
            tpath = t["path"]
            prefix = _table_prefix(db, tpath)
            since_date = tmax.get(prefix)  # None -> first landing (full fetch)

            try:
                rows, outcome = _fetch_table(sess, db, tpath, prefix, since_date)
            except TransientError:
                tally.transient_unit(tpath)  # -> partial; existing rows for this table kept
                continue

            if outcome == "structural":
                tally.structural_unit(tpath)  # finalize() raises DefinitiveError
                continue
            if outcome in ("empty", "quiet"):
                tally.empty_unit(tpath)
                continue

            # outcome == 'data'. Seed tbl_max from the SANE boundary only: if the on-disk
            # since_date is a corrupt far-future sentinel, start from None so the real
            # fetched max (from this run's trailing-window rows) becomes the cursor instead
            # of carrying the corrupt date forward (which would re-mask staleness).
            tbl_max = sane_since(since_date)
            n_acc = 0
            for k, d, v in rows:
                keys.append(k); dates.append(d); vals.append(v)
                n_acc += 1
                if tbl_max is None or d > tbl_max:
                    tbl_max = d
            # never write a corrupt far-future cursor (a row whose obs_date is itself a
            # sentinel year): cap so max(cursors) reflects an honest frontier for health.py.
            if tbl_max is not None and sane_since(tbl_max) is not None:
                cursors[prefix] = tbl_max.isoformat()
            # a 200 that flowed real rows is a SUCCESSFUL sub-unit even if every row is
            # at/below the boundary and nets zero new after merge — count added so it
            # doesn't feed the all-empty structural floor.
            tally.added_unit(n_acc)

        # Merge this db's accumulated new rows (one atomic publish per file).
        if vals:
            new_tbl = pa.table({
                "series_key": pa.array(keys, pa.string()),
                "obs_date":   pa.array(dates, pa.date32()),
                "value":      pa.array(vals, pa.float64()),
            })
            n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
            total += n
            if md:
                md_d = dt.date.fromisoformat(md)
                if maxd is None or md_d > maxd:
                    maxd = md_d
        else:
            total += before

    # Bookmark after a complete pass too, so the wrap goes through this same path and no
    # branch can quietly stop the rotation.
    if last_db:
        save_rotation(out_dir, last_db)

    last_obs = maxd.isoformat() if maxd else None
    # empty_window_floor = <#subunits> - 1 (per the S3 contract). The blunt all-empty
    # floor only fires when added==0 AND every attempted sub-unit was empty AND
    # attempted > floor — i.e. a true WHOLESALE outage where not one of the ~1900
    # tables returned any rows. A healthy run does NOT trip it: because the date-tail
    # re-fetches each table's boundary period INCLUSIVELY, every active table flows
    # real rows and is recorded added_unit() (added>0), so the floor stays dormant
    # while the precise per-table structural_unit() signal remains the real break
    # detector. (attempted = tables we actually reached this run; subset in test mode.)
    n_sub = tally.attempted if tally.attempted else len(catalog)
    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=max(n_sub - 1, 1))
