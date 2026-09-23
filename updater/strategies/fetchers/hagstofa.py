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
# {"db/path": {"verdict": "withdrawn" | "moved", "to": [...], "date": "YYYY-MM-DD"}}, blob-routed:
# the whole-tree search for a stored table that answers 400/404, cached and re-verified monthly
LABELS_FILE = "_labels.json"          # {table prefix: {dimension: {code: label}}} (R1124)
WITHDRAWN_FILE = "_withdrawn.json"
WITHDRAWN_RECHECK_DAYS = 30


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
# The Icelandic site. Same paths; the English site has been renaming variable codes and dropping
# tables this one still serves (reviews R1108, R1112). NEVER fetched from (its value codes are
# positions in a different order, R1119/R1120): used only to NAME a table dropped from the English
# site, and as the second tree searched before calling a table gone.
BASE_IS = "https://px.hagstofa.is/pxis/api/v1/is"
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


def _key_scheme(key: str, prefix: str) -> tuple:
    """The dimension NAMES of a key after its table prefix: 'ICE:db:t.px:Land=1:Eining=0' ->
    ('Land', 'Eining'). Two schemes in one table = two id systems for the same data. Only the
    '='-bearing segments are dimensions: some VALUE codes contain ':' (THJ11002, UTA05000, THJ05551,
    MAN10001 - review R1112), and their fragments are not dimension names."""
    rest = key[len(prefix) + 1:]
    return tuple(seg.split("=", 1)[0] for seg in rest.split(":") if "=" in seg) if rest else ()


def _per_table_profile(path: str, boundary: dict | None = None, all_keys: dict | None = None
                       ) -> tuple[dict[str, dt.date], dict[str, dict]]:
    """For a db parquet, per TABLE prefix: max(obs_date), and the key schemes stored ({scheme: rows}).

    A series_key is 'ICE:<db>:<path>.px:<dim>=...'. The table prefix is the substring
    through the '.px' segment. We bucket every row's max obs_date under that prefix so a
    table's date-tail boundary is its OWN latest period, not the whole db's.

    `boundary`, when given, is filled from the same read with {prefix: (date, {series_key: value})} at
    each table's newest SANE date - what the shift check compares a fetch against. Not the raw max: a
    far-future placeholder (MAN02007's 2100-12-31) would leave nothing to compare (review R1124).
    """
    out: dict[str, dt.date] = {}
    sane_out: dict[str, dt.date] = {}
    schemes: dict[str, set] = {}
    if not blob.exists(path):
        return out, schemes
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out, schemes
    keys = t.column("series_key").to_pylist()
    dates = t.column("obs_date").to_pylist()
    prefs: list = []
    for k, o in zip(keys, dates):
        prefs.append(None)
        if not k:
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
        prefs[-1] = pref
        if all_keys is not None:
            all_keys.setdefault(pref, set()).add(k)     # every series ever stored (the R1140 re-code rule)
        sc = schemes.setdefault(pref, {})
        ks = _key_scheme(k, pref)
        sc[ks] = sc.get(ks, 0) + 1                  # rows per scheme: which one DOMINATES (R1139)
        if o is None:
            continue
        if isinstance(o, dt.datetime):
            o = o.date()
        prev = out.get(pref)
        if prev is None or o > prev:
            out[pref] = o
        if sane_since(o) is not None:
            prev = sane_out.get(pref)
            if prev is None or o > prev:
                sane_out[pref] = o
    if boundary is not None and "value" in t.column_names:
        for k, o, v, pref in zip(keys, dates, t.column("value").to_pylist(), prefs):
            if pref is None or o is None:
                continue
            if isinstance(o, dt.datetime):
                o = o.date()
            if o == sane_out.get(pref):
                boundary.setdefault(pref, (o, {}))[1][k] = v
    return out, schemes


def _key_codes(key: str, prefix: str) -> dict:
    """{dimension: code} of a series key after its table prefix ('...SKO02102.px:Skóli=61:Kyn=1')."""
    rest = key[len(prefix) + 1:]
    return dict(seg.split("=", 1) for seg in rest.split(":") if "=" in seg) if rest else {}


def _labels_moved(stored: dict, now: dict, only_codes: dict | None = None) -> str | None:
    """IDENTITY BY LABEL (review R1124 (a)). `stored` / `now`: {dimension: {code: label}}. Hagstofa's
    codes are positions in each release's label list (R1119), so the label is the identity: a label
    that now sits under a DIFFERENT code means the codes were renumbered, whatever the values say. A
    label renamed in place (same code, the old label gone from the list) is not a shift.

    `only_codes` ({dimension: set of codes}) limits the check to codes the table actually STORES
    (review R1130): 66 tables keep one code of a large dimension (UTA02801 stores 1 of 4,162 HS
    numbers), and a description moving between codes it never stored must not refuse it for ever."""
    for dim, old in stored.items():
        cur = now.get(dim)
        if not cur:
            continue
        keep = (only_codes or {}).get(dim)
        where = {}
        for code, label in cur.items():
            where.setdefault(label, []).append(code)
        moved = [(c, lab, where[lab]) for c, lab in old.items()
                 if (keep is None or c in keep)
                 and cur.get(c) != lab and lab in where and c not in where[lab]]
        if moved:
            c, lab, to = moved[0]
            return (f"{len(moved)} label(s) of {dim!r} moved to another code since the stored release "
                    f"(e.g. {lab!r}: code {c} -> {to[0]})")
    return None


def _neighbour_shift(fetched, boundary: dict, bdate, prefix: str, positional: set) -> str | None:
    """THE SHIFT SIGNATURE (reviews R1124, R1130). A renumbering moves each series one or a few
    positions along a positional dimension, so after it most CHANGED values at the newest stored period
    equal the stored value of the key 1-3 positions away - on ONE consistent offset. SKO02102 (school
    codes, inserted at 61): 1,143 of 1,143 changed values equal the stored value one code lower. A
    revision gives new numbers, which match a neighbour only by chance, and never most of them on one
    offset.

    The rows AT the insertion point carry the new member's values and match no neighbour. When every
    miss sits at ONE code, ALL changes at that code - its misses and any chance hits - leave the count
    (R1130: dropping only the misses refused a revision confined to one member whenever 3 of its other
    rows matched a neighbour by chance - modelled 12% of one-member revisions). What is left must hold
    at least 3 changes, and at least 80% of them must be neighbour matches: with fewer, a revision and a
    shift cannot be told apart. Digit codes only - a non-digit member ('Alls') cannot sit at +-delta."""
    if bdate is None or not boundary or not positional:
        return None
    new = {k: v for k, d, v in fetched if d == bdate}
    diff = [k for k in new.keys() & boundary.keys() if not _same(new[k], boundary[k])]
    if len(diff) < 3:
        return None
    best = None
    for dim in positional:
        for delta in (1, 2, 3, -1, -2, -3):
            rec = []                                         # (code, hit) per change on this axis
            for k in diff:
                segs = k[len(prefix) + 1:].split(":")
                at = next((i for i, s in enumerate(segs) if s.startswith(f"{dim}=")), None)
                if at is None:
                    continue
                c = segs[at][len(dim) + 1:]
                hit = False
                if c.isdigit() and int(c) - delta >= 0:
                    segs[at] = f"{dim}={int(c) - delta}"
                    src = f"{prefix}:" + ":".join(segs)
                    hit = src in boundary and _same(new[k], boundary[src])
                rec.append((c, hit))
            misses = {c for c, h in rec if not h}
            if len(misses) == 1:
                rec = [(c, h) for c, h in rec if c not in misses]
            hits = sum(1 for _c, h in rec if h)
            if hits >= 3 and 5 * hits >= 4 * len(rec) and (best is None or hits > best[0]):
                best = (hits, len(rec), dim, delta)
    if best is None:
        return None
    hits, n, dim, delta = best
    return (f"{hits} of {n} changed values at {bdate} equal the stored value {abs(delta)} code(s) "
            f"{'lower' if delta > 0 else 'higher'} along {dim!r} - the codes were renumbered")


def _same(a, b) -> bool:
    if a is None or b is None:
        return a is b
    if a != a or b != b:                                     # NaN
        return a != a and b != b
    return abs(a - b) <= 1e-9 * max(1.0, abs(a), abs(b))


def _per_table_max(path: str) -> dict[str, dt.date]:
    """max(obs_date) per TABLE prefix (see _per_table_profile)."""
    return _per_table_profile(path)[0]


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


def _listing(sess, url):
    """One PxWeb folder listing, or None when it could not be READ (network, non-200, not a
    non-empty JSON list). 429 is waited out a few times, as _get_meta does."""
    for a in range(4):
        try:
            r = sess.get(url, timeout=TIMEOUT)
        except requests.RequestException:
            return None
        if r.status_code == 429 and a < 3:
            time.sleep(30)
            continue
        if r.status_code != 200:
            return None
        try:
            items = r.json()
        except ValueError:
            return None
        # An EMPTY folder is a folder with nothing in it, not an unreadable one: returning None for []
        # voided a whole tree (review R1124). Only a non-list body is unreadable.
        return items if isinstance(items, list) else None
    return None


def _table_tree(sess, base=None, dl=None):
    """{table id: [db/path, ...]} over EVERY database of one language site (`base`: BASE, the English
    site, by default; BASE_IS the Icelandic), or None when any listing could not be read.

    A table missing from its own folder may have MOVED, not been withdrawn: review R1108 found 3 of
    the 4 stored tables answering 400 (FYR02103, FYR02104, FYR03002) live at
    fyrirtaeki/skradfyrirtaeki/9_eldraefni/. The catalogue cache is never re-crawled, so only a
    search of the whole tree can tell the two apart - and a PARTIAL search cannot, which is why one
    unreadable listing voids the answer instead of shrinking it. Built at most once per run (cached
    on the session), and only when a stored table answers 400/404. A search takes ~6 min, so it
    stops at the run's Deadline `dl` (review R1120 (d)) and then answers None: unknown."""
    base = base or BASE
    cache = getattr(sess, "_hagstofa_trees", None)
    if cache is None:
        cache = {}
        try:
            sess._hagstofa_trees = cache
        except AttributeError:
            pass
    if base in cache:
        return cache[base]
    tree: dict = {}
    root = _listing(sess, f"{base}/")
    ok = root is not None
    queue = [(item.get("dbid"), "") for item in (root or []) if isinstance(item, dict) and item.get("dbid")]
    ok = ok and bool(queue)
    while ok and queue:
        if dl is not None and dl.spent():
            ok = False              # out of budget: an unfinished search is evidence of nothing
            try:
                sess._hagstofa_tree_cut = True      # -> _missing_verdict books it DEFERRED
            except AttributeError:
                pass
            break
        db, folder = queue.pop()
        items = _listing(sess, f"{base}/{db}/{folder}/" if folder else f"{base}/{db}/")
        time.sleep(RATE)
        if items is None:
            ok = False
            break
        for it in items:
            if not isinstance(it, dict) or not it.get("id"):
                continue
            child = f"{folder}/{it['id']}".lstrip("/")
            if it.get("type") == "t":
                tree.setdefault(it["id"], []).append(f"{db}/{child}")
            elif it.get("type") == "l":
                queue.append((db, child))
    result = tree if ok else None
    cache[base] = result
    return result


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


def _fetch_table(sess, db, path, prefix, since_date, dl=None):
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
    if meta is None and since_date is not None:
        # THE ENGLISH SITE DROPS TABLES THE ICELANDIC SITE STILL SERVES AT THE SAME PATH (review
        # R1112: SJA04901 answers 400 on pxen but 200 on pxis). That is a NAMED BREAK, never a
        # fallback: Hagstofa's value codes are POSITIONS in each site's own alphabetical order of
        # labels, not identities (R1119 - pxen SJA04905 Country 3 is Australia, pxis Land 3 is
        # Azerbaijan), and they shift between releases on one site. Fetched from pxis, SJA04901's
        # 2024 values landed one species code higher - 134 of 214 stored (key, date) pairs would have
        # been overwritten with a neighbour's value (R1120). Matching dimension NAMES proves nothing.
        meta_is = _get_meta(sess, f"{BASE_IS}/{db}/{path}/")
        time.sleep(RATE)
        if isinstance(meta_is, dict) and meta_is.get("variables"):
            print(f"[hagstofa] {path}: DROPPED from the English site; the Icelandic site still serves "
                  f"it at the same path, but its value codes are positions in a different order, so "
                  f"it is not merged into the stored series - a re-key with a value-derived code map "
                  f"(R1119/R1120)", flush=True)
            return [], "structural"
        return _missing_verdict(sess, db, path, since_date, dl)
    if meta is None or not isinstance(meta, dict):
        # a never-stored table that 404/400s is simply absent -> empty; a non-dict body on a
        # stored table is a break.
        return [], ("structural" if since_date is not None else "empty")
    # A table the English site RENAMED (SJA0490x, UMH51101, SKO02108, VIN00002/3) is fetched here as
    # it is and refused by update()'s key-scheme guard as RESTRUCTURED. It is not fetched from the
    # Icelandic site instead, even when that site still produces the stored dimension names: the value
    # codes behind the names differ by site (R1119).
    return _fetch_with_meta(sess, url, meta, path, prefix, since_date)


def _missing_verdict(sess, db, path, since_date, dl=None):
    """A STORED table that answers 400/404 on BOTH language sites: moved, withdrawn, or unknown -
    decided by a search of BOTH WHOLE TREES (reviews R1108, R1112).
      listed at its own path in either tree -> structural (mid-republication / broken endpoint);
      found elsewhere in either tree       -> MOVED: structural and named. The path is in every
                                              series key, so following it is a re-key;
      absent from both trees, read in full -> WITHDRAWN: stored history kept frozen ('quiet');
      either tree not fully read           -> cannot tell -> structural.
    A verdict is cached for WITHDRAWN_RECHECK_DAYS: one tree search costs ~6 min (395 listings,
    measured 2026-09-23) and these tables answer 400 on EVERY run."""
    verdicts = getattr(sess, "_hagstofa_withdrawn", None)
    me = f"{db}/{path}"
    seen = (verdicts or {}).get(me)
    try:
        fresh = (isinstance(seen, dict) and seen.get("verdict") in ("withdrawn", "moved") and
                 (dt.date.today() - dt.date.fromisoformat(seen["date"])).days < WITHDRAWN_RECHECK_DAYS)
    except (KeyError, TypeError, ValueError):
        fresh = False
    if fresh:
        if seen["verdict"] == "moved":
            print(f"[hagstofa] {path}: MOVED to {', '.join(seen.get('to') or [])} (verified "
                  f"{seen['date']}) - a re-key, not followed automatically", flush=True)
            return [], "structural"
        return [], "quiet"
    trees = [_table_tree(sess, BASE, dl), _table_tree(sess, BASE_IS, dl)]
    leaf = path.rpartition("/")[2]
    if any(t is None for t in trees):
        if getattr(sess, "_hagstofa_tree_cut", False):
            # the run's budget ran out inside the search: nothing is known, nothing failed (R1124)
            print(f"[hagstofa] {path}: HTTP 400/404; the table-tree search stopped at the budget - "
                  f"moved or withdrawn is decided next run", flush=True)
            return [], "deferred"
        print(f"[hagstofa] {path}: HTTP 400/404, and a table tree could not be read in full - "
              f"moved or withdrawn is unknown", flush=True)
        return [], "structural"
    listed = [p for t in trees for p in t.get(leaf, [])]
    if me in listed:
        # still listed where it was, yet its metadata answers 400/404: mid-republication or a
        # broken endpoint - never evidence of withdrawal
        print(f"[hagstofa] {path}: HTTP 400/404 although a folder still lists it", flush=True)
        return [], "structural"
    elsewhere = sorted(set(listed))
    today = dt.date.today().isoformat()
    if elsewhere:
        print(f"[hagstofa] {path}: MOVED by the publisher to {', '.join(elsewhere)} - "
              f"following it re-keys its series (the path is in the key); not followed "
              f"automatically", flush=True)
        if verdicts is not None:
            verdicts[me] = {"verdict": "moved", "to": elsewhere, "date": today}
        return [], "structural"
    print(f"[hagstofa] {path}: withdrawn by the publisher (HTTP 400/404 on both language sites and "
          f"absent from both table trees); stored data to {since_date} kept frozen", flush=True)
    if verdicts is not None:
        verdicts[me] = {"verdict": "withdrawn", "date": today}
    return [], "quiet"


def _fetch_with_meta(sess, url, meta, path, prefix, since_date):
    """_fetch_table's work once the table's metadata is in hand (from either language site)."""
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

    # THE RELEASE'S LABELS, for update()'s identity check (review R1124): {dimension: {code: label}}.
    labels = getattr(sess, "_hagstofa_labels", None)
    if labels is not None:
        labels[prefix] = {v.get("code", ""): dict(zip((str(c) for c in v.get("values") or []),
                                                     (str(x) for x in v.get("valueTexts") or [])))
                          for v in variables if v.get("code") != tvar.get("code")}

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
    db_schemes: dict[str, dict[str, dict]] = {}
    db_boundary: dict[str, dict[str, dict]] = {}
    db_keys: dict[str, dict[str, set]] = {}
    for db in by_db:
        db_boundary[db] = {}
        db_keys[db] = {}
        db_table_max[db], db_schemes[db] = _per_table_profile(os.path.join(out_dir, f"{db}.parquet"),
                                                              boundary=db_boundary[db], all_keys=db_keys[db])

    sess = _session()
    wpath = os.path.join(out_dir, WITHDRAWN_FILE)
    try:
        raw = blob.read_bytes(wpath)
        withdrawn = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception:                                        # noqa: BLE001 - unreadable: re-verify
        withdrawn = {}
    sess._hagstofa_withdrawn = withdrawn if isinstance(withdrawn, dict) else {}
    withdrawn_before = dict(sess._hagstofa_withdrawn)
    # {table prefix: {dimension: {code: label}}} as of each table's last clean merge (review R1124).
    lpath = os.path.join(out_dir, LABELS_FILE)
    try:
        raw_l = blob.read_bytes(lpath)
        label_maps = json.loads(raw_l.decode("utf-8")) if raw_l else {}
    except Exception:                                        # noqa: BLE001 - unreadable: re-seed
        label_maps = {}
    label_maps = label_maps if isinstance(label_maps, dict) else {}
    labels_before = json.dumps(label_maps, sort_keys=True)
    sess._hagstofa_labels = {}
    unchecked = []                   # tables that merged with no stored value to compare against
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
        pending_labels: dict = {}       # prefix -> this release's labels, kept once the db merges
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
                rows, outcome = _fetch_table(sess, db, tpath, prefix, since_date, dl=dl)
            except TransientError:
                tally.transient_unit(tpath)  # -> partial; existing rows for this table kept
                continue

            labels_now = sess._hagstofa_labels.pop(prefix, None)
            if outcome == "deferred":
                tally.deferred_unit(f"{tpath} (budget: table-tree search)")
                continue
            if outcome == "structural":
                tally.structural_unit(tpath)  # finalize() raises DefinitiveError
                continue
            if outcome in ("empty", "quiet"):
                tally.empty_unit(tpath)
                continue

            # THE KEY SCHEME IS PART OF THE CONTRACT WITH THE STORE. Hagstofa's 2026-09-15
            # republications renamed variable codes (SJA04903: Tegund/Land/Afurdaflokkur/Eining ->
            # Species/Country/Product category/Unit). A renamed key never collides with its stored
            # twin, so a merge publishes BOTH schemes in one table: the old series freeze, the new
            # ones appear beside them, nothing 404s and never-shrink cannot see it, because the
            # table grows (R519; measured on R2 2026-09-23 for SJA04903/04/05 and UMH51101). A
            # scheme the table has never stored is therefore refused, named, and left to a
            # deliberate re-key. (Tables that already hold two schemes are cleaned by that re-key,
            # not here.)
            #
            # AGAINST THE DOMINANT SCHEME, NOT ANY SCHEME STORED (R1139). SJA04903 - the example named
            # above - holds 4,226 series under Tegund/Land/Afurdaflokkur/Eining AND ONE stray series
            # under Species/Country/Product category/Unit. "Never stored" was tested against the SET
            # of schemes, so that one stray row let 4,241 English-scheme series merge beside the
            # Icelandic ones in every dry run (4, 5, 6), and the table is catalogued as ONE id: its
            # CSV would have mixed both. The incoming scheme must be the one that holds the most
            # stored rows; a tie is ambiguous and refused too.
            stored_schemes = db_schemes.get(db, {}).get(prefix)
            if stored_schemes:
                incoming = {_key_scheme(k, prefix) for k, _d, _v in rows}
                top = max(stored_schemes.values())
                dominant = {s for s, n in stored_schemes.items() if n == top}
                new = incoming - dominant if len(dominant) == 1 else incoming
                if new:
                    minor = {s: n for s, n in stored_schemes.items() if s in new}
                    why = (f"{tpath}: RESTRUCTURED by the publisher - key scheme(s) "
                           f"{sorted(new)[:2]} are not the table's stored scheme (stored rows by "
                           f"scheme: {sorted(stored_schemes.items(), key=lambda x: -x[1])[:2]}"
                           + (f", of which the incoming hold {sum(minor.values()):,} stray row(s)"
                              if minor else "")
                           + "); merging would publish two id schemes in one table. Not merged: "
                           "re-key it deliberately")
                    print(f"[{SOURCE}] {why}", flush=True)      # the result error is clipped (R1124)
                    tally.structural_unit(why)
                    continue
            # THE SAME NAMES CAN HIDE RENUMBERED CODES (R1119, R1120, R1124). With a label map from the
            # last clean merge the label decides, exactly; without one, the neighbour-shift signature.
            bdate, bvals = db_boundary.get(db, {}).get(prefix, (None, {}))
            fetched_schemes = {_key_scheme(k, prefix) for k, _d, _v in rows}
            bvals = {k: v for k, v in bvals.items() if _key_scheme(k, prefix) in fetched_schemes}
            # A RE-CODE BRINGS SERIES THE TABLE NEVER HELD (R1130, R1140). VIN00001's codes changed from
            # positions ('Kyn/aldur=0') to label text ('Kyn/aldur=Alls') under the SAME dimension names:
            # the scheme guard passed and 60 new series merged beside 60 frozen ones. The first rule -
            # "no stored boundary key came back" - was beaten by ONE survivor: a text total ('Alls')
            # keeps its key through such a re-code, so 20 re-coded series merged beside 20 frozen ones
            # (R1140 P1; 85 tables have a boundary key that would survive). The rule is a PROPORTION:
            # at the boundary date, series never stored ANYWHERE in the table must not outnumber the
            # stored boundary series that came back. A new member (one country added) is far below
            # that; series re-appearing from earlier dates (SKO00000's 4) are stored, so they count
            # for nothing; a subset of the stored series coming back is not a re-code at all.
            fetched_b = {k for k, d, _v in rows if d == bdate} if bdate is not None else set()
            ever = db_keys.get(db, {}).get(prefix, set())
            never = sorted(k for k in fetched_b if k not in ever)
            came_back = [k for k in fetched_b if k in bvals]
            if bvals and never and len(never) > len(came_back):
                shifted = (f"RE-CODED - {len(never)} of the {len(fetched_b)} series fetched at {bdate} were "
                           f"never stored in this table, more than the {len(came_back)} stored series that "
                           f"came back (e.g. stored {next(iter(bvals))[len(prefix) + 1:][:60]!r}, fetched "
                           f"{never[0][len(prefix) + 1:][:60]!r}); the value codes changed")
            else:
                stored_codes: dict = {}
                for k in bvals:
                    for dim, code in _key_codes(k, prefix).items():
                        stored_codes.setdefault(dim, set()).add(code)
                # LABEL AND VALUE, not label OR value (R1130): a shift released together with a relabel
                # of every member passes the label check alone.
                shifted = None
                if label_maps.get(prefix) and labels_now:
                    shifted = _labels_moved(label_maps[prefix], labels_now, only_codes=stored_codes)
                if not shifted:
                    seen_codes: dict = {}
                    for k, _d, _v in rows:
                        for dim, code in _key_codes(k, prefix).items():
                            seen_codes.setdefault(dim, set()).add(code)
                    codes_of = {d: set(m) for d, m in (labels_now or {}).items() if m} or seen_codes
                    # positional = MOSTLY digit codes (R1130: 223 dimensions carry one non-digit member
                    # such as a total, and all-digits skipped them); the check uses only digit codes.
                    positional = {d for d, cs in codes_of.items()
                                  if 2 * sum(1 for c in cs if c.isdigit()) > len(cs)}
                    shifted = _neighbour_shift(rows, bvals, bdate, prefix, positional)
            if shifted:
                # The consequence differs (R1140): a SHIFT overwrites stored series with neighbours'
                # values; a RE-CODE adds a second code system beside the stored one.
                harm = ("it would add a second code system beside the stored series"
                        if shifted.startswith("RE-CODED") else
                        "'new wins' would overwrite stored series with other series' values")
                why = (f"{tpath}: CODES SHIFTED - {shifted}. Not merged: {harm}; it stays refused until "
                       f"the table is re-keyed deliberately")
                print(f"[{SOURCE}] {why}", flush=True)
                tally.structural_unit(why)
                continue
            checked = bdate is not None and any(d == bdate and k in bvals for k, d, _v in rows)
            if bdate is not None and not checked:
                unchecked.append(tpath)
            if labels_now and (checked or not bvals):
                # seeded only from a CHECKED merge (or a first landing, where nothing is stored to
                # shift): an unchecked merge could bake a shifted release into the map (R1130)
                pending_labels[prefix] = labels_now

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
            label_maps.update(pending_labels)      # the labels these merged rows were keyed under
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
    if sess._hagstofa_withdrawn != withdrawn_before:
        try:
            blob.write_bytes_atomic(wpath, json.dumps(sess._hagstofa_withdrawn, indent=1,
                                                      sort_keys=True).encode("utf-8"))
        except Exception as e:                               # noqa: BLE001
            # losing it costs one more tree search next run, never a wrong verdict - but say so
            print(f"[{SOURCE}] could not save {wpath} ({type(e).__name__}: {e})", flush=True)

    if unchecked:
        print(f"[{SOURCE}] {len(unchecked)} table(s) merged with no stored value at their newest period "
              f"to compare, so no shift check could run: {', '.join(unchecked[:10])}"
              f"{' ...' if len(unchecked) > 10 else ''}", flush=True)
    if json.dumps(label_maps, sort_keys=True) != labels_before:
        try:
            blob.write_bytes_atomic(lpath, json.dumps(label_maps, sort_keys=True,
                                                      ensure_ascii=False).encode("utf-8"))
        except Exception as e:                               # noqa: BLE001
            # losing it costs a re-seed through the neighbour check next run - but say so
            print(f"[{SOURCE}] could not save {lpath} ({type(e).__name__}: {e})", flush=True)

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
