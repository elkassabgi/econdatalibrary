"""S1 fetcher — CSO Ireland (Central Statistics Office, PxStat).

License CC BY 4.0, no key (ws.cso.ie JSON-RPC + REST). The data lives in ~44+ per-
subject grouped parquets under clean_full/cso/<SbjCode>_<SbjValue>.parquet, each with
schema (series_key, obs_date, value) and series_key = "CSO:<MtrCode>:<dim>=<code>:...".
A table revision lands in its owning subject parquet, so we read-existing + append +
dedup on (series_key, obs_date) per subject (NEVER skip a whole subject).

PxStat's ReadDataset has NO startPeriod param, so we can't do an SDMX-style server-side
date delta — we re-pull WHOLE changed tables. Change detection is the PxStat collection's
per-table `updated` release datetime: `PxStat.Data.Cube_API.ReadCollection` returns a
JSON-stat collection of every dataset carrying {extension.matrix -> updated}. We gate on
that: re-pull + overwrite only matrices whose `updated` moved vs. the stored cursor.

  current_vintage(unit): cheap-ish probe — ReadCollection restricted to a recent window
      (trailing ~120d via the `datefrom` param) returns only RECENTLY-revised tables (a few
      MB, ~1-2k matrices vs. the full 22MB/12.7k). Hash the {matrix: updated} map of that
      window: it changes iff any table was revised recently. None if it can't be fetched.
  update(unit, since): full ReadCollection {matrix: updated}; diff against stored
      _collupd.json; re-fetch only NEW/CHANGED matrices (newest-revision-first, bounded by
      MAX_TABLES per run so a run stays finite and converges across ticks); route each into
      its owning subject parquet (from the cached _catalog.json) and merge.merge_and_write
      (mode='merge', dedup (series_key,obs_date)). Persist the release-date cursor only for
      matrices actually pulled, so a transient mid-run failure re-pulls next tick.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import json
import os
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, finalize, merge_cursor_map
from ._vintage import content_hash, UA

# MAX_TABLES bounds how MANY changed matrices a run pulls; this bounds how LONG. CSO's
# ReadDataset endpoint is rate-throttled by the ingester, so a batch of large tables can
# outlast the whole run on its own while every serial source behind it waits.
BUDGET_MIN = 20

SOURCE = "cso"
DEDUP = ("series_key", "obs_date")
JSONRPC_URL = "https://ws.cso.ie/public/api.jsonrpc"
COLLECTION_METHOD = "PxStat.Data.Cube_API.ReadCollection"
VINTAGE_WINDOW_DAYS = 120     # trailing window for the cheap probe
COLL_EPOCH = "2000-01-01"     # datefrom for the full collection (everything)
MAX_TABLES = int(os.environ.get("CSO_MAX_TABLES", "60"))  # cap re-pulls per run (bounded/convergent)


def _parse_period(s, _orig):
    """Date parser that ALSO handles PxStat's bare-numeric TLIST period codes (the live API
    returns monthly periods as 'YYYYMM' e.g. '197511', quarterly as 'YYYYQ' e.g. '20223',
    weekly as 'YYYYWW') which the ingester's parse_date — written for the 'YYYYMmm' form —
    drops to None, silently yielding 0 parsed rows for whole tables (e.g. CPM01). We try the
    bare-numeric forms first, then fall back to the ingester's original parser so every other
    format keeps its exact existing behaviour (the published day-of-month convention is day=1
    for sub-annual periods, matching parse_date's '2022M01'->2022-01-01)."""
    t = (s or "").strip()
    if t.isdigit():
        if len(t) == 6:                      # YYYYMM monthly  (e.g. 197511)
            yr, mo = int(t[:4]), int(t[4:6])
            if 1 <= mo <= 12:
                return dt.date(yr, mo, 1)
        if len(t) == 5:                      # YYYYQ quarterly (e.g. 20223 -> Q3)
            yr, q = int(t[:4]), int(t[4])
            if 1 <= q <= 4:
                return dt.date(yr, (q - 1) * 3 + 1, 1)
    return _orig(s)


# ---- reuse the production ingester's URL(s) + JSON-stat2 parse logic verbatim -------------
def _ingester():
    path = os.path.join(config.JOBS_DIR, "ingest_cso_ireland.py")
    spec = importlib.util.spec_from_file_location("_cso_ingest", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # parse_jsonstat2 calls the module-global parse_date by name; wrap it at runtime (we may
    # only edit our own fetcher, not the shared ingester file) so reused parsing handles the
    # live API's bare-numeric period codes instead of silently producing 0 rows.
    _orig = mod.parse_date
    mod.parse_date = lambda s, _o=_orig: _parse_period(s, _o)
    return mod


def _out_dir():
    d = config.source_dir(SOURCE)
    os.makedirs(d, exist_ok=True)
    return d


def _catalog_path():
    return os.path.join(_out_dir(), "_catalog.json")


def _cursor_path():
    return os.path.join(_out_dir(), "_collupd.json")


def fold_unlisted(search_vintages_map, listing, scope):
    """Which SCOPED matrices Search knows about that the collection listing does not.

    A real function rather than an expression inline in update(), because the test for it
    used to retype the expression in its own body and therefore agreed with itself by
    construction -- deleting the fold from production left the suite green (R511 rule 4).

    SCOPE MATTERS. Search carries 5,764 matrices we do not catalogue and ~180 of those are
    unlisted too. Folding them wholesale would seed the HEAD of the queue with unheld
    strangers (unheld sorts first) and pulling them would mint store objects for series
    nobody published.
    """
    return {m for m in search_vintages_map if m not in listing and m in scope}


UNLISTED_RESERVED_FRAC = 0.25


def take_batch(ordered, cap, unlisted=frozenset()):
    """The bounded slice a run actually pulls, with a RESERVED share for the unlisted set.

    WHY A RESERVATION AND NOT JUST A RANK (R511, second attempt). Giving the unlisted set its
    own priority class moved it from queue position 12,318 to 5,191 -- and `cap` is 60, so
    the number of them a run reached went from zero to zero. Class 0 is "not held", and the
    publisher lists 12,985 matrices of which 5,191 are not in our store, so class 0 alone is
    86 runs deep. Measured by the reviewer against live PxStat and the production sidecars:
    the healthy run and the fully-degraded run pulled an IDENTICAL first 60.

    Rank is a statement about ORDER. Reachability is a statement about the CUTOFF. A set that
    is 5,191 deep in a 60-wide window is not scheduled, whatever its class says, and the
    honest test is `index < cap`, never `is in the list`.

    So a fixed fraction of every batch is held for matrices that could not be selected AT ALL
    before R510. They are a finite backlog (495 today), not a standing claim: once they carry
    a cursor they stop differing from it and drop out of `changed` naturally, and the
    reservation costs nothing on a run where none are pending.
    """
    if not unlisted or cap <= 0:
        return ordered[:cap]
    quota = max(1, int(cap * UNLISTED_RESERVED_FRAC))
    picked, rest = [], []
    for m in ordered:
        if m in unlisted and len(picked) < quota:
            picked.append(m)
        else:
            rest.append(m)
    return picked + rest[:cap - len(picked)]


def order_changed(changed, cur_upd, held, unlisted=frozenset()):
    """Run order for a bounded cso batch. Module-level and pure so the scheduling rule can be
    asserted directly.

    THREE priority classes, best first:
      0. UNHELD          - catalogued with no rows at all; we serve nothing for these.
      1. UNLISTED        - absent from the publisher's collection listing, so until R510 they
                           could never enter `changed` AT ALL. Nothing else in the queue has
                           been starved by construction.
      2. everything else - ordinary revisions, newest first.

    WHY CLASS 1 HAD TO EXIST (R511). Making the unlisted set visible was not enough and I
    shipped that mistake once. They are HELD by definition, and their vintages are 2020-era —
    the oldest in the corpus — so under the previous two-class rule they sorted last in the
    last group. Simulated against the live listing and the production sidecars: all of them
    landed at queue positions 12,318-12,377 of 12,378, zero inside a 60-table batch, roughly
    six to twelve months from being pulled, while every run printed a line that read like
    success. A queue position is a MEASURABLE thing; "it is in the list now" is not.

    Unheld still outranks unlisted: no rows at all is worse than stale rows. The 27 matrices
    that are catalogued with zero rows AND unlisted are in both classes and sort first, which
    is right — they are the ones a reader can reach and get nothing from.

    Stable passes, least-significant key first: a single tuple key cannot express it because
    the keys need opposite directions (class ascending, revision descending).
    """
    out = list(changed)
    out.sort(key=lambda m: (cur_upd.get(m) or ""), reverse=True)
    out.sort(key=lambda m: 0 if m not in held else (1 if m in unlisted else 2))
    return out


SEARCH_METHOD = "PxStat.System.Navigation.Navigation_API.Search"


def search_vintages(timeout: int = 300, tries: int = 3):
    """{matrix: release datetime} for EVERY matrix the publisher indexes, or {} on failure.

    WHY THIS AND NOT ReadCollection. `ReadCollection` is a catalogue of the CURRENT collection
    and omits 495 of our 7,896 catalogued matrices, which is R510: `changed` is built from it,
    so those 495 could never be selected for a re-pull and were frozen in silence. Measured
    2026-08-30, this endpoint — which `jobs/ingest_cso_ireland.py:102` ALREADY calls to build
    the catalogue — returns 13,660 matrices, every one carrying `RlsLiveDatetimeFrom`:

        catalogued matrices          7,896
        unlisted by ReadCollection     495   -> present in Search: 495 / 495
        our catalogue NOT in Search      0

    One call per run, total coverage, and it agrees with ReadCollection's `updated` on 12,981
    of 12,985 (the 4 differences are formatting). My first repair instead probed a rotating
    60-matrix slice with per-matrix ReadMetadata calls: 12.8% coverage per run, an unbounded
    network phase before the run's Deadline was even constructed, and it structurally could
    not reach the 27 matrices that are catalogued with ZERO rows (absent from `_held.json`,
    so absent from a `held - listing` population). Search reaches all of them. R511 — grep for
    what the neighbouring job already calls before adding a probe.

    Failure returns {} and the caller keeps ReadCollection alone: degraded to the old
    behaviour, never worse, and the caller says so out loud.
    """
    body = {"jsonrpc": "2.0", "id": 1, "method": SEARCH_METHOD,
            "params": {"LngIsoCode": "en"}}
    hdr = {"User-Agent": UA["User-Agent"], "Content-Type": "application/json"}
    for attempt in range(tries):
        if attempt:
            time.sleep(min(2 ** attempt, 15))
        try:
            r = requests.post(JSONRPC_URL, json=body, headers=hdr, timeout=timeout)
            if r.status_code != 200:
                continue
            d = r.json()
        except (requests.RequestException, ValueError):
            continue
        if d.get("error"):
            return {}
        out = {}
        for row in (d.get("result") or []):
            mtr, when = row.get("MtrCode"), row.get("RlsLiveDatetimeFrom")
            if mtr and when:
                out[mtr] = when
        return out
    return {}


def _held_path():
    """Which matrices the STORE holds — distinct from _collupd.json, which records the
    publisher REVISION we last saw. Held answers "can we serve this at all", the cursor
    answers "is what we hold current"; conflating them is what let a bounded run re-pull
    held matrices while catalogued-but-empty ones waited. Seeded by
    tools/seed_cso_held.py, extended by every successful pull."""
    return os.path.join(_out_dir(), "_held.json")


def _subject_key(t) -> str:
    """Reproduce the ingester's per-subject file stem EXACTLY (so revisions land in the
    same parquet the ingest wrote)."""
    s = f"{t['SbjCode']}_{str(t.get('SbjValue', ''))[:30].replace(' ', '_').replace('/', '_')}"
    return s[:50]


def _matrix_subject_map(force: bool = False) -> dict[str, str]:
    # R36: read the sidecar through blob. os.path.exists/open address the LOCAL disk, and under
    # AQUEDUCT_BACKEND=r2 that directory holds only what this run wrote — so on a runner this
    # returned {} and every changed matrix lost its owning subject parquet.
    #
    # Measured 2026-08-03: _catalog.json is 3,140,483 B on the workstation and ABSENT from
    # r2://econ-data/clean_full/cso/ entirely, because only the INGESTER ever wrote it and it
    # wrote it locally. Routing the read alone would not have helped — the object simply is not
    # there — so if the store has no copy we BUILD one from CSO's own Search API and cache it to
    # the store, and the next run finds it. One request, and the source stops depending on a
    # file that happens to exist on one machine.
    # force=True is the refresh-on-miss path: a matrix absent from the cache IS the evidence
    # that the cache is stale, and without this the cache was only ever written when ABSENT, so
    # a present-but-stale one froze permanently and 222 matrices stayed unroutable.
    raw = None if force else blob.read_bytes(_catalog_path())
    cat = None
    if raw:
        try:
            cat = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            cat = None
    if not cat:
        try:
            # refresh=force so the ingester bypasses its own on-disk short-circuit; without it
            # the rebuild re-reads the same stale bytes and re-uploads them unchanged.
            cat = _ingester().build_catalog(refresh=force)
        except TypeError:
            cat = _ingester().build_catalog()                # older signature: still works
        except Exception:                                    # noqa: BLE001
            return {}                                        # never sink a run over a cache
        if not cat:
            return {}
        try:
            blob.write_bytes_atomic(_catalog_path(),
                                    json.dumps(cat).encode("utf-8"))
        except Exception:                                    # noqa: BLE001
            pass                                             # cache is an optimisation
    return {t["MtrCode"]: _subject_key(t) for t in cat if t.get("MtrCode")}


def _subject_from_metadata(mtr: str, timeout: int = 60) -> "str | None":
    """Per-matrix routing fallback (2026-08-05). SIH13/SIA208 (and up to 222 more) EXIST
    upstream with full metadata — probed live, label 'At Risk of Poverty Rate Threshold' —
    yet are absent from CSO's Search API listing, so the Search-built map can never route
    them and they retried forever, consuming ~45% of every run's budget (the R61 class:
    absence from a LISTING is not absence from the API). ReadMetadata's extension.subject
    carries {code, value} — exactly the fields _subject_key needs. One bounded RPC per
    still-unroutable matrix per run; a pulled matrix leaves `changed`, so it is not repaid."""
    body = {"jsonrpc": "2.0", "id": 1, "method": "PxStat.Data.Cube_API.ReadMetadata",
            "params": {"matrix": mtr, "language": "en",
                       "format": {"type": "JSON-stat", "version": "2.0"}}}
    hdr = {"User-Agent": UA["User-Agent"], "Content-Type": "application/json"}
    try:
        r = requests.post(JSONRPC_URL, json=body, headers=hdr, timeout=timeout)
        if r.status_code != 200:
            return None
        sub = ((r.json().get("result") or {}).get("extension") or {}).get("subject") or {}
        if sub.get("code") is None:
            return None
        return _subject_key({"SbjCode": sub["code"], "SbjValue": sub.get("value", "")})
    except (requests.RequestException, ValueError):
        return None


def _collection_updates(datefrom: str, timeout: int, tries: int = 3):
    """{MtrCode: updated_iso} from PxStat ReadCollection >= datefrom. Returns (map, error_kind).
    error_kind in {None, 'transient', 'structural'}; map is {} on error.

    The full collection is a ~22MB chunked response, so the body can end prematurely mid-stream
    (ChunkedEncodingError) — that, like a timeout/5xx, is TRANSIENT (retried, then surfaced as
    'partial'), never laundered into success. Only a clean 200 with a 0-matrix body is structural."""
    body = {"jsonrpc": "2.0", "id": 1, "method": COLLECTION_METHOD,
            "params": {"datefrom": datefrom, "language": "en"}}
    hdr = {"User-Agent": UA["User-Agent"], "Content-Type": "application/json"}
    d = None
    for attempt in range(tries):
        if attempt:
            time.sleep(min(2 ** attempt, 15))  # backoff between retries (don't hammer a flaky host)
        try:
            # Stream + accumulate so a mid-body truncation is caught cleanly and retried, rather
            # than letting requests buffer the whole 22MB and fail atomically with no diagnostics.
            r = requests.post(JSONRPC_URL, json=body, headers=hdr, timeout=timeout, stream=True)
            if r.status_code in (429, 500, 502, 503, 504):
                r.close()
                continue
            if r.status_code != 200:
                r.close()
                return {}, "structural"
            chunks = bytearray()
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk:
                    chunks.extend(chunk)
            r.close()
            d = json.loads(bytes(chunks))  # truncated body -> JSONDecodeError -> retry (transient)
            break
        except requests.RequestException:  # Timeout/Connection/ChunkedEncoding/etc. -> transient
            continue
        except ValueError:  # truncated/non-JSON body -> transient (retry), not a clean structural break
            continue
    if d is None:
        return {}, "transient"
    if d.get("error"):
        return {}, "structural"
    res = d.get("result")
    items = (res or {}).get("link", {}).get("item", []) if isinstance(res, dict) else []
    out = {}
    for it in items:
        mtr = (it.get("extension") or {}).get("matrix")
        if mtr:
            out[mtr] = it.get("updated")
    # A 200 JSON-stat collection that yields zero matrices is a structural break, not a quiet day.
    if not out:
        return {}, "structural"
    return out, None


def current_vintage(unit) -> str | None:
    """Cheap probe: hash the {matrix: updated} map of the trailing-window collection. Changes
    iff any table was revised in the window. None if the probe can't be fetched cheaply
    (the strategy then re-fetches anyway, which is safe — merge dedups + never shrinks)."""
    since = (dt.date.today() - dt.timedelta(days=VINTAGE_WINDOW_DAYS)).isoformat()
    upd, err = _collection_updates(since, timeout=120)
    if err or not upd:
        return None
    blob_bytes = json.dumps(upd, sort_keys=True).encode("utf-8")
    return f"coll120:{content_hash(blob_bytes)}:{len(upd)}"


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = _out_dir()
    tally = Tally()

    # 1) Full per-table release-date map (the vintage source).
    cur_upd, err = _collection_updates(COLL_EPOCH, timeout=300)
    if err == "transient":
        # Named like the per-matrix failures, and for a stronger reason: this one is the WHOLE
        # run. Without the collection map there is no diff, so nothing is fetched at all — a
        # very different fact from "23 of 26 tables were flaky", and previously the two were
        # reported identically as an anonymous transient.
        print(f"[cso] ReadCollection ({COLLECTION_METHOD}) failed transiently — no vintage map, "
              f"so no matrices could be diffed this run", flush=True)
        tally.transient_unit("ReadCollection: transient (no vintage map)")
        return finalize(tally, _total_rows(out_dir), None, source=SOURCE)
    if err == "structural" or not cur_upd:
        tally.structural_unit()  # 200 collection that parsed 0 matrices -> schema break
        return finalize(tally, _total_rows(out_dir), None, source=SOURCE)

    # 2) Diff against stored release-date cursor -> NEW or CHANGED matrices.
    cur_path = _cursor_path()
    stored = {}
    # R36: read through blob — see _write_cursor. A local read here means stored={} on every
    # runner, which makes every matrix look changed and prevents convergence entirely.
    _raw = blob.read_bytes(cur_path)
    if _raw:
        try:
            stored = json.loads(_raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            stored = {}

    held = set()
    _rawh = blob.read_bytes(_held_path())
    if _rawh:
        try:
            held = set(json.loads(_rawh.decode("utf-8")) or [])
        except (ValueError, UnicodeDecodeError):
            held = set()

    # R510/R511: FOLD IN THE MATRICES ReadCollection DOES NOT LIST.
    # `changed` is built from `cur_upd`, so anything missing from the listing can never be
    # selected for a re-pull — 495 of our 7,896 catalogued matrices were frozen that way, in
    # silence. Search indexes all of them (verified 495/495, and 0 of our catalogue absent).
    #
    # SCOPED, NOT WHOLESALE. Search carries 5,764 matrices we do not catalogue, and ~180 of
    # those are absent from the listing too. Folding them all would seed the HEAD of the queue
    # with unheld STRANGERS — unheld sorts first — starving the very set this rescues, and
    # pulling them would mint store objects for series nobody published (the R487 shape).
    #
    # Scope = what we HOLD, plus anything an operator names explicitly. `held` covers the 468
    # matrices with stale rows. The other 27 of R510's 495 are catalogued with ZERO rows, so
    # they are not in `_held.json` and the fetcher has no way to know they exist — that is
    # catalogue knowledge it does not carry. CSO_ONLY_MATRICES is the mechanism that already
    # exists for exactly this ("a known set of holes filled out-of-band in one pass"), so
    # naming them there now reaches them, instead of my inventing a second catalogue here.
    _only_raw = os.environ.get("CSO_ONLY_MATRICES", "").strip()
    want = {x.strip() for x in _only_raw.split(",") if x.strip()} if _only_raw else set()
    # FAIL CLOSED ON A DEGENERATE VALUE. `CSO_ONLY_MATRICES=","` parses to an empty set, and
    # treating that as "unset" turns an operator's malformed restriction into a full,
    # unrestricted 60-table batch -- the opposite of what they asked for, silently.
    _only_degenerate = bool(_only_raw) and not want
    if _only_degenerate:
        print("[cso] CSO_ONLY_MATRICES was set but names no matrix (%r) - refusing to run "
              "unrestricted; fix the value or unset it" % _only_raw, flush=True)
        tally.empty_unit()
        return finalize(tally, _total_rows(out_dir), None, source=SOURCE, series_cursors={})
    scope = held | want
    unlisted = set()
    sv = search_vintages()
    # COVERAGE FLOOR. The whole fix rests on Search indexing everything -- measured 13,660
    # matrices against a 12,985-entry listing. A short response silently shrinks the fold
    # (a 10% response drops it from 468 to 44) and prints the identical success line, which
    # is the "measured but not asserted" shape this repo keeps paying for. Compare against
    # the listing we already have in hand rather than a hardcoded number, so the floor moves
    # with the publisher.
    if sv and len(sv) < len(cur_upd):
        print(f"[cso] {SEARCH_METHOD} returned {len(sv):,} matrices, FEWER than the "
              f"{len(cur_upd):,} the collection listing already carries - treating as a "
              f"short response and NOT folding; the gap stays open this run", flush=True)
        sv = {}
    if sv:
        unlisted = fold_unlisted(sv, cur_upd, scope)
        if unlisted:
            cur_upd = {**cur_upd, **{m: sv[m] for m in unlisted}}
            print(f"[cso] {len(unlisted):,} catalogued matrices are absent from "
                  f"{COLLECTION_METHOD} and were invisible to the diff (R510); vintages taken "
                  f"from {SEARCH_METHOD} and given their own queue priority", flush=True)
    else:
        # Degraded, and said out loud: this is the pre-R510 behaviour, not a healthy run.
        print(f"[cso] {SEARCH_METHOD} unavailable — falling back to {COLLECTION_METHOD} alone, "
              f"which cannot see matrices it does not list (R510). Nothing is lost that was "
              f"not already lost before 2026-08-30, but this run cannot repair the gap.",
              flush=True)

    # NORMALISE THE VINTAGE STRINGS BEFORE COMPARING. ReadCollection says
    # '2020-11-10T11:00:00Z' and Search says '2020-11-10T11:00:00' -- the same instant,
    # differing only by a trailing 'Z', and 0 of 12,985 matrices agree byte-for-byte across
    # the two endpoints. Comparing raw would make every matrix that moves between the two
    # vocabularies look changed on every run, for ever. No churn today (all 732 stored
    # cursors end in 'Z' and none of the folded matrices has one yet), which is exactly when
    # to fix it.
    changed = [m for m, u in cur_upd.items()
               if (stored.get(m) or "").rstrip("Z") != (u or "").rstrip("Z")]
    # UNHELD MATRICES FIRST, then newest revisions.
    #
    # Newest-first alone is right only when the store is level with the cursor. It was not:
    # the cursor is written through the blob layer as of 2026-08-03 (see _write_cursor) and
    # therefore RESTARTED EMPTY, while the store already held 7,608 matrices. With 120 of
    # the publisher's 12,908 timestamps stored, nearly everything looks "changed", so a
    # bounded 60-matrix run spent itself re-pulling data we already had while 290 CATALOGUED
    # matrices with zero rows in the store waited — ~213 runs away at 60/run, and a run is
    # already at its time budget (2,017.8 s measured for 60), so MAX_TABLES cannot buy this.
    #
    # `_held.json` is the set of matrices the store can actually serve (seeded by
    # tools/seed_cso_held.py, extended below by each run's successful pulls). Sorting unheld
    # first fills real holes before re-pulls. It asserts nothing about freshness: a held
    # matrix is still re-pulled whenever its publisher revision differs from the cursor, it
    # merely yields priority to a matrix that has no rows at all.
    # (`held` is loaded ABOVE the diff now — the unlisted fold scopes on it.)
    # TARGETED BACKFILL. CSO_ONLY_MATRICES=<comma-list> restricts this run to named
    # matrices, so a known set of holes can be filled out-of-band in one pass instead of
    # waiting ~86 scheduled runs for the rotation to reach them. It only ever NARROWS the
    # set the normal diff already selected, so it cannot pull something the cursor says is
    # current, and it leaves every other mechanism (subject routing, merge, cursor and held
    # advancement on pulled_ok only) untouched. Unset in normal operation.
    if want:
        before = len(changed)
        changed = [m for m in changed if m in want]
        missing = want - set(changed)
        print(f"[cso] CSO_ONLY_MATRICES: {before:,} changed -> {len(changed):,} targeted"
              + (f"; {len(missing)} requested matrix/matrices are NOT in the publisher's "
                 f"changed set and will NOT be pulled: {sorted(missing)[:8]}" if missing else ""),
              flush=True)
    changed = order_changed(changed, cur_upd, held, unlisted)
    if held:
        _unheld = sum(1 for m in changed if m not in held)
        print(f"[cso] {len(changed):,} changed; {_unheld:,} are NOT in the store and go "
              f"first (store holds {len(held):,} matrices)", flush=True)

    if not changed:
        # nothing moved upstream — persist the (unchanged) cursor and report no_change honestly
        _write_cursor(cur_path, cur_upd)
        tally.empty_unit()
        return finalize(tally, _total_rows(out_dir), None, source=SOURCE,
                        series_cursors={})

    batch = take_batch(changed, MAX_TABLES, unlisted)
    m2s = _matrix_subject_map()
    refreshed_map = False   # the subject-catalog rebuild below is allowed ONCE per run
    ing = _ingester()

    # 3) Group changed matrices by owning subject parquet, fetch + parse each table.
    by_subject: dict[str, dict] = {}   # subject_key -> {"keys":[],"dates":[],"vals":[],"matrices":[]}
    pulled_ok = []                      # matrices we successfully pulled (advance cursor for these)
    series_cursors: dict[str, str] = {}
    cursors_capped = False

    dl = Deadline(minutes=BUDGET_MIN)
    for mtr in batch:
        if dl.spent():
            # MAX_TABLES bounds the COUNT per run; this bounds the TIME. A matrix not
            # reached is simply left out of pulled_ok, which drives the cursor write, so
            # it stays "changed" and is picked up first on the next tick.
            print(f"[cso] budget {BUDGET_MIN} min spent — {mtr} not pulled this run; "
                  f"retries next tick", flush=True)
            tally.deferred_unit(mtr)
            continue
        sbj = m2s.get(mtr)
        if not sbj and not refreshed_map:
            # REFRESH ON MISS, ONCE PER RUN, before giving up on the matrix.
            #
            # The cached _catalog.json goes stale the moment CSO publishes a matrix it did not
            # contain, and NOTHING ever rebuilt it: _matrix_subject_map only writes the cache
            # when it is ABSENT, so a present-but-stale cache froze permanently. Measured in CI
            # run 30796923747 — 27 of 36 failures were this, and 222 matrices are unroutable
            # today. They consume ~45% of every run's 60-table budget while never publishing,
            # which is what has kept cso from converging.
            #
            # A miss IS the evidence that the cache is stale, so it is the right moment to pay
            # for one rebuild. Bounded to a single attempt per run so a genuinely retired
            # matrix cannot trigger a multi-MB fetch per occurrence.
            refreshed_map = True
            print(f"[cso] {mtr}: not in the cached subject catalog — rebuilding it once from "
                  f"CSO's Search API", flush=True)
            fresh = _matrix_subject_map(force=True)
            if fresh:
                m2s = fresh
                sbj = m2s.get(mtr)
        if not sbj:
            # The matrix's OWN metadata is the authority the listing is not (R61).
            sbj = _subject_from_metadata(mtr)
            if sbj:
                print(f"[cso] {mtr}: routed via its own ReadMetadata subject "
                      f"(absent from the Search listing)", flush=True)
                m2s[mtr] = sbj
        if not sbj:
            # Still unroutable after the refresh AND the metadata probe: genuinely retired
            # upstream (metadata 404/empty). Transient rather than dropped, so it re-tries.
            #
            # NAMED IN THE LOG, not only in the tally. This was the one failure branch that
            # recorded itself in the error string and printed NOTHING — which is why 27 of 36
            # failures were invisible in CI while the other 9 were diagnosable at a glance.
            print(f"[cso] {mtr}: no subject mapping in catalog — cannot route to a subject "
                  f"parquet; retries next tick", flush=True)
            tally.transient_unit(f"{mtr}: no subject mapping in catalog")
            continue
        # EVERY FAILURE BELOW NAMES ITSELF. These four branches used to call transient_unit()
        # with no argument and no log line, so a run could report "23/26 sub-unit(s)
        # transient-failed" and there was NOTHING anywhere saying whether that was a timeout, a
        # 429, a 403, a parse error or an empty 200 — four different problems with four
        # different fixes. Measured 2026-08-03: CI failed 23 of 26 (and 60 of 60 the run
        # before) while the SAME matrices fetched fine from the workstation seconds later
        # (AKA03/AKM01/AKM02/AKM03, 4 of 4, ~1s each), which points at upstream throttling the
        # runner — but the fetcher's own output could not distinguish that from a schema break,
        # so the routing decision had no evidence to stand on. Tally.transient_unit already
        # takes an id (the branch above uses it); these simply never passed one.
        try:
            # _detailed, so an unreadable body is not reported as the publisher's bad hour.
            rows, outcome = ing.fetch_table_detailed(mtr)
        except (requests.Timeout, requests.ConnectionError) as e:
            print(f"[cso] {mtr}: network {type(e).__name__}: {str(e)[:120]}", flush=True)
            tally.transient_unit(f"{mtr}: {type(e).__name__}")
            continue
        except Exception as e:                                   # noqa: BLE001
            print(f"[cso] {mtr}: {type(e).__name__}: {str(e)[:160]}", flush=True)
            tally.transient_unit(f"{mtr}: {type(e).__name__}")
            continue
        if not rows:
            # The ingester's get_json() retries then returns None on a persistent network
            # failure (premature stream / timeout / 5xx), and fetch_table() also returns []
            # for a 200 that parsed 0 obs — the two are indistinguishable from the return
            # value. Classify as TRANSIENT (re-pulls next tick; cursor NOT advanced) rather
            # than structural, so a flaky-upstream empty can't false-raise DefinitiveError.
            # A real structural break recurs and is still re-pulled every tick (never frozen).
            #
            # Named anyway. The classification stays TRANSIENT — that reasoning is unchanged —
            # but "which matrices came back empty" is the evidence that tells a persistent
            # schema break apart from a flaky hour, and discarding the id threw exactly that
            # away. An unnamed failure is one nobody can act on.
            # NO LONGER ONE SENTENCE FOR TWO OPPOSITE CAUSES. Saying "network failure after
            # retries, or a 200 that parsed 0 obs" made a permanent parser gap read as the
            # publisher's bad hour, and nine matrices sat that way holding ~6M rows (R299).
            if outcome == "unparsed":
                print(f"[cso] {mtr}: HTTP 200 with a real body that parsed 0 observations — "
                      f"this is OURS, not the publisher's, and retrying will not fix it "
                      f"(unhandled period grammar or dimension shape)", flush=True)
            else:
                print(f"[cso] {mtr}: no body after retries (network/5xx) — publisher-side, "
                      f"retried next tick", flush=True)
            # Classification stays TRANSIENT for both: the cursor must not advance over a matrix
            # we failed to read, and a structural break recurs and is re-pulled every tick rather
            # than frozen. What changed is that the two are now distinguishable in the log, which
            # is the only thing that would have surfaced the parser gap.
            tally.transient_unit(f"{mtr}: {outcome}")
            continue
        buf = by_subject.setdefault(sbj, {"keys": [], "dates": [], "vals": []})
        for key, d, v in rows:
            buf["keys"].append(key)
            buf["dates"].append(d)
            buf["vals"].append(v)
            # BOUNDED (2026-07-30) — found by tools/audit_cursor_blowup.py.
            # 49,057,386 store rows folded one cursor per series with no cap.
            if d is not None and merge_cursor_map(series_cursors,
                                                  ((key, d.isoformat()),)):
                cursors_capped = True
        pulled_ok.append(mtr)

    # 4) Merge each affected subject parquet (atomic, dedup, never-shrink). One Tally unit
    #    per subject-publish so the honest status reflects what was actually written.
    last_obs = None
    for sbj, buf in by_subject.items():
        if not buf["vals"]:
            tally.empty_unit()
            continue
        tbl = pa.table({
            "series_key": pa.array(buf["keys"], pa.string()),
            "obs_date":   pa.array(buf["dates"], pa.date32()),
            "value":      pa.array(buf["vals"],  pa.float64()),
        })
        path = os.path.join(out_dir, f"{sbj}.parquet")
        before = blob.row_count(path)
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        tally.added_unit(max(0, n - before))
        if md and (last_obs is None or md > last_obs):
            last_obs = md

    # 5) Advance the release-date cursor ONLY for matrices we actually pulled (a transient
    #    mid-batch failure re-pulls next tick; never stamps unfetched tables as fresh).
    new_cursor = dict(stored)
    for mtr in pulled_ok:
        new_cursor[mtr] = cur_upd[mtr]
    _write_cursor(cur_path, new_cursor)

    # 5b) Record that the store now HOLDS these matrices, so the next run's ordering does not
    #     send it back to ground we have already covered. Same pulled_ok gate as the cursor:
    #     a matrix we failed to pull is not claimed as held. Union, never replace — this file
    #     is seeded from the whole store (tools/seed_cso_held.py) and a run only ever adds.
    if pulled_ok:
        if held:
            held.update(pulled_ok)
        else:
            # No seed present: start from what this run proved, rather than writing an empty
            # set that would assert the store holds nothing.
            held = set(pulled_ok)
        blob.write_bytes_atomic(
            _held_path(), json.dumps(sorted(held), separators=(",", ":")).encode("utf-8"))

    if cursors_capped:
        print(f"[cso] cursor set hit the {CURSOR_CAP:,} cap — further changed series are "
              f"not individually reported", flush=True)
    return finalize(tally, _total_rows(out_dir), last_obs, source=SOURCE,
                    series_cursors=series_cursors)


def _write_cursor(path, mapping):
    """Persist the per-matrix LastUpdated cursor to the STORE, not the runner's disk.

    THIS IS WHY cso COULD NEVER CONVERGE. The cursor decides what to fetch:
        changed = [m for m, u in cur_upd.items() if stored.get(m) != u]
    It was written with open()/os.replace and read with os.path.exists/open — the LOCAL disk.
    Under AQUEDUCT_BACKEND=r2 that file is ephemeral scratch on the runner, so every run began
    with stored={}, saw EVERY matrix as changed, pulled the newest MAX_TABLES=60, and threw the
    cursor away. Next run: identical. Measured — neither _catalog.json nor _cursor.json exists
    in r2://econ-data/clean_full/cso/ at all, and cso's last run was "60/60 sub-unit(s)
    transient-failed" on a store of 48,960,271 rows it can never finish revisiting.

    blob.write_bytes_atomic is the same store-routed, atomic path ons_uk's sidecar already uses.
    """
    blob.write_bytes_atomic(path, json.dumps(mapping, sort_keys=True).encode("utf-8"))


def _total_rows(out_dir) -> int:
    # R36: the read (blob.row_count) was routed but the LISTING was a raw local glob, so under
    # AQUEDUCT_BACKEND=r2 there is no such directory on the runner, the loop iterates nothing,
    # and this reports 0 rows — a number rather than an error, so nothing downstream objects.
    total = 0
    for name in blob.list_parquets(out_dir):
        total += blob.row_count(os.path.join(out_dir, name))
    return total
