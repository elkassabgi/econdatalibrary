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
    changed = [m for m, u in cur_upd.items() if stored.get(m) != u]
    # newest revisions first so the bounded run always advances the freshest data
    changed.sort(key=lambda m: (cur_upd.get(m) or ""), reverse=True)

    if not changed:
        # nothing moved upstream — persist the (unchanged) cursor and report no_change honestly
        _write_cursor(cur_path, cur_upd)
        tally.empty_unit()
        return finalize(tally, _total_rows(out_dir), None, source=SOURCE,
                        series_cursors={})

    batch = changed[:MAX_TABLES]
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
            # Still unroutable after the refresh: either retired upstream, or absent from CSO's
            # Search API. Transient rather than dropped, so it re-tries.
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
