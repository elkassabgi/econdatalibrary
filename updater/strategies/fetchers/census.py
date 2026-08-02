"""S3 (extend_by_date) fetcher — U.S. Census Economic Indicators Time Series (EITS).

SCOPE IS THE 20 EITS FLOWS, DELIBERATELY. clean_full/census holds 80 parquets across SIXTY
distinct schemas, and census is _NATIVE_ONLY (tidy_ok=False) with two resolvers:
  * eits__<flow>.parquet — uniform shape, 1,965,285 rows / 10,950 series, served by
    _resolve_census. Real time series; the API supports a date tail. THIS module.
  * the other 60 — hundreds of string columns each, served at TABLE grain by
    _resolve_census_table. They are periodic snapshots (economic census, annual surveys,
    e.g. aies/basic at time=2023). A date tail is the wrong instrument: they do not gain
    periods, they gain a whole new reference year. They need their own vintage check and
    this module must not pretend to cover them.

WHY IT EXISTS: census is catalogued, served and reachable, and NOTHING updated it. Measured
2026-08-02, all 20 flows probed with zero failures: 19 are BEHIND upstream — monthly flows by
1-2 periods, qfr and qpr by 5 quarters (ours 2025-10 vs upstream 2026-Q1).

THE ONE FLOW THAT IS NOT BEHIND IS NOT A BUG. eits__mhs sits at 2014-10-01: it is the RETIRED
Manufactured Housing Survey, and its replacement eits__mhs2 is present and current. Any
freshness sweep will flag it forever. Do not "repair" it.

API CONTRACT (each clause below was a 400 until it was right):
    GET https://api.census.gov/data/timeseries/eits/<flow>
    time=from YYYY   — a REAL SPACE. The registry note says "time=from+YYYY", which is the
                       URL-ENCODED form; sending the literal string "from+2026" transmits
                       from%2B2026 and returns 400 "unsupported date/time format: +2026".
    get=...          — MUST include time_slot_id. Omit it and every flow except marts returns
                       400 "missing required variable/predicate: time_slot_id".
    for=us:*         — `us` and `time` come back as columns without being asked for.
Verified: the response carries every native column the store holds, so a merged row is never
null in a column the existing data populates.

KEY CONSTRUCTION — RECOVERED FROM THE STORE, NOT RE-CLASSIFIED. ingest_census builds series_key
by joining the columns its heuristic classifier calls dimensions:
    eits/marts|data_type_code=MPCSM|seasonally_adj=yes|category_code=452|program_code=MARTS|
    geo_level_code=US|error_data=no|us=1
Re-running that classifier here would be a silent catastrophe: if it classified ONE column
differently than it did at ingest time, the keys we write would not equal the keys we store,
merge's dedup on (series_key, obs_date) would not match, and every series would DOUBLE. So the
dimension list is parsed back out of an existing stored key instead — the store is the only
authority on what its own keys mean. Same defect class as the cursor key-shape bugs fixed in
fed_board, fhfa and adb.

COVERAGE, NOT JUST FRESHNESS. A date tail asks each flow for periods at or after the newest one
we hold. Over a flow whose history was truncated by an earlier budget-capped ingest, that
freezes the gap permanently while reporting success — the bls failure (R230). So every flow's
series count is compared before and after: a flow that comes back with FEWER distinct series
than we store has not been fully covered, and the run says so rather than reporting a clean ok.

HONEST STATUS (Tally + finalize):
  added_unit(n)     rows merged for a flow
  empty_unit()      flow legitimately returned no rows at/after our boundary (it is current)
  transient_unit()  timeout / 5xx / 429 / unparseable -> partial, existing data untouched
  structural_unit() a 200 with a real header and 0 usable rows from a BOUNDARY re-fetch, which
                    must always re-return at least the boundary period -> DefinitiveError
"""
from __future__ import annotations

import datetime as dt
import io
import os
import re
import sys
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import (CURSOR_CAP, Deadline, Tally, api_key, cursors_from_table, finalize,
                      merge_cursor_map)

SOURCE = "census"
BASE = "https://api.census.gov/data/timeseries"
DEDUP = ("series_key", "obs_date")
PREFIX = "eits__"
BUDGET_MIN = float(os.environ.get("CENSUS_BUDGET_MIN", "20"))
RATE = 0.3
TIMEOUT = 120
MAX_ATTEMPTS = 3
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)

# Columns the API returns without being listed in get= (they are the predicates).
_IMPLICIT = ("time", "us")
# Never requested in get=: derived here, not served by the API.
_DERIVED = ("series_key", "obs_date")


def implemented() -> bool:
    return True


def _api_key() -> str | None:
    """CENSUS_API_KEY from the environment, else from .env (registry key_env).

    Thin wrapper over the shared _common.api_key, which bea now uses too — the same
    "the key is in .env and nothing loads .env" problem, found twice in one day.
    """
    return api_key("CENSUS_API_KEY")


def _ingest():
    """The ingester, for parse_obs_date and series_key_of. Reused, never reimplemented."""
    root = config.ROOT
    if root not in sys.path:
        sys.path.insert(0, root)
    from jobs import ingest_census as J
    return J


def _flows(out_dir: str) -> list[str]:
    """EITS flow names that already exist on disk. We never mint a new flow here — a flow
    that has never been ingested is backfill, not a date tail."""
    return sorted(f[len(PREFIX):-len(".parquet")]
                  for f in blob.list_parquets(out_dir)
                  if f.startswith(PREFIX) and not f.endswith("__series.parquet"))


def _dims_from_store(path: str) -> tuple[list[str], list[str]]:
    """(dim_cols, data_cols) recovered from the stored data itself.

    dim_cols come from parsing one existing series_key — see the module docstring for why
    re-running the ingest classifier here would silently double every series. data_cols is
    every non-derived column the parquet holds, so what we write matches what is there.
    """
    schema = blob.read_schema(path)
    cols = [c for c in schema.names if c not in _DERIVED]
    tbl = blob.read_table(path, columns=["series_key"])
    if tbl.num_rows == 0:
        return [], cols

    # THE UNION ACROSS EVERY KEY SHAPE, NOT ONE SAMPLE.
    #
    # series_key_of omits a dimension whose value is empty, so a flow with heterogeneous
    # geography has MORE THAN ONE key shape and no single key lists them all. eits/qtax is
    # the case: its US rows carry `us` and no STATE (STATE is null at national level), its
    # state rows carry STATE and no `us`. Sampling key[0] — a US row — produced a dimension
    # list with `us` and without STATE, so every state row got built with the wrong shape.
    # That is what created 1,209 duplicate series before the shape guard existed.
    #
    # Taking the union restores both: on a US row STATE is empty and drops out, on a state
    # row `us` is absent and drops out, and each reproduces its own stored shape exactly.
    # Order is first-seen so keys stay byte-identical to what the ingester wrote.
    dims: list[str] = []
    seen: set = set()
    shapes: dict = {}
    for k in tbl.column("series_key").to_pylist():
        if not k:
            continue
        order = [seg.split("=", 1)[0] for seg in k.split("|")[1:] if "=" in seg]
        shapes.setdefault(frozenset(order), order)           # one ORDER per dimension SET
        for name in order:
            if name not in seen:
                seen.add(name)
                dims.append(name)
    return dims, cols, list(shapes.items())


def _key_for(J, row, hidx, dims, shapes, path):
    """Rebuild a row's series_key using the ORDER of the stored shape it belongs to.

    A flat dimension list is not enough. series_key_of joins in the order it is given, so a
    row whose dimensions are correct but ORDERED differently yields a permuted key — which
    merge treats as a brand-new series, not an update. eits/qtax stores STATE fourth in its
    state keys while a union built over its US-majority rows appends STATE last; feeding that
    union to series_key_of produced 1,209 keys the store had never held.

    So: work out which dimensions this row actually populates, find the stored shape with
    exactly that dimension SET, and join in THAT shape's order. Returns None when no stored
    shape matches — a genuinely new shape, which the caller refuses rather than invents.
    """
    present = frozenset(
        c for c in dims
        if (i := hidx.get(c)) is not None and i < len(row)
        and row[i] is not None and row[i] != "")
    for keyset, order in shapes:
        if keyset == present:
            return J.series_key_of(row, hidx, order, path)
    return None


def _stored_max(path: str):
    """Newest obs_date we hold for a flow, or None."""
    try:
        tbl = blob.read_table(path, columns=["obs_date"])
        if tbl.num_rows == 0:
            return None
        import pyarrow.compute as pc
        return pc.max(tbl.column("obs_date")).as_py()
    except Exception:                                        # noqa: BLE001
        return None


def _geo_col(cols: list[str]) -> str | None:
    for c in cols:
        if c.lower() == "geo_level_code":
            return c
    return None


def _store_geo_levels(path: str, gcol: str) -> set:
    try:
        tbl = blob.read_table(path, columns=[gcol])
        return {v for v in tbl.column(gcol).to_pylist() if v}
    except Exception:                                        # noqa: BLE001
        return set()


def _predicates(levels: set) -> list[str]:
    """`for=` predicates this tail requests. US ONLY, deliberately — see below.

    20 of the 21 EITS flows are entirely geo_level_code=US, so us:* is complete for them.
    eits/qtax is not: it stores 77 US series and 1,344 STATE ones, and a us-only tail cannot
    refresh those. I tried adding `for=state:*` and REVERTED it, because the state response
    does not carry the same dimensions as the stored state rows:

        stored  ...|GEO_ID=0400000US32|...|STATE=32|...|GEO_LEVEL_CODE=NV
        state:* ...|GEO_ID=0400000US01|...            |GEO_LEVEL_CODE=AL|us=01

    No STATE, an extra us — so series_key_of produces a key SHAPE the store has never held,
    and the merge adds 1,209 brand-new series instead of extending the 1,344 that exist.
    Measured in a sandbox: qtax went 1,421 -> 2,630 distinct series. That is silent
    duplication of real data, which is far worse than the gap it was meant to close.

    Two causes, one fixed and one not:
      * _dims_from_store sampled ONE key. qtax's US rows carry `us` and no STATE (null at
        national level); its state rows carry STATE and no `us`. A single sampled US key
        produced a dimension list with no STATE at all. FIXED — the dimension list is now
        the union across every stored key.
      * ORDER still differs. The stored state key places STATE fourth
        (GEO_ID|CATEGORY_CODE|SEASONALLY_ADJ|STATE|ERROR_DATA|...), while a first-seen union
        over US-majority rows appends it last. series_key_of joins in dim order, so the
        rebuilt key is a permutation of the stored one — a DIFFERENT key. Re-enabling
        state:* with the union still produced 1,209 unknown shapes; the guard refused all of
        them, which is the guard working.
    Both are fixed: dimensions come from the union across stored keys, and each row is keyed
    with the ORDER of the stored shape whose dimension set it matches (see _key_for). A row
    matching no stored shape is dropped rather than invented, and the shape guard remains as
    the backstop.
    """
    preds = []
    if not levels or "US" in levels:
        preds.append("us:*")
    if levels - {"US"}:
        preds.append("state:*")
    return preds or ["us:*"]


_SHAPE = re.compile(r"=[^|]*")


def _shape(key: str) -> str:
    """A series_key with its VALUES blanked: the structural signature of the key."""
    return _SHAPE.sub("=*", key)


def _store_keys(path: str) -> set:
    """Every series_key the flow already publishes."""
    try:
        return {k for k in
                blob.read_table(path, columns=["series_key"]).column("series_key").to_pylist()
                if k}
    except Exception:                                        # noqa: BLE001
        return set()


def _stored_series(path: str) -> int:
    try:
        import pyarrow.compute as pc
        return pc.count_distinct(
            blob.read_table(path, columns=["series_key"]).column("series_key")).as_py()
    except Exception:                                        # noqa: BLE001
        return 0


def _fetch(sess: requests.Session, flow: str, get_cols: list[str], year: int, key: str | None,
           pred: str = "us:*"):
    """One flow's date tail -> the raw JSON matrix. Raises TransientError on flaky failures,
    DefinitiveError on a hard 4xx that is not a rate limit (a broken request, not a bad day)."""
    params = {"get": ",".join(get_cols), "time": f"from {year}", "for": pred}
    if key:
        params["key"] = key
    url = f"{BASE}/eits/{flow}"
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, params=params, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 200:
            # A MISSING KEY IS AN HTTP *200*. Census answers an unauthenticated request with
            # status 200 and an HTML "Missing Key" page — not 401, not 403. Anything that
            # trusted the status code would record a clean empty run every day forever, which
            # is the same false green this fetcher's coverage check exists to prevent. Detect
            # it by content and refuse DEFINITIVELY: a missing credential is a configuration
            # fault that retrying cannot fix, and the existing data must be kept untouched.
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "html" in ctype or r.text.lstrip()[:6].lower() == "<html":
                if "missing key" in r.text[:2000].lower():
                    raise DefinitiveError(
                        f"{SOURCE}: CENSUS_API_KEY is not set, so nothing can be fetched "
                        f"(the API returns HTTP 200 with a 'Missing Key' page). Existing data "
                        f"kept. The key lives in .env on the workstation — this source is "
                        f"routed run_location: local for exactly that reason.")
                raise TransientError(f"{SOURCE} {flow}: HTML body on a 200 (upstream error page)")
            try:
                return r.json()
            except Exception as e:                           # noqa: BLE001
                raise TransientError(f"{SOURCE} {flow}: unparseable 200 body: {e!r}") from e
        if r.status_code == 204:
            return []                                        # no content at/after boundary
        if r.status_code in _TRANSIENT_HTTP:
            last = f"HTTP {r.status_code}"
            time.sleep(2 ** attempt)
            continue
        raise DefinitiveError(f"{SOURCE} {flow}: HTTP {r.status_code}: {r.text[:160]}")
    raise TransientError(f"{SOURCE} {flow}: {last} after {MAX_ATTEMPTS} attempts")


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    flows = _flows(out_dir)
    if not flows:
        raise DefinitiveError(
            f"{SOURCE}: no eits__*.parquet in {out_dir}; run the first-pass ingest "
            f"(jobs/ingest_census.py) before incremental updates")

    J = _ingest()
    key = _api_key()
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"})

    tally = Tally()
    dl = Deadline(minutes=BUDGET_MIN)
    cursors: dict[str, str] = {}
    total = 0
    max_last = None
    under: list[str] = []
    deferred = 0

    for flow in flows:
        path = os.path.join(out_dir, f"{PREFIX}{flow}.parquet")
        before_rows = blob.row_count(path)
        total += before_rows
        if dl.spent():
            deferred += 1
            continue

        mx = _stored_max(path)
        if mx is None:
            tally.empty_unit(flow)
            continue
        dim_cols, data_cols, shapes = _dims_from_store(path)
        if not dim_cols:
            tally.structural_unit(f"{flow}: no dimensions recoverable from stored series_key")
            continue
        get_cols = [c for c in data_cols if c.lower() not in _IMPLICIT]
        gcol = _geo_col(data_cols)
        levels = _store_geo_levels(path, gcol) if gcol else set()

        # EACH PREDICATE KEEPS ITS OWN HEADER. They are not the same shape: `for=us:*`
        # answers qtax with 16 columns ending in `us`, `for=state:*` with 17 ending in
        # `state`. Concatenating the bodies under the first header — which is what this did
        # first — shifts every column of the second response, so the dimensions read as
        # garbage, no stored shape matches, and the rows are dropped. Silently, because the
        # drop counter was not reported either. Parse each response against its own header.
        parts = []
        failed = False
        for pred in _predicates(levels):
            try:
                part = _fetch(sess, flow, get_cols, mx.year, key, pred)
            except TransientError:
                tally.transient_unit(f"{flow} for={pred}")
                failed = True
                break
            except DefinitiveError:
                tally.structural_unit(f"{flow} for={pred}")
                failed = True
                break
            time.sleep(RATE)
            if part and len(part) >= 2:
                parts.append(part)
        if failed:
            continue
        rows = parts[0] if parts else []

        if not parts:
            # A BOUNDARY re-fetch must re-return at least the boundary period. Nothing at all
            # means the shape we ask for is gone, not that the flow is quiet.
            tally.structural_unit(f"{flow}: boundary re-fetch returned no rows")
            continue

        # CASE-INSENSITIVE for the two well-known columns. eits/qtax names its variables in
        # UPPERCASE both in our store and upstream (CELL_VALUE, TIME_SLOT_ID, ...) while the
        # other 20 flows are lowercase — and it is not cosmetic: asking qtax for `cell_value`
        # returns 400 "unknown variable". A case-sensitive lookup here found no cell_value,
        # counted a structural break, and finalize turned that into a DefinitiveError, so ONE
        # oddly-cased flow failed the entire source every run. dim_cols and the get= list are
        # both read from the store, so they already carry each flow's own casing; only these
        # two fixed names needed it.
        cols: dict[str, list] = {c: [] for c in data_cols}
        unknown_shape = 0
        keys: list[str] = []
        dates: list[dt.date] = []
        seen_series: set = set()
        got_levels: set = set()
        broke = False
        for part in parts:
            header = part[0]
            hidx = {c: i for i, c in enumerate(header)}
            lidx = {c.lower(): i for i, c in enumerate(header)}
            ti = lidx.get("time")
            vi = lidx.get("cell_value")
            if ti is None or vi is None:
                tally.structural_unit(f"{flow}: response lacks time/cell_value")
                broke = True
                break
            gi = lidx.get("geo_level_code")
            for row in part[1:]:
                d = J.parse_obs_date(row[ti])
                if d is None or d < mx:
                    continue                                 # strictly the tail (boundary incl.)
                if row[vi] in (None, ""):
                    continue
                sk = _key_for(J, row, hidx, dim_cols, shapes, f"timeseries/eits/{flow}")
                if sk is None:
                    unknown_shape += 1
                    continue
                keys.append(sk)
                dates.append(d)
                seen_series.add(sk)
                if gi is not None and gi < len(row) and row[gi]:
                    got_levels.add(row[gi])
                for c in data_cols:
                    i = hidx.get(c)
                    cols[c].append(row[i] if i is not None and i < len(row) else None)
                iso = d.isoformat()
                if cursors.get(sk, "") < iso:
                    cursors[sk] = iso
        if broke:
            continue

        # A DROP MUST NEVER BE SILENT. Rows whose dimension set matches no stored shape are
        # skipped rather than invented — correct — but skipping thousands of rows without
        # saying so is how a fetcher looks healthy while covering a fraction of its source.
        if unknown_shape:
            print(f"[{SOURCE}] {flow}: DROPPED {unknown_shape:,} row(s) whose dimension set "
                  f"matches no stored key shape", flush=True)
            under.append(f"{flow} unknown-shape x{unknown_shape}")

        if not keys:
            tally.empty_unit(flow)                           # already at the publisher's frontier
            continue

        # THIS IS A TAIL, NOT A BACKFILL — merge only series the flow ALREADY publishes.
        #
        # merge dedups on (series_key, obs_date), so an unrecognised key does not update a
        # series, it CREATES one: silently, with real values, and nothing downstream looks
        # broken. Two distinct ways that happened here, both caught by diffing distinct
        # series counts against production rather than by any error:
        #   * a permuted key (dimensions right, ORDER wrong) — qtax 1,421 -> 2,630
        #   * genuinely NEW coverage — `for=state:*` returns GEO_LEVEL_CODE=DC, which this
        #     store has never held, adding 23 real series that no catalogue row describes
        # The first is corruption; the second is legitimate data that still must not arrive
        # this way, because a date tail that widens the published id space produces series
        # users cannot find (same rule the boc fetcher states for its extra 3,044 series).
        #
        # So: keep what we publish, count what we skip, and let a deliberate backfill decide
        # about the rest. The shape check is subsumed — an unknown shape cannot be a known key.
        known = _store_keys(path)
        if known:
            keep = [i for i, k in enumerate(keys) if k in known]
            skipped_new = len(keys) - len(keep)
            if skipped_new:
                print(f"[{SOURCE}] {flow}: skipped {skipped_new:,} row(s) for "
                      f"{len({keys[i] for i in range(len(keys)) if keys[i] not in known}):,} "
                      f"series this flow does not yet publish (new coverage needs a backfill "
                      f"and a catalogue row, not a tail)", flush=True)
            if not keep:
                tally.empty_unit(flow)
                continue
            keys = [keys[i] for i in keep]
            dates = [dates[i] for i in keep]
            for c in data_cols:
                cols[c] = [cols[c][i] for i in keep]
            seen_series = set(keys)

        held = _stored_series(path)
        arrays = {"series_key": pa.array(keys, pa.string()),
                  "obs_date": pa.array(dates, pa.date32())}
        for c in data_cols:
            arrays[c] = pa.array([None if v is None else str(v) for v in cols[c]], pa.string())
        tbl = pa.table(arrays)

        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            tally.transient_unit(f"{flow}: merge refused")
            continue
        added = max(0, n - before_rows)

        # COVERAGE IS A STRUCTURAL QUESTION — "did we ASK for everything?" — not a statistical
        # one. The first version of this guard compared distinct series returned against
        # distinct series stored, and it conflated two unrelated things:
        #   * eits/qtax 66 of 1,421 — REAL: the us-only predicate could not reach 1,344 state
        #     series, so they would never have refreshed. Our fault, and invisible.
        #   * eits/bfs 336 of 462, mrts 555 of 568 — NORMAL: those flows are 100% geo US, we
        #     asked for all of them, and the publisher simply did not release every series in
        #     the window. Nothing is behind.
        # A count test flags both, so it would have reported `partial` forever on healthy
        # flows — the same cry-wolf that eits/mhs already caused once. What actually matters
        # is whether a geography we STORE was one we never REQUESTED, which is exact.
        if gcol:
            missed = levels - got_levels                     # accumulated across ALL predicates
            if missed:
                under.append(f"{flow} geo {','.join(sorted(missed)[:4])}"
                             f"{'...' if len(missed) > 4 else ''}")

        total += added
        tally.added_unit(added, flow)
        if md and (max_last is None or str(md) > str(max_last)):
            max_last = str(md)
        merge_cursor_map(cursors, cursors_from_table(tbl, cap=CURSOR_CAP,
                                                     key_col="series_key"), cap=CURSOR_CAP)
        print(f"[{SOURCE}] {flow}: +{max(0, n - before_rows):,} row(s) "
              f"({len(seen_series):,} series at/after {mx})", flush=True)

    if deferred:
        print(f"[{SOURCE}] budget of {BUDGET_MIN} min spent after {dl.elapsed_min():.1f} min; "
              f"{deferred} of {len(flows)} flow(s) deferred to the next tick", flush=True)

    res = finalize(tally, total, max_last or (since or None), source=SOURCE,
                   series_cursors=cursors or None,
                   empty_window_floor=max(10, len(flows)))

    # Never a clean result over flows whose tail did not span them, and never over a run that
    # stopped on its budget with flows untouched.
    if under or deferred:
        note = []
        if under:
            note.append("tail UNDER-COVERED " + ", ".join(sorted(under)[:6]))
        if deferred:
            note.append(f"{deferred} flow(s) deferred on budget")
        msg = "; ".join(note)
        print(f"[{SOURCE}] {msg}", flush=True)
        if res.status in ("ok", "no_change"):
            res = Result(status="partial", obs=res.obs, last_obs_date=res.last_obs_date,
                         new_vintage=res.new_vintage, series_cursors=res.series_cursors,
                         error=msg)
    return res
