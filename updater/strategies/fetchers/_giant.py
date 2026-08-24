"""Shared engine for S4 giant_changed_units fetchers (eurostat, oecd, ...).

A "giant" is a source whose data lives as THOUSANDS of per-flow parquet files in
one directory (~1,400+ files, ~6B obs each for eurostat/oecd). A blind re-crawl is
many hours, so the refresh is a CHANGE-FEED diff:

  1. Re-download the source's catalogue / table-of-contents.
  2. DIFF it against a stored snapshot to pick only the flows whose upstream
     "last update" / version MOVED  (plus genuinely NEW flows).
  3. ALWAYS re-include any flow whose last run was partial / failed / empty /
     absent — else a flow that broke once would freeze forever (the exact
     skip-if-exists freeze bug this whole framework exists to kill).
  4. For each selected flow, fetch INCREMENTALLY (server-side startPeriod tail
     from the flow's stored max obs_date) and merge into that one parquet under
     the never-shrink / dedup-on-(series_key,obs_date) invariant.

Honest status (the dominant correctness rule): a 429 / timeout / 5xx is a
TRANSIENT sub-failure -> the flow stays selected (not bumped), the SOURCE result
is `partial`, the orchestrator does NOT stamp last_success, the flow re-runs next
tick. A 200 that parsed 0 rows from a non-trivial body is STRUCTURAL. Nothing is
ever laundered into ok/no_change, and merge_and_write means a bad fetch can never
shrink or duplicate good data.

Per-flow change-feed STATE (each flow's upstream token + last status + last
obs_date) lives in a strategy-managed sidecar JSON next to the data
(<source_dir>/_giant_state.json), the same sidecar pattern existing fetchers use
(_catalog.json). It is keyed by flow_id, so it survives across runs and is the
authoritative "did this flow change / did it last succeed?" record. (The
orchestrator's unit_state row is per-SOURCE; per-flow facts need their own home.)

CRITICAL — STABLE series_key: the emitted series_key MUST NOT contain anything
that changes every release, or the flow's parquet duplicates wholesale on each
publish. For eurostat in particular the SDMX-CSV carries a `LAST UPDATE` column
that the legacy ingest leaked into the key (`LAST UPDATE=13/05/26 11:00:00:freq=A:
...`); the eurostat fetcher DROPS it (and the obs-status/flag columns) from the
key and routes lastUpdate to the change-feed sidecar / footer instead. See the
RE-KEY data-op note in giant_changed_units.py — existing files still carry the old
unstable key and need a one-time re-key (designed, NOT executed here).
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import time

import pyarrow as pa
import requests

from ..base import Result
from ...errors import TransientError, DefinitiveError
from ... import blob, merge
from ._common import Tally, finalize, sane_since

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}

# A flow is RE-SELECTED regardless of the catalogue diff when its last run ended in
# any of these states — otherwise a once-broken flow is skipped forever ("freeze").
_REDO_STATUSES = {"partial", "transient_fail", "definitive_fail", "empty", "error", "missing", None}

# Selecting / fetching MORE than this many flows in a single tick is almost
# certainly a corrupt catalogue diff (e.g. the stored snapshot was wiped so every
# flow looks "changed"); a full sweep is the giant's force-refresh path, not an
# update tick. We cap, fetch the cap, and report `partial` so the rest re-run next
# tick instead of hammering the API for hours and looking falsely "fresh".
DEFAULT_MAX_FLOWS_PER_TICK = 400


def state_path(source_dir: str) -> str:
    return os.path.join(source_dir, "_giant_state.json")


def load_state(source_dir: str) -> dict:
    p = state_path(source_dir)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # A corrupt sidecar must NOT crash the run; treat as empty (every flow then
        # looks new -> selection cap + partial keeps it honest and bounded).
        return {}


def save_state(source_dir: str, state: dict) -> None:
    """Atomic sidecar publish (write tmp -> os.replace), same as blob.write_table_atomic."""
    os.makedirs(source_dir, exist_ok=True)
    p = state_path(source_dir)
    tmp = f"{p}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, separators=(",", ":"))
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def http_get(url, accept, timeout, *, retries=4, rate=1.0, session=None):
    """GET with the giant failure contract.

    Returns bytes on 200. Raises TransientError on 429 / 5xx / network after
    retries (so the caller can mark the flow transient and re-run). Returns None on
    a hard 400/404/413 (caller decides: structural vs genuine-no-data)."""
    s = session or requests
    hdrs = {**UA, "Accept": accept}
    last = None
    for attempt in range(retries):
        try:
            r = s.get(url, headers=hdrs, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = f"net:{e}"
            time.sleep(min(60, rate * (attempt + 1) * 4))
            continue
        if r.status_code == 200:
            return r.content
        if r.status_code in (400, 404, 413):
            return None
        if r.status_code == 429:
            last = "429"
            time.sleep(min(120, 35 * (attempt + 1)))  # honor documented OECD/Eurostat cooldown
            continue
        if r.status_code in (500, 502, 503, 504):
            last = f"http{r.status_code}"
            time.sleep(min(60, rate * (attempt + 1) * 4))
            continue
        # Any other 4xx: hard, definitive (handled by caller as structural).
        return None
    raise TransientError(f"{url[-90:]} -> {last} after {retries} attempts")


def _max_obs_date(out_path: str) -> str | None:
    """The flow file's stored max obs_date — the date-tail boundary for the next fetch.

    NEVER MATERIALISE THE FILE TO ANSWER THIS (2026-08-02). This used to be a bare
    `blob.read_table(out_path)`: every column, every row, to take one max. That is the
    same defect fixed on 2026-07-30 in statcan.py and merge._max_obs_date — the SHARED
    giant driver was missed, and it is on the hot path of both callers (oecd, eurostat),
    once per selected flow.

    The cost scales with the LARGEST file in the store, not with the work:
    oecd's OECD.STI.PIE__DSD_TIVA_FDVA@DF_FDVA.parquet is 1,792,000,000 rows over five
    columns (two of them strings) — 15.4 GB compressed on disk and well past 125 GB
    decoded. No runner or workstation we own can hold that, so the read did not merely
    cost memory: it ALWAYS threw, and the bare `except: return None` below turned that
    into `since=None`, which `_since_param` renders as an empty string — i.e. a silent
    FULL-HISTORY re-pull of that flow, reported as success. Self-perpetuating, too: the
    re-pull keeps the file huge, so the next tick fails the same way forever.

    Two bounded paths, in order:
      1. Row-group statistics from the FOOTER — parquet already stores each row group's
         obs_date min/max, so the answer needs no decode at all (measured: complete stats
         on 8,960/8,960 oecd row groups and 13/13 eurostat).
      2. If any row group lacks stats, fall back to blob.iter_batches — one batch of one
         column in memory, whatever the file size. Never the whole table.
    """
    if not blob.exists(out_path):
        return None
    best = None
    try:
        md = blob.read_metadata(out_path)
        if md.num_rows == 0:
            return None
        names = [md.schema.column(i).name for i in range(md.num_columns)]
        if "obs_date" not in names:
            return None
        idx = names.index("obs_date")
        complete = True
        for g in range(md.num_row_groups):
            st = md.row_group(g).column(idx).statistics
            if st is None or not st.has_min_max:
                complete = False   # one gap and the footer cannot be trusted for a MAX
                break
            if st.max is not None and (best is None or st.max > best):
                best = st.max
        if complete:
            return str(best) if best is not None else None
    except Exception as e:                                   # noqa: BLE001
        # Fall through to the batched scan rather than returning None: see below for
        # why a silent None here is expensive, not merely imprecise.
        print(f"[giant] footer stats unusable for {os.path.basename(out_path)} "
              f"({type(e).__name__}: {e}); scanning obs_date in batches", flush=True)

    best = None
    try:
        import pyarrow.compute as pc
        for batch in blob.iter_batches(out_path, columns=["obs_date"]):
            if batch.num_rows == 0:
                continue
            m = pc.max(batch.column("obs_date")).as_py()
            if m is not None and (best is None or m > best):
                best = m
        return str(best) if best is not None else None
    except Exception as e:                                   # noqa: BLE001
        # LOUD, because the caller cannot tell this apart from "no data yet". Returning
        # None is still the right fallback (a full re-pull is correct, just expensive) —
        # but it must never again be invisible. Ledger: every early exit has to answer
        # "does this status let the gate advance?"
        print(f"[giant] WARNING: cannot read max obs_date from "
              f"{os.path.basename(out_path)} ({type(e).__name__}: {e}); falling back to a "
              f"FULL re-pull of this flow (no startPeriod tail)", flush=True)
        return None


def select_flows(catalog: dict, state: dict, *, max_flows=DEFAULT_MAX_FLOWS_PER_TICK):
    """catalog: {flow_id: {"vintage": <token>, "filename": <str>, **meta}}.
    state:   {flow_id: {"vintage": <token>, "status": <str>, "last_obs_date": <str>}}.

    Returns (selected_flow_ids, capped: bool). A flow is selected if:
      - it is brand new (absent from state), OR
      - its catalogue vintage token moved vs the stored one, OR
      - its last run status is in _REDO_STATUSES (partial/failed/empty/absent).
    Deterministic order (new+changed first, then redo) so the per-tick cap always
    makes forward progress on real changes before retrying stale partials."""
    changed, redo = [], []
    for fid, meta in catalog.items():
        st = state.get(fid)
        if st is None:
            changed.append(fid)
            continue
        if str(meta.get("vintage")) != str(st.get("vintage")):
            changed.append(fid)
        elif st.get("status") in _REDO_STATUSES:
            redo.append(fid)
    ordered = changed + redo
    capped = len(ordered) > max_flows
    return ordered[:max_flows], capped


def run_giant(unit, *, source, fetch_catalog, fetch_flow, csv_accept, rate, timeout,
              max_flows=DEFAULT_MAX_FLOWS_PER_TICK, min_ratio=0.97):
    """Generic S4 driver. Sources supply two callables:

      fetch_catalog() -> {flow_id: {"vintage", "filename", **meta}}   (raises Transient on net fail)
      fetch_flow(flow_id, meta, since, session) -> (table_or_None, status)
          where status in {"ok","no_change","empty","structural","transient"} and
          table is a pyarrow.Table with at least (series_key, obs_date, value) using
          a STABLE series_key. `since` is the flow's stored max obs_date (or None).

    The driver: diffs the catalogue, selects changed+redo flows (cap-bounded),
    incrementally fetches each, merges per-flow under never-shrink/dedup, updates the
    per-flow sidecar state, and returns one honest SOURCE-level Result."""
    source_dir = (unit.out_paths or [None])[0]
    if source_dir is None:
        raise DefinitiveError(f"{source}: unit has no out_paths (source dir unknown)")
    os.makedirs(source_dir, exist_ok=True)

    catalog = fetch_catalog()  # TransientError propagates -> source transient_fail, nothing touched
    if not catalog:
        raise TransientError(f"{source}: catalogue download returned no flows (treat as transient)")

    state = load_state(source_dir)
    selected, capped = select_flows(catalog, state, max_flows=max_flows)

    # ANNOUNCE THE SCOPE, then report progress through it. A giant is the longest-running
    # thing in any pass - oecd ran for over three hours on 2026-08-01 - and the orchestrator
    # only prints once per SOURCE, so the run log said nothing at all between ">>> oecd/_all"
    # and its eventual completion. Working and wedged looked identical, and the only way to
    # tell them apart was to go and stat the output directory. That is the same defect as the
    # orchestrator's silent no-adapter skip (R211), one level down.
    print(f"[{source}] catalogue {len(catalog):,} flow(s); selected {len(selected):,} "
          f"changed/new/redo{' (CAPPED — remainder next tick)' if capped else ''}", flush=True)
    t_start = time.time()

    tally = Tally()
    sess = requests.Session()
    total_rows = 0
    max_last = None

    for n_done, fid in enumerate(selected, 1):
        # Every 25 flows, and always on the last one. Bounded on purpose: one line per flow
        # would bury a 1,400-flow sweep's real events in noise.
        if n_done % 25 == 0 or n_done == len(selected):
            print(f"[{source}] {n_done:,}/{len(selected):,} flows — "
                  f"+{tally.added:,} rows, {tally.empty:,} quiet, "
                  f"{tally.transient:,} transient, {tally.structural:,} structural, "
                  f"{time.time() - t_start:,.0f}s", flush=True)
        meta = catalog[fid]
        out_path = os.path.join(source_dir, meta["filename"])
        since = sane_since(_max_obs_date(out_path))
        flow_st = dict(state.get(fid, {}))
        try:
            table, status = fetch_flow(fid, meta, since, sess)
        except TransientError as e:
            # NAME THE FLOW. _giant drives the biggest sources (oecd et al) over `selected`
            # flows, so an unlabelled count is the least actionable row in the system: hundreds
            # of flows, one number, five different causes below.
            tally.transient_unit(f"{fid}: {str(e)[-60:]}")
            flow_st.update(status="transient_fail")  # vintage NOT advanced -> reselected next tick
            state[fid] = flow_st
            time.sleep(rate)
            continue
        except DefinitiveError as e:
            # A structural/hard error on ONE flow must not abort the whole giant.
            tally.structural_unit(f"{fid}: {str(e)[-60:]}")
            flow_st.update(status="definitive_fail", error=str(e)[:200])
            state[fid] = flow_st
            time.sleep(rate)
            continue
        except Exception as e:                      # noqa: BLE001
            # NEITHER OF THE TWO HANDLERS ABOVE CATCHES AN UNEXPECTED ERROR, AND ONE FLOW
            # THEN KILLS THE SWEEP. On 2026-08-24 a single Eurostat flow raised _csv.Error
            # - not a TransientError, not a DefinitiveError - so it escaped run_giant,
            # skipped save_state below, and surfaced in orchestrate.py's blanket handler as
            # "UNEXPECTED:..." with NO flow id. 50 of 400 flows had been merged; the other
            # ~350 were abandoned, and the log never named the culprit.
            #
            # Transient, not structural, deliberately: the vintage is NOT advanced, so the
            # flow is reselected next tick, and _common.finalize does not downgrade the
            # whole source on a transient count the way it can on a structural one. This
            # recovers no rows by itself - it converts a run-killing crash into one named,
            # retryable flow.
            tally.transient_unit(f"{fid}: UNEXPECTED {type(e).__name__}: {str(e)[-60:]}")
            flow_st.update(status="transient_fail")
            state[fid] = flow_st
            time.sleep(rate)
            continue

        if status == "transient":
            tally.transient_unit(f"{fid}: fetch_flow reported transient")
            flow_st.update(status="transient_fail")
            state[fid] = flow_st
            time.sleep(rate)
            continue
        if status == "structural":
            tally.structural_unit(f"{fid}: fetch_flow reported structural")
            flow_st.update(status="definitive_fail")
            state[fid] = flow_st
            time.sleep(rate)
            continue
        if status in ("empty", "no_change") or table is None or table.num_rows == 0:
            # 200 with no NEW rows in the tail = genuine quiet flow; SAFE to advance the
            # change-feed vintage (we proved upstream's catalogue token == fetched data).
            tally.empty_unit()
            flow_st.update(status="empty", vintage=meta.get("vintage"))
            state[fid] = flow_st
            time.sleep(rate)
            continue

        # status == "ok": merge the tail into the per-flow parquet (never-shrink/dedup).
        try:
            n, last = merge.merge_and_write(out_path, table, mode="merge", min_ratio=min_ratio)
        except DefinitiveError as e:
            # A would-shrink / column-drop / 0-row merge: keep old data, surface partial,
            # do NOT advance vintage so it is reattempted (could be a truncated upstream).
            # WHICH guard tripped is the difference between "upstream truncated" and "we broke
            # the schema", so carry the merge's own message.
            tally.transient_unit(f"{fid}: merge guard — {str(e)[-60:]}")
            flow_st.update(status="partial", error=str(e)[:200])
            state[fid] = flow_st
            time.sleep(rate)
            continue

        added = table.num_rows
        tally.added_unit(added)
        total_rows += n
        if last and (max_last is None or str(last) > str(max_last)):
            max_last = str(last)
        flow_st.update(status="ok", vintage=meta.get("vintage"), last_obs_date=last,
                       obs_count=n)
        state[fid] = flow_st
        time.sleep(rate)

    # Mark flows present in the catalogue but never touched (not selected) so first-ever
    # runs don't perpetually re-select everything: only flows we DIDN'T select keep their
    # prior state; brand-new-but-unselected (cap overflow) stay absent -> reselected.
    save_state(source_dir, state)

    if capped:
        # We deliberately fetched only a slice of a very large changed set. That is a
        # legitimate-partial: report it so the source is NOT stamped fully fresh and the
        # remainder runs next tick. (Never launder a known-incomplete sweep into ok.)
        return Result(status="partial", obs=total_rows, last_obs_date=max_last,
                      new_vintage=_catalog_token(catalog),
                      error=f"selected>cap: fetched {len(selected)} of a larger changed set; "
                            f"remainder re-runs next tick (+{tally.added} rows)")

    # finalize() raises structural DefinitiveError if every sub-unit broke; returns
    # partial if any transient; else ok/no_change. empty_window_floor guards a giant
    # where a handful of selected flows all happen to be quiet (legit), but a wholesale
    # all-empty over many flows is a structural break.
    res = finalize(tally, total_rows, max_last, source=source,
                   empty_window_floor=max(10, len(selected) // 2))
    # carry a catalogue-level vintage token so detect_change can cheaply short-circuit
    # next tick when the whole catalogue is unmoved.
    res.new_vintage = _catalog_token(catalog)
    return res


def _catalog_token(catalog: dict) -> str:
    """A cheap stable token over the whole catalogue's per-flow vintages — changes iff
    ANY flow's last-update/version moved. Lets detect_change skip a no-op tick."""
    import hashlib
    h = hashlib.sha256()
    for fid in sorted(catalog):
        h.update(fid.encode("utf-8"))
        h.update(b"=")
        h.update(str(catalog[fid].get("vintage")).encode("utf-8"))
        h.update(b";")
    return "cat:" + h.hexdigest()[:16]
