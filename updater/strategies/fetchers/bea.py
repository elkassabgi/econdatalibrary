"""S2 fetcher - U.S. Bureau of Economic Analysis: refresh the stored dataset tree in place (v1, 2026-09-23).

WHAT CHANGED AND WHY. Until 2026-09-23 this fetcher refreshed ONE file, bea.parquet (NIPA +
NIUnderlyingDetail, 17,699 series), while 913,230 series are served from twelve dataset directories
that jobs/ingest_bea_full.py wrote once and nothing revisited (ledger R762/R766). Worse, every one
of bea.parquet's keys also sits in a subdirectory that sorts first, so the refreshed copy never
served. The registry always described the intended mechanism - read each group file's newest year,
re-pull recent years, merge back - and this is it.

ONLY THE SEVEN DATASETS WHOSE KEYS ARE SOUND (see SOUND). A design review (ledger R1104) measured
566,894 of 913,230 keys COLLIDING across tables in Regional, InputOutput, MNE, ITA and GDPbyIndustry:
the key omits the table, so one id holds different values in different files and users download a
patchwork. Refreshing those would extend it; their fix is a re-key (pending-actions 0ad).

PER GROUP, under a RotationCycle (ok = every group refreshed since the last ok):
  * the stored file's profile - rows, newest year, EXACT duplicate rows, CONFLICTING (key, date)
    pairs. Conflicts refuse the merge (a sound dataset should hold none); exact duplicates collapse
    in the merge, so the shrink guard is set to exactly (rows - exact duplicates) / rows;
  * the year window: newest stored year - LOOKBACK_YEARS through next year (BEA's Year is a LIST,
    not a range: "2023,2027" returns only 2023 - measured). A group whose newest year is more than
    DISCONTINUED_YEARS old is skipped between yearly full re-pulls; every group is re-pulled with
    Year=ALL once a year (FULL_REPULL_DAYS) so old revisions are not spliced under a newer vintage;
  * the fetch goes through the ingester's OWN parse (fetch_* in jobs/ingest_bea_full.py) with
    strict=True: a call that exhausts its retries raises instead of reading as an empty answer, and
    an empty answer for a window that starts inside the stored data is a failure too;
  * merge.merge_and_write (keep-new, never-shrink); the group is visited only if nothing failed.

bea.parquet is no longer written, and is kept (retiring it would be a deletion). _tree_frontier
still reports the whole served tree's newest observation; the run prints each dataset's.

REQUIRES BEA_API_KEY (environment, then .env); absent -> TransientError, never a silent no-op.
"""
from __future__ import annotations

import datetime as dt
import json
import os

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import (CURSOR_CAP, Deadline, RotationCycle, Tally, api_key, cursors_from_table,
                      finalize, load_rotation, merge_cursor_map, rotate_after, save_rotation)

SOURCE = "bea"
DEDUP = ("series_key", "obs_date")
BUDGET_MIN = float(os.environ.get("AQUEDUCT_BEA_BUDGET_MIN", "35"))
# Years of overlap re-requested each run. BEA revises prior years on a normal schedule, so a
# window that starts exactly at the stored frontier would never see those corrections.
LOOKBACK_YEARS = 3
# THE DATASETS WHOSE KEYS ARE SOUND (bea design review R1104, 2026-09-23). The other five -
# Regional (`LineCode:GeoFips`), InputOutput (`Row|Col`), MNE, ITA (no frequency) and GDPbyIndustry
# (a dimension dropped) - hold 566,894 keys whose values COLLIDE across tables, so refreshing them
# would extend a patchwork users already download. They are not touched here; their fix is a re-key
# (pending-actions 0ad, the owner's to sequence behind the frozen D1 sync).
SOUND = ("NIPA", "NIUnderlyingDetail", "FixedAssets", "UnderlyingGDPbyIndustry", "IIP",
         "IntlServTrade", "IntlServSTA")
# Per-group refresh record (blob-routed): {"<Dataset>/<stem>.parquet": {"last_full": "YYYY-MM-DD"}}.
GROUP_STATE = "_group_refresh.json"
# Once a year a group is re-pulled with Year=ALL, so revisions older than the lookback are not
# spliced under a newer vintage for ever (review R1104, condition 4).
FULL_REPULL_DAYS = 365
# The store was built by one Year=ALL ingest from the manifest dated 2026-06-03: that is every
# group's first "full" pull, so the first yearly re-pull falls in 2027 rather than all at once now.
INITIAL_FULL = "2026-06-03"
# A group whose newest stored year is this far behind is DISCONTINUED (NIPA holds tables ending in
# 1966): between yearly full re-pulls it is skipped, and it does not drag any window back.
DISCONTINUED_YEARS = 3


def current_vintage(unit):
    """None by design: BEA publishes no library-wide vintage or last-modified feed, so the
    cadence gates the fetch and merge dedup makes a re-pull harmless. A fabricated token would
    either freeze the source or make it re-pull for ever."""
    return None


def _tree_frontier(out_dir: str) -> dt.date | None:
    """Newest obs_date across the WHOLE bea tree — which is the store that actually serves.

    _resolve_bea opens `clean_full/bea/` as ONE dataset and exact-matches series_key, so the
    served store is every parquet under that directory: 591 per-dataset files from an earlier
    full ingest (67,445,770 rows / 913,230 series) PLUS the bea.parquet this fetcher writes
    (106,074 rows / 17,699 series). Taking the frontier from bea.parquet alone read
    2026-01-01 while the tree was already at 2026-04-01 — three months stale, from a file
    holding under 2% of the series.

    Too-early a start is only wasteful (merge dedups the overlap), so this was not losing
    data. But it is the wrong store: if the grouped file were ever AHEAD of the tree the
    window would begin after data the tree still lacks, and the gap would be silent.

    Uses per-file column STATISTICS, not a read: pulling 67.4M obs_date values to compute one
    max would cost more than the fetch it is sizing.

    R36 — THIS WALKED THE TREE WITH A RAW LOCAL GLOB, so it did nothing in the only place it
    matters. `glob.glob(out_dir/**)` and `pq.ParquetFile(path)` both address the local disk;
    under AQUEDUCT_BACKEND=r2 that directory does not exist on the runner, so the loop had
    nothing to iterate, `best` stayed None, and the caller fell back to the grouped
    bea.parquet — reinstating, silently and only in CI, the exact 2026-01-01-vs-2026-04-01
    staleness this function was written to remove. It looked correct in every local run,
    which is what let it survive: the local and blob paths resolve to the same file there.

    Both halves are now blob-routed. The listing must be RECURSIVE: bea is one of the stores
    that is not flat (clean_full/bea/<Dataset>/<Table>.parquet), and the default
    non-recursive listing returns [] for it — the same empty answer as a missing store.
    """
    best = None
    for rel in blob.list_parquets(out_dir, recursive=True):
        f = os.path.join(out_dir, rel)
        try:
            md = blob.read_metadata(f)
            idx = md.schema.names.index("obs_date") if "obs_date" in md.schema.names else None
            if idx is None:
                continue
            for rg in range(md.num_row_groups):
                st = md.row_group(rg).column(idx).statistics
                if st is None or st.max is None:
                    continue
                v = st.max
                v = v if isinstance(v, dt.date) else dt.date.fromisoformat(str(v)[:10])
                if best is None or v > best:
                    best = v
        except Exception:                                    # noqa: BLE001
            continue                                         # one unreadable file must not blind the rest
    return best


def _group_units(out_dir) -> list:
    """The refreshable units: every stored group file of a SOUND dataset, as
    '<Dataset>/<stem>.parquet'. Only files that exist - a table the manifest lists but the ingest
    never wrote has no served series to keep fresh."""
    units = []
    for rel in blob.list_parquets(out_dir, recursive=True):
        rel = rel.replace("\\", "/")
        parts = rel.split("/")
        if len(parts) == 2 and parts[0] in SOUND:
            units.append(rel)
    return sorted(units)


def _fetch(ig, M, rel, year) -> "pa.Table":
    """One group, through the ingester's own parse (jobs/ingest_bea_full.fetch_*), strict: a call
    that exhausts its retries raises instead of reading as an empty answer."""
    dataset, fname = rel.split("/")
    stem = fname[:-len(".parquet")]
    extra = None
    if dataset in ("NIPA", "NIUnderlyingDetail"):
        sk, ds, vs = ig.fetch_table_freq(dataset, stem, year=year, strict=True)
    elif dataset == "FixedAssets":
        sk, ds, vs = ig.fetch_fixedassets(stem, year=year, strict=True)
    elif dataset == "UnderlyingGDPbyIndustry":
        sk, ds, vs = ig.fetch_under_gdpbyindustry(M, stem[1:], year=year, strict=True)
    elif dataset == "IIP":
        sk, ds, vs, extra = ig.fetch_iip(M, year=year, strict=True)
    elif dataset == "IntlServTrade":
        sk, ds, vs, extra = ig.fetch_intlservtrade(M, year=year, strict=True)
    elif dataset == "IntlServSTA":
        sk, ds, vs, extra = ig.fetch_intlservsta(M, year=year, strict=True)
    else:
        raise ValueError(f"not a sound dataset: {dataset}")
    cols = {"series_key": pa.array(sk, pa.string()), "obs_date": pa.array(ds, pa.date32()),
            "value": pa.array(vs, pa.float64())}
    if extra is not None:
        cols["time_series_id"] = pa.array(extra, pa.string())
    return pa.table(cols)


def _stored_profile(path) -> dict:
    """rows, newest year, EXACT duplicate rows, CONFLICTING (key, date) pairs - by DuckDB over one
    local copy (blob-routed). Measured by the review: 54 files of the sound datasets carry exact
    duplicates (NIPA 32, NIUnderlyingDetail 11, FixedAssets 11; worst 16.6%), which the merge
    collapses - past the default 97% shrink guard."""
    import duckdb                                                    # noqa: PLC0415
    copy = blob.local_copy(path)
    if copy is None:
        raise FileNotFoundError(path)
    try:
        con = duckdb.connect()
        try:
            f = copy[0].replace("\\", "/").replace("'", "''")
            rows, mx, kdv, kd = con.execute(
                f"SELECT count(*), max(obs_date), "
                f"count(DISTINCT (series_key, obs_date, value)), "
                f"count(DISTINCT (series_key, obs_date)) FROM read_parquet('{f}')").fetchone()
        finally:
            con.close()
    finally:
        if copy[1]:
            try:
                os.remove(copy[0])
            except OSError:
                pass
    return {"rows": int(rows), "max_year": mx.year if mx else None,
            "exact_dups": int(rows) - int(kdv), "conflicts": int(kdv) - int(kd)}


def update(unit, since) -> Result:
    """v1 (2026-09-23): refresh every stored group of the seven SOUND datasets in place, under a
    RotationCycle. The bea.parquet loop that wrote a shadowed copy is retired (every one of its
    17,699 keys is also in a subdirectory, and the subdirectory sorts first, so it never served)."""
    key = api_key("BEA_API_KEY")
    if not key:
        raise TransientError(
            f"{SOURCE}: BEA_API_KEY is not set (checked the environment and .env), so nothing "
            f"can be fetched. Existing data kept.")
    os.environ.setdefault("BEA_API_KEY", key)
    from jobs import ingest_bea_full as ig                   # rate limiter + parser + keys

    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    try:
        M = ig.load_manifest()
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{SOURCE}: BEA parameter manifest unavailable: {e!r}") from e

    units = _group_units(out_dir)
    if not units:
        raise TransientError(f"{SOURCE}: no group file of a sound dataset is visible under "
                             f"{out_dir} - the store is unreachable, not current")
    raw = blob.read_bytes(os.path.join(out_dir, GROUP_STATE))
    try:
        gstate = json.loads(raw.decode("utf-8")) if raw else {}
    except ValueError:
        gstate = {}

    def _save_gstate():
        blob.write_bytes_atomic(os.path.join(out_dir, GROUP_STATE),
                                json.dumps(gstate, indent=1, sort_keys=True)
                                .encode("utf-8"))

    tally = Tally()
    dl = Deadline(minutes=BUDGET_MIN)
    today = dt.date.today()
    cycle = RotationCycle(out_dir, units)
    cursors: dict[str, str] = {}
    total = 0
    frontier_by_ds: dict[str, str] = {}
    for rel in rotate_after(units, load_rotation(out_dir)):
        if cycle.done(rel):
            continue                         # refreshed this cycle: no work owed (R1105 P1)
        if dl.spent():
            n = cycle.defer_unvisited(tally, label=lambda u: f"{u} (budget {BUDGET_MIN:.0f} min)")
            print(f"[{SOURCE}] budget of {BUDGET_MIN:.0f} min spent; {n} group(s) not yet "
                  f"refreshed this cycle", flush=True)
            break
        save_rotation(out_dir, rel)
        path = os.path.join(out_dir, rel)
        fails_before = cycle.failures(tally)
        try:
            prof = _stored_profile(path)
        except Exception as e:                               # noqa: BLE001
            tally.transient_unit(f"{rel}: stored group unreadable - {type(e).__name__}: {e}")
            cycle.visit(rel, failed=True)
            continue
        if prof["conflicts"]:
            # A sound dataset should hold none; a merge would silently pick one value per pair.
            tally.structural_unit(f"{rel}: {prof['conflicts']:,} (key, date) pair(s) with "
                                  f"different values in the stored file - not merged")
            cycle.visit(rel, failed=True)
            continue
        g = gstate.setdefault(rel, {"last_full": INITIAL_FULL})
        full_due = (today - dt.date.fromisoformat(g["last_full"])).days >= FULL_REPULL_DAYS
        mx = prof["max_year"]
        if not full_due and (mx is None or mx < today.year - DISCONTINUED_YEARS):
            tally.empty_unit(f"{rel}: discontinued (newest {mx}), next full re-pull due "
                             f"{dt.date.fromisoformat(g['last_full']) + dt.timedelta(days=FULL_REPULL_DAYS)}")
            cycle.visit(rel)
            continue
        if full_due or mx is None:
            year = "ALL"
            start = None
        else:
            start = mx - LOOKBACK_YEARS
            # BEA's Year is a LIST, not a range (measured, see the old loop's note in git history)
            year = ",".join(str(y) for y in range(start, today.year + 2))
        try:
            tbl = _fetch(ig, M, rel, year)
        except Exception as e:                               # noqa: BLE001
            tally.transient_unit(f"{rel}: fetch failed - {type(e).__name__}: {str(e)[:140]}")
            cycle.visit(rel, failed=True)
            continue
        if tbl.num_rows == 0:
            # The window starts inside the stored data, so a correct answer is never empty: an
            # empty one is a failure the strict call did not see (an unknown API error reads as
            # 'no data'), never a quiet group (review R1104, condition 3).
            tally.transient_unit(f"{rel}: 0 rows for Year={year[:40]} although the store holds "
                                 f"data through {mx}")
            cycle.visit(rel, failed=True)
            continue
        ratio = ((prof["rows"] - prof["exact_dups"]) / prof["rows"]) if prof["exact_dups"] else 0.97
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP,
                                          min_ratio=min(0.97, ratio))
        except Exception as e:                               # noqa: BLE001
            tally.structural_unit(f"{rel}: merge refused - {str(e)[:160]}")
            cycle.visit(rel, failed=True)
            continue
        total += n
        tally.added_unit(max(0, n - (prof["rows"] - prof["exact_dups"])), rel)
        merge_cursor_map(cursors, cursors_from_table(tbl, cap=CURSOR_CAP), cap=CURSOR_CAP)
        if year == "ALL":
            g["last_full"] = today.isoformat()
        _save_gstate()
        ds_name = rel.split("/")[0]
        if md and str(md) > frontier_by_ds.get(ds_name, ""):
            frontier_by_ds[ds_name] = str(md)
        cycle.visit(rel, failed=cycle.failures(tally) > fails_before)
    cycle.close_if_complete(tally)
    if frontier_by_ds:
        print(f"[{SOURCE}] newest merged per dataset this pass: "
              + ", ".join(f"{k} {v}" for k, v in sorted(frontier_by_ds.items())), flush=True)
    print(f"[{SOURCE}] NOT refreshed (keys collide across tables, R1104): Regional, InputOutput, "
          f"MNE, ITA, GDPbyIndustry", flush=True)
    # The frontier of the WHOLE served tree, for the reason _tree_frontier gives.
    tf = _tree_frontier(out_dir)
    res = finalize(tally, total, tf.isoformat() if tf else (since or None), source=SOURCE,
                   series_cursors=cursors or None)
    if len(cursors) >= CURSOR_CAP:
        res.cursor_cap_hit = True        # the orchestrator books the owed full re-derive
    return res
