"""Shared UNESCO UIS fetcher for the multi-segment sources (natmon, sdg).

WHY THESE EXIST AT ALL. unesco_natmon (1,876,322 obs) and unesco_sdg (734,662 obs)
sat in the local store with NO catalog rows, NO objects on R2 and a denylist entry —
hosted nowhere, served to nobody, and invisible to every freshness check because a
source with no registry entry is never iterated. Their data stops in 2020; UIS is
still publishing.

WHY A SEPARATE MODULE FROM unesco_dem. dem's keys are `{indicator}.{geo}.{freq}`.
These carry a fourth segment whose POSITION depends on the geoUnit's type, measured
over the full store rather than assumed:

    {indicator}.NA.{ISO3}.A              79,331 natmon keys   geoUnit is NATIONAL
    {indicator}.{region_slug}.NA.A       19,333 natmon keys   geoUnit is REGIONAL

/definitions/geounits supplies that type ({id, name, type}), and the regional slug is
a lowercase-underscore transform of the region's display name ("AIMS: Asia and the
Pacific" -> "aims_asia_and_the_pacific"). Indicator codes themselves contain dots
(`E.5T8.FOREIGN.ORG40500`), so the indicator is everything BEFORE the trailing three
segments — never a fixed field count.

PROVEN BEFORE BEING WRITTEN, on the full published indicator set, no sampling:
    natmon  421/428 indicators still publish; 420 of those 421 rebuild to ids we
            already publish; indicator `10` matched 110 of 110 upstream geoUnits
    sdg     68,067 exact id matches, 0 fetch failures, 0 unclassified geoUnits

RECALL IS DELIBERATELY NOT THE GATE (ledger R105). Rebuilding reproduces only ~72%
(natmon) and ~67% (sdg) of our stored ids — not because the reconstruction is wrong
but because UIS's current release carries far fewer country x indicator cells than
the 2022-era snapshot we hold. A recall floor would refuse to merge and freeze a
healthy source. The merge is never-shrink, so the series upstream has dropped keep
their stored values and the refresh is purely additive.
"""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import io
import json
import os
import re
import urllib.parse
import urllib.request

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after, save_rotation)

BASE = "https://api.uis.unesco.org/api/public"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
GRAMMAR_FLOOR = 0.95
WORKERS = int(os.environ.get("AQUEDUCT_UIS_WORKERS", "6"))
# Indicators submitted to the pool per deadline check. ex.map() queues everything it is
# given at once, so the chunk size — not the loop — is what actually bounds the run.
_UIS_CHUNK = int(os.environ.get("AQUEDUCT_UIS_CHUNK", "24"))
HERE = os.path.dirname(os.path.abspath(__file__))


def _get(url: str, timeout: int = 300):
    return json.load(urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=timeout))


def _slug(text) -> str:
    """'AIMS: Asia and the Pacific' -> 'aims_asia_and_the_pacific'."""
    return re.sub(r"[^A-Za-z0-9]+", "_", str(text).strip()).strip("_").lower()


def load_cfg(source_id: str) -> dict:
    p = os.path.join(HERE, "_uis_maps", f"{source_id}.json")
    return json.load(io.open(p, encoding="utf-8"))


def current_vintage(unit):
    """UIS's own release version + per-theme lastUpdate — the PUBLISHER's field.

    These sources froze in the first place because the change signal belonged to
    DBnomics, whose hash stays constant while UNESCO keeps publishing (R73).
    """
    try:
        v = _get(f"{BASE}/versions", timeout=120)
    except Exception:                                         # noqa: BLE001
        return None
    if not isinstance(v, list) or not v:
        return None
    cur = v[0]
    themes = ";".join(sorted(f"{t.get('theme')}={t.get('lastUpdate')}"
                             for t in cur.get("themeDataStatus", [])))
    return f"uis:{cur.get('version')}:{themes}"


def _geounits():
    """{id: (type, name)} — the National/Regional split the key form depends on."""
    return {g["id"]: (g.get("type"), g.get("name"))
            for g in _get(f"{BASE}/definitions/geounits", timeout=180)}


def update(source_id: str, unit, since) -> Result:
    cfg = load_cfg(source_id)
    prefix = cfg["key_prefix"]
    out_dir = config.source_dir(source_id)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{source_id}.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0
    tally = Tally()

    inds = cfg.get("indicators") or []
    if not inds:
        tally.structural_unit("no indicator codes in map")
        return finalize(tally, before, None, source=source_id)

    geo = _geounits()

    def fetch(ind):
        try:
            d = _get(f"{BASE}/data/indicators?indicator={urllib.parse.quote(ind)}")
            return ind, (d.get("records") or []), None
        except Exception as e:                                # noqa: BLE001
            return ind, [], type(e).__name__

    keys, dates, vals = [], [], []
    failed, unclassified, n_bad = [], set(), 0

    # BOUND THIS BELOW THE ORCHESTRATOR'S 45-MINUTE CAP, AND ROTATE.
    # Measured in cloud state: unesco_natmon took 50.0 min and unesco_sdg 48.5 min — both
    # OVER the cap. They happened to complete, but there is exactly one merge_and_write in
    # this module and it is AFTER the loop, so a kill one slow day later does not truncate
    # the run, it DISCARDS it: every indicator fetched, nothing stored, and the log looks
    # identical to a busy healthy run (R243, which is precisely how worldbank_wdi stored
    # nothing for its entire existence).
    #
    # Submissions are CHUNKED because ex.map(fetch, inds) queues every indicator up front —
    # breaking out of that loop stops consuming results but not the work, and the executor's
    # shutdown then waits for all of it anyway. Chunking is what makes the budget real.
    #
    # The order rotates, because a bound over a fixed order is a truncation, not a budget
    # (R190): without a bookmark the same leading indicators would be re-fetched every run
    # and the tail would never be reached at all.
    budget_min = float(os.environ.get("UIS_BUDGET_MIN", "35"))
    dl = Deadline(minutes=budget_min)
    order = rotate_after(list(inds), load_rotation(out_dir))
    stopped_early = False
    done_inds = 0
    last_ind = ""
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for start in range(0, len(order), _UIS_CHUNK):
            if dl.spent():
                stopped_early = True
                print(f"[{source_id}] budget of {budget_min:.0f} min spent after "
                      f"{dl.elapsed_min():.1f} min — {done_inds:,}/{len(order):,} "
                      f"indicators done, {len(order) - done_inds:,} deferred to the next "
                      f"tick (resuming after {last_ind!r})", flush=True)
                break
            chunk = order[start:start + _UIS_CHUNK]
            for ind, recs, err in ex.map(fetch, chunk):
                done_inds += 1
                last_ind = ind
                if err:
                    failed.append(f"{ind}:{err}")
                    continue
                for r in recs:
                    v, y = r.get("value"), r.get("year")
                    gu, iid = r.get("geoUnit"), r.get("indicatorId")
                    if v is None or y is None or not gu or not iid:
                        continue
                    gtype, gname = geo.get(gu, (None, None))
                    if gtype == "NATIONAL":
                        k = f"{prefix}:{iid}.NA.{gu}.A"
                    elif gtype == "REGIONAL":
                        k = f"{prefix}:{iid}.{_slug(gname or gu)}.NA.A"
                    else:
                        # A geoUnit /definitions/geounits does not classify. COUNTED,
                        # never guessed — picking a form here is how part of a source
                        # silently forks into ids that resolve to nothing.
                        unclassified.add(gu)
                        continue
                    try:
                        fv, yr = float(v), int(y)
                    except (TypeError, ValueError):
                        n_bad += 1
                        continue
                    keys.append(k)
                    dates.append(dt.date(yr, 1, 1))           # period-START
                    vals.append(fv)

    # Bookmark BEFORE the gates below, which can return early: the indicators fetched this
    # tick were fetched whatever the grammar check then decides, and re-walking the same
    # prefix tomorrow is the pathology rotation exists to prevent.
    if last_ind:
        save_rotation(out_dir, last_ind)

    if failed:
        print(f"[{source_id}] {len(failed)} indicator(s) failed: {failed[:5]}",
              flush=True)
        # Ratio measured against what this tick ATTEMPTED, not the whole indicator list.
        # Against len(inds) a budget-stopped pass could never reach the majority threshold
        # however badly upstream was failing — the check would quietly stop existing.
        if len(failed) > max(done_inds, 1) // 2:
            raise TransientError(
                f"{source_id}: {len(failed)}/{done_inds} attempted indicators unreachable")
    if unclassified:
        print(f"[{source_id}] {len(unclassified)} geoUnit(s) unclassified by "
              f"/definitions/geounits, SKIPPED (not guessed): "
              f"{sorted(unclassified)[:5]}", flush=True)
    if not keys:
        tally.structural_unit("UIS returned no usable observations")
        return finalize(tally, before, None, source=source_id)

    # --- key-grammar gate (NOT recall) -----------------------------------------
    # If the grammar is wrong, almost no indicator group produces a matching id. If
    # upstream merely trimmed its release, nearly every group still matches on the
    # countries it does publish. Both sides derive the group key identically —
    # deriving them differently makes the sets share no keys and silently DISABLES
    # the check instead of failing it.
    known_groups = set(cfg.get("known_groups") or [])
    built_groups = {k.split(":", 1)[1].split(".")[0] for k in keys}
    checkable = built_groups & known_groups
    if known_groups and not checkable:
        # AN EMPTY CHECK IS NOT A PASS: a reconstruction that lands in the wrong
        # namespace is exactly the case that leaves nothing to compare.
        print(f"[{source_id}] FAIL: none of the {len(built_groups):,} rebuilt "
              f"indicator groups match any of the {len(known_groups):,} recorded "
              f"ones — wrong namespace. Refusing to merge.", flush=True)
        tally.structural_unit("no rebuilt indicator group matches a recorded one")
        return finalize(tally, before, None, source=source_id)
    if known_groups:
        agree = len(checkable) / max(len(built_groups), 1)
        if agree < GRAMMAR_FLOOR:
            print(f"[{source_id}] FAIL: only {len(checkable):,}/{len(built_groups):,} "
                  f"rebuilt indicator groups ({100 * agree:.1f}%) are groups we "
                  f"already publish (floor {100 * GRAMMAR_FLOOR:.0f}%) — the key "
                  f"grammar is wrong. Refusing to merge.", flush=True)
            tally.structural_unit(f"grammar agreement {100 * agree:.1f}%")
            return finalize(tally, before, None, source=source_id)
        print(f"[{source_id}] {len(keys):,} obs / {len(set(keys)):,} series | "
              f"grammar {len(checkable):,}/{len(built_groups):,} groups agree "
              f"({100 * agree:.1f}%)"
              + (f" | {n_bad:,} unreadable values" if n_bad else ""), flush=True)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    # State the coverage of the pass being merged. A partial sweep merging cleanly and a
    # full sweep merging cleanly produce identical row counts when upstream is quiet, so
    # without this line there is nothing in the log that distinguishes them.
    print(f"[{source_id}] merging {len(keys):,} obs from {done_inds:,}/{len(order):,} "
          f"indicators ({'PARTIAL sweep, rest next tick' if stopped_early else 'full sweep'})",
          flush=True)
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before), source_id)
    cursors = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if d is not None and (k not in cursors or d > cursors[k]):
            cursors[k] = d
    return finalize(tally, n, md, source=source_id,
                    series_cursors={k: v.isoformat() for k, v in cursors.items()})
