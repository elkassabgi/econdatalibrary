"""Shared fetcher for IMF sources that are SERVED but have no updater at all.

These are not stale relays reporting healthy — they are worse. Ten IMF sources
(imf_hpdd, imf_pctot, imf_bopagg, imf_fiscaldecentralization, imf_gender_*,
imf_pgi, imf_pgcs, imf_unsdg_imf_inputs, imf_psbsfad — 44,979 series) sit in the
catalog and in the worker's supported list, downloadable today, with NO registry
entry and therefore no fetcher. They have never had one. They cannot go stale
because nothing ever attempts them.

They looked unfixable because IMF's exact-id lookup misses them: the flows were
RENAMED (PSBSFAD->PSBS, PCTOT->CTOT, HPDD->HPD, GENDER_*->GS_*, BOPAGG->BOP_AGG,
FISCALDECENTRALIZATION->FD). Reading "no such flow" as "discontinued" nearly cost
45,000 series (ledger R75).

WHY A CONFIG FILE AND NOT CONSTANTS. Repairing these in place means reconstructing
OUR series ids from IMF's current vocabulary — IMF says COUNTRY=AFG where our ids
say AF, INDICATOR=G63G_S13_POFYGDP where ours say GGXWDG_GDP. Those maps are
derived by tools/prove_direct_repair.py from VALUE agreement across every shared
observation, and they run to hundreds of entries. Retyping them into a module is
exactly the kind of transcription that fails silently and swaps two real series
under ids people cite, so the prover emits JSON and this reads it. Nobody hand-edits
a code map.

THE RUNTIME SELF-CHECK IS THE POINT. A wrong map does not raise — it mints new ids
next to the old ones, doubling the source while every live id stays frozen, and the
run goes green. So before merging anything, the reconstructed keys are checked
against the ids the catalog already holds. Below the floor, the unit fails
STRUCTURALLY and nothing is written.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sqlite3
import xml.etree.ElementTree as ET

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from . import _imf_direct as _base
from jobs import ingest_imf_direct as ing

DEDUP = ("series_key", "obs_date")
CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_imf_maps")
# A repair that reproduces fewer than this share of the ids we already publish is a
# re-key wearing a repair's clothes. Refuse rather than merge.
ID_FLOOR = 0.95


def load(source_id: str) -> dict:
    p = os.path.join(CONF_DIR, f"{source_id}.json")
    return json.load(io.open(p, encoding="utf-8"))


def _period(p: str, conv: str):
    p = (p or "").strip()
    try:
        if len(p) == 4:
            return dt.date(int(p), 1, 1) if conv == "start" else dt.date(int(p), 12, 31)
        if "-M" in p:
            y, m = p.split("-M"); y, m = int(y), int(m)
        elif "-Q" in p:
            y, q = p.split("-Q"); y, m = int(y), int(q) * 3
            if conv == "start":
                m -= 2
        elif len(p) == 7 and "-" in p:
            y, m = p.split("-"); y, m = int(y), int(m)
        elif len(p) == 10:
            y, m, d = int(p[:4]), int(p[5:7]), int(p[8:10])
            return dt.date(y, m, 1) if conv == "start" else dt.date(y, m, d)
        else:
            return None
        if conv == "start":
            return dt.date(y, m, 1)
        return dt.date(y + (m == 12), (m % 12) + 1, 1) - dt.timedelta(days=1)
    except (ValueError, TypeError):
        return None


def _catalog_ids(source_id: str) -> set:
    db = os.path.join(config.ROOT if hasattr(config, "ROOT") else ".",
                      "data", "catalog.db")
    if not os.path.exists(db):
        db = os.path.join("data", "catalog.db")
    try:
        con = sqlite3.connect(db)
        return {r[0].split(":", 1)[1] for r in con.execute(
            "SELECT series_id FROM series WHERE source_id=?", (source_id,))}
    except Exception:                                         # noqa: BLE001
        return set()


def run(source_id: str) -> Result:
    cfg = load(source_id)
    flow, agency = cfg["flow"], cfg["agency"]
    tally = Tally()
    out_dir = config.source_dir(source_id)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{source_id}.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0

    try:
        raw = ing.http_get(f"{ing.BASE}/data/{agency},{flow}/all")
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise TransientError(f"{flow}: unparseable XML ({len(raw):,} B): {e}") from e
    except Exception as e:                                    # noqa: BLE001
        raise TransientError(f"{flow}: {e!r}") from e

    series = [e for e in root.iter() if e.tag.split("}")[-1] == "Series"]
    if not series:
        tally.structural_unit(f"{flow}: 200 with zero series")
        return finalize(tally, before, None, source=source_id)

    slots, maps = cfg["slots"], cfg["code_maps"]
    conv, arity, prefix = cfg["date_convention"], cfg["arity"], cfg["key_prefix"]
    keys, dates, vals = [], [], []
    n_unmapped = n_empty = n_bad = 0
    for s in series:
        parts = [None] * arity
        ok = True
        for dim, slot in slots.items():
            v = s.attrib.get(dim)
            m = maps.get(dim) or {}
            mapped = m.get(v, v if not m else None)
            if mapped is None:
                ok = False
                break
            parts[slot] = mapped
        if not ok or any(p is None for p in parts):
            n_unmapped += 1
            continue
        skey = f"{prefix}:" + ".".join(parts)
        for o in s:
            if o.tag.split("}")[-1] != "Obs":
                continue
            v = o.attrib.get("OBS_VALUE")
            if v in (None, "", "NaN"):
                n_empty += 1
                continue
            d = _period(o.attrib.get("TIME_PERIOD", ""), conv)
            if d is None:
                n_bad += 1
                continue
            try:
                fv = float(v)
            except ValueError:
                n_bad += 1
                continue
            keys.append(skey)
            dates.append(d)
            vals.append(fv)

    if not keys:
        tally.structural_unit(f"{flow}: no usable observations")
        return finalize(tally, before, None, source=source_id)

    # THE SELF-CHECK. A wrong map does not raise; it invents ids beside the real
    # ones and reports success. Compare what we just built against what we publish.
    known = _catalog_ids(source_id)
    built = set(keys)
    if known:
        hit = len(built & known) / len(known)
        if hit < ID_FLOOR:
            print(f"[imf_mapped] FAIL {source_id}: reconstructed keys match only "
                  f"{100 * hit:.1f}% of the {len(known):,} ids in the catalog "
                  f"(floor {100 * ID_FLOOR:.0f}%). The code map is wrong or IMF "
                  f"changed a vocabulary — merging would MINT NEW IDS beside the "
                  f"live ones and leave every published series frozen. Refusing.",
                  flush=True)
            tally.structural_unit(f"{flow}: id match {100 * hit:.1f}%")
            return finalize(tally, before, None, source=source_id)
        print(f"[imf_mapped] {source_id}: {len(keys):,} obs / {len(built):,} series, "
              f"{100 * hit:.1f}% of catalog ids reproduced"
              + (f", {n_unmapped:,} series skipped (unmapped codes)"
                 if n_unmapped else ""), flush=True)

    if n_unmapped:
        print(f"[imf_mapped] WARNING {source_id}: {n_unmapped:,} upstream series "
              f"carry codes absent from the map — IMF extended a vocabulary. "
              f"Re-run tools/prove_direct_repair.py --emit to refresh it.", flush=True)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before), flow)
    cursors = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if d is not None and (k not in cursors or d > cursors[k]):
            cursors[k] = d
    return finalize(tally, n, md, source=source_id,
                    series_cursors={k: v.isoformat() for k, v in cursors.items()})


def vintage(source_id: str):
    try:
        return _base.vintage(load(source_id)["flow"])
    except Exception:                                         # noqa: BLE001
        return None
