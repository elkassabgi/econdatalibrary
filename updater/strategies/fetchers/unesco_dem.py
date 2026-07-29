"""UNESCO UIS demographic & socio-economic series — DIRECT from api.uis.unesco.org.

WHY. unesco_dem's 7,080 series arrive via DBnomics, whose UNESCO index was last
refreshed 2022-04-04 — over four years ago. UIS itself runs a public API whose
catalogue reports every theme "lastUpdate 02/09/2026, February 2026 Data Release".
Four years stale on our side, current at the publisher, and nothing in our pipeline
could tell, because the change signal we watched belonged to the relay (ledger R73).

ONLY THIS UNESCO SOURCE IS REPAIRABLE THIS WAY, and that was measured rather than
assumed. Of our five unesco_* sources, the indicator codes we publish are present in
UIS's live catalogue at: dem 35/35 (100%), clte 21/408 (5.1%), film 1/76 (1.3%),
cltt 0/34, inno 0/638. The current API exposes 30 CULTURE and 12 SCIENCE indicators
against the hundreds the DBnomics-era snapshot carried, so the other four cannot be
rebuilt from it. That is a statement about this endpoint, NOT a claim that those
series are gone — UIS bulk downloads and the SDG database are unchecked (R75).

KEYS RECONSTRUCT WITH NO TRANSLATION: our `UNESCO_DEM:200101.ABW.A` is UIS's own
indicatorId, geoUnit and an annual frequency marker. Verified before writing any of
this: for indicator 200101, 229 of 233 rebuilt keys land on published ids.

Dates are period-START (01-01), this source's existing convention — every one of its
278,720 observations is stamped 01-01 and that is left alone.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import urllib.parse
import urllib.request

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "unesco_dem"
DEDUP = ("series_key", "obs_date")
PREFIX = "UNESCO_DEM"
BASE = "https://api.uis.unesco.org/api/public"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
ID_FLOOR = 0.95


def _get(url: str, timeout: int = 300):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def current_vintage(unit):
    """UIS's own release version and per-theme lastUpdate.

    The publisher's field, deliberately — these sources sat four years stale because
    the previous signal certified DBnomics' index rather than UNESCO's release.
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
    return f"uis:{cur.get('version')}:{themes}" or None


def _published_indicators(source_id: str):
    """Indicator codes THIS source publishes, read from our own catalog."""
    import sqlite3
    db = os.path.join(config.ROOT, "data", "catalog.db")
    if not os.path.exists(db):
        db = os.path.join("data", "catalog.db")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    out = set()
    for (sid,) in con.execute("SELECT series_id FROM series WHERE source_id=?",
                              (source_id,)):
        parts = sid.split(":", 2)
        if len(parts) == 3:
            bits = parts[2].split(".")
            if len(bits) >= 3:
                out.add(".".join(bits[:-2]))
    return out


def _catalog_ids(source_id: str) -> set:
    import sqlite3
    db = os.path.join(config.ROOT, "data", "catalog.db")
    if not os.path.exists(db):
        db = os.path.join("data", "catalog.db")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    return {r[0].split(":", 1)[1] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (source_id,)) if ":" in r[0]}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{SOURCE}.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0
    tally = Tally()

    inds = sorted(_published_indicators(SOURCE))
    if not inds:
        tally.structural_unit("no published indicator codes to fetch")
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = [], [], []
    n_bad = 0
    failed = []
    for ind in inds:
        # One request per INDICATOR returns every country (233 for 200101), which is
        # 35 calls rather than 35 x 233. Each indicator is independent, so one that
        # fails is recorded and cannot sink the rest.
        try:
            d = _get(f"{BASE}/data/indicators?indicator={urllib.parse.quote(ind)}")
        except Exception as e:                                # noqa: BLE001
            failed.append(f"{ind}:{type(e).__name__}")
            continue
        for r in d.get("records", []) or []:
            v, y = r.get("value"), r.get("year")
            gu, iid = r.get("geoUnit"), r.get("indicatorId")
            if v is None or y is None or not gu or not iid:
                continue
            try:
                fv, yr = float(v), int(y)
            except (TypeError, ValueError):
                n_bad += 1
                continue
            keys.append(f"{PREFIX}:{iid}.{gu}.A")
            dates.append(dt.date(yr, 1, 1))                   # period-START
            vals.append(fv)

    if failed:
        print(f"[unesco_dem] {len(failed)} indicator(s) failed: {failed[:5]}",
              flush=True)
        if len(failed) > len(inds) // 2:
            raise TransientError(f"unesco_dem: {len(failed)}/{len(inds)} indicators "
                                 f"unreachable")
    if not keys:
        tally.structural_unit("UIS returned no usable observations")
        return finalize(tally, before, None, source=SOURCE)

    known = _catalog_ids(SOURCE)
    built = set(keys)
    if known:
        hit = len(built & known) / len(known)
        if hit < ID_FLOOR:
            # A wrong reconstruction does not raise — it mints ids beside the live
            # ones and leaves every published series frozen while reporting success.
            print(f"[unesco_dem] FAIL: rebuilt keys match only {100 * hit:.1f}% of "
                  f"the {len(known):,} published ids (floor "
                  f"{100 * ID_FLOOR:.0f}%). Refusing to merge.", flush=True)
            tally.structural_unit(f"id match {100 * hit:.1f}%")
            return finalize(tally, before, None, source=SOURCE)
        print(f"[unesco_dem] {len(keys):,} obs / {len(built):,} series, "
              f"{100 * hit:.1f}% of published ids reproduced"
              + (f", {n_bad:,} unreadable values" if n_bad else ""), flush=True)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before), SOURCE)
    cursors = {}
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if d is not None and (k not in cursors or d > cursors[k]):
            cursors[k] = d
    return finalize(tally, n, md, source=SOURCE,
                    series_cursors={k: v.isoformat() for k, v in cursors.items()})
