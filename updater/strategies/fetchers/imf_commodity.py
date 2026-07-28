"""S1 fetcher — IMF Primary Commodity Prices (PCPS), DIRECT from api.imf.org.

WHY THIS WAS REWRITTEN (2026-07-28). This fetcher used to relay DBnomics. It ran
green every day for a year — `status=no_change`, `err="no new rows"` — while serving
commodity prices frozen at 2025-06. Nothing was broken: the change signal was
DBnomics' own dataset hash, and DBnomics stopped indexing IMF/PCPS on 2025-07-16, so
"nothing changed" was true of the relay and false of the publisher. IMF has been
publishing straight through to 2026-M06 the entire time. A borrowed freshness signal
certifies the intermediary, not the source (ledger R73).

IDS ARE PRESERVED. This is a repair, not a re-key: every one of the 1,236 live
series_ids keeps its exact identity. That takes a translation, because IMF's current
API uses a different vocabulary for the same concepts than the DBnomics-era ids do.
The map below was proven by VALUE AGREEMENT on shared (indicator, frequency, period)
points, not by reading the code names — INDEX_PCH and INDEX_PCHY are both "percent
change" and pairing them the wrong way round would have silently swapped two real
series under ids people already cite. Each mapping below agrees ~92-98% while every
alternative pairing agrees <=5.6%, which is what makes it a proof rather than a
plausible guess. The residual disagreement is rounding in IMF's computed aggregates
(individual commodities like PGOLD/PCOPP/POILAPSP match to ratio 1.00000000 exactly;
aggregates such as PALLFNF differ by ~0.005% median, max ~0.5%), so the two vintages
are the same data and merge-with-new-wins simply adopts IMF's current figures.

DATES ARE PERIOD-START (2025-M06 -> 2025-06-01), matching what this source already
stores. The other imf_*_direct sources use period-END; imposing that here would have
re-stamped all 230,092 published rows and changed every date users have downloaded.
The convention belongs to the source, not to the transport.

IMF revises history each release, so we re-fetch the whole dataset and MERGE (dedup
series_key+obs_date, new wins on revision, never-shrink).
"""
from __future__ import annotations
import datetime as dt
import os
import xml.etree.ElementTree as ET

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from . import _imf_direct as _base
from jobs import ingest_imf_direct as ing

SOURCE = "imf_commodity"
DEDUP = ("series_key", "obs_date")
FLOW, AGENCY = "PCPS", "IMF.RES"

# Proven by value agreement — see the module docstring. Do not "tidy" these by name.
AREA_MAP = {"G001": "W00"}                       # IMF COUNTRY -> our REF_AREA
UNIT_MAP = {"INDEX": "IX",                       # index level
            "INDEX_PCH": "PC_PP_PT",             # change vs previous period
            "INDEX_PCHY": "PC_CP_A_PT",          # change vs same period, year prior
            "USD": "USD"}                        # US dollar price


def current_vintage(unit):
    """Dataflow version — moves when IMF republishes, including back-revisions that
    rewrite history without extending it. Deliberately NOT a date-tail probe and
    emphatically not anything owned by a third party (R73)."""
    return _base.vintage(FLOW)


def _period_start(p: str):
    """SDMX TIME_PERIOD -> first day of the period (this source's convention)."""
    p = (p or "").strip()
    try:
        if len(p) == 4:                                      # 2026
            return dt.date(int(p), 1, 1)
        if "-M" in p:                                        # 2026-M06
            y, m = p.split("-M")
            return dt.date(int(y), int(m), 1)
        if "-Q" in p:                                        # 2026-Q2
            y, q = p.split("-Q")
            return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if "-S" in p:                                        # semester
            y, s = p.split("-S")
            return dt.date(int(y), (int(s) - 1) * 6 + 1, 1)
        if len(p) == 7 and "-" in p:                         # 2026-06
            y, m = p.split("-")
            return dt.date(int(y), int(m), 1)
        if len(p) == 10:                                     # 2026-06-30
            return dt.date(int(p[:4]), int(p[5:7]), 1)
    except (ValueError, TypeError):
        return None
    return None


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(),
                    tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "imf_commodity.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0
    tally = Tally()

    try:
        raw = ing.http_get(f"{ing.BASE}/data/{AGENCY},{FLOW}/all")
    except Exception as e:                                    # noqa: BLE001
        raise TransientError(f"{FLOW}: {e!r}") from e

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise TransientError(f"{FLOW}: unparseable XML ({len(raw):,} bytes): {e}") from e

    series = [e for e in root.iter() if e.tag.split("}")[-1] == "Series"]
    if not series:
        tally.structural_unit(f"{FLOW}: 200 with zero series")
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = [], [], []
    n_empty = n_baddate = n_badval = 0
    unmapped: dict[str, int] = {}
    for s in series:
        a = s.attrib
        area = AREA_MAP.get(a.get("COUNTRY"))
        unit_c = UNIT_MAP.get(a.get("DATA_TRANSFORMATION"))
        freq = a.get("FREQUENCY")
        ind = a.get("INDICATOR")
        if not (area and unit_c and freq and ind):
            # An unmapped code is a VOCABULARY CHANGE, not a row to drop quietly.
            # Passing the raw code through would mint ids that look like ours but
            # are not, silently doubling series; dropping it silently would lose
            # real data. So: skip, count, and name it loudly below.
            miss = (f"COUNTRY={a.get('COUNTRY')}" if not area else
                    f"DATA_TRANSFORMATION={a.get('DATA_TRANSFORMATION')}"
                    if not unit_c else "FREQUENCY/INDICATOR")
            unmapped[miss] = unmapped.get(miss, 0) + 1
            continue
        skey = f"IMF_COMMODITY:{freq}.{area}.{ind}.{unit_c}"
        for o in s:
            if o.tag.split("}")[-1] != "Obs":
                continue
            v = o.attrib.get("OBS_VALUE")
            if v in (None, "", "NaN"):
                n_empty += 1
                continue
            d = _period_start(o.attrib.get("TIME_PERIOD", ""))
            if d is None:
                n_baddate += 1
                continue
            try:
                fv = float(v)
            except ValueError:
                n_badval += 1
                continue
            if fv != fv:                                     # NaN
                n_badval += 1
                continue
            keys.append(skey)
            dates.append(d)
            vals.append(fv)

    if unmapped:
        print(f"[imf_commodity] WARNING unmapped codes, series SKIPPED: "
              f"{unmapped} — IMF changed a code vocabulary; extend AREA_MAP/UNIT_MAP "
              f"after verifying the new code by VALUE, not by name", flush=True)
    if n_baddate or n_badval:
        print(f"[imf_commodity] WARNING {n_baddate:,} unparseable TIME_PERIOD and "
              f"{n_badval:,} unreadable OBS_VALUE — DROPPED REAL DATA, check the "
              f"parser", flush=True)

    if not keys:
        tally.structural_unit(f"{FLOW}: series present, no usable observations")
        return finalize(tally, before, None, source=SOURCE)

    # COMPLETENESS GATE. IMF can serve a well-formed, properly closed document that
    # declares every series and carries ~5% of the observations (seen 2026-07-28:
    # 1,264 series / 10,100 obs against the same URL's 1,270 / 221,749). It parses
    # cleanly, so neither check above fires. Merge is never-shrink so such a pull
    # cannot destroy anything — but it would be reported as a successful no-op run,
    # which is the same class of lie this rewrite exists to end.
    if before and len(keys) < before // 2:
        print(f"[imf_commodity] FAIL PARTIAL RESPONSE — {len(keys):,} usable obs "
              f"across {len(set(keys)):,} series vs {before:,} published "
              f"({len(raw):,} bytes). Well-formed, but the DATA is missing. "
              f"Refusing to merge.", flush=True)
        tally.structural_unit(f"{FLOW}: partial response ({len(keys):,} obs)")
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    print(f"[imf_commodity] {tbl.num_rows:,} obs / {len(set(keys)):,} series "
          f"(empty={n_empty:,})", flush=True)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before), FLOW)
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
