"""Shared WHO fetcher — the Global Health Observatory OData API, WHO's OWN publisher.

REPLACES THE DBNOMICS RELAY. who_rs / who_hwf / who_sdg used to read DBnomics' WHO mirror.
DBnomics is banned (CLAUDE.md §0, ledger R251): every source comes from its publisher. The
ban is also right on the merits here — a mirror's vintage signal is the mirror's own hash, so
a frozen dataset reports `no_change` for ever while the health gate sees a source succeeding
every day. WHO GHO removes the middleman entirely: no key, no quota, one request per
indicator.

  base:  https://ghoapi.azureedge.net/api
  list:  GET /Indicator                      -> 3,072 indicator codes
  data:  GET /{IndicatorCode}                -> every row for that indicator

KEY GRAMMAR — reconstructed from GHO's own fields, PROVEN against the stored ids before this
module was written (no sampling, both the simple and the multi-dimensional shape):

    {PREFIX}:{IndicatorCode}.{SpatialDim}[.{Dim1}][.{Dim2}][.{Dim3}].A

    RS_1845     181 WHO rows -> 181 keys vs 181 stored:     181/181 exact, 0 WHO-only, 0 stored-only
    SDGSUICIDE  19,041 rows  -> 6,693 keys vs 6,693 stored: 6,693/6,693 exact, 0 either way

Dim1..Dim3 already carry their own type prefix in GHO (Dim1Type="SEX", Dim1="SEX_BTSX"), so a
segment is the Dim VALUE verbatim — do NOT re-join the type, that would double it. Absent dims
are omitted rather than emitted empty, which is what makes a 2-dim and a 3-dim indicator both
land on the ids we already publish.

obs_date is PERIOD-START: TimeDim is a year and the stored convention is Jan-1 (verified:
WHO_RS:RS_1845.AFG.A carries 2010-01-01 for TimeDim 2010).

WHICH INDICATORS: taken from the CATALOGUE — the ids we actually publish — never a hardcoded
list, so the set cannot drift from what we serve (same rule as worldbank's _published_keys).

COVERAGE, measured against WHO on the full indicator set: who_rs 2,207/2,207 and who_hwf
4,421/4,421 are fully available; who_sdg is 18,902/28,160 because `SDGAIRBOD` returns
@odata.count 0 at WHO (9,258 keys). Those rows are KEPT — merge never shrinks — and the gap is
reported every run rather than silently dropped. SDGAIRBODA (which we also publish) does serve,
so SDGAIRBOD looks superseded; that is a lead to confirm with WHO, not a fact to act on here.

HONEST STATUS: an indicator that fails after retries -> transient_unit (kept, retried); one
that legitimately returns no rows -> empty_unit; parsed rows -> added_unit. A budget stop is
reported and rotates, so the remainder actually arrives next tick (R190/R243).
"""
from __future__ import annotations
import datetime as dt
import json
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after, save_rotation)

BASE = "https://ghoapi.azureedge.net/api"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
DEDUP = ("series_key", "obs_date")
TIMEOUT = 180
TRIES = 4
RATE = 0.2


def _get(url: str):
    """GET GHO JSON. TransientError after the retry budget — never a silent None."""
    last = None
    for a in range(TRIES):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as f:
                return json.loads(f.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code in (400, 404):
                return None                       # indicator genuinely absent
            if a == TRIES - 1:
                raise TransientError(f"who_gho GET {url[-50:]}: {last}")
        except Exception as e:                    # noqa: BLE001 — timeouts, conn resets, bad JSON
            last = f"{type(e).__name__}: {e}"
            if a == TRIES - 1:
                raise TransientError(f"who_gho GET {url[-50:]}: {last}")
        time.sleep(min(2 ** a, 20))
    raise TransientError(f"who_gho GET {url[-50:]}: {last}")


def _published_indicators(source_id: str, prefix: str) -> list[str]:
    """The indicator codes behind the series ids we PUBLISH, read from the catalogue.

    Deliberately not a hardcoded list: the published set is data, and a Python copy of it
    drifts the moment an indicator is added or withdrawn. Catalogue ids look like
    `<source>:<PREFIX>:<INDICATOR>.<dims...>`, so the indicator is the first dot-segment of
    the part after the prefix.
    """
    path = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    if not os.path.exists(path):
        raise TransientError(f"{source_id}: catalog.db not present at {path}; cannot "
                             f"determine which indicators to fetch")
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = con.execute("SELECT series_id FROM series WHERE source_id=?", (source_id,)).fetchall()
    finally:
        con.close()
    tag = f"{prefix}:"
    out = set()
    for (sid,) in rows:
        _, _, rest = sid.partition(f"{source_id}:")
        if rest.startswith(tag):
            out.add(rest[len(tag):].split(".")[0])
    return sorted(out)


def _key(v: dict, prefix: str) -> str | None:
    code, spatial = v.get("IndicatorCode"), v.get("SpatialDim")
    if not code or spatial is None:
        return None
    parts = [str(code), str(spatial)]
    for i in (1, 2, 3):
        dv = v.get(f"Dim{i}")
        if dv:
            parts.append(str(dv))
    return f"{prefix}:" + ".".join(parts) + ".A"


def current_vintage(unit, source_id: str):
    """No cheap upstream vintage exists: GHO exposes no dataset-level updated-at, and the only
    honest token would be a hash of every row, which costs the whole fetch. So this is an
    always-fetch source (the `date-tail` sentinel finalize already emits). A FABRICATED token
    would either freeze the source or make it re-pull for ever — worldbank's rule, same here.
    The full pass is ~60 requests across the three sources, and merge dedup makes a re-run
    harmless."""
    return "date-tail"


def run(source_id: str, prefix: str, budget_env: str = "WHO_GHO_BUDGET_MIN") -> Result:
    out_dir = config.source_dir(source_id)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{source_id}.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0

    indicators = _published_indicators(source_id, prefix)
    if not indicators:
        raise TransientError(f"{source_id}: catalogue lists no {prefix} indicators to fetch")

    # Bound below the orchestrator's 45-minute cap and ROTATE, so a stop resumes somewhere new
    # instead of re-walking the same head for ever (R190). Writes land per indicator, so an
    # interruption keeps everything already merged (R243).
    budget_min = float(os.environ.get(budget_env, "30"))
    dl = Deadline(minutes=budget_min)
    order = rotate_after(list(indicators), load_rotation(out_dir))

    tally = Tally()
    keys: list[str] = []
    dates: list[dt.date] = []
    vals: list[float] = []
    empty_at_who: list[str] = []
    last_code = ""
    done = 0

    for code in order:
        if dl.spent():
            print(f"[{source_id}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — {done}/{len(order)} indicators done, "
                  f"{len(order) - done} deferred to the next tick (resuming after "
                  f"{last_code!r})", flush=True)
            break
        done += 1
        last_code = code
        try:
            d = _get(f"{BASE}/{urllib.parse.quote(code)}")
        except TransientError:
            tally.transient_unit(code)
            time.sleep(RATE)
            continue
        rows = (d or {}).get("value") or []
        if not rows:
            # WHO lists the indicator but serves no rows for it (e.g. SDGAIRBOD). Recorded and
            # REPORTED, never silently dropped — our existing rows for it stay, because merge
            # never shrinks.
            empty_at_who.append(code)
            tally.empty_unit(code)
            time.sleep(RATE)
            continue
        n = 0
        for v in rows:
            val, yr = v.get("NumericValue"), v.get("TimeDim")
            if val is None or yr is None:
                continue
            k = _key(v, prefix)
            if not k:
                continue
            try:
                fy = int(yr)
                fv = float(val)
            except (TypeError, ValueError):
                continue
            keys.append(k)
            dates.append(dt.date(fy, 1, 1))       # period-START, matching the stored convention
            vals.append(fv)
            n += 1
        tally.added_unit(n, code)
        time.sleep(RATE)

    if last_code:
        save_rotation(out_dir, last_code)
    if empty_at_who:
        print(f"[{source_id}] {len(empty_at_who)} indicator(s) listed by WHO but serving 0 "
              f"rows: {', '.join(empty_at_who[:8])}{' …' if len(empty_at_who) > 8 else ''} — "
              f"existing rows kept (merge never shrinks)", flush=True)

    if not keys:
        return finalize(tally, before, None, source=source_id)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    print(f"[{source_id}] merging {len(keys):,} obs / {len(set(keys)):,} series from "
          f"{done}/{len(order)} indicators", flush=True)
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)

    cursors: dict[str, str] = {}
    for k, dte in zip(keys, dates):
        iso = dte.isoformat()
        if k not in cursors or iso > cursors[k]:
            cursors[k] = iso
    return finalize(tally, n, md, source=source_id, series_cursors=cursors)
