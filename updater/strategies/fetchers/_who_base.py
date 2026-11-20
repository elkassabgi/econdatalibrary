"""Shared WHO fetcher — the Global Health Observatory OData API, WHO's OWN publisher.

REPLACES THE BANNED RELAY. who_rs / who_hwf / who_sdg used to read the relay's WHO mirror.
The relay aggregator is banned (CLAUDE.md §0, ledger R251): every source comes from its publisher. The
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

COVERAGE is measured at SERIES grain, never indicator grain (R252). CORRECTED 2026-11-20: this
paragraph used to say "who_rs 2,207/2,207 and who_hwf 4,421/4,421 are fully available". For
who_rs that was false. R252 measured 1,558 of 2,207 published keys (70.6%) on the first real
fetch, and the forced run of 2026-11-20 fetched rows for 1,944 keys of which 1,356 map to the
2,207 catalogued ids (851 catalogued ids got no rows). 647 of those 851 belong to RS_208, RS_212
and RS_217, which carry their numbers in the text field `Value` with `NumericValue` null; the
TEXT-VALUE SWITCH below takes them (who_rs only). The other 204 are RS_198 keys that WHO now
publishes under different dimension codes, an open re-key question. who_hwf was not re-measured
here. who_sdg is 18,902/28,160 because `SDGAIRBOD` returns @odata.count 0 at WHO (9,258 keys).
Rows WHO no longer serves are KEPT — merge never shrinks — and the empty-indicator gap is
reported every run rather than silently dropped. SDGAIRBODA (which we also publish) does serve,
so SDGAIRBOD looks superseded; that is a lead to confirm with WHO, not a fact to act on here.

TEXT-VALUE SWITCH (per source, opt-in, empty by default). A row is kept when NumericValue is
non-null (it always wins). When NumericValue is null, the row is kept ONLY if its IndicatorCode
is in the `text_value_indicators` set the SOURCE passes to run(), and str(Value).strip() is
exactly `^-?\\d+(\\.\\d+)?$` (ASCII digits). Everything else stays skipped: ranges ("11 - 24"),
multi-part ("13 (GB), 23 (NI)"), footnote letters ("18.4h"), "<1", "No", "None", mojibake.
Only who_rs passes a set ({RS_208, RS_212, RS_217}); who_hwf and who_sdg pass nothing, so their
behaviour is unchanged. A shared rule could mint uncatalogued keys there, so it is NOT shared.
The values were compared before the change: the relay-era store's 647 values for these keys
equal WHO's text values (647 of 647).

HONEST STATUS: an indicator that fails after retries -> transient_unit (kept, retried); one
that legitimately returns no rows -> empty_unit; parsed rows -> added_unit. A budget stop is
reported and rotates, so the remainder actually arrives in a later pass (R190/R243).
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
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
                raise TransientError(f"WHO GHO GET {url[-50:]}: {last}")
        except Exception as e:                    # noqa: BLE001 — timeouts, conn resets, bad JSON
            last = f"{type(e).__name__}: {e}"
            if a == TRIES - 1:
                raise TransientError(f"WHO GHO GET {url[-50:]}: {last}")
        time.sleep(min(2 ** a, 20))
    raise TransientError(f"WHO GHO GET {url[-50:]}: {last}")


def _published_indicators(source_id: str, prefix: str) -> list[str]:
    """The indicator codes behind the series ids we PUBLISH, read from the catalogue.

    Deliberately not a hardcoded list: the published set is data, and a Python copy of it
    drifts the moment an indicator is added or withdrawn. Catalogue ids look like
    `<source>:<PREFIX>:<INDICATOR>.<dims...>`, so the indicator is the first dot-segment of
    the part after the prefix.
    """
    from core import catalog_path                                  # noqa: PLC0415 - plan step 1
    path = catalog_path.under(config.ROOT)
    if not os.path.exists(path):
        raise TransientError(f"{source_id}: no catalogue at {path}; cannot "
                             f"determine which indicators to fetch")
    try:
        con = catalog_path.connect_path(path, write=False)
        try:
            rows = con.execute("SELECT series_id FROM series WHERE source_id=?", (source_id,)).fetchall()
        finally:
            con.close()
    except catalog_path.CutoverRefused:
        raise                                                 # a wrong checkout after T0: not transient
    except Exception as e:                                    # noqa: BLE001 - garbage, a hot journal
        raise TransientError(f"{source_id}: catalogue unreadable: {e!r}") from e
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


_TEXT_NUMBER = re.compile(r"^-?\d+(\.\d+)?$", re.ASCII)


def _row_value(v: dict, text_value_indicators=frozenset()) -> float | None:
    """The row's value, or None to skip it.

    NumericValue wins whenever it is present. Only when it is null, and only for an indicator in
    the caller's `text_value_indicators`, is the text field `Value` read, and only when it is a
    plain decimal number after strip(). See TEXT-VALUE SWITCH in the module docstring.
    """
    nv = v.get("NumericValue")
    if nv is not None:
        try:
            return float(nv)
        except (TypeError, ValueError):
            return None
    if v.get("IndicatorCode") not in text_value_indicators:
        return None
    tv = v.get("Value")
    if tv is None:
        return None
    s = str(tv).strip()
    if not _TEXT_NUMBER.fullmatch(s):
        return None
    return float(s)


def current_vintage(unit, source_id: str):
    """No cheap upstream vintage exists: GHO exposes no dataset-level updated-at, and the only
    honest token would be a hash of every row, which costs the whole fetch. So this is an
    always-fetch source, and the probe says so with None ("undeterminable"): the strategy then
    fetches at every due tick. A FABRICATED token would either freeze the source or make it
    re-pull for ever — worldbank's rule, same here. The full pass is ~60 requests across the
    three sources, and merge dedup makes a re-run harmless.

    NOT the literal "date-tail". It returned that until 2026-11, meaning "always fetch", but
    finalize stamps the same literal as new_vintage, so the unit stored it, the probe compared
    equal, and the orchestrator booked an earned no_change with no fetch on every due tick:
    who_hwf/who_rs were last fetched 2026-08-02 and who_sdg 2026-08-11 while health read them
    fresh. The strategies now also read that literal as None (base.probe_token)."""
    return None


def run(source_id: str, prefix: str, budget_env: str = "WHO_BASE_BUDGET_MIN",
        text_value_indicators: frozenset = frozenset()) -> Result:
    """`text_value_indicators`: indicator codes of THIS source whose number WHO puts in the text
    field `Value` (NumericValue null). Empty by default; only who_rs passes a set."""
    text_value_indicators = frozenset(text_value_indicators)
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
    text_taken: dict[str, int] = {}
    last_code = ""
    done = 0

    for code in order:
        if dl.spent():
            print(f"[{source_id}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — {done}/{len(order)} indicators done, "
                  f"{len(order) - done} deferred to a later pass (resuming after "
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
            yr = v.get("TimeDim")
            if yr is None:
                continue
            fv = _row_value(v, text_value_indicators)
            if fv is None:
                continue
            k = _key(v, prefix)
            if not k:
                continue
            try:
                fy = int(yr)
            except (TypeError, ValueError):
                continue
            if v.get("NumericValue") is None:
                text_taken[code] = text_taken.get(code, 0) + 1
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
    if text_taken:
        print(f"[{source_id}] rows parsed from the text Value field (NumericValue null): "
              + ", ".join(f"{c} {text_taken[c]:,}" for c in sorted(text_taken)), flush=True)

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
