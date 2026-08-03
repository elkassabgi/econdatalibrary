"""S2 fetcher — BCRP (Banco Central de Reserva del Perú). Public domain, no key.

Layout: a single parquet under clean_full/bcrp/bcrp.parquet, schema
(series_key, obs_date, value). The public JSON API is per-series and accepts a
path-based date range /{code}/json/{start}/{end}/, so we read each series' last
obs_date from the existing file and request only newer observations, then MERGE
(dedup on series_key+obs_date, new values win on revision, never-shrink).

Only the daily PD* exchange-rate series return data via the public API; the
other PD* series (EUR/GBP/JPY here) come back as an empty-HTML stub — a 200 with
an empty <body>, BCRP's "no data for this series" sentinel — which we treat as a
legitimate EMPTY sub-unit, not a failure.

HONEST-STATUS classification (see _common.finalize):
  * A 200 whose body is JSON ([ or {)                  -> parse (data / empty).
  * A 200 that is the empty-HTML stub (empty <body>,
    no error markers) or a genuinely empty body, or a
    real 404 for one series/range                       -> EMPTY sub-unit.
  * A 200 whose body is HTML/text with CONTENT or known
    throttle/maintenance/"invalid"/"error" markers      -> TRANSIENT (retry; if
    the retry budget is exhausted, raise TransientError so the run is `partial`).
  * timeout / 5xx / 429 / connection drop               -> TRANSIENT.
A run where every requested series returned a malformed/throttle body (i.e. the
whole window came back non-JSON) surfaces as a transient `partial`, never a
silent no_change.
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, _max_by_key, finalize, revision_since

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "application/json"}
BASE = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api"
DEDUP = ("series_key", "obs_date")
FILE = "bcrp.parquet"
SOURCE = "bcrp"

# (api_code, label) — BCRP daily exchange-rate series. Verified against BCRP's daily FX
# catalog 2026-07-24 (estadisticas.bcrp.gob.pe/estadisticas/series/diarias/tipo-de-cambio).
# Labels for the USD series are kept as-is for on-disk series_key continuity.
SERIES = [
    ("PD04638PD", "USDPEN_mid"),    # TC Interbancario (S/ por US$) - Venta
    ("PD04639PD", "USDPEN_buy"),    # TC Sistema bancario SBS (S/ por US$) - Compra
    ("PD04640PD", "USDPEN_sell"),   # TC Sistema bancario SBS (S/ por US$) - Venta
    ("PD04647PD", "EURPEN_buy"),    # TC Euro (S/ por Euro) - Compra
    ("PD04648PD", "EURPEN_sell"),   # TC Euro (S/ por Euro) - Venta
    # DROPPED, no replacement: BCRP's daily FX catalog is USD + EUR ONLY — there is no
    # daily GBP/PEN or JPY/PEN series (verified 2026-07-24). The prior codes PD04628PD
    # (EUR), PD04635PD (GBP) and PD04629PD (JPY) were invalid: the API returns an
    # anti-bot HTML page for them and they produced ZERO rows on disk. EUR is restored
    # above with its correct codes; GBP/JPY have no daily equivalent to point to.
]

EARLIEST = dt.date(1996, 1, 2)

MONTH_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
}

# Markers that identify a throttle / maintenance / error page (HTTP 200 body) —
# these are TRANSIENT, never "no data".
_THROTTLE_MARKERS = (
    "requisi",          # "Requisição/Requisición inválida"
    "bad request",
    "invalid",
    "inválid",
    "no válid",
    "no valid",
    "error",
    "mantenimiento",    # maintenance (ES)
    "maintenance",
    "temporarily",
    "temporalmente",
    "no disponible",
    "unavailable",
    "service",
    "503",
    "502",
    "504",
    "gateway",
    "overload",
    "try again",
)

# What _get() yields when no exception is raised:
_EMPTY = object()   # 200 with the empty-HTML "no data" stub, real 404, or empty body


def _classify_body(text: str):
    """Classify a 200 body. Returns ('json', None) to signal the caller should
    json-parse, (_EMPTY, None) for a legitimate no-data sentinel, or
    ('transient', reason) for a throttle/maintenance/error page."""
    s = (text or "").strip()
    if not s:
        return _EMPTY, None
    if s[0] in "[{":
        return "json", None
    low = s.lower()
    # An HTML doc with an EMPTY <body> and no error markers is BCRP's "no data"
    # sentinel for a series/range that simply has nothing.
    body_m = re.search(r"<body[^>]*>(.*?)</body>", low, re.DOTALL)
    has_error_marker = any(m in low for m in _THROTTLE_MARKERS)
    if body_m is not None and not body_m.group(1).strip() and not has_error_marker:
        return _EMPTY, None
    # Otherwise this is a non-empty HTML/text page (throttle, maintenance,
    # error, or unexpected content) — treat as transient.
    return "transient", f"non-JSON 200 body ({s[:60]!r})"


def _parse_period(s: str) -> dt.date | None:
    """Parse BCRP daily period strings like '02.Ene.97' or '15.Mar.2023'."""
    s = (s or "").strip()
    try:
        m = re.match(r"(\d{1,2})\.([A-Za-z]{3})\.(\d{2,4})$", s)
        if m:
            day = int(m.group(1))
            mon = MONTH_ES.get(m.group(2).lower())
            yr_raw = int(m.group(3))
            yr = yr_raw + (1900 if yr_raw >= 50 else 2000) if yr_raw < 100 else yr_raw
            if mon:
                return dt.date(yr, mon, day)
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            return dt.date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        pass
    return None


def _per_series_last(path) -> dict[str, dt.date]:
    """Map series_key -> max obs_date from the existing parquet (empty if none)."""
    if not blob.exists(path):
        return {}
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return {}
        # _max_by_key, NOT group_by. Arrow indexes string data with int32 offsets; past 2 GiB in one
    # column group_by dereferences past the overflowed offsets and KILLS THE PROCESS
    # (0xC0000005 / SIGABRT) - it does not raise, so no try/except catches it. ons_uk died that
    # way on 2026-08-01 after 8h56m. merge.py documented it; the fetchers never got the memo.
    keys_map = _max_by_key(t)
    out = {}
    for k, d in zip(list(keys_map.keys()),
                    list(keys_map.values())):
        if isinstance(d, dt.datetime):
            d = d.date()
        out[k] = d
    return out


def _get(session, code, start, end, tries=5):
    """Fetch one series/range. Returns a parsed JSON object on success, the _EMPTY
    sentinel for a legitimate no-data response (empty-HTML stub / real 404 / empty
    body), or raises TransientError for timeouts/5xx/429/throttle pages once the
    retry budget is exhausted.

    Critically: a 200 with a non-JSON throttle/maintenance body is TRANSIENT
    (retried, then surfaced as partial) — NOT a definitive "no data". That is the
    laundering bug this fetcher previously had.
    """
    url = f"{BASE}/{code}/json/{start}/{end}"
    for a in range(tries):
        try:
            r = session.get(url, headers=UA, timeout=180)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"BCRP {code}: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            kind, reason = _classify_body(r.text)
            if kind == "json":
                try:
                    return r.json()
                except ValueError:
                    # Looked like JSON but failed to parse — a truncated/partial
                    # 200 body is a transient fault, retry then surface.
                    if a == tries - 1:
                        raise TransientError(f"BCRP {code}: 200 but body did not parse as JSON")
                    time.sleep(min(2 ** a, 30)); continue
            if kind is _EMPTY:
                return _EMPTY  # genuine no-data sentinel — not a failure
            # kind == "transient": throttle/maintenance/error page on a 200.
            if a == tries - 1:
                raise TransientError(f"BCRP {code}: {reason}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 404:
            return _EMPTY  # this series/range has no data — not fatal, not structural
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"BCRP {code} HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"BCRP {code} HTTP {r.status_code}")


def _parse(label, payload, since_date):
    """Parse a JSON payload into (keys, dates, vals, structural). `structural` is
    True only when the body was a JSON object whose expected envelope (a dict with
    a `periods` list) is gone — i.e. a real schema break, not a quiet day."""
    keys, dates, vals = [], [], []
    if payload is _EMPTY or payload is None:
        return keys, dates, vals, False
    if not isinstance(payload, dict) or "periods" not in payload:
        # A 200 that parsed as JSON but is not the expected {config, periods, ...}
        # envelope (e.g. drifted to a bare list, or the periods key vanished) is a
        # structural break, not a no-data day.
        return keys, dates, vals, True
    for period in (payload.get("periods") or []):
        values = period.get("values") or []
        if not values:
            continue
        v_raw = values[0]
        if v_raw is None or str(v_raw).strip() in ("", "n.d.", "null", "None"):
            continue
        try:
            v = float(str(v_raw).replace(",", "."))
        except (ValueError, TypeError):
            continue
        d = _parse_period(period.get("name", ""))
        if d is None:
            continue
        if since_date is not None and d < since_date:
            continue
        keys.append(f"BCRP:{label}")
        dates.append(d)
        vals.append(v)
    return keys, dates, vals, False


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, FILE)

    before = blob.row_count(path)
    last_by_series = _per_series_last(path)

    # Caller-provided lower bound (None or 'YYYY-MM-DD').
    since_global = None
    if since:
        try:
            since_global = dt.date.fromisoformat(str(since)[:10])
        except ValueError:
            since_global = None

    session = requests.Session()
    end = dt.date.today()

    tally = Tally()
    # per-series new max obs_date actually written this run (cursor); seeded with
    # the existing on-disk max so a frozen series reports its true frontier, never
    # hides behind the unit-level max.
    cursors: dict[str, str] = {}
    for skey, d in last_by_series.items():
        if d is not None:
            # _per_series_last passes through whatever _max_by_key returned, and _max_by_key
            # returns ISO STRINGS. `d.isoformat()` therefore raised
            #     'str' object has no attribute 'isoformat'
            # and left bcrp in transient_fail on every run. a1c42881 fixed exactly this for boc
            # and tcmb, and recorded that "bcrp and scb work only because ISO strings sort and
            # compare exactly like dates" — true of the _max_by_key CALL SITE, which is what was
            # checked, and false here, 120 lines downstream. bcrp attempted at 2026-08-03 09:33Z,
            # six hours AFTER that fix, and crashed with the identical message.
            #
            # Accepts either type rather than asserting which arrives: cursors are stored as ISO
            # strings, so a string passes straight through and a date is normalised.
            cursors[skey] = d if isinstance(d, str) else d.isoformat()

    all_keys, all_dates, all_vals = [], [], []
    for code, label in SERIES:
        skey = f"BCRP:{label}"
        last = last_by_series.get(skey)
        if last is not None:
            # revision-lookback window (edge-case fix): catches BCRP back-postings
            # behind the stored frontier, not just same-day revisions
            start_date = revision_since(last, unit)
        elif not last_by_series:
            # whole parquet empty (true first run) -> the unit-level hint is fine
            start_date = since_global if since_global is not None else EARLIEST
        else:
            # new/never-landed series while others already have data -> backfill
            # from origin, NOT the unit-wide frontier (else its history is skipped).
            start_date = EARLIEST
        if start_date > end:
            # Nothing requestable for this series this run — legitimately empty.
            tally.empty_unit()
            continue

        start = start_date.strftime("%Y-%m-%d")
        ends = end.strftime("%Y-%m-%d")
        try:
            payload = _get(session, code, start, ends)
        except TransientError:
            # timeout / 5xx / 429 / throttle-200 with budget exhausted — DO NOT
            # abort the whole run; record and keep going so one bad series can't
            # freeze the rest, and the run surfaces as partial.
            tally.transient_unit()
            time.sleep(0.5)
            continue

        k, d, v, structural = _parse(label, payload, start_date)
        if structural:
            # 200 + JSON whose expected {periods} envelope is gone — schema break.
            tally.structural_unit()
            time.sleep(0.5)
            continue

        if k:
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
            mx = max(d)
            cur = cursors.get(skey)
            if cur is None or mx.isoformat() > cur:
                cursors[skey] = mx.isoformat()
            # We always re-fetch the boundary day, so `k` is non-empty even on a
            # quiet run. Count a sub-unit as ADDED only when it produced a date
            # strictly newer than its prior stored max (genuinely new data);
            # otherwise it's an honest empty (boundary-only re-fetch / revision).
            genuinely_new = any((di > last) for di in d) if last is not None else bool(d)
            if genuinely_new:
                tally.added_unit(len(k))
            else:
                tally.empty_unit()
        else:
            # 200 with the empty-HTML stub / no new periods — legitimately empty.
            tally.empty_unit()
        time.sleep(0.5)

    # Second site of the same defect. last_by_series holds ISO STRINGS (see the cursor loop
    # above), so max() returns a string and .isoformat() on it raises. This one is never reached
    # today only because the loop above crashes first — fixing one without the other would move
    # the same failure 70 lines down and look like a new bug.
    # since_global IS a real date (dt.date.fromisoformat above), so its .isoformat() is correct.
    _last_seen = max(last_by_series.values()) if last_by_series else None
    last_db = (_last_seen if isinstance(_last_seen, str) else
               _last_seen.isoformat() if _last_seen is not None else
               (since_global.isoformat() if since_global else None))

    if not all_keys:
        # Nothing new merged; existing file untouched. finalize() decides whether
        # this is an honest no_change/partial or a structural break (and raises).
        return finalize(tally, before, last_db, source=SOURCE, series_cursors=cursors)

    new_table = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date": pa.array(all_dates, pa.date32()),
        "value": pa.array(all_vals, pa.float64()),
    })

    n, maxd = merge.merge_and_write(path, new_table, mode="merge", dedup_keys=DEDUP)
    # finalize already counted added rows per sub-unit via tally.added_unit; pass
    # the real on-disk frontier (maxd) and let it decide ok/partial honestly.
    last_obs = maxd or last_db
    return finalize(tally, n, last_obs, source=SOURCE, series_cursors=cursors)
