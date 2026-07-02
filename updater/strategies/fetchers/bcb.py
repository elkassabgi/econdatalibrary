"""S2 fetcher — Banco Central do Brasil (BCB), SGS time-series system. No key.

Layout: a SINGLE parquet under clean_full/bcb/bcb.parquet with schema
(series_key, obs_date, value) where series_key = "label:sid" (composite key the
ingester wrote). We learn each curated series' last obs_date from that parquet,
then request observations from that boundary forward via the SGS native
Brazilian-date filter (dataInicial / dataFinal, DD/MM/YYYY). New rows are merged
into the one file (dedup on series_key+obs_date, new wins on revision, never-shrink).

API: GET https://api.bcb.gov.br/dados/serie/bcdata.sgs.{sid}/dados?formato=json
     daily series 406 on full-history -> require a date range; we always send one.
Date format is Brazilian DD/MM/YYYY in BOTH request and response.

Curated series list / endpoint / labels are reused verbatim from jobs/ingest_bcb.py
(do not re-discover). On a Jan-1 boundary we widen the window to the prior year so
a year-rollover never misses the trailing observation of the old year.

HONEST-STATUS CONTRACT (Tally + finalize):
  Each curated series is a sub-unit. Per series we record on a Tally:
    added_unit(n)    rows merged for the series (n>0 new, n==0 empty)
    empty_unit()     series legitimately returned no data (404/400, or — after the
                     retry budget — a 200 "Requisição inválida" page, since the
                     hardcoded catalog can legitimately hold a retired sid)
    transient_unit() the series hit a TransientError (timeout/5xx/429/network, or a
                     genuinely-empty/non-JSON 200) — record and KEEP GOING
  finalize() then returns 'ok'/'no_change' only when no series transient/structural-
  failed; 'partial' on any transient (so the orchestrator does NOT stamp success and
  re-runs the unit next tick); and raises DefinitiveError on a large all-empty window
  (a wholesale outage masquerading as "every series is quiet").
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{sid}/dados"
DEDUP = ("series_key", "obs_date")
SOURCE = "bcb"
RATE = 0.3
# SGS daily series cap a single request at ~10 years; chunk backfills accordingly.
WINDOW = dt.timedelta(days=9 * 365)
HISTORY_START = dt.date(1994, 1, 1)

# ── Curated SGS series catalog — copied from jobs/ingest_bcb.py (series_id, label, desc).
# Reused verbatim so the fetcher writes the SAME series_key "label:sid" the ingester did.
SERIES = [
    (1,      "USDBRL_sell",   "USD/BRL commercial selling rate (daily)"),
    (10813,  "USDBRL_buy",    "USD/BRL commercial buying rate (daily)"),
    (21619,  "EURBRL_sell",   "EUR/BRL selling rate"),
    (21620,  "EURBRL_buy",    "EUR/BRL buying rate"),
    (3697,   "GBPBRL",        "GBP/BRL rate"),
    (3688,   "JPYBRL",        "JPY/BRL rate"),
    (3696,   "CHFBRL",        "CHF/BRL rate"),
    (3700,   "CADBRL",        "CAD/BRL rate"),
    (3692,   "CNYBRL",        "CNY/BRL rate"),
    (3699,   "ARSBRL",        "ARS/BRL rate"),
    (3694,   "MXNBRL",        "MXN/BRL rate"),
    (10792,  "REER",          "Real effective exchange rate (broad basket)"),
    (11,     "SELIC_overnight","SELIC overnight interbank rate (daily %)"),
    (12,     "CDI",           "CDI interbank deposit rate (daily %)"),
    (25,     "SELIC_target",  "SELIC target rate (%)"),
    (226,    "TR",            "Referential interest rate (TR, monthly)"),
    (253,    "TJLP",          "Long-term interest rate (TJLP, monthly)"),
    (7812,   "TLP",           "Long-term rate TLP"),
    (1,      "USDBRL",        "BRL/USD PTAX (daily)"),
    (7326,   "swap_pre_DI360","360-day pre-DI swap rate"),
    (7327,   "swap_pre_DI720","720-day pre-DI swap rate"),
    (7333,   "NTNB_5Y",       "NTNB real yield 5Y"),
    (7341,   "NTNB_10Y",      "NTNB real yield 10Y"),
    (7344,   "NTNB_20Y",      "NTNB real yield 20Y"),
    (7347,   "NTNF_5Y",       "NTNF nominal yield 5Y"),
    (13522,  "IPCA",          "IPCA consumer price index (monthly % change)"),
    (433,    "IPCA_12m",      "IPCA accumulated 12 months (%)"),
    (189,    "IGPM",          "IGP-M general price index (monthly %)"),
    (190,    "IGPDI",         "IGP-DI general price index (monthly %)"),
    (191,    "IPA",           "IPA wholesale price index (monthly %)"),
    (188,    "IPC_FIPE",      "IPC-FIPE consumer price index"),
    (188,    "INPC",          "INPC national consumer price index"),
    (10764,  "IPCA_admin",    "IPCA administered prices"),
    (10765,  "IPCA_free",     "IPCA free market prices"),
    (4447,   "IPCA_food",     "IPCA food and beverages"),
    (4448,   "IPCA_housing",  "IPCA housing"),
    (4449,   "IPCA_transport","IPCA transport"),
    (4451,   "IPCA_health",   "IPCA health"),
    (7478,   "PPI_industry",  "Producer price index - industry"),
    (1823,   "M0",            "Monetary base M0 (BRL millions)"),
    (27790,  "M1",            "Money supply M1 (BRL millions)"),
    (27791,  "M2",            "Money supply M2 (BRL millions)"),
    (27792,  "M3",            "Money supply M3 (BRL millions)"),
    (27793,  "M4",            "Money supply M4 (BRL millions)"),
    (20539,  "credit_total",  "Total credit outstanding (BRL millions)"),
    (20541,  "credit_household","Household credit (BRL millions)"),
    (20542,  "credit_corporate","Corporate credit (BRL millions)"),
    (21082,  "default_total", "Credit default rate - total (%)"),
    (21083,  "default_household","Credit default rate - households (%)"),
    (21084,  "default_corporate","Credit default rate - corporates (%)"),
    (20619,  "avg_rate_total","Average credit interest rate - total (%)"),
    (20620,  "avg_rate_hh",   "Average credit interest rate - households (%)"),
    (20621,  "avg_rate_corp", "Average credit interest rate - corporates (%)"),
    (22706,  "trade_balance", "Trade balance (USD millions, monthly)"),
    (22707,  "exports",       "Exports FOB (USD millions, monthly)"),
    (22708,  "imports",       "Imports FOB (USD millions, monthly)"),
    (22562,  "current_account","Current account balance (USD millions, monthly)"),
    (3546,   "FDI",           "Foreign direct investment inflows (USD millions)"),
    (13008,  "FX_reserves",   "Foreign exchange reserves (USD millions)"),
    (7456,   "FX_reserves_liq","Liquid foreign exchange reserves (USD millions)"),
    (22099,  "GDP_quarterly", "GDP at current prices - quarterly (BRL millions)"),
    (22100,  "GDP_real",      "GDP at constant prices - quarterly (BRL millions)"),
    (7326,   "GDP_real_growth","Real GDP growth QoQ SA (%)"),
    (4380,   "GDP_deflator",  "Implicit GDP deflator"),
    (4513,   "primary_balance","Central govt primary balance (BRL millions)"),
    (4514,   "nominal_balance","Central govt nominal balance (BRL millions)"),
    (13762,  "NFSP_nominal",  "Public sector borrowing requirement - nominal (% GDP)"),
    (4537,   "net_debt_GDP",  "Net public debt (% GDP)"),
    (13762,  "gross_debt_GDP","Gross public debt (% GDP)"),
    (4472,   "revenue",       "Government revenue - federal (BRL millions)"),
    (4473,   "expenditure",   "Government expenditure - federal (BRL millions)"),
    (24369,  "unemployment",  "Unemployment rate - PNAD Contínua (%)"),
    (28763,  "employment",    "Number of employed persons (thousands)"),
    (25239,  "CAGED_net",     "Formal employment net creation - CAGED"),
    (17623,  "avg_real_wage", "Average real wage (BRL)"),
    (7440,   "IBOVESPA",      "Ibovespa stock index (points)"),
    (11753,  "B3_market_cap", "B3 market capitalization (BRL billions)"),
    (11754,  "B3_volume",     "B3 trading volume (BRL millions)"),
    (4189,   "CDS_5Y",        "Brazil 5Y CDS spread (bps)"),
    (1401,   "industrial_prod","Industrial production index"),
    (21864,  "services_vol",  "Services volume index"),
    (1475,   "retail_vol",    "Retail trade volume index"),
    (7376,   "PMI_mfg",       "PMI manufacturing (Markit)"),
    (7377,   "PMI_services",  "PMI services (Markit)"),
    (7378,   "PMI_composite", "PMI composite (Markit)"),
    (7470,   "oil_production","Oil production (thousands barrels/day)"),
    (10837,  "electricity_consumption","Electricity consumption (GWh)"),
    (31916,  "agri_production","Agricultural production value"),
    (7396,   "soy_price",     "Soybean price (USD/bushel)"),
    (7397,   "corn_price",    "Corn price (USD/bushel)"),
    (7420,   "coffee_price",  "Coffee price (USD/60kg bag)"),
    (7421,   "sugar_price",   "Sugar price (USD/lb)"),
    (7431,   "cotton_price",  "Cotton price (USD/lb)"),
    (24369,  "population",    "Population estimate (thousands)"),
]

# Deduplicate by series_id (preserve first label) — matches the ingester exactly.
_seen: set[int] = set()
_dedup_series = []
for _sid, _label, _desc in SERIES:
    if _sid not in _seen:
        _seen.add(_sid)
        _dedup_series.append((_sid, _label, _desc))
SERIES = _dedup_series

# Sentinel returned by _get when a 200 body is the SGS "Requisição inválida" page
# AFTER the retry budget is exhausted — i.e. a (likely-retired) invalid sid, treated
# as legitimately-empty for that series rather than a transient throttle.
_INVALID = object()


def _parse_bcb_date(s: str) -> dt.date | None:
    """Parse BCB date formats: DD/MM/YYYY, MM/YYYY, YYYY."""
    s = (s or "").strip()
    try:
        if len(s) == 10 and s[2] == "/":
            return dt.date(int(s[6:10]), int(s[3:5]), int(s[0:2]))
        if len(s) == 7 and s[2] == "/":
            return dt.date(int(s[3:7]), int(s[0:2]), 1)
        if len(s) == 4 and s.isdigit():
            return dt.date(int(s), 12, 31)
    except (ValueError, TypeError):
        pass
    return None


def _get(sid: int, di: dt.date, df: dt.date, sess: requests.Session,
         tries: int = 5, html_tries: int = 3):
    """Fetch one SGS chunk for [di, df] (Brazilian dates).

    Returns:
      list[dict]  -> parsed observations (possibly empty list on a quiet range)
      None        -> 404/400: series/range not available (legitimately empty)
      _INVALID    -> 200 "Requisição inválida / Bad request" page that PERSISTED
                     through the whole retry budget (a likely-retired invalid sid —
                     legitimately empty for that series, NOT a structural break)

    Raises:
      TransientError  -> timeout/5xx/429/network, OR a genuinely empty/non-JSON 200
                         body, OR an HTML "Requisição inválida" page that is still
                         being served (retried within budget — BCB SGS is known to
                         return this 200 HTML page under load/throttle, so it is
                         NOT an unconditional permanent skip).
      DefinitiveError -> other hard 4xx.
    """
    url = BASE.format(sid=sid)
    params = {
        "formato": "json",
        "dataInicial": di.strftime("%d/%m/%Y"),
        "dataFinal": df.strftime("%d/%m/%Y"),
    }
    for a in range(tries):
        try:
            r = sess.get(url, params=params, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"BCB sid={sid}: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            body = r.text
            stripped = body.lstrip()
            if stripped[:1] in ("[", "{"):
                try:
                    payload = r.json()
                except ValueError:
                    payload = None
                # A JSON array (possibly empty) is the success shape; a non-list 200
                # JSON is an unexpected envelope -> retry as transient.
                if isinstance(payload, list):
                    return payload
                if a == tries - 1:
                    raise TransientError(f"BCB sid={sid}: 200 non-list JSON body")
                time.sleep(min(2 ** a, 30)); continue
            # 200 + non-JSON body. SGS returns an HTML "Requisição inválida! / Bad
            # request!" page (HTTP 200) BOTH for genuinely invalid/nonexistent series
            # ids AND under load/throttle. We can't tell them apart from one response,
            # so RETRY within the backoff budget (transient) and only after it is
            # exhausted treat it as a (likely-retired) invalid sid -> legitimately
            # empty for that series. This stops a transient overload being laundered
            # into "fresh / no new rows" while still tolerating a dead catalog entry.
            low = stripped.lower()
            if ("requisi" in low) or ("bad request" in low) or ("<html" in low) or ("<!doctype" in low):
                # Retry on a SHORT, bounded schedule: a real throttle clears quickly,
                # and an actually-invalid sid serves this page forever, so a long
                # exponential budget would just waste minutes per dead catalog entry.
                if a >= html_tries - 1:
                    return _INVALID  # persisted across the short budget -> invalid sid, skip
                time.sleep(min(1.5 * (a + 1), 5)); continue
            # Genuinely empty / unexpected 200 body -> transient (back off and retry).
            if a == tries - 1:
                raise TransientError(f"BCB sid={sid}: empty/non-JSON 200 body")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code in (404, 400):
            return None  # series/range not available — legitimately empty for this series
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"BCB sid={sid} HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"BCB sid={sid} HTTP {r.status_code}")
    # Defensive: loop fell through without returning (should not happen).
    raise TransientError(f"BCB sid={sid}: no response after {tries} attempts")


def _series_start(sid: int, last_max: dict, since: str | None, today: dt.date) -> dt.date:
    """First date to request for a series (INCLUSIVE of the last stored obs).

    - known last obs in parquet -> that same day (re-fetch it so an in-place
      revision to the latest value is captured; merge dedups the overlap). On a
      Jan-1 boundary widen to prior year so a rollover never drops the old year's
      trailing observation.
    - else 'since' -> that date.
    - else full history from HISTORY_START (first-time backfill of a missing series).
    """
    md = last_max.get(sid)
    if md is not None:
        start = md  # inclusive: re-request the boundary day to catch same-day revisions
        if today.month == 1 and today.day == 1:
            start = min(start, dt.date(today.year - 1, 1, 1))
        return start
    # md is None: this series has NO on-disk history. Only trust the unit-level
    # `since` when the ENTIRE parquet is empty (a genuine first run). If other
    # series have already landed, this is a new/never-landed series and MUST be
    # backfilled from origin — using the unit-wide frontier here would silently
    # skip its entire pre-`since` history forever.
    if since and not last_max:
        try:
            return dt.date.fromisoformat(since)
        except ValueError:
            pass
    return HISTORY_START


def _last_max_by_sid(path: str) -> dict[int, dt.date]:
    """Per-series last obs_date keyed by sid, parsed from existing series_key 'label:sid'."""
    out: dict[int, dt.date] = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    keys = t.column("series_key")
    for sk in set(keys.to_pylist()):
        if ":" not in sk:
            continue
        try:
            sid = int(sk.split(":")[1])
        except (ValueError, IndexError):
            continue
        mask = pc.equal(keys, sk)
        m = pc.max(t.filter(mask).column("obs_date")).as_py()
        if isinstance(m, dt.datetime):
            m = m.date()
        if m is not None:
            prev = out.get(sid)
            if prev is None or m > prev:
                out[sid] = m
    return out


def update(unit, since) -> Result:
    out_dir = config.source_dir("bcb")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "bcb.parquet")
    before = blob.row_count(path)

    last_max = _last_max_by_sid(path)
    today = dt.date.today()
    sess = requests.Session()

    tally = Tally()
    # Per-series cursor: max obs_date we WROTE for each series_key this run, seeded
    # from the on-disk frontier so an untouched series still reports its real cursor
    # (a frozen series can't hide behind the unit-level max).
    cursors: dict[str, dt.date] = {}
    for sid, label, _desc in SERIES:
        md = last_max.get(sid)
        if md is not None:
            cursors[f"{label}:{sid}"] = md

    keys: list[str] = []
    dates: list[dt.date] = []
    vals: list[float] = []

    for sid, label, _desc in SERIES:
        series_key = f"{label}:{sid}"
        start = _series_start(sid, last_max, since, today)
        if start > today:
            tally.empty_unit()  # already current through today — nothing to request
            continue

        cur = start
        s_keys: list[str] = []
        s_dates: list[dt.date] = []
        s_vals: list[float] = []
        had_transient = False
        had_data = False     # any chunk came back as a (possibly empty) JSON list
        had_invalid = False  # any chunk persisted the "Requisição inválida" 200 page
        while cur <= today:
            end = min(cur + WINDOW, today)
            try:
                chunk = _get(sid, cur, end, sess)
            except TransientError:
                had_transient = True
                break  # stop chunking this series; record transient and move on
            if chunk is _INVALID:
                # "Requisição inválida" 200 page that survived the retry budget.
                had_invalid = True
                cur = end + dt.timedelta(days=1)
                time.sleep(0.2)
                continue
            if chunk is None:
                # 404/400 -> no data for this window (legitimately empty).
                cur = end + dt.timedelta(days=1)
                time.sleep(0.2)
                continue
            had_data = True
            for item in chunk:
                d_str = item.get("data", "")
                v_str = item.get("valor", "")
                if not d_str or v_str in ("", None, "null"):
                    continue
                obs_date = _parse_bcb_date(d_str)
                if obs_date is None or obs_date < start:
                    continue
                try:
                    v = float(str(v_str).replace(",", "."))
                except (ValueError, TypeError):
                    continue
                s_keys.append(series_key)
                s_dates.append(obs_date)
                s_vals.append(v)
            cur = end + dt.timedelta(days=1)
            time.sleep(0.2)
        time.sleep(RATE)

        if had_transient:
            tally.transient_unit()  # -> partial; existing data for this series untouched
            continue
        # A series that ALREADY has on-disk history but now only returns the
        # "Requisição inválida" 200 page (and got no data) is NOT a retired catalog
        # entry — a previously-working series cannot legitimately become "invalid".
        # That signature is the SGS throttle/overload page, so treat it as transient
        # (-> partial, re-queued) rather than laundering it into a quiet no_change.
        # This also routes a WHOLESALE throttle (every series serving the HTML page)
        # to 'partial' instead of tripping the all-empty structural floor.
        if had_invalid and not s_dates and last_max.get(sid) is not None:
            tally.transient_unit()
            continue
        # No transient: accumulate this series' rows and record its outcome.
        keys.extend(s_keys); dates.extend(s_dates); vals.extend(s_vals)
        if s_dates:
            smax = max(s_dates)
            prev = cursors.get(series_key)
            if prev is None or smax > prev:
                cursors[series_key] = smax
            # A series that returned a 200 with real rows is a SUCCESSFUL sub-unit
            # (data flowed), even if every row is at/below the stored boundary and the
            # merge nets zero new rows. We mark it added_unit so it does NOT feed the
            # all-empty structural floor in finalize() — otherwise a perfectly healthy
            # idempotent re-run (every active series re-returns its boundary day, zero
            # net-new) would have empty==attempted and wrongly raise DefinitiveError.
            # The real net-new delta is reflected in obs (n) and the note via merge.
            tally.added_unit(len(s_dates))
        elif had_data:
            # 200 returned a JSON list but it held no usable points in the window —
            # a legitimately quiet tail for an active series. This is a successful
            # fetch with no data; count empty (floor only trips if EVERY sub-unit is
            # empty AND none added — i.e. a true wholesale outage).
            tally.empty_unit()
        else:
            # Every chunk was a 404/400 or a persistent invalid-sid page on a series
            # with NO on-disk history -> a likely-retired/never-valid catalog entry,
            # legitimately empty for this run.
            tally.empty_unit()

    last_obs = max(cursors.values()).isoformat() if cursors else (since or None)
    series_cursors = {k: v.isoformat() for k, v in cursors.items()}

    if not vals:
        # Nothing new fetched. finalize() decides honest status: 'partial' if any
        # series transient-failed, DefinitiveError on a large all-empty window
        # (wholesale outage), else 'no_change' reporting the REAL on-disk frontier.
        return finalize(tally, before, last_obs, source=SOURCE,
                        series_cursors=series_cursors)

    new_tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })

    n, maxd = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
    # finalize() honors transient -> 'partial' even though rows were merged.
    return finalize(tally, n, maxd or last_obs, source=SOURCE,
                    series_cursors=series_cursors)
