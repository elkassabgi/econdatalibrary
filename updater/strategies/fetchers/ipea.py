"""S3 date-tail fetcher — IPEA (Instituto de Pesquisa Econômica Aplicada, Brazil).

API: OData v4 at http://www.ipeadata.gov.br/api/odata4 — no key.
  catalog  GET /Metadados                      -> every series + SERSTATUS + SERNUMERICA
  values   GET /ValoresSerie(SERCODIGO='X')    -> VALDATA / VALVALOR observations
series_key IS the SERCODIGO, exactly as jobs/ingest_ipea.py writes it.

WHY A DATE-TAIL FETCHER AND NOT A WRAPPER. The first-pass ingest resumes by SKIPPING any
series it already holds:

    done   = set(tbl.column("series_key").to_pylist())
    to_do  = [s for s in active if s["SERCODIGO"] not in done]

That is correct for a backfill and inert as an updater: a series already on disk is never
looked at again, so IPEA can publish ten more years of a series and we would never fetch one
of them. Running that on a schedule would only ever pick up BRAND-NEW series codes — the
green run would assert currency it had not checked.

THE SERVER-SIDE DATE FILTER DOES NOT WORK — TESTED, NOT ASSUMED. The registry's
strategy_reason claims "IPEA OData4 ValoresSerie accepts a server-side date filter
($filter=VALDATA gt {date})". It does not. Probed with raw `$filter`, URL-encoded `%24filter`,
and cutoffs of 2020-01-01 and 2026-01-01: every variant returns HTTP 200 and the FULL series
(68 of 68 observations on ABATE_ABPEAV). The API accepts the query string and ignores the
filter, which is the worst failure mode — a silent no-op that looks like it worked.

So no filter is sent. Each due series is fetched whole and `merge_and_write` dedups on
(series_key, obs_date); existing series still EXTEND, the run just carries more bytes than a
real date-tail would. That is affordable here — ~1,500 series of modest history — and it is
honest, whereas sending a filter the server discards would leave the next reader believing the
transfer was narrowed.

VINTAGE. `current_vintage` hashes the catalogue's active series codes. That moves when IPEA
adds or retires a series, which is a real change worth a run — but it deliberately does NOT
try to encode "some series got new observations", because IPEA exposes no catalogue-level
timestamp for that. Returning a token that cannot see the common case would be worse than
returning one that only sees additions: the strategy falls back to the cadence, which is
monthly here, and each series is a single small call.

BUDGET. One HTTP call per due series, so a full sweep is ~1,200 calls at RATE. A wall-clock
budget stops cleanly and defers the rest to the next tick rather than being killed at the job
ceiling; deferred series are untouched, not failed.

HONEST-STATUS: catalogue unreachable -> TransientError (partial, retried, data kept). A
per-series fetch/parse failure -> transient_unit for that series only. A series that returns
no usable observations is NOT an error — it is the normal steady state and is counted as
unchanged, never as empty.
"""
from __future__ import annotations
import datetime as dt
import hashlib
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Deadline, Tally, finalize
from jobs import ingest_ipea as ig          # reuse BASE + the production date parser

SOURCE = "ipea"
DEDUP = ("series_key", "obs_date")
BUDGET_MIN = 15
RATE = 0.15
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def _session():
    s = requests.Session()
    s.headers.update(UA)
    return s


def _catalog(sess, raise_transient: bool):
    """Active, numeric series from /Metadados — the same filter the ingest applies."""
    try:
        r = sess.get(f"{ig.BASE}/Metadados", timeout=180)
        r.raise_for_status()
        data = r.json()
    except Exception as e:                                   # noqa: BLE001
        if raise_transient:
            raise TransientError(f"ipea: catalogue unreachable: {e!r}") from e
        return None
    rows = data.get("value") or []
    out = [s for s in rows
           if s.get("SERSTATUS") == "A" and s.get("SERNUMERICA", True) and s.get("SERCODIGO")]
    return out or None


def current_vintage(unit) -> "str | None":
    """Hash of the active series-code set. Moves on additions/retirements, not on new obs."""
    got = _catalog(_session(), raise_transient=False)
    if not got:
        return None
    h = hashlib.sha256()
    for code in sorted(s["SERCODIGO"] for s in got):
        h.update(f"{code};".encode())
    return f"ipea:{len(got)}:{h.hexdigest()[:16]}"


def _local_maxes(path) -> dict:
    """{series_key: max obs_date} from what we already hold."""
    if not blob.exists(path):
        return {}
    tbl = blob.read_table(path, columns=["series_key", "obs_date"])
    out: dict[str, dt.date] = {}
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return out


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "ipea.parquet")
    sess = _session()

    active = _catalog(sess, raise_transient=True)
    maxes = _local_maxes(path)

    tally = Tally()
    keys, dates, vals = [], [], []
    cursors: dict[str, str] = {}
    unchanged = 0
    dl = Deadline(minutes=BUDGET_MIN)

    for meta in active:
        code = meta["SERCODIGO"]
        if dl.spent():
            # Deferral, not a verdict: untouched, retried next tick.
            tally.transient_unit(code)
            continue

        url = f"{ig.BASE}/ValoresSerie(SERCODIGO='{code}')"
        # NO $filter: IPEA ignores it (see module docstring). maxes is still read and kept
        # because it costs nothing and documents intent — if IPEA ever honours the filter,
        # this is the one line that changes.
        try:
            r = sess.get(url, timeout=120)
            if r.status_code >= 500:
                tally.transient_unit(code)
                time.sleep(RATE)
                continue
            r.raise_for_status()
            obs = (r.json() or {}).get("value") or []
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(code)
            time.sleep(RATE)
            continue
        time.sleep(RATE)

        if not obs:
            unchanged += 1          # normal steady state, NOT an error and NOT empty
            continue

        n_new = 0
        for item in obs:
            v = item.get("VALVALOR")
            if v is None:
                continue
            d = ig.parse_ipea_date(item.get("VALDATA", ""))
            if d is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if fv != fv:                                     # NaN
                continue
            keys.append(code)
            dates.append(d)
            vals.append(fv)
            n_new += 1
            cur = cursors.get(code)
            if cur is None or d.isoformat() > cur:
                cursors[code] = d.isoformat()
        if n_new:
            tally.added_unit(n_new, code)

    if unchanged:
        print(f"[ipea] {unchanged} series already current (empty date-tail window)", flush=True)

    total = blob.row_count(path) if blob.exists(path) else 0
    maxd = None
    if keys:
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        before = total
        try:
            total, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            raise
        print(f"[ipea] merged {total - before:,} new row(s) across "
              f"{len(set(keys)):,} series", flush=True)

    return finalize(tally, total, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors)
