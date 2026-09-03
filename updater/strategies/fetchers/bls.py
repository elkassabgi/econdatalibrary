"""S5 bulk fetcher — U.S. BLS (Bureau of Labor Statistics), public domain.

WHY bulk_snapshot_if_changed AND NOT extend_by_date
----------------------------------------------------
The library holds ~154M distinct BLS series across 63 surveys (482M obs). BLS's
public time-series API (api.bls.gov/publicAPI/v2, the BLS_API_KEY in .env) DOES
take a server-side year filter, but it is series-addressed: you must NAME every
series_id, max 50 per request, 500 requests/day = ~25k series/day. Date-tailing
all 154M on-disk series through it would take thousands of days — extend_by_date
(a per-series server-side date filter over the existing universe) is categorically
infeasible for this giant.

The honest, affordable incremental path is the one the registry documents: the
BLS flat-file site ships, per survey, a `<survey>.data.*.Current*` cut that lists
the CURRENTLY-ACTIVE series (all of them, latest periods) in a small file vs the
full multi-part `AllData` history. Re-downloading only the Current cut, parsing to
long, and MERGING on (series_id, obs_date) under never-shrink/dedup extends every
active series with new periods and revises restated values — affordable because a
cheap per-survey VINTAGE PROBE (the Current file's HTTP Last-Modified) skips any
survey whose upstream file has not moved. That is exactly bulk_snapshot_if_changed
(== the source's current registry strategy), so this fetcher exposes its contract:

    current_vintage(unit) -> str | None   # hash of every covered survey's Last-Modified
    update(unit, since)    -> Result        # per-survey gate -> download Current -> merge

Surveys with NO Current cut (small/discontinued: bg, bp, eb, ...) ship only one
`AllData` file; we gate THAT file's Last-Modified and merge it the same way (it is
tiny and idempotent). We NEVER re-download the giant multi-part AllData histories
of large surveys here — that is the full-rebuild path (jobs/ingest_bls.py --force),
deliberately out of scope for an update tick.

ON-DISK FORMAT (verified, June 2026) — two schemas coexist and we MATCH each file:
  * 58 surveys: (series_id:string, obs_date:STRING, value:double, period:string)
                    [written by legacy jobs/ingest_bls_full.py]
  * 5 surveys (ap,bd,bg,bp,ca): (series_id:string, obs_date:DATE32, value:double,
                    period:string, footnote:string)  [written by jobs/ingest_bls.py]
The emitted table is conformed PER FILE to the existing schema (obs_date dtype +
optional footnote column) so merge._concat sees an identical schema and EXTENDS,
never duplicating via a schema-mismatch union. The key is the 3-col identity
dedup_keys=("series_id","obs_date","period") — NOT the framework default
`series_key`, and NOT 2 cols: quarterly Q03 and semiannual S02/S03 legitimately
map to the SAME derived obs_date with different values, so obs_date under-keys
mixed-frequency series (see DEDUP above).

DUPLICATION INVARIANT: (series_id, obs_date, period) is the production identity,
so re-merging the same Current cut dedups to 0 new rows and a revised cut UPDATES
the affected rows — it can never duplicate nor shrink. The per-survey
Last-Modified vintage lives ONLY in the `_bulk_vintages.json` sidecar +
unit_state.upstream_vintage, never in the data.

LEGACY DEFECT — FIXED LOCALLY 2026-07-20: the retired ingest_bls_full.py had
written 39/63 surveys with massive exact-repeat inflation (154.5M repeat rows;
cu 49%, sm 69%), which froze those surveys: the clean merge shrank below
merge.merge_and_write's never-shrink floor (min_ratio=0.97) and was correctly
refused every tick. tools/dedup_bls_legacy.py deduped the local store on the
3-col identity (verified lossless; ee's 2,152 true value conflicts resolved
keep-last with an audit CSV); backups in data/_backup_bls_dedup_20260720/.
The R2 copies of the survey parquets still carry the inflation until the
separately-approved R2 replacement — do not run r2-backend bls merges until
then. NEVER run ingest_bls_full.py (it caused the inflation; QCEW dbl-count).

HONEST STATUS (Tally/finalize): each SURVEY is one sub-unit.
  * directory listing / manifest unreachable in update()      -> TransientError (retry)
  * a survey's Current download times out / 5xx / net drop    -> tally.transient (-> partial)
  * a 200 Current body that parses >0 rows                    -> tally.added/empty
  * a 200 Current body (non-trivial) that parses 0 rows, or a
    survey that lists data files but exposes none we can read  -> tally.structural (-> DefinitiveError)
  * an UNCHANGED survey (Last-Modified vintage match)         -> NOT counted (up to date)
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import tempfile

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import CURSOR_CAP, Tally, cursors_from_table, finalize, merge_cursor_map

# Reuse the AUTHORITATIVE production parsers so the updater and the first-pass
# ingest agree byte-for-byte on obs_date mapping, value cleaning, and series_id.
from jobs import ingest_bls as ig

SOURCE = "bls"
BASE = ig.BASE                       # https://download.bls.gov/pub/time.series
UA = {"User-Agent": ig.UA}           # BLS blocks generic User-Agents
DEDUP = ("series_id", "obs_date", "period")  # 3-col identity: quarterly Q03 and
# semiannual S02/S03 legitimately map to the SAME obs_date with different values
# (e.g. cu CUUS0300SACL1E 1984-07-01 S02=105.8 vs S03=104.8), so obs_date alone
# under-keys and a 2-col merge would collapse real observations (cu: 97,452 rows)
# and trip never-shrink. Verified 2026-07-20: 3-col key is value-conflict-free in
# 39/40 dup-affected surveys (logs/_bls_key3_check_0719.json).
VINTAGE_SIDECAR = "_bulk_vintages.json"
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)
RATE = 0.2                           # polite gap between survey downloads

# A survey whose chosen file(s) total fewer than this many parsed rows AND that has
# a non-trivial body is suspicious; but we do NOT hard-fail on small surveys (some
# legitimately have a few hundred rows). Structural detection is "non-empty body
# parsed to 0 rows", handled per file below.


# --------------------------------------------------------------------------- #
# directory listing + Current-file discovery (per survey)
# --------------------------------------------------------------------------- #
_DATA_RE = re.compile(r'HREF="/pub/time\.series/[^/]+/([^"]+\.data\.[^"]+)"')


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    return s


def _list_data_files(sess: requests.Session, survey: str) -> list[str]:
    """Return the .data.* filenames in a survey folder. Raises TransientError on
    timeout/5xx/network; returns [] on a hard 404 (folder gone)."""
    url = f"{BASE}/{survey}/"
    try:
        r = sess.get(url, timeout=60)
    except (requests.Timeout, requests.ConnectionError) as e:
        raise TransientError(f"BLS list {survey}: {e}")
    if r.status_code == 404:
        return []
    if r.status_code in _TRANSIENT_HTTP:
        raise TransientError(f"BLS list {survey} HTTP {r.status_code}")
    if r.status_code != 200:
        raise DefinitiveError(f"BLS list {survey} HTTP {r.status_code}")
    return _DATA_RE.findall(r.text)


COVERAGE_SIDECAR = "_tail_coverage.json"
COVERAGE_MIN = 0.90
"""Fraction of a survey's STORED series the chosen tail must reach before the survey
is allowed to report a clean result. Measured, never assumed — see _measure_coverage."""
SPAN_BUDGET_BYTES = 1_200_000_000
"""Ceiling on an escalated (spanning) file set. la already pulls 856 MB of Current
files each publish, so this is in-band; it exists so escalation can never turn into
the multi-GB download that OOM'd the runner on abs and bis."""
COVERAGE_MAX_ROWS = 50_000_000
"""Skip (and disclose) coverage measurement above this store size: counting distinct
series_id on ca/cb (52M+ series each) would read many GB every tick to answer a
question those surveys cannot have — they ship no Current cut to under-cover with."""
MEASURE_PER_TICK = 6
"""Surveys force-measured per tick when they have no coverage record yet. Bounded so
the first pass after this lands cannot blow the per-source wall-clock cap; the set
that CAN under-cover is small (>=2 data files AND a Current cut), so it converges in
a few ticks. Same rotating-sweep idiom as the stat_slovenia fix."""


def _spanning_files(data_files: list[str]) -> list[str]:
    """The cheapest file set that spans a survey's whole series list.

    Prefer ONE spanning file when the survey publishes one (ce.data.0.AllCESSeries is
    349 MB and covers 100% of stored CES series; taking all 54 non-Current files would
    cost 774 MB for the same coverage). Otherwise the full non-Current set."""
    non_current = [f for f in data_files if "Current" not in f]
    if not non_current:
        return []
    single = sorted(f for f in non_current
                    if f.endswith(".AllData") or ".data.0.All" in f)
    if single:
        return [single[0]]
    return sorted(non_current)


def _can_undercover(data_files: list[str]) -> bool:
    """Could this survey's Current cut miss stored series? Only if it HAS a Current cut
    and ships other data files beside it. A single-file survey is trivially complete,
    so it never needs a (costly) coverage measurement."""
    return bool([f for f in data_files if "Current" in f]) and len(data_files) >= 2


def _choose_tail_files(data_files: list[str], ratio: float | None = None) -> list[str]:
    """Pick the tail cut for a survey.

    Prefer every `*Current*` data file (the active-series latest-periods cut) — but
    ONLY while it is known to span the stored series. `ratio` is the last MEASURED
    coverage of the Current cut (None = never measured). Below COVERAGE_MIN we escalate
    to the spanning set, because the Current cut of some surveys covers a small
    fraction of their series: measured 2026-08-02, ce 3.8% (842 of 22,049 series — the
    other 96.2% had been frozen ~3 months while the source reported 'no new rows'),
    wd 47.8%, ap 71.6%, ci 81.6%. Escalation is bounded by SPAN_BUDGET_BYTES.

    If a survey ships no Current file (small/discontinued surveys: bg, bp, eb, cx, ep,
    and also ln/mp/nb/is/nw), fall back to its single `AllData` file (idempotent to
    re-merge) — those already span, which is why they are not in the list above. If
    there is neither a Current nor a single AllData file, return [] (caller treats as
    no-tail -> empty)."""
    current = [f for f in data_files if "Current" in f]
    if current:
        if ratio is not None and ratio < COVERAGE_MIN:
            span = _spanning_files(data_files)
            if span:
                return span
        return sorted(current)
    alldata = [f for f in data_files if f.endswith(".AllData")]
    if len(alldata) == 1:
        return alldata
    # No Current and not a single-AllData survey -> no cheap tail; skip honestly.
    return []


def _head(sess: requests.Session, survey: str, fname: str) -> tuple[str | None, int]:
    """HEAD a data file -> (Last-Modified, Content-Length). Last-Modified None if
    undeterminable (then the survey is treated as 'changed' and re-fetched); size 0
    when the header is absent. Raises TransientError on timeout/5xx/network so a
    flaky probe re-runs."""
    url = f"{BASE}/{survey}/{fname}"
    try:
        r = sess.head(url, timeout=60, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError) as e:
        raise TransientError(f"BLS head {survey}/{fname}: {e}")
    if r.status_code in _TRANSIENT_HTTP:
        raise TransientError(f"BLS head {survey}/{fname} HTTP {r.status_code}")
    if r.status_code != 200:
        return None, 0
    try:
        size = int(r.headers.get("Content-Length") or 0)
    except (TypeError, ValueError):
        size = 0
    return r.headers.get("Last-Modified"), size


def _last_modified(sess: requests.Session, survey: str, fname: str) -> str | None:
    """Back-compat shim: just the Last-Modified half of _head."""
    return _head(sess, survey, fname)[0]


def _survey_vintage(sess: requests.Session, survey: str,
                    ratio: float | None = None) -> tuple[str | None, list[str], bool, bool]:
    """Return (vintage_token, tail_files, escalated) for a survey: the Last-Modified of
    each chosen tail file joined into one token, plus the file list. vintage None means
    'could not determine' -> caller re-fetches (never silently skip on probe gap).

    `ratio` is the last measured coverage of the Current cut; below COVERAGE_MIN the
    tail escalates to the spanning set. The token is built from the CHOSEN filenames,
    so an escalation necessarily changes the token and cannot be swallowed by the
    unchanged-vintage skip — without that, a frozen survey would keep skipping before
    it ever downloaded enough to notice it was frozen.

    Escalation is reverted (and reported as not-escalated) when the spanning set
    exceeds SPAN_BUDGET_BYTES, so a huge survey degrades to the old cheap cut rather
    than OOMing the runner."""
    data_files = _list_data_files(sess, survey)
    can_uc = _can_undercover(data_files)
    tail = _choose_tail_files(data_files, ratio)
    if not tail:
        return None, [], False, can_uc
    current = sorted(f for f in data_files if "Current" in f)
    escalated = bool(current) and tail != current

    heads = [(f, *_head(sess, survey, f)) for f in tail]
    if escalated:
        total = sum(sz for _, _lm, sz in heads)
        if total > SPAN_BUDGET_BYTES:
            print(f"[{SOURCE}] {survey}: spanning set is {total/1e6:.0f} MB "
                  f"(> {SPAN_BUDGET_BYTES/1e6:.0f} MB budget); keeping the Current cut — "
                  f"this survey stays UNDER-COVERED and is reported, not silently passed",
                  flush=True)
            tail, escalated = current, False
            heads = [(f, *_head(sess, survey, f)) for f in tail]

    parts = [f"{f}={lm or '?'}" for f, lm, _sz in heads]
    # If EVERY part is unknown ('?'), treat vintage as None (force re-fetch).
    if all(p.endswith("=?") for p in parts):
        return None, tail, escalated, can_uc
    return "|".join(parts), tail, escalated, can_uc


# --------------------------------------------------------------------------- #
# vintage sidecar (per-survey Last-Modified token; lives beside data, not in it)
# --------------------------------------------------------------------------- #
def _sidecar_path(out_dir: str) -> str:
    return os.path.join(out_dir, VINTAGE_SIDECAR)


def _load_vintages(out_dir: str) -> dict:
    # blob-routed: the sidecar lives beside the data, so under AQUEDUCT_BACKEND=r2
    # it is an R2 object — a plain open() would see nothing on a CI runner and
    # every survey's vintage would reset (full re-fetch every tick).
    data = blob.read_bytes(_sidecar_path(out_dir))
    if data is None:
        return {}
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {}


def _save_vintages(out_dir: str, vintages: dict) -> None:
    blob.write_bytes_atomic(
        _sidecar_path(out_dir),
        json.dumps(vintages, separators=(",", ":")).encode("utf-8"))


# --------------------------------------------------------------------------- #
# tail coverage: does the cut we fetch actually reach the series we store?
#
# Exists because a fetcher that looks at 3.8% of a survey and merges "no new rows"
# reports the same clean green as one that looks at all of it. Every probe of the
# covered sliver agrees with upstream, so sampling cannot detect the freeze — only
# the population ratio can.
# --------------------------------------------------------------------------- #
def _coverage_path(out_dir: str) -> str:
    return os.path.join(out_dir, COVERAGE_SIDECAR)


def _load_coverage(out_dir: str) -> dict:
    data = blob.read_bytes(_coverage_path(out_dir))
    if data is None:
        return {}
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {}


def _save_coverage(out_dir: str, cov: dict) -> None:
    blob.write_bytes_atomic(_coverage_path(out_dir),
                            json.dumps(cov, indent=2, sort_keys=True).encode("utf-8"))


def _store_series(path: str, rows: int) -> int | None:
    """Distinct series_id in the stored parquet, or None when it is too big to be
    worth counting (see COVERAGE_MAX_ROWS). Read-only."""
    if rows > COVERAGE_MAX_ROWS:
        return None
    try:
        import pyarrow.compute as pc
        t = blob.read_table(path, columns=["series_id"])
        return pc.count_distinct(t.column("series_id")).as_py()
    except Exception:
        return None


def _measure_coverage(cov: dict, survey: str, tbl, path: str, escalated: bool) -> float | None:
    """Record what fraction of the STORED series the tail we just parsed reaches, and
    return that ratio.

    The ratio is an UPPER BOUND on the share of stored series refreshed (a tail series
    absent from the store inflates it), which is exactly what a refuse-to-pass gate
    wants: if the bound is low, coverage is certainly low."""
    try:
        import pyarrow.compute as pc
        tail_n = pc.count_distinct(tbl.column("series_id")).as_py()
    except Exception:
        return None
    rows = blob.row_count(path)
    prev = cov.get(survey) or {}
    store_n = prev.get("store_series") if prev.get("store_rows") == rows else None
    if store_n is None:
        store_n = _store_series(path, rows)
    if not store_n:
        return None
    ratio = tail_n / store_n
    rec = {
        "store_rows": rows,
        "store_series": store_n,
        "tail_series": tail_n,
        "ratio": round(ratio, 4),
        "escalated": escalated,
        "measured_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }
    # `current_ratio` is the escalation input and records the coverage of the CURRENT
    # CUT only. Writing an escalated measurement into it would flip-flop the survey:
    # escalate -> measure 100% -> de-escalate -> refreeze -> escalate. So an escalated
    # run preserves the prior value instead of overwriting it. The decision is
    # therefore sticky; if BLS ever widens a Current cut the only cost is that we keep
    # taking the (already budget-capped) spanning set.
    rec["current_ratio"] = prev.get("current_ratio") if escalated else round(ratio, 4)
    cov[survey] = rec
    return ratio


# --------------------------------------------------------------------------- #
# which surveys do we manage? -> exactly those already on disk (never mint a new
# survey shape; never run the destructive full rebuild)
# --------------------------------------------------------------------------- #
def _preexisting_dups(path: str) -> int:
    """Count EXACT (series_id, obs_date, period) duplicate rows already in an
    existing parquet (the legacy ingest_bls_full.py defect). Uses the same 3-col
    identity as DEDUP so legitimate period collisions (Q03/S02 on one obs_date)
    are never miscounted as dups. 0 if the file is clean/missing.
    Read-only — never modifies production."""
    if not blob.exists(path):
        return 0
    try:
        import pyarrow.compute as pc
        t = blob.read_table(path, columns=["series_id", "obs_date", "period"])
        n = t.num_rows
        if n == 0:
            return 0
        grp = t.group_by(["series_id", "obs_date", "period"]).aggregate([])
        return n - grp.num_rows
    except Exception:
        return 0


def _record_dataop(out_dir: str, survey: str, dups: int, reason: str) -> None:
    """Append a FLAGGED data-op note to a sidecar (never rewrites prod parquet).

    The existing-data dedup of legacy-inflated surveys is an offline data-op, not an
    update-tick action. We record what needs doing so it is visible to monitoring."""
    p = os.path.join(out_dir, "_dataops_needed.json")
    try:
        raw = blob.read_bytes(p)
        cur = json.loads(raw.decode("utf-8")) if raw is not None else {}
    except Exception:
        cur = {}
    cur[survey] = {
        "op": "dedup_existing_parquet",
        "exact_dups": dups,
        "detail": f"merge refused by never-shrink ({reason}); existing parquet carries "
                  f"{dups} exact (series_id,obs_date) dup rows from legacy ingest_bls_full.py. "
                  f"One-time offline dedup required OUTSIDE the never-shrink path; "
                  f"production parquet left untouched.",
        "flagged_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }
    blob.write_bytes_atomic(p, json.dumps(cur, indent=2).encode("utf-8"))


def _existing_surveys(out_dir: str) -> list[str]:
    # blob-routed: the survey set must be visible under AQUEDUCT_BACKEND=r2.
    return [fn[:-len(".parquet")] for fn in blob.list_parquets(out_dir)
            if not fn.startswith("_")]


# --------------------------------------------------------------------------- #
# download + parse one survey's tail file(s) -> long table conformed to the
# EXISTING parquet's schema (obs_date dtype + optional footnote column)
# --------------------------------------------------------------------------- #
def _download(sess: requests.Session, survey: str, fname: str, dest: str) -> None:
    """Stream one tail file to dest. Raises TransientError on timeout/5xx/network,
    DefinitiveError on a hard non-200/non-404 (404 -> TransientError: the listing
    promised this file, a 404 now is a flaky/racey fetch, retry next tick)."""
    url = f"{BASE}/{survey}/{fname}"
    try:
        with sess.get(url, stream=True, timeout=300) as r:
            if r.status_code == 404:
                raise TransientError(f"BLS {survey}/{fname}: 404 after listing (racey)")
            if r.status_code in _TRANSIENT_HTTP:
                raise TransientError(f"BLS {survey}/{fname} HTTP {r.status_code}")
            if r.status_code != 200:
                raise DefinitiveError(f"BLS {survey}/{fname} HTTP {r.status_code}")
            tmp = dest + ".part"
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    if chunk:
                        fh.write(chunk)
            os.replace(tmp, dest)
    except (requests.Timeout, requests.ConnectionError) as e:
        raise TransientError(f"BLS {survey}/{fname}: {e}")


def _existing_schema(path: str) -> pa.Schema | None:
    if not blob.exists(path):
        return None
    try:
        return blob.read_schema(path)
    except Exception:
        return None


def _parse_tail(raw_paths: list[str], schema: pa.Schema | None):
    """Parse downloaded tail file(s) into (table, n_body_lines).

    Uses the production parsers (ig.parse_obs_date / ig.parse_value) and strips the
    whitespace-padded series_id, exactly as jobs/ingest_bls.py does. The table is
    conformed to `schema` (the existing parquet's): obs_date as date32 OR string,
    and a footnote column only if the existing file has one. n_body_lines is the
    count of non-header data lines seen across the files (to distinguish a genuinely
    empty body from a non-trivial body that parsed 0 rows = structural)."""
    # Decide the obs_date representation + footnote presence from the existing schema.
    cols = set(schema.names) if schema is not None else set()
    obs_is_date32 = False
    if schema is not None and "obs_date" in cols:
        obs_is_date32 = pa.types.is_date32(schema.field("obs_date").type)
    want_footnote = "footnote" in cols

    sids: list[str] = []
    obs_d: list[dt.date] = []
    obs_s: list[str] = []
    vals: list[float] = []
    pers: list[str] = []
    fns: list[str | None] = []
    n_body = 0

    for rp in raw_paths:
        with open(rp, encoding="utf-8", errors="replace") as fh:
            first = fh.readline()
            if "series_id" not in first:
                fh.seek(0)  # no header; rewind
            for line in fh:
                cells = line.rstrip("\n").split("\t")
                if len(cells) < 4:
                    continue
                sid = cells[0].strip()
                if not sid:
                    continue
                n_body += 1
                od = ig.parse_obs_date(cells[1], cells[2].strip())
                if od is None:
                    continue
                v = ig.parse_value(cells[3])
                if v is None:
                    continue
                sids.append(sid)
                if obs_is_date32:
                    obs_d.append(od)
                else:
                    obs_s.append(od.isoformat())
                vals.append(v)
                pers.append(cells[2].strip())
                if want_footnote:
                    fn = cells[4].strip() if len(cells) > 4 else ""
                    fns.append(fn or None)

    data = {
        "series_id": pa.array(sids, pa.string()),
        "obs_date": (pa.array(obs_d, pa.date32()) if obs_is_date32
                     else pa.array(obs_s, pa.string())),
        "value": pa.array(vals, pa.float64()),
        "period": pa.array(pers, pa.string()),
    }
    if want_footnote:
        data["footnote"] = pa.array(fns, pa.string())

    # Build the table in the EXACT column order of the existing schema so concat sees
    # an identical schema (extend, not permissive-union). If there is no existing
    # schema (defensive — we only manage on-disk surveys), fall back to ingest_bls's
    # canonical order.
    if schema is not None:
        order = [n for n in schema.names if n in data]
        # Include any data columns not in schema at the end (shouldn't happen).
        order += [n for n in data if n not in order]
        tbl = pa.table({n: data[n] for n in order})
    else:
        order = ["series_id", "obs_date", "value", "period"]
        if want_footnote:
            order.append("footnote")
        tbl = pa.table({n: data[n] for n in order})
    return tbl, n_body


# --------------------------------------------------------------------------- #
# strategy contract: current_vintage + update
# --------------------------------------------------------------------------- #
def current_vintage(unit) -> str | None:
    """Cheap probe across ALL on-disk surveys: a hash of every survey's chosen-tail
    Last-Modified token. Returns None if the probe cannot run (no surveys, or a
    listing/HEAD network failure) so the strategy fetches anyway (cadence-gated)."""
    out_dir = config.source_dir(SOURCE)
    surveys = _existing_surveys(out_dir)
    if not surveys:
        return None
    sess = _session()
    h = hashlib.sha256()
    any_known = False
    for sv in surveys:
        try:
            # Unpack all FOUR. _survey_vintage returns (vintage, tail, escalated, can_uc);
            # this call unpacked two and had done so since the initial release, while the
            # return widened in 8b34bf5a and only the caller in update() (below) was updated.
            # A two-way unpack of a 4-tuple is not a type warning, it is a ValueError at
            # RUNTIME on every call where any survey exists on disk — so this function never
            # returned a token and never returned None either. It raised, through
            # BulkSnapshotIfChanged.detect_change, which does not catch ValueError. The
            # docstring's promise ("returns None ... so the strategy fetches anyway") had not
            # been true for the whole life of that commit.
            vintage, _tail, _escalated, _can_uc = _survey_vintage(sess, sv)
        except TransientError:
            return None  # a probe network failure -> don't claim 'unchanged'
        h.update(sv.encode("utf-8"))
        h.update(b"=")
        h.update((vintage or "?").encode("utf-8"))
        h.update(b";")
        if vintage is not None:
            any_known = True
    return ("bls:" + h.hexdigest()[:16]) if any_known else None


def update(unit, since) -> Result:
    cursors: dict[str, str] = {}
    """Per-survey: gate on Last-Modified, download the cheap tail, parse, and MERGE
    into the existing parquet (dedup on series_id+obs_date, never-shrink, atomic).
    Only surveys ALREADY on disk are touched; unchanged surveys are skipped."""
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    surveys = _existing_surveys(out_dir)
    if not surveys:
        # Nothing to update incrementally and nothing to fabricate. Treat as a
        # structural/config problem rather than a silent ok: the first-pass bulk
        # ingest must run before incremental updates make sense.
        raise DefinitiveError(
            f"{SOURCE}: no existing survey parquet in {out_dir}; run the first-pass "
            f"bulk ingest (jobs/ingest_bls.py) before incremental updates")

    sess = _session()
    vintages = _load_vintages(out_dir)
    cov = _load_coverage(out_dir)
    tally = Tally()
    total_rows = 0
    max_last: str | None = None
    forced = 0                      # coverage measurements taken this tick
    under: list[str] = []           # surveys whose tail does not span their series
    tmpdir = tempfile.mkdtemp(prefix="bls_tail_")

    try:
        for sv in surveys:
            path = os.path.join(out_dir, f"{sv}.parquet")
            before = blob.row_count(path)

            # 1) cheap vintage gate (Last-Modified of the chosen tail file[s]).
            try:
                vintage, tail, escalated, can_uc = _survey_vintage(
                    sess, sv, (cov.get(sv) or {}).get("current_ratio"))
            except TransientError:
                tally.transient_unit()  # probe failed -> partial, re-run next tick
                continue

            if not tail:
                # No Current cut and not a single-AllData survey -> no cheap tail
                # exists; this survey can only be refreshed by the full rebuild path.
                # That is a legitimate "nothing to do here", NOT a structural break.
                tally.empty_unit()
                continue

            stored = vintages.get(sv)
            if vintage is not None and stored is not None and vintage == stored:
                # Upstream tail file(s) unmoved since last publish -> normally skip.
                #
                # EXCEPT when this survey could under-cover and has never been measured.
                # The skip happens BEFORE any download, so an under-covered survey would
                # keep skipping on a Current file that never moves and never accumulate
                # the evidence that it is frozen — which is exactly how ce sat 3 months
                # stale on 96.2% of its series while reporting "no new rows". Force one
                # measuring download, rotated across ticks so the bill stays bounded.
                if not (can_uc and sv not in cov and forced < MEASURE_PER_TICK):
                    continue  # NOT counted as a sub-unit attempt (it is up to date)
                forced += 1
                print(f"[{SOURCE}] {sv}: vintage unchanged but tail coverage has never "
                      f"been measured — fetching once to measure "
                      f"({forced}/{MEASURE_PER_TICK} this tick)", flush=True)

            # 2) download the tail file(s) to temp.
            raw_paths = []
            try:
                for f in tail:
                    dest = os.path.join(tmpdir, f"{sv}__{f}")
                    _download(sess, sv, f, dest)
                    raw_paths.append(dest)
            except TransientError:
                tally.transient_unit()
                continue
            except DefinitiveError:
                tally.structural_unit()
                continue

            # 3) parse -> table conformed to the existing parquet's schema.
            schema = _existing_schema(path)
            tbl, n_body = _parse_tail(raw_paths, schema)

            if tbl.num_rows == 0:
                if n_body > 0:
                    # Non-trivial body that parsed to 0 usable rows = schema/format
                    # break (BLS always serves parseable rows in a Current cut).
                    tally.structural_unit()
                else:
                    # Genuinely empty file body (rare) — honest no-data sub-unit.
                    tally.empty_unit()
                # Do NOT advance vintage on structural; advance on a real empty body
                # so we don't re-parse the same empty file forever.
                if n_body == 0 and vintage is not None:
                    vintages[sv] = vintage
                continue

            # 3b) how much of this survey did we actually look at? Measured BEFORE the
            # merge, so the denominator is the series we already store rather than the
            # union we are about to write (which would read back as trivially complete).
            ratio = _measure_coverage(cov, sv, tbl, path, escalated)
            if ratio is not None and ratio < COVERAGE_MIN:
                under.append(f"{sv}={ratio:.1%}")
                print(f"[{SOURCE}] {sv}: tail reaches only {ratio:.1%} of stored series"
                      + (" even after escalating to the spanning set"
                         if escalated else " — escalating on the next tick"), flush=True)
            elif escalated:
                print(f"[{SOURCE}] {sv}: spanning set reaches {ratio:.1%} of stored "
                      f"series (Current cut was "
                      f"{(cov.get(sv) or {}).get('current_ratio', 0):.1%})", flush=True)

            # 4) merge (dedup on the 3-col identity, never-shrink, atomic). A
            # would-shrink / column-drop / 0-row merge keeps old data and surfaces
            # partial; we do NOT advance the vintage so it is reattempted.
            #
            # LEGACY-INFLATED SURVEYS (fixed locally 2026-07-20, see module docstring):
            # if a store copy still carries the ingest_bls_full.py repeat inflation
            # (e.g. the R2 objects until their replacement is approved), the dedup'd
            # union is SMALLER than the inflated file, merge_and_write trips its
            # never-shrink guard and refuses — correctly leaving data untouched and
            # surfacing `partial` with the DATA-OP reason below (do NOT lower
            # min_ratio — that would let a genuinely-truncated upstream silently
            # shrink good data; run tools/dedup_bls_legacy.py on that store instead).
            try:
                n, last = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        # Report WHICH series moved, or the orchestrator cannot re-derive their CSVs
            # (contract §5.7) and the published downloads silently drift away from the
            # parquet: right data in the store, stale values in every CSV a user fetches.
            # Cursors come from the NEW table rather than the merged file - on a source
            # this size, reporting every series would re-derive millions of unchanged CSVs.
                # key_col='series_id': bls stores (series_id, obs_date, value, period),
                # NOT the (series_key, obs_date) shape most sources use - its DEDUP is
                # ('series_id','obs_date','period'). Calling cursors_from_table with the
                # default key would raise inside, be swallowed by its `except: return {}`,
                # and report NOTHING while looking wired up. Verified against the store:
                # catalog ids are bls:<series_id> (bls:CUUR0000SA0), so series_id is exactly
                # what _catalog_ids_for needs to map back.
                merge_cursor_map(cursors,
                                 cursors_from_table(tbl, cap=CURSOR_CAP,
                                                    key_col="series_id"),
                                 cap=CURSOR_CAP)
            except DefinitiveError as e:
                tally.transient_unit()
                self_dups = _preexisting_dups(path)
                if self_dups > 0:
                    _record_dataop(out_dir, sv, self_dups, str(e)[:160])
                continue

            added = max(0, n - before)
            tally.added_unit(added)
            total_rows += n
            if last and (max_last is None or str(last) > str(max_last)):
                max_last = str(last)
            if vintage is not None:
                vintages[sv] = vintage  # advance only on a clean publish

        _save_vintages(out_dir, vintages)
        _save_coverage(out_dir, cov)
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)

    # finalize: any structural -> DefinitiveError; any transient -> partial; else
    # ok (added>0) / no_change. empty_window_floor scales with survey count so a few
    # no-tail/quiet surveys don't trip the all-empty structural floor.
    res = finalize(tally, total_rows, max_last, source=SOURCE,
                   series_cursors=cursors or None,
                   empty_window_floor=max(10, len(surveys)))

    # A source must never report a clean result over data it did not look at. If any
    # survey's tail still under-covers after escalation (only possible when the
    # spanning set blew SPAN_BUDGET_BYTES), downgrade no_change/ok to partial: partial
    # does not set last_success_utc, so the source stops reading as healthy while a
    # fraction of its series go unrefreshed. This is the invariant whose absence let
    # ce report green for three months.
    if under:
        note = (f"tail UNDER-COVERS {len(under)} survey(s) (<{COVERAGE_MIN:.0%} of stored "
                f"series fetched): " + ", ".join(sorted(under)[:6])
                + (f", +{len(under) - 6} more" if len(under) > 6 else ""))
        print(f"[{SOURCE}] {note}", flush=True)
        if res.status in ("ok", "no_change"):
            res = Result(status="partial", obs=res.obs, last_obs_date=res.last_obs_date,
                         new_vintage=res.new_vintage, series_cursors=res.series_cursors,
                         merged_rows=res.merged_rows, cursor_cap_hit=res.cursor_cap_hit,
                         changed_keys=res.changed_keys,
                         error=note)
    return res
