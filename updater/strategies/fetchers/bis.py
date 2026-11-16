"""S1 bulk fetcher — BIS Consolidated & Locational Banking Statistics (data.bis.org, no key).

BIS publishes two flat-CSV bulk zips at STABLE urls (no date or version in the path):
    CBS  https://data.bis.org/static/bulk/WS_CBS_PUB_csv_flat.zip
    LBS  https://data.bis.org/static/bulk/WS_LBS_D_PUB_csv_flat.zip
so the honest change signal is each object's own HTTP validator (ETag / Last-Modified /
Content-Length) — re-download only when it moves.

WHY THE INGEST COULD NOT SIMPLY BE WRAPPED. jobs/ingest_bis_cbs_lbs.py is backfill-shaped:
`download()` returns early when the zip is already on disk and `ingest_zip()` returns early
when the parquet already exists. Both are right for a one-off load and inert as an updater —
scheduled, they would do nothing forever while reporting success. The parse was also inline in
`ingest_zip`, writing straight to a ParquetWriter, so there was nothing callable to reuse.

Rather than copy the parser (which breaks the duplication invariant the moment either side is
edited), the parse was EXTRACTED into `ingest_bis_cbs_lbs.iter_rows()` — a pure generator
yielding (series_key, obs_date, value, freq) with no skip rules, no writer and no logging
policy. The ingest now delegates to it too, so there is exactly ONE parse and the fetcher and
the first-pass ingest cannot drift apart.

MEMORY. The CBS/LBS flat CSVs are tens of millions of rows, so rows are accumulated in
BATCH-sized chunks and merged incrementally rather than materialised whole.

HONEST-STATUS: a HEAD/validator failure -> the zip is treated as changed (fetch anyway, which
merge dedups) rather than silently skipped. A download or parse failure -> transient_unit for
that zip only, so CBS failing cannot stop LBS publishing. A zip that parses to ZERO rows ->
structural_unit and its vintage is NOT advanced, so a parser break resurfaces on a later pass
instead of being sealed in.

THE SERVED DATA IS NOT IN THE ZIPS (added 2026-11-16). Every catalogued bis id is
`bis:WS_CBPOL:<country>` (central bank policy rates), served by _resolve_bis from
clean_full/bis/WS_CBPOL.parquet at native key 'M.<country>'. That file was written once by the
first-pass SDMX ingest (jobs/ingest_bis_full.py) on 2026-06-30 and nothing refreshed it, so users
got May policy rates while this unit reported success on the uncatalogued CBS/LBS zips. The SDMX
phase below refreshes every flow in SERVED_SDMX_FLOWS from stats.bis.org:

  * ONE REQUEST PER FREQ the store holds for the flow (`/data/BIS,<flow>/<FREQ>`), the served
    FREQ first. The whole-flow request is NOT usable: measured 2026-11-16, three HTTP 200s
    (curl exit 0) returned three DIFFERENT bodies, each cut mid-line (24.0 / 28.6 / 37.8 MB),
    while '/M' (1.43 MB) and '/D' (42.5 MB) each hashed identically on repeat.
  * CHANGE GATE = sha256 of the body against `_sdmx_vintages.json` (the CBS/LBS sidecar is not
    touched). BIS has no honest validator here: the ETag is base64 of the REQUEST time
    ('guest$<ms>$*$1$WS_CBPOL', new on every call) and there is no Last-Modified.
  * A CHANGED body must pass, before it can merge: it ends with a newline and its last line has
    the header's field count (a mid-line cut); every row's STRUCTURE_ID is the pinned DSD and
    ACTION is 'I'; a second fetch hashes the same (a cut that changes between calls); every
    stored key of that FREQ is in the body and the body holds at least the stored row count (a
    deterministic cut at a line boundary - merge only grows, so it would freeze those series
    silently). Only keys the store already holds are merged (R278; new upstream keys are counted
    and skipped - nothing new is catalogued, and the D1 catalogue sync is frozen anyway).
  * FETCH FIRST, MERGE LAST. The bodies are fetched and validated before the CBS/LBS zips, so
    the zips' long merges cannot spend the Deadline and defer the served data; but they MERGE
    only after the zips, outside the Deadline (about 735k rows, seconds). The workstation route
    kills a pass hard (taskkill, no SIGALRM on Windows), and a kill between this merge and
    update() returning loses the CSV phase while the sidecar says "current" - the R1131/R1137
    class. Merging last shrinks that window from the whole CBS/LBS phase to the seconds between
    the merge and the return. A FREQ that fails never blocks another: 'D' serves no catalogue id
    and must never hold back 'M'.
  * THE CHANGED SET IS FOLDED TO THE CATALOGUE GRAIN. merge_and_write(report_changed_keys=True)
    measures which store keys changed value; 'M.<cc>' folds to 'WS_CBPOL:<cc>' (the exact tier
    of orchestrate._catalog_ids_for maps it to bis:WS_CBPOL:<cc>), and 'D.<cc>' is dropped (no
    catalogue id serves it). Result.changed_keys is therefore ALWAYS set, possibly {}: no
    catalogue id is served from CBS.parquet or LBS.parquet, so a pass that merged only the zips
    changed nothing a user can download. That claim is guarded at run time: a catalogued bis
    flow outside SERVED_SDMX_FLOWS books a named transient, so {} cannot hide it.
  * NOT MONITORED BY THE GATE: bis is `live: false` (workstation route), and the cadence is
    monthly, so served M data can lag upstream by up to about two cadence periods.
"""
from __future__ import annotations
import csv
import datetime as dt
import hashlib
import io
import json
import os
import sqlite3
import tempfile
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, cursors_from_parquet, finalize, merge_cursors
from ._vintage import http_vintage
from jobs import ingest_bis_cbs_lbs as ig      # BULK urls + the EXTRACTED pure parser
from jobs import ingest_bis_full as sdmx       # SDMX base url + the EXTRACTED SDMX-CSV parser

SOURCE = "bis"
# The catalogued SDMX dataflows - the ONLY bis data a user can download. A curated constant pinned
# by test (the defillama SERVED_CHAINS precedent), checked against the catalogue at run time.
SERVED_SDMX_FLOWS = ("WS_CBPOL",)
# The DSD each served flow's rows must carry (STRUCTURE_ID column). A new version is a possible
# re-grain (R22), so it refuses as structural until a human checks it.
SDMX_STRUCTURE = {"WS_CBPOL": "BIS:WS_CBPOL(1.0)"}
SDMX_SIDECAR = "_sdmx_vintages.json"
SDMX_PACE_S = 2.0          # seconds between SDMX requests (public API, no key)
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"
BUDGET_MIN = 25
# Backstop for the size-only gate: re-pull a zip whose Content-Length has not moved in this
# many days, so a same-byte-count revision cannot hide forever.
MAX_AGE_DAYS = 30
# RAISED from 500,000 on 2026-07-30, after bis aborted the whole daily run.
#
# Every merge_and_write re-reads and rewrites the ENTIRE existing parquet, so this constant
# sets how many times that happens. LBS.parquet holds 36,379,671 rows, so 500,000 meant 73
# merge cycles over a table growing toward 36M — the quadratic shape the ledger calls R169,
# and the exact trap cepii_gravity's docstring already spells out (it uses 20M for this
# reason; bis never got the same treatment). Memory went 1,516MB -> 15,700MB in under seven
# minutes and Arrow aborted the process: std::length_error: vector::_M_default_append,
# exit 134 (SIGABRT), taking the run down after only 3 sources had completed.
#
# 4M keeps the Python row buffer near 1 GB (these are 4-tuples, not cepii's 3-tuples) while
# cutting LBS from 73 merges to 10. Note this buys TOTAL WORK and pool churn, not a lower
# ceiling: the per-merge peak is set by the size of the existing file, not by the batch.
# The ceiling is what merge_and_write's new pool release addresses.
BATCH = 4_000_000
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def _validator(url, sess) -> "str | None":
    """Content-Length-first validator for the BIS bulk zips. NOT http_vintage — MEASURED.

    data.bis.org is served by several origin replicas whose copies of the same file carry
    DIFFERENT mtimes, so the ETag and Last-Modified FLAP between requests while the bytes are
    unchanged. Two HEADs 15s apart on WS_LBS_D_PUB_csv_flat.zip:

        ETag W/"153f3e65-19f89d4b74e"  LM Wed, 22 Jul 2026 12:37:26 GMT  CL 356466277
        ETag W/"153f3e65-19f88bb24fb"  LM Wed, 22 Jul 2026 07:29:53 GMT  CL 356466277

    Last-Modified went BACKWARDS by five hours, which no real republish can do, and
    Content-Length was identical. These are Apache-style ETags, `"<size-hex>-<mtime-hex>"`:
    0x153f3e65 = 356,466,277 = exactly Content-Length, so only the mtime half moves. Since
    http_vintage prefers ETag > Last-Modified > Content-Length, it picks the one field that
    flaps, and the gate would report "changed" on nearly every run — re-downloading and
    re-parsing 440 MB daily while looking like a working cache (the fed_board defect, R164).

    So the size is the honest signal. It is WEAKER than a hash — a revision that preserves the
    byte count would be invisible — which is why update() also force-refreshes on MAX_AGE_DAYS.
    """
    try:
        r = sess.head(url, headers=UA, timeout=60, allow_redirects=True)
    except Exception:                                        # noqa: BLE001
        return None
    cl = r.headers.get("Content-Length")
    if cl:
        return f"len={cl}"
    return http_vintage(url, session=sess)                   # no CL -> best available


def current_vintage(unit) -> "str | None":
    """Hash of both bulk objects' size validators. Moves iff BIS republishes either zip."""
    sess = requests.Session()
    parts = []
    for name, url in sorted(ig.BULK.items()):
        v = _validator(url, sess)
        if not v:
            return None                                      # unknown -> let cadence decide
        parts.append(f"{name}={v}")
    h = hashlib.sha256(";".join(parts).encode()).hexdigest()[:16]
    return f"bis:{h}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def _download(sess, url, dest) -> None:
    """Straight download — deliberately NOT ig.download(), which skips when a file exists."""
    with sess.get(url, headers=UA, stream=True, timeout=1800) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)


def _older_than(iso, days) -> bool:
    """True when `iso` is absent or older than `days` — the size gate's staleness backstop."""
    if not iso:
        return True
    try:
        return (dt.date.today() - dt.date.fromisoformat(iso)).days >= days
    except ValueError:
        return True


def _table(rows):
    return pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date": pa.array([r[1] for r in rows], pa.date32()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
        "freq": pa.array([r[3] for r in rows], pa.string()),
    })


# --------------------------------------------------------------------------------------------- #
# SDMX phase: the catalogued flows (see the module docstring)
# --------------------------------------------------------------------------------------------- #
def _served_native(flow, store_key) -> "str | None":
    """The catalogue native ('<flow>:<country>', i.e. the id minus 'bis:') that a store key serves,
    or None. The exact inverse of clients/python/econdl/_resolve.py::_resolve_bis, which maps
    bis:<flow>:<country> to native key 'M.<country>' (tests round-trip it through the resolver)."""
    parts = str(store_key).split(".")
    if flow in SERVED_SDMX_FLOWS and len(parts) == 2 and parts[0] == "M" and parts[1]:
        return f"{flow}:{parts[1]}"
    return None


def _catalogue_flows() -> "set | None":
    """The bis dataflows the catalogue serves (bis:<flow>:...), or None when it cannot be read.
    PK range, never `source_id=?` (one index) and never LIKE (R492)."""
    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    if not os.path.exists(cat):
        return None
    try:
        con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
        try:
            ids = [r[0] for r in con.execute(
                "SELECT series_id FROM series WHERE series_id >= ? AND series_id < ?",
                (SOURCE + ":", SOURCE + ";"))]
        finally:
            con.close()
    except Exception:                                        # noqa: BLE001
        return None
    return {i.split(":")[1] for i in ids if i.count(":") >= 2}


def _sdmx_get(sess, flow, freq) -> "tuple[int, bytes]":
    """(HTTP status, body) for one FREQ slice of a flow. The version-less selector follows the
    flow's current version; the STRUCTURE_ID pin is what refuses a version change."""
    url = f"{sdmx.BASE}/data/BIS,{flow}/{freq}?detail=dataonly"
    last = None
    for attempt in range(2):
        try:
            r = sess.get(url, headers={**UA, "Accept": sdmx.CSVMIME}, timeout=(30, 300))
            return r.status_code, r.content
        except Exception as e:                               # noqa: BLE001
            last = e
            time.sleep(SDMX_PACE_S * (attempt + 1))
    raise last


def _check_body(body, flow, freq, stored_keys, stored_rows):
    """Validate one SDMX-CSV body. Returns (rows, None, n_new_keys) or (None, (kind, why), 0),
    kind 'transient' (a cut or partial body: a later pass may get a whole one) or 'structural'
    (the shape itself changed: a later pass will not help)."""
    if not body.endswith(b"\n"):
        return None, ("transient", "body does not end with a newline (stream cut mid-line)"), 0
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        return None, ("structural", "body is not UTF-8"), 0
    lines = text.splitlines()
    if not lines:
        return None, ("structural", "empty body"), 0
    rd = list(csv.reader(lines))
    hdr = rd[0]
    if len(rd[-1]) != len(hdr):
        return None, ("transient", f"last line has {len(rd[-1])} of {len(hdr)} fields (stream cut)"), 0
    want = SDMX_STRUCTURE.get(flow)
    si = hdr.index("STRUCTURE_ID") if "STRUCTURE_ID" in hdr else None
    ai = hdr.index("ACTION") if "ACTION" in hdr else None
    if si is None or ai is None:
        return None, ("structural", "no STRUCTURE_ID/ACTION column"), 0
    bad = sum(1 for r in rd[1:] if len(r) != len(hdr) or r[si] != want or r[ai] != "I")
    if bad:
        return None, ("structural", f"{bad} row(s) not {want} / ACTION I / full width"), 0
    try:
        parsed = list(sdmx.iter_rows(io.StringIO(text)))
    except sdmx.NoObservations as e:
        return None, ("structural", f"no observations: {e}"), 0
    kept = [r for r in parsed if r[0] in stored_keys]
    new_keys = {r[0] for r in parsed} - set(stored_keys)
    if not kept:
        return None, ("structural", f"0 of {len(stored_keys)} stored series in a "
                                    f"{len(parsed)}-row body"), 0
    missing = set(stored_keys) - {r[0] for r in kept}
    if missing:
        return None, ("transient", f"{len(missing)} stored series absent from the body "
                                   f"(e.g. {sorted(missing)[0]})"), 0
    if len(kept) < stored_rows:
        return None, ("transient", f"body has {len(kept)} rows for stored series, store holds "
                                   f"{stored_rows}"), 0
    return kept, None, len(new_keys)


def _stored_layout(path) -> "dict | None":
    """{FREQ: (set of stored keys, stored row count)} read through the store layer (R36), or None."""
    if not blob.exists(path):
        return None
    t = blob.read_table(path, columns=["series_key"])
    out: dict = {}
    for k in t.column("series_key").to_pylist():
        if k is None:
            continue
        fr = str(k).split(".")[0]
        ks, n = out.get(fr, (set(), 0))
        ks.add(k)
        out[fr] = (ks, n + 1)
    return out


def _sdmx_prefetch(out_dir, sess, tally, dl, vint) -> dict:
    """Fetch + validate every changed FREQ slice of every served flow. NO store write.
    Returns {flow: [(freq, sha, arrow table)]} of the slices that may merge."""
    ready: dict = {}
    requested = 0
    cat = _catalogue_flows()
    if cat is None:
        print(f"[bis] catalogue unreadable - refreshing the pinned SERVED_SDMX_FLOWS "
              f"{list(SERVED_SDMX_FLOWS)} without the catalogue cross-check", flush=True)
    else:
        for f in sorted(cat - set(SERVED_SDMX_FLOWS)):
            tally.transient_unit(f"{f}: catalogued but bis.py has no refresh path for it "
                                 f"(permanent until SERVED_SDMX_FLOWS covers it)")
    for flow in SERVED_SDMX_FLOWS:
        path = os.path.join(out_dir, f"{flow}.parquet")
        try:
            layout = _stored_layout(path)
        except Exception as e:                               # noqa: BLE001
            tally.transient_unit(f"{flow}: store unreadable ({type(e).__name__})")
            continue
        if not layout:
            tally.transient_unit(f"{flow}: store file absent or empty - refusing to refresh "
                                 f"a served flow from nothing")
            continue
        order = sorted(layout, key=lambda fr: (fr != "M", fr))     # the served FREQ first
        for fr in order:
            label = f"{flow}:{fr}"
            if dl.spent():
                print(f"[bis] budget {BUDGET_MIN} min spent - {label} deferred to a later pass",
                      flush=True)
                tally.deferred_unit(label)
                continue
            if requested:
                time.sleep(SDMX_PACE_S)
            requested += 1
            try:
                st, body = _sdmx_get(sess, flow, fr)
            except Exception as e:                           # noqa: BLE001
                tally.transient_unit(f"{label}: request failed ({type(e).__name__})")
                continue
            if st != 200:
                tally.transient_unit(f"{label}: HTTP {st} for a FREQ the store holds")
                continue
            sha = hashlib.sha256(body).hexdigest()
            if (vint.get(label) or {}).get("sha256") == sha:
                continue                                     # unchanged upstream: costs nothing more
            keys, n_rows = layout[fr]
            rows, problem, n_new = _check_body(body, flow, fr, keys, n_rows)
            if problem:
                kind, why = problem
                (tally.structural_unit if kind == "structural" else tally.transient_unit)(
                    f"{label}: {why}")
                continue
            time.sleep(SDMX_PACE_S)
            try:
                st2, body2 = _sdmx_get(sess, flow, fr)
            except Exception as e:                           # noqa: BLE001
                tally.transient_unit(f"{label}: confirming request failed ({type(e).__name__})")
                continue
            if st2 != 200 or hashlib.sha256(body2).hexdigest() != sha:
                tally.transient_unit(f"{label}: body differed between two fetches (cut stream?)")
                continue
            if n_new:
                print(f"[bis] {label}: {n_new} upstream series not in the store - skipped "
                      f"(only stored keys are refreshed, R278)", flush=True)
            ready.setdefault(flow, []).append((fr, sha, _table(rows)))   # Arrow, not tuples, while the zips run
    return ready


def _sdmx_merge(out_dir, ready, tally, vint, cursors) -> "tuple[dict, int, object]":
    """Merge the validated slices (one merge per flow), advance their hashes, and return
    (changed_keys at catalogue grain, store rows after, newest obs date)."""
    changed_cat: dict = {}
    total = 0
    maxd = None
    for flow, slices in ready.items():
        path = os.path.join(out_dir, f"{flow}.parquet")
        label = flow + ":" + "+".join(fr for fr, _, _ in slices)
        try:
            before = blob.row_count(path)
            new = pa.concat_tables([t for _, _, t in slices])
            n, md, changed = merge.merge_and_write(path, new, mode="merge",
                                                   dedup_keys=DEDUP, report_changed_keys=True)
        except Exception as e:                               # noqa: BLE001
            tally.transient_unit(f"{label}: merge refused ({type(e).__name__}: {str(e)[:80]})")
            continue
        total += n
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        for k, v in (changed or {}).items():
            nat = _served_native(flow, k)
            if nat is not None:
                changed_cat[nat] = v
        if n > before:
            tally.added_unit(n - before, label)
        elif changed:
            tally.revised_unit(label)
        else:
            tally.empty_unit(label)
        got = cursors_from_parquet(path)                     # the file this process just wrote
        for k, v in got.items():
            nat = _served_native(flow, k)
            if nat is not None and len(cursors) < CURSOR_CAP:
                cursors[nat] = v
        today = dt.date.today().isoformat()
        for fr, sha, _ in slices:
            vint[f"{flow}:{fr}"] = {"sha256": sha, "pulled": today}
        blob.write_bytes_atomic(os.path.join(out_dir, SDMX_SIDECAR),
                                json.dumps(vint, indent=2, sort_keys=True).encode("utf-8"))
    return changed_cat, total, maxd


def _load_sdmx(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SDMX_SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = requests.Session()
    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    maxd = None
    cursors: dict = {}          # §5.7 changed-series set, per republished bulk zip
    dl = Deadline(minutes=BUDGET_MIN)

    # SDMX phase, step 1 of 2: fetch + validate the served flows BEFORE the zips (no store write).
    vint = _load_sdmx(out_dir)
    ready = _sdmx_prefetch(out_dir, sess, tally, dl, vint)

    for name, url in sorted(ig.BULK.items()):
        path = os.path.join(out_dir, f"{name}.parquet")
        cur_v = _validator(url, sess) or ""
        stale = _older_than(sidecar.get(f"{name}__pulled"), MAX_AGE_DAYS)
        if cur_v and sidecar.get(name) == cur_v and blob.exists(path) and not stale:
            continue                                         # already current, costs nothing
        if stale and cur_v and sidecar.get(name) == cur_v:
            # Size unchanged for MAX_AGE_DAYS. Because the size gate cannot see a same-byte-count
            # revision, re-pull anyway rather than trust it indefinitely. merge dedups, so the
            # only cost is bandwidth.
            print(f"[bis] {name}: size unchanged {MAX_AGE_DAYS}d — forced re-pull", flush=True)

        if dl.spent():
            print(f"[bis] budget {BUDGET_MIN} min spent — {name} deferred to a later pass",
                  flush=True)
            tally.deferred_unit(name)
            continue

        tmp = os.path.join(tempfile.gettempdir(), f"bis_{name}.zip")
        try:
            _download(sess, url, tmp)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(name)
            continue

        n_rows = 0
        before = blob.row_count(path) if blob.exists(path) else 0
        buf = []
        try:
            for row in ig.iter_rows(name, tmp):
                buf.append(row)
                if len(buf) >= BATCH:
                    n, md = merge.merge_and_write(path, _table(buf), mode="merge",
                                                  dedup_keys=DEDUP)
                    n_rows, buf = n, []
                    if md and (maxd is None or str(md) > str(maxd)):
                        maxd = md
            if buf:
                n, md = merge.merge_and_write(path, _table(buf), mode="merge",
                                              dedup_keys=DEDUP)
                n_rows = n
                if md and (maxd is None or str(md) > str(maxd)):
                    maxd = md
        except DefinitiveError:
            tally.transient_unit(name)                       # one zip must not sink the source
            continue
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(name)
            continue
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass

        if not n_rows:
            # Parsed to nothing from a zip that downloaded fine — a real break. Do NOT
            # advance the vintage, so it resurfaces instead of being sealed in.
            tally.structural_unit(f"{name}: zip parsed to 0 rows")
            continue

        published += n_rows
        merge_cursors(cursors, path)
        tally.added_unit(max(0, n_rows - before), name)
        if cur_v:
            sidecar[name] = cur_v                            # advance ONLY after a clean publish
            sidecar[f"{name}__pulled"] = dt.date.today().isoformat()

    if len(cursors) >= CURSOR_CAP:
        print(f"[bis] cursor set hit the {CURSOR_CAP:,} cap — further changed series are not "
              f"individually reported; the orchestrator's derive-all path covers small "
              f"catalogs", flush=True)
    _save(out_dir, sidecar)

    # SDMX phase, step 2 of 2: merge the validated served slices LAST, outside the Deadline.
    served_cursors: dict = {}
    changed_keys, sdmx_rows, sdmx_maxd = _sdmx_merge(out_dir, ready, tally, vint, served_cursors)
    published += sdmx_rows
    if sdmx_maxd and (maxd is None or str(sdmx_maxd) > str(maxd)):
        maxd = sdmx_maxd
    # The served series' cursors go FIRST, so a zip that filled CURSOR_CAP cannot crowd them out.
    if served_cursors:
        merged_cur = dict(served_cursors)
        for k, v in cursors.items():
            if len(merged_cur) >= CURSOR_CAP:
                break
            merged_cur[k] = v
        cursors = merged_cur

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    last_obs = maxd or (since or None)
    try:
        res = finalize(tally, published, last_obs, source=SOURCE,
                       series_cursors=cursors or None)
    except DefinitiveError as e:
        # THE SERVED CHANGED SET MUST SURVIVE A STRUCTURAL RAISE (the hagstofa Route B / _giant
        # precedent): a zip that parsed to 0 rows, or a refused SDMX slice, raises in finalize, and
        # the orchestrator maps a raise to `partial` with NO Result - no CSV phase for the policy
        # rates this pass DID merge. Re-raised unchanged when nothing served changed.
        if not changed_keys:
            raise
        print(f"[bis] finalize raised structural with {len(changed_keys)} served id(s) changed - "
              f"returning partial WITH the changed set: {str(e)[:120]}", flush=True)
        return Result(status="partial", obs=published, last_obs_date=last_obs,
                      new_vintage="date-tail", series_cursors=cursors or None,
                      changed_keys=changed_keys, error=str(e))
    # ALWAYS set (possibly {}): the merge-measured change of what the catalogue serves. CBS/LBS
    # serve no catalogue id, so their merges contribute nothing here (see the module docstring).
    res.changed_keys = changed_keys
    return res
