"""S1 bulk fetcher — FHFA House Price Indexes (fhfa.gov, US public domain, no key).

Nine source files (hpi_master.csv, the 3-zip quarterly workbook, and seven annual XLSXs) are
rebuilt into 9 cubes = 18 parquets (obs + __series each).

THE INGEST IS ALREADY SAFE TO RE-RUN, which is rarer than it sounds in this tree. `download()`
re-fetches every file unconditionally — no skip-if-exists — and `build()` reparses from RAW
each time, so there is no staleness bomb to defuse here (contrast zillow's cached raw copy or
maddison's `if os.path.exists(out_path): return`). Both are therefore reused verbatim; this
module adds only the change gate and the R2 publish.

VINTAGE — MEASURED (the R164 rule). fhfa.gov serves stable validators: two HEADs 20s apart on
three files returned identical values — hpi_master.csv Content-Length 17,046,224;
hpi_at_3zip.xlsx 3,166,176; annual_hpi_at_national.xlsx Last-Modified Mon, 30 Mar 2026 17:44:29
plus Content-Length 19,550. There is NO ETag, so http_vintage falls through to Last-Modified or
Content-Length depending on the file.

That makes the weakest files SIZE-only, which cannot see a revision that preserves the byte
count — the same limitation as bis. So MAX_AGE_DAYS forces a rebuild when nothing has moved for
a month. That forced rebuild was NOT cheap: before the diff below, it re-derived up to 50,000
CSVs, a 30-48 min csv phase (runs of 2026-10-02: 2,902 s; 2026-10-29: 2,051 s), every 30 days.

CHANGED SET - DIFF-MEASURED (2026-11-11). A rebuild overwrites every cube, so "every series was
rewritten" is not "every series changed". Before the rebuild, update() reads the 9 previous obs
cubes through blob (R2 under AQUEDUCT_BACKEND=r2 - the runner's local dir holds only this run's
files, R36/R151); after it, _changed_in_cube() diffs each cube per (series_key, obs_date) in
DuckDB (a row on one side only, or any value column IS DISTINCT FROM its counterpart), and the
Result carries changed_keys = {"<cube>:<series_key>": max obs_date}, the key form the
orchestrator already maps to 'fhfa:<cube>:<key>'. A forced rebuild where nothing moved reports
{} (no derive, no debt); a monthly release reports hpi_master (+ the 3-zip cube in a quarterly
month). The capped series_cursors stay, for health's freshness reading only (the ilostat shape).
Before this, 89,706 series against CURSOR_CAP=50,000 booked full_rederive_owed on EVERY rebuild,
and the 882 hpi_at_3zip_quarterly ids sat beyond the cap on every pass.

The diff FAILS TOWARD THE DEBT: if listing or reading a previous cube raises (other than an
absent object, which reports that whole cube), if a diff raises, or if the set exceeds
CHANGED_KEYS_BOUND, changed_keys stays None and cursor_cap_hit carries the cap check, so the
orchestrator books full_rederive_owed exactly as before. Series present only in the OLD cube
(withdrawn upstream) are counted and printed, not reported - the cursors never reported them
either, and their catalogue ids would only fail to resolve.

REBUILD IS ALL-OR-NOTHING, deliberately. `build()` regenerates all 9 cubes from RAW in one
call; there is no per-cube entry point. So the gate is on the union of the 9 files and any
single change rebuilds everything. Splitting that would mean reimplementing build(), i.e. a
second parser — exactly what the duplication invariant forbids.

HONEST-STATUS: a download or build failure -> TransientError (partial, retried, existing
parquets kept untouched). A build that produces NO parquet -> structural, with the vintage NOT
recorded so it resurfaces on a later pass.
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import os

import requests

from ... import blob, config
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Tally, finalize, merge_cursors
from ._vintage import http_vintage
from jobs import ingest_fhfa as ig     # RAW_FILES + the production downloader and builder

SOURCE = "fhfa"
SIDECAR = "_raw_vintages.json"
# Backstop for the size-only files: rebuild if nothing has moved in this many days, so a
# same-byte-count revision cannot hide indefinitely.
MAX_AGE_DAYS = 30
# The diff-measured changed set is bounded far above the measured universe (89,706 series over
# the 9 cubes, 2026-10-29 build). Above it the set is dropped and the debt booked (fail toward it).
CHANGED_KEYS_BOUND = 200_000
_KEY_COLS = ("series_key", "obs_date")


def _is_obs_cube(name: str) -> bool:
    return (name.endswith(".parquet") and not name.endswith("__series.parquet")
            and not name.startswith("_"))


def _read_prior(out_dir, names) -> dict:
    """{cube: previous Arrow table, or None when the object is ABSENT} for every obs cube named.

    Read through blob BEFORE the rebuild: under the local backend build() overwrites these files
    in place, and under r2 the runner's local dir is empty (R36). An absent object is a cube with
    no previous version (every key reported); any OTHER failure raises, and the caller falls back
    to booking the debt."""
    prior = {}
    for f in names:
        if not _is_obs_cube(f):
            continue
        try:
            prior[f[:-len(".parquet")]] = blob.read_table(os.path.join(out_dir, f))
        except FileNotFoundError:
            prior[f[:-len(".parquet")]] = None
    return prior


def _iso(v):
    if v is None:
        return None
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def _changed_in_cube(old, new) -> dict:
    """{series_key: max obs_date (ISO) in NEW} for every key of NEW whose rows differ from OLD.

    A key changed when a row exists on one side only, or any non-key column IS DISTINCT FROM its
    counterpart (NULL == NULL and NaN == NaN read as the same, as merge._same_vals does). No old
    table, or a different schema, reports every key of NEW. Keys only in OLD are not reported."""
    import duckdb
    con = duckdb.connect()
    try:
        con.register("n", new)
        if old is None or old.schema.remove_metadata() != new.schema.remove_metadata():
            rows = con.execute("SELECT series_key, max(obs_date) FROM n GROUP BY 1").fetchall()
            return {k: _iso(d) for k, d in rows}
        con.register("o", old)
        vcols = [c for c in new.column_names if c not in _KEY_COLS]
        diff = " OR ".join(f'n."{c}" IS DISTINCT FROM o."{c}"' for c in vcols) or "FALSE"
        rows = con.execute(
            "WITH ch AS ("
            " SELECT DISTINCT coalesce(n.series_key, o.series_key) AS k"
            " FROM n FULL OUTER JOIN o"
            "   ON n.series_key IS NOT DISTINCT FROM o.series_key"
            "  AND n.obs_date IS NOT DISTINCT FROM o.obs_date"
            f" WHERE n.series_key IS NULL OR o.series_key IS NULL OR ({diff}))"
            " SELECT n.series_key, max(n.obs_date) FROM n JOIN ch ON n.series_key = ch.k"
            " GROUP BY 1").fetchall()
        gone = con.execute("SELECT count(DISTINCT series_key) FROM o WHERE series_key NOT IN "
                           "(SELECT series_key FROM n)").fetchone()[0]
        if gone:
            print(f"[fhfa] {gone:,} series present only in the previous cube (withdrawn "
                  f"upstream) - counted, not reported as changed", flush=True)
        return {k: _iso(d) for k, d in rows}
    finally:
        con.close()


def _validators(sess) -> dict:
    """{filename: validator} across all nine source files; missing ones map to ''."""
    out = {}
    for name, path in sorted(ig.RAW_FILES.items()):
        try:
            out[name] = http_vintage(ig.BASE + path, session=sess) or ""
        except Exception:                                    # noqa: BLE001
            out[name] = ""
    return out


def current_vintage(unit):
    """Hash of all nine files' validators. Moves iff FHFA republishes any of them."""
    sess = requests.Session()
    sess.headers.update({"User-Agent": ig.UA})
    vals = _validators(sess)
    if not any(vals.values()):
        return None                                          # nothing readable -> cadence
    h = hashlib.sha256()
    for name, v in sorted(vals.items()):
        h.update(f"{name}={v};".encode())
    return f"fhfa:{len(vals)}:{h.hexdigest()[:16]}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _older_than(iso, days) -> bool:
    if not iso:
        return True
    try:
        return (dt.date.today() - dt.date.fromisoformat(iso)).days >= days
    except ValueError:
        return True


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    sess = requests.Session()
    sess.headers.update({"User-Agent": ig.UA})
    vals = _validators(sess)
    token = hashlib.sha256(
        ";".join(f"{k}={v}" for k, v in sorted(vals.items())).encode()).hexdigest()[:16]

    sidecar = _load(out_dir)
    tally = Tally()
    stale = _older_than(sidecar.get("built"), MAX_AGE_DAYS)
    have = bool(blob.list_parquets(out_dir))

    if sidecar.get("token") == token and have and not stale:
        print("[fhfa] all 9 source files unchanged — skipped", flush=True)
        return finalize(tally, sum(blob.row_count(os.path.join(out_dir, f))
                                   for f in blob.list_parquets(out_dir)),
                        since or None, source=SOURCE)
    if stale and sidecar.get("token") == token:
        print(f"[fhfa] validators unchanged {MAX_AGE_DAYS}d — forced rebuild (size-only gate "
              f"cannot see a same-length revision)", flush=True)

    # The previous cubes, read BEFORE build() can overwrite them (module docstring).
    diff_err = None
    try:
        prior = _read_prior(out_dir, blob.list_parquets(out_dir))
    except Exception as e:                                   # noqa: BLE001 - fall back to the debt
        prior, diff_err = {}, f"previous cubes unreadable: {e!r}"

    try:
        ig.download()
        ig.build()
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"fhfa: download/build failed: {e!r}") from e

    names = [f for f in os.listdir(out_dir) if f.endswith(".parquet")]
    if not names:
        tally.structural_unit("build() produced no parquet")
        return finalize(tally, 0, since or None, source=SOURCE)

    import pyarrow.parquet as pq
    published = 0
    cursors: dict = {}          # freshness cursors (capped); health reads them
    changed: dict = {}          # §5.7 changed set, diff-measured per cube (module docstring)
    # CURSORS COLLECT FRESHEST-FILE-FIRST (2026-08-06). Alphabetical iteration filled
    # the 50,000-cursor cap with annual_* series (newest obs 2025-12-31) before
    # hpi_master's monthly series ever reported — measured: the store's
    # hpi_master.parquet reaches 2026-05-01, LEVEL with the publisher's master CSV
    # (yr 2026, period 5), yet the gate read newest_obs 2025-12-31 (218d) off the
    # capped cursor set and RED-DATA'd a healthy, current source. hpi_master carries
    # the recency signal, so its cursors go in first; the rest stay alphabetical.
    for f in sorted(names, key=lambda n: (n != "hpi_master.parquet", n)):
        local = os.path.join(out_dir, f)
        blob.publish_file(local)
        if not f.endswith("__series.parquet"):
            published += pq.ParquetFile(local).metadata.num_rows
            # key_prefix=<cube>: — catalog ids are fhfa:<CUBE>:<series_key> while the
            # parquet holds the bare key, so an unprefixed read-back maps to nothing and
            # the source reports `partial` forever with stale published CSVs.
            # Verified against catalog.db: fhfa:annual_cbsa:01, fhfa:annual_county:01001.
            merge_cursors(cursors, local,
                          key_prefix=f"{f[:-len('.parquet')]}:")
            if diff_err is None:
                cube = f[:-len(".parquet")]
                try:
                    old = prior.get(cube)
                    if old is None:
                        print(f"[fhfa] {cube}: no previous cube in the store - every key "
                              f"reported changed", flush=True)
                    part = _changed_in_cube(old, pq.read_table(local))
                    changed.update({f"{cube}:{k}": v for k, v in part.items()})
                except Exception as e:                       # noqa: BLE001 - fall back to the debt
                    diff_err = f"{cube}: diff failed: {e!r}"
    cap_hit = len(cursors) >= CURSOR_CAP
    if cap_hit:
        print(f"[fhfa] cursor set hit the {CURSOR_CAP:,} cap — freshness cursors only; the CSV "
              f"phase reads the diff-measured changed set when it is complete", flush=True)
    tally.added_unit(published, "fhfa")
    print(f"[fhfa] rebuilt {len(names)} parquet(s), {published:,} obs rows published",
          flush=True)

    blob.write_bytes_atomic(
        os.path.join(out_dir, SIDECAR),
        json.dumps({"token": token, "built": dt.date.today().isoformat(),
                    "files": vals}, indent=1, sort_keys=True).encode("utf-8"))
    res = finalize(tally, published, since or None, source=SOURCE,
                   series_cursors=cursors or None)
    if diff_err is None and len(changed) > CHANGED_KEYS_BOUND:
        diff_err = f"{len(changed):,} changed keys exceed the {CHANGED_KEYS_BOUND:,} bound"
    if diff_err is None:
        # The CSV phase reads this complete, diff-measured set. merge_and_write's
        # report_changed_keys is merge-mode only and fhfa overwrite-rebuilds, so the set is
        # measured here, against the previous published cubes, instead.
        res.changed_keys = changed
        res.cursor_cap_hit = False
        print(f"[fhfa] diff-measured changed set: {len(changed):,} series key(s) changed in "
              f"this rebuild", flush=True)
        return res
    # FALLBACK = the WU-2b behaviour: a rebuild touches all ~89,706 catalogued fhfa series,
    # and a capped cursor set GUARANTEES starved catalogued ids (39,706 measured). Say so on
    # the Result - the orchestrator books the full_rederive_owed debt from this.
    print(f"[fhfa] changed set NOT measured ({diff_err}) - falling back to the cursor set; "
          f"a cap hit books the full re-derive debt", flush=True)
    res.cursor_cap_hit = cap_hit
    return res
