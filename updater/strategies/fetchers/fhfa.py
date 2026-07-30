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
a month. FHFA is quarterly-ish, so this costs one wasted rebuild every 30 days at most.

REBUILD IS ALL-OR-NOTHING, deliberately. `build()` regenerates all 9 cubes from RAW in one
call; there is no per-cube entry point. So the gate is on the union of the 9 files and any
single change rebuilds everything. Splitting that would mean reimplementing build(), i.e. a
second parser — exactly what the duplication invariant forbids.

HONEST-STATUS: a download or build failure -> TransientError (partial, retried, existing
parquets kept untouched). A build that produces NO parquet -> structural, with the vintage NOT
recorded so it resurfaces next run.
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
from ._common import Tally, cursors_from_parquet, finalize
from ._vintage import http_vintage
from jobs import ingest_fhfa as ig     # RAW_FILES + the production downloader and builder

SOURCE = "fhfa"
SIDECAR = "_raw_vintages.json"
# Backstop for the size-only files: rebuild if nothing has moved in this many days, so a
# same-byte-count revision cannot hide indefinitely.
MAX_AGE_DAYS = 30


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
    cursors: dict = {}          # §5.7 changed-series set across every rebuilt cube
    for f in sorted(names):
        local = os.path.join(out_dir, f)
        blob.publish_file(local)
        if not f.endswith("__series.parquet"):
            published += pq.ParquetFile(local).metadata.num_rows
            cursors.update(cursors_from_parquet(local))
    tally.added_unit(published, "fhfa")
    print(f"[fhfa] rebuilt {len(names)} parquet(s), {published:,} obs rows published",
          flush=True)

    blob.write_bytes_atomic(
        os.path.join(out_dir, SIDECAR),
        json.dumps({"token": token, "built": dt.date.today().isoformat(),
                    "files": vals}, indent=1, sort_keys=True).encode("utf-8"))
    return finalize(tally, published, since or None, source=SOURCE,
                    series_cursors=cursors or None)
