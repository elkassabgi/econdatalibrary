"""S1 bulk fetcher — CEPII Gravity database (cepii.fr, Etalab 2.0, no key).

1,143,250 series / 69,666,545 observations. This source was REGISTERED and in the updater-heavy
matrix and had no fetcher, so the orchestrator printed "PENDING cepii_gravity — no adapter
built" and skipped it forever. The 05:50 heavy run on 2026-07-30 showed exactly that: all four
matrix jobs reported "0 unit(s) processed" and exited 0. Scheduled on paper, structurally unable
to run.

WHY IT HAD NO FETCHER, almost certainly: `parse_gravity_csv` took the decompressed CSV as one
Python str and returned three lists of ~69.6 MILLION elements. The CSV is 1.25 GB uncompressed,
so that is 10 GB+ resident — an instant OOM on a 7 GB runner. The parse is now EXTRACTED into
`ingest_cepii_gravity.iter_gravity_rows()`, a generator over an open file object, and the
whole-file function delegates to it so there is exactly one parser. This fetcher streams the zip
member straight into it and merges in BATCH-sized chunks, so memory stays flat.

PICK THE LARGEST CSV IN THE ZIP, NOT THE FIRST. The archive ships 12 CSVs: the 1.25 GB
`Gravity_V202211.csv`, a 15 KB `Countries_V202211.csv` lookup, and ten ~60-byte label files.
Taking the first `.csv` alphabetically selects the Countries lookup — which is what a first
attempt did, and the parser correctly refused it ("Cannot find origin/destination ISO cols",
0 rows). Selecting by size is version-proof.

VINTAGE — MEASURED (the R164 rule). CEPII serves stable validators on the zip: two HEADs 12s
apart returned identical ETag `"39925a161e8fda1:0"`, Last-Modified `Mon, 15 Apr 2024 10:17:14
GMT` and Content-Length `206,707,748` — matching DATABASE_LICENSES_VERBATIM.md's recorded
2024-04-15 exactly. Unlike federalreserve.gov (Last-Modified regenerated per request) or
data.bis.org (ETags flapping across replicas), an http_vintage gate genuinely hits here.

ONLY GATE ON A URL THAT 200s. `ig.GRAVITY_URLS` lists five candidates and two of them return a
STABLE 404. Hashing a 404 yields a token that never moves and means nothing — the same trap as
maddison's dataverse mirror, which 403s. The active url is chosen by probing.

STEADY STATE IS no_change, AND THAT IS CORRECT. V202211 is the newest release CEPII lists and
the file has not moved since 2024-04-15, so this source should report no_change indefinitely and
only wake when CEPII republishes. That is the source being current, not frozen — which is
precisely why the gate keys on the publisher's own validator rather than on a clock.

HONEST-STATUS: no reachable url or a download failure -> TransientError (partial, retried,
existing data kept). A zip that downloads but streams ZERO rows -> structural, with the vintage
NOT recorded, so a parser break resurfaces next run instead of being sealed in.
"""
from __future__ import annotations
import hashlib
import io
import json
import os
import zipfile

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import Tally, cursors_from_parquet, finalize
from ._vintage import http_vintage
from jobs import ingest_cepii_gravity as ig     # URLS + the EXTRACTED streaming parser

SOURCE = "cepii_gravity"
SIDECAR = "_zip_vintage.json"
DEDUP = ("series_key", "obs_date")
# Rows per merge chunk. Each merge_and_write re-reads and rewrites the WHOLE parquet, so the
# batch size trades memory against quadratic I/O: 2M-row batches would mean ~35 merges over a
# growing 69.6M-row file (~1.2 BILLION row-rewrites — the R169 shape). 20M keeps it to 4 merges
# at roughly 900 MB of Arrow per batch, which a 7 GB runner absorbs while memory stays bounded.
BATCH = 20_000_000
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}


def _urls():
    return list(getattr(ig, "GRAVITY_URLS", None) or getattr(ig, "URLS", []))


def _active(sess):
    """First candidate url that actually 200s, with its validator. (None, None) if none do."""
    for u in _urls():
        try:
            r = sess.head(u, headers=UA, timeout=60, allow_redirects=True)
        except Exception:                                    # noqa: BLE001
            continue
        if r.status_code == 200:
            v = (r.headers.get("ETag") or r.headers.get("Last-Modified")
                 or r.headers.get("Content-Length") or "")
            return u, v
    return None, None


def current_vintage(unit):
    """The active zip's own HTTP validator. Moves iff CEPII republishes the file."""
    sess = requests.Session()
    url, v = _active(sess)
    if not url or not v:
        return None
    return f"cepii_gravity:{hashlib.sha256(f'{url}|{v}'.encode()).hexdigest()[:16]}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _table(rows):
    return pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date": pa.array([r[1] for r in rows], pa.date32()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
    })


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "cepii_gravity.parquet")

    sess = requests.Session()
    url, cur_v = _active(sess)
    if not url:
        raise TransientError("cepii_gravity: no CEPII url responded 200")

    sidecar = _load(out_dir)
    tally = Tally()
    if cur_v and sidecar.get("validator") == cur_v and blob.exists(path):
        print(f"[cepii_gravity] zip unchanged ({cur_v}) — skipped", flush=True)
        return finalize(tally, blob.row_count(path), since or None, source=SOURCE)

    try:
        r = sess.get(url, headers=UA, timeout=1800)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"cepii_gravity: download failed: {e!r}") from e

    members = [i for i in z.infolist() if i.filename.lower().endswith(".csv")]
    if not members:
        tally.structural_unit("zip contains no CSV")
        return finalize(tally, blob.row_count(path) if blob.exists(path) else 0,
                        since or None, source=SOURCE)
    member = max(members, key=lambda i: i.file_size)         # see the module docstring
    print(f"[cepii_gravity] {member.filename} ({member.file_size:,} B uncompressed)", flush=True)

    total = blob.row_count(path) if blob.exists(path) else 0
    maxd = None
    n_rows = 0
    buf = []
    try:
        with z.open(member.filename) as fh:
            text = io.TextIOWrapper(fh, encoding="utf-8-sig", errors="replace")
            for row in ig.iter_gravity_rows(text, ig.KEEP_VARS):
                buf.append(row)
                n_rows += 1
                if len(buf) >= BATCH:
                    total, md = merge.merge_and_write(path, _table(buf), mode="merge",
                                                      dedup_keys=DEDUP)
                    buf = []
                    if md and (maxd is None or str(md) > str(maxd)):
                        maxd = md
        if buf:
            total, md = merge.merge_and_write(path, _table(buf), mode="merge",
                                              dedup_keys=DEDUP)
            if md and (maxd is None or str(md) > str(maxd)):
                maxd = md
    except DefinitiveError:
        raise
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"cepii_gravity: stream/merge failed after "
                             f"{n_rows:,} rows: {e!r}") from e

    if not n_rows:
        # Downloaded and opened fine, streamed nothing — a real break. Vintage NOT recorded.
        tally.structural_unit(f"{member.filename}: streamed 0 observations")
        return finalize(tally, total, since or None, source=SOURCE)

    tally.added_unit(n_rows, "gravity")
    print(f"[cepii_gravity] streamed {n_rows:,} obs; store -> {total:,}", flush=True)
    if cur_v:
        blob.write_bytes_atomic(
            os.path.join(out_dir, SIDECAR),
            json.dumps({"validator": cur_v, "url": url}, indent=1).encode("utf-8"))
    return finalize(tally, total, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors_from_parquet(path))
