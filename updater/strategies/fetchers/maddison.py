"""S1 bulk fetcher — Maddison Project Database (rug.nl, GGDC).

One workbook (mpd2020.xlsx) holding long-run GDP-per-capita and population estimates:
36,905 observations across 338 series.

WHY A FETCHER AND NOT A WRAPPER OF main(). `jobs/ingest_maddison.py:main()` begins with

    if os.path.exists(out_path):
        log(f"Already done: {n:,} rows"); return

which is right for a one-off load and inert as an updater: scheduled, it would log "Already
done" forever and report success. Its parse was also inline in main() with no callable to
reuse, so — rather than copy it and let the two drift — the parse was EXTRACTED into
`ingest_maddison.parse_xlsx()`, a pure function with no download, no skip rule and no error
policy. main() now delegates to it too, so there is exactly ONE parser. Verified: main no
longer mentions openpyxl, and the extracted parser reproduces the published store EXACTLY —
36,905 rows / 338 series parsed from the live workbook against 36,905 rows on disk.

VINTAGE — MEASURED (the R164 rule). rug.nl serves a stable validator on the workbook: two HEADs
15s apart returned identical ETag (W/"69d3b691-…-33.36;1605281601766"), Last-Modified
(Fri, 13 Nov 2020 15:33:21 GMT) and Content-Length (1,764,793). Unlike federalreserve.gov
(Last-Modified regenerated per request) or data.bis.org (ETags flapping across replicas), an
http_vintage gate genuinely hits here.

The dataverse.nl mirror in ig.URLS returns **403** to us and is kept only as the ingest's
fallback; it is NOT part of the vintage, because hashing a 403 would produce a stable token
that means nothing. The primary URL is the one gated.

MADDISON IS A RELEASE DATASET, NOT A TICKING SERIES. mpd2020 has not moved since 2020 and the
next release will be a new file. So the honest expectation is `no_change` essentially forever,
with the gate moving when GGDC republishes — that is the source being correctly current, not a
frozen one, and it is exactly why the vintage must key on the publisher's own validator rather
than on a clock.

HONEST-STATUS: download failure -> TransientError (retried, data kept). A workbook that
downloads but parses to ZERO rows -> DefinitiveError via finalize's structural path, with the
vintage NOT advanced, so a parser break resurfaces instead of being sealed in.
"""
from __future__ import annotations
import json
import os

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage
from jobs import ingest_maddison as ig     # URLS + the EXTRACTED pure parser

SOURCE = "maddison"
SIDECAR = "_workbook_vintage.json"
DEDUP = ("series_key", "obs_date")
PRIMARY = ig.URLS[0]


def current_vintage(unit):
    """The workbook's own HTTP validator. Moves iff GGDC republishes it."""
    try:
        v = http_vintage(PRIMARY)
    except Exception:                                        # noqa: BLE001
        return None
    return f"maddison:{v}" if v else None


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "maddison.parquet")

    sess = requests.Session()
    try:
        cur_v = http_vintage(PRIMARY, session=sess) or ""
    except Exception:                                        # noqa: BLE001
        cur_v = ""                                           # unknown -> fetch anyway

    sidecar = _load(out_dir)
    tally = Tally()
    if cur_v and sidecar.get("primary") == cur_v and blob.exists(path):
        print(f"[maddison] workbook unchanged ({cur_v}) — skipped", flush=True)
        return finalize(tally, blob.row_count(path), since or None, source=SOURCE)

    try:
        r = sess.get(PRIMARY, headers=ig.UA, timeout=300)
        r.raise_for_status()
        content = r.content
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"maddison: workbook download failed: {e!r}") from e

    try:
        keys, dates, vals = ig.parse_xlsx(content)
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"maddison: workbook parse failed: {e!r}") from e

    if not vals:
        # Downloaded fine, parsed to nothing — a real break. The vintage is NOT recorded, so
        # this resurfaces next run instead of being sealed in behind a green status.
        tally.structural_unit("mpd2020.xlsx parsed to 0 observations")
        return finalize(tally, blob.row_count(path) if blob.exists(path) else 0,
                        since or None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    before = blob.row_count(path) if blob.exists(path) else 0
    total, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, total - before), "mpd2020")
    print(f"[maddison] parsed {len(keys):,} obs / {len(set(keys)):,} series; "
          f"store {before:,} -> {total:,}", flush=True)

    if cur_v:
        blob.write_bytes_atomic(
            os.path.join(out_dir, SIDECAR),
            json.dumps({"primary": cur_v, "url": PRIMARY}, indent=1).encode("utf-8"))
    return finalize(tally, total, maxd or (since or None), source=SOURCE)
