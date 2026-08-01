"""S1 fetcher — NOAA NCEI GSOM + GSOY (US federal, public domain, no key).

3,135,873 series / 552,548,787 observations. This source was in the registry and routed to the
workstation, and had no fetcher. It therefore never ran: the orchestrator's no-adapter branch
printed nothing during a run (fixed 2026-08-01), so noaa vanished out of a 14-source pass
between `NOT DUE istat` and `>>> oecd` leaving no trace at all, and I read that silence as the
pass being busy with it (R211).

WHY THERE WAS NO FETCHER. jobs/ingest_noaa.py refreshes from `.../data/<ds>/access/`, which is
ONE WIDE CSV PER STATION — roughly 130,000 HTTP requests per dataset. That is a defensible
one-off backfill and an impossible nightly. The fix is not to parallelise it: NCEI publishes the
identical corpus as a single object per dataset,

    https://www.ncei.noaa.gov/data/gsom/archive/gsom-latest.tar.gz   1.50 GB
    https://www.ncei.noaa.gov/data/gsoy/archive/gsoy-latest.tar.gz   141.7 MB

so the probe is two HEADs and the refresh is two downloads.

OVERWRITE, NOT MERGE — AND THAT IS A MEASURED CLAIM, NOT A CONVENIENCE. Every member holds its
station's WHOLE history (checked 2026-08-01: AE000041196.csv spans 1945..2023, ACW00011647.csv
spans 1958..2026), so the tarball is a complete restatement and a rewrite loses nothing. This
matters enormously at this size: merging would mean reading back and rewriting a 262-million-row
gsom__US shard on every refresh, which is the same wall statcan and oecd are stuck behind. If
NOAA ever switches to publishing increments this assumption breaks silently, so update() ASSERTS
the corpus is whole-history before it publishes anything.

MEMBER ORDER IS ARBITRARY (AEM…, AE0…, ACW…, AEM…, AG0…), so no code here may assume a country
prefix arrives contiguously. ShardWriter keeps a buffer and an open writer per prefix, which is
the right shape for that — with a total_cap added, because a per-prefix flush threshold across
~250 prefixes bounds nothing.

THE PARSE IS THE INGEST'S. jobs.ingest_noaa.melt_stream is the same function the backfill uses,
fed a tar member instead of a path. A second parser reading the same columns is how two paths
drift into disagreeing about what an element means.

NO SERIES CURSORS, DISCLOSED. A restatement changes every series, so honest cursors would name
3,135,873 keys — 62x _common.CURSOR_CAP. Reporting a truncated 50,000 would tell the CSV derive
that those and only those changed, which is worse than reporting none: the other 3.08 million
CSVs would silently keep serving the previous vintage. update() returns no cursors and says so,
and noaa's CSVs are re-derived wholesale when its vintage moves.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
import tempfile

import requests

from jobs import ingest_noaa as J

from ... import blob, config
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "noaa"
SIDECAR = "_archive_vintage.json"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}

# A station whose record starts before this cannot have come from an increment. NOAA's GHCN
# archive reaches back to the 19th century, so a tarball in which NO station predates it is a
# publisher change, not a quiet month - see the assert in _stream_dataset.
WHOLE_HISTORY_BEFORE = 1990


def _validator(sess, url):
    """The object's own HTTP validator, or None if it does not 200."""
    try:
        r = sess.head(url, headers=UA, timeout=120, allow_redirects=True)
    except Exception:                                        # noqa: BLE001
        return None
    if r.status_code != 200:
        return None
    v = (r.headers.get("ETag") or r.headers.get("Last-Modified")
         or r.headers.get("Content-Length"))
    return v or None


def current_vintage(unit):
    """One token over BOTH archives. Moves iff NCEI republishes either dataset.

    Both must answer. A token built from whichever half responded would go stale-but-stable
    when one dataset's URL broke, and the source would report itself current forever.
    """
    sess = requests.Session()
    parts = []
    for ds in sorted(J.DATASETS):
        v = _validator(sess, J.DATASETS[ds]["archive"])
        if v is None:
            return None                                      # undeterminable -> cadence-gated fetch
        parts.append(f"{ds}={v}")
    return "noaa:" + hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _stream_dataset(ds: str, sess, station_meta: dict, tally: Tally) -> tuple[int, int]:
    """Download one archive and melt every member into its country shard.

    -> (observations emitted, members parsed). Raises rather than publishing a partial
    corpus: a truncated download that overwrote the shards would DELETE published history,
    which no vintage bump could undo.
    """
    url = J.DATASETS[ds]["archive"]
    tmp = os.path.join(tempfile.gettempdir(), f"noaa_{ds}_latest.tar.gz")
    try:
        with sess.get(url, headers=UA, timeout=3600, stream=True) as r:
            r.raise_for_status()
            declared = int(r.headers.get("Content-Length") or 0)
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8 << 20):
                    fh.write(chunk)
        got = os.path.getsize(tmp)
        if declared and got != declared:
            # A short read here would overwrite complete shards with a partial corpus.
            raise TransientError(f"noaa/{ds}: truncated download — got {got:,} of "
                                 f"{declared:,} bytes")
        print(f"[noaa] {ds}: {got:,} bytes downloaded", flush=True)

        sw = J.ShardWriter(ds, batch_rows=400_000, total_cap=1_000_000)
        n_obs = n_members = 0
        earliest = None
        with tarfile.open(tmp, mode="r:gz") as tf:
            for m in tf:
                if not m.isfile() or not m.name.lower().endswith(".csv"):
                    continue
                sid = os.path.basename(m.name)[:-4]
                if len(sid) < 3:
                    continue
                fh = tf.extractfile(m)
                if fh is None:
                    continue
                got_n = J.melt_stream(ds, io.TextIOWrapper(fh, encoding="utf-8",
                                                           errors="replace", newline=""),
                                      sid, sid[:2], sw)
                n_obs += got_n
                n_members += 1
                if n_members % 20_000 == 0:
                    print(f"[noaa] {ds}: {n_members:,} stations, {n_obs:,} obs", flush=True)
        for (_, _, _), (_, dmin, _) in sw._series_span.items():
            if earliest is None or dmin < earliest:
                earliest = dmin

        if not n_obs:
            tally.structural_unit(f"{ds}: archive downloaded but melted 0 observations")
            return 0, n_members
        # THE OVERWRITE PRECONDITION, checked before anything is published. Publishing by
        # overwrite is only safe while the tarball restates all history; if NOAA ever ships
        # increments instead, the deepest record in the whole corpus jumps forward and this
        # refuses rather than replacing decades of published data with one recent window.
        if earliest is None or earliest.year >= WHOLE_HISTORY_BEFORE:
            raise DefinitiveError(
                f"noaa/{ds}: the archive's deepest observation is "
                f"{earliest}, not before {WHOLE_HISTORY_BEFORE} — this no longer looks like a "
                f"whole-history restatement, and these shards are published by OVERWRITE. "
                f"Existing data kept; re-check the publisher before enabling this source.")

        sw.close(station_meta)
        tally.added_unit(n_obs, ds)
        print(f"[noaa] {ds}: {n_members:,} stations, {n_obs:,} obs, deepest {earliest}",
              flush=True)
        return n_obs, n_members
    except (DefinitiveError, TransientError):
        raise
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"noaa/{ds}: {e!r}") from e
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    J.OUT = out_dir                                          # the ingest writes where we read

    sess = requests.Session()
    cur = current_vintage(unit)
    sidecar = _load(out_dir)
    tally = Tally()

    if cur and sidecar.get("validator") == cur:
        print(f"[noaa] both archives unchanged ({cur}) — skipped", flush=True)
        return finalize(tally, sidecar.get("rows", 0), since or None, source=SOURCE)

    station_meta = J.load_station_meta()
    if not station_meta:
        print("[noaa] ghcnd-stations.txt absent — sidecars will carry no station names "
              "or coordinates (observations are unaffected)", flush=True)

    total = 0
    for ds in sorted(J.DATASETS):
        n, _ = _stream_dataset(ds, sess, station_meta, tally)
        total += n

    # Publish every shard this run produced. write_table_atomic is not usable here: the shards
    # are written by the ingest's own pq.ParquetWriter, and reading a 704 MB shard back just to
    # hand it to the blob layer would materialise it whole in RAM.
    published = bytes_out = 0
    for name in sorted(os.listdir(out_dir)):
        if not name.endswith(".parquet"):
            continue
        n_bytes = blob.publish_file(os.path.join(out_dir, name))
        if n_bytes:
            published += 1
            bytes_out += n_bytes
    print(f"[noaa] published {published:,} object(s), {bytes_out / 1e9:,.2f} GB", flush=True)

    if cur:
        blob.write_bytes_atomic(
            os.path.join(out_dir, SIDECAR),
            json.dumps({"validator": cur, "rows": total, "objects": published},
                       indent=1).encode("utf-8"))

    # series_cursors deliberately omitted — see the module docstring. Say so where the operator
    # reads the run, not only in a file they may never open.
    print("[noaa] no series cursors reported: a whole-corpus restatement changes every one of "
          "3,135,873 series, so noaa's CSVs need a full re-derive rather than a cursor-driven "
          "incremental one", flush=True)
    return finalize(tally, total, since or None, source=SOURCE)
