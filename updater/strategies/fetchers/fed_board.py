"""S1 bulk fetcher — Federal Reserve Board DDP releases (federalreserve.gov, no key).

The Fed publishes one ZIP per Data Download Program release (H15, G17, Z1, …), each
containing a `*_data.xml` of every series in that release.

DISCOVERY IS ALREADY HONEST HERE, unlike most ingests in this tree: `discover_releases()`
scrapes the DDP home page for `Choose.aspx?rel=<REL>` codes and the ingest UNIONS that with a
pinned list, so a newly added release is picked up rather than silently missed. That is reused
verbatim — the fetcher does not carry its own release list, because a second list is a second
thing to go stale.

THE HTTP VALIDATOR ON THIS ENDPOINT IS A LIE — MEASURED, NOT ASSUMED. The registry's
adapter note proposes gating on "HTTP ETag / Last-Modified on Output.aspx?rel=<REL>". Probed
directly, Output.aspx is generated per request and:
    Last-Modified   advances on EVERY call — 03:17:40, 03:18:01, 03:18:21 on three HEADs
                    20s apart. It is the generation time, not the content date.
    ETag            absent.
    Content-Length  absent on HEAD.
    Range           unsupported: `Range: bytes=-65536` returns 200 with the WHOLE body and
                    no Content-Range, so the zip's central directory (which carries a CRC32
                    per entry) cannot be sampled cheaply.
An http_vintage gate here would therefore report "changed" on every single run and re-download,
re-parse and re-upload all 18 releases daily, forever, while looking like a working cache.

What IS stable is the BODY: two full GETs of CHGDEL returned byte-identical content
(sha256 0f741306efb4ccd7…, 279,024 B both times). So the honest signal is a CONTENT HASH.

THAT IS AFFORDABLE, ALSO MEASURED. All 18 releases are 79.9 MB and download in 9.4 s
(largest is Z1 at 36.4 MB / 3.2 s — the registry's "Z.1 ~590MB" is the uncompressed XML, not
the zip). So each run pays one cheap download per release and gates the genuinely expensive
work — XML iterparse, parquet write, R2 upload — on the hash actually moving.

DOWNLOADING IS DELEGATED to ig.download_zip: it already has 5-try exponential backoff and
validates that the payload is a real ZIP containing <REL>_data.xml (so an HTML error page
cannot be hashed and cached as if it were data). It reuses an existing valid file, which is
why the temp zip is always removed in a finally — a stale temp must never satisfy a later run.

PUBLISHING. `ingest_release` writes with a raw `pq.ParquetWriter` to a local path, and only
blob knows about R2, so a local write never reaches the published store (ledger R36) — the run
would look successful and publish nothing. ig.OUT and config.source_dir(SOURCE) are the SAME
directory, so the bytes are already at their store path and what is missing is just the PUT:
blob.publish_file streams them up without materialising a multi-GB table in RAM.

OVERWRITE, NOT MERGE, IS CORRECT HERE. This is bulk_snapshot_if_changed: when the Fed
republishes a release the new ZIP is the whole truth for it, including revisions to history, so
the parser's overwrite semantics are what we want. Merging would preserve superseded values.

HONEST-STATUS: discovery failure -> TransientError (partial, retried, data kept). A per-release
download/parse failure -> transient_unit for that release only, so one bad ZIP cannot stop the
others publishing. A release that parses to ZERO observations -> structural_unit with its hash
NOT recorded, so a parser break resurfaces next run instead of being sealed in.
"""
from __future__ import annotations
import hashlib
import json
import os
import tempfile

from ... import config, blob
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, finalize, merge_cursors
from jobs import ingest_fed_board as ig     # reuse discovery + downloader + production parser

SOURCE = "fed_board"
SIDECAR = "_release_hashes.json"
BUDGET_MIN = 25


def _zip_url(rel: str) -> str:
    """The EXACT url ig.download_zip fetches — built from its own OUTPUT_URL constant.

    Not guessed. A first draft appended `&label=include`, which download_zip does NOT send
    (`params={"rel": rel, "filetype": "zip"}`). Deriving it keeps one source of truth.
    """
    return f"{ig.OUTPUT_URL}?rel={rel}&filetype=zip"


def _releases():
    try:
        live = ig.discover_releases()
    except Exception:                                        # noqa: BLE001
        live = []
    return sorted(set(live) | set(getattr(ig, "RELEASES", ())))


def _hash_file(path: str) -> str:
    """sha256[:16] of a file, streamed — same digest shape as _vintage.content_hash."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _fetch_hash(rel: str):
    """Download a release to a temp path and hash it. Returns (tmp_path, hash), or None.

    The caller OWNS tmp_path and must delete it — including on the unchanged path, or
    ig.download_zip's reuse-if-valid check would serve a stale file to a later run.
    """
    tmp = os.path.join(tempfile.gettempdir(), f"fed_{rel}.zip")
    try:
        if not ig.download_zip(rel, tmp):
            raise IOError("empty download")
        return tmp, _hash_file(tmp)
    except Exception:                                        # noqa: BLE001
        try:
            os.remove(tmp)
        except OSError:
            pass
        return None


def current_vintage(unit):
    """Hash of the release set + each release's CONTENT hash (~80 MB / ~10 s, measured).

    Deliberately NOT an HTTP-validator hash: see the module docstring — Last-Modified on this
    endpoint moves every request, so a validator-based token would never match and the
    due-check would be permanently, invisibly hot.
    """
    rels = _releases()
    if not rels:
        return None
    h = hashlib.sha256()
    for rel in rels:
        got = _fetch_hash(rel)
        if got is None:
            return None                                      # unknown -> let cadence decide
        tmp, digest = got
        try:
            os.remove(tmp)
        except OSError:
            pass
        h.update(f"{rel}={digest};".encode())
    return f"fed_board:{len(rels)}:{h.hexdigest()[:16]}"


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


def _publish(out_dir, rel) -> int:
    """PUT the parquet(s) ingest_release just wrote to the store. Returns rows published.

    Row count comes from the parquet FOOTER, not from reading the file — a release's obs
    table can be multi-GB and nothing here needs its contents.
    """
    import pyarrow.parquet as pq
    total = 0
    for suffix in (f"{rel}.parquet", f"{rel}__series.parquet"):
        local = os.path.join(ig.OUT, suffix)
        if not os.path.exists(local):
            continue
        blob.publish_file(os.path.join(out_dir, suffix))
        if suffix == f"{rel}.parquet":
            total = pq.ParquetFile(local).metadata.num_rows
    return total


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(ig.OUT, exist_ok=True)

    rels = _releases()
    if not rels:
        raise TransientError("fed_board: DDP release discovery returned nothing")

    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    unchanged = 0
    cursors: dict = {}          # §5.7 changed-series set, per republished release
    dl = Deadline(minutes=BUDGET_MIN)

    for rel in rels:
        if dl.spent():
            print(f"[fed_board] budget {BUDGET_MIN} min spent — {rel} deferred", flush=True)
            tally.transient_unit(rel)
            continue

        got = _fetch_hash(rel)
        if got is None:
            tally.transient_unit(rel)
            continue
        tmp, digest = got
        stats = None

        try:
            stored = os.path.join(out_dir, f"{rel}.parquet")
            if sidecar.get(rel) == digest and blob.exists(stored):
                unchanged += 1        # identical bytes -> no parse, no upload. The whole point.
                continue

            try:
                stats = ig.ingest_release(rel, tmp)
            except Exception:                                # noqa: BLE001
                tally.transient_unit(rel)
                continue
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass

        n_obs = (stats or {}).get("n_obs", 0)
        if not n_obs:
            # Downloaded and validated fine, parsed to nothing — a real break. Hash NOT
            # recorded, so this resurfaces next run instead of being sealed in.
            tally.structural_unit(f"{rel}: release parsed to 0 observations")
            continue

        published += _publish(out_dir, rel)
        merge_cursors(cursors, os.path.join(out_dir, f"{rel}.parquet"))
        tally.added_unit(n_obs, rel)
        sidecar[rel] = digest                                # record ONLY after publishing

    if len(cursors) >= CURSOR_CAP:
        print(f"[fed_board] cursor set hit the {CURSOR_CAP:,} cap — further changed series are not "
              f"individually reported; the orchestrator's derive-all path covers small "
              f"catalogs", flush=True)
    if unchanged:
        print(f"[fed_board] {unchanged}/{len(rels)} release(s) byte-identical — skipped",
              flush=True)
    _save(out_dir, sidecar)
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    return finalize(tally, published, since or None, source=SOURCE,
                    series_cursors=cursors or None)
