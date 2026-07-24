"""S5 bulk fetcher — GLEIF LEI golden copy (CC0). A REFERENCE TABLE, not a time series.

One parquet: clean_full/gleif/lei_records.parquet (3,330,161 rows on R2), columns
(LEI, LegalName, LegalJurisdiction, EntityLegalFormCode, EntityStatus, RegistrationStatus,
ManagingLOU). There is NO series_key/obs_date/value here — row identity is the 20-char LEI —
so merge.merge_and_write(dedup_keys=("series_key","obs_date")) is INAPPLICABLE and would error.
The golden copy is the complete current truth, so the publish is a FULL-SNAPSHOT OVERWRITE.

Gate: https://leidata.gleif.org/api/v1/concatenated-files/lei2 is a real machine manifest whose
newest entry carries id + content_date + record_count + filesize and a PINNED download URL. Token
= "{id}|{content_date}|{record_count}". We download the manifest's pinned .../get/<id>/zip rather
than /latest/zip, so the bytes we publish are exactly the vintage we gated on (GLEIF refreshes
~3x/day, so 'latest' can advance mid-run). The published ETag is sha1('') — a fixed placeholder
identical across files — so If-None-Match is USELESS here and deliberately not used.

Memory: the zip is ~530 MB / 3.4M records. We stream-parse with the ingester's memory-bounded
iterparse writer (200k-row batches) into a TEMP parquet, then upload that file's BYTES via
blob.write_bytes_atomic — so we never materialise the whole table in RAM.

NEVER-SHRINK, by hand: an overwrite forfeits merge's shrink guard, so we refuse to publish a
snapshot with fewer than SHRINK_FLOOR x the current row count (a truncated download must not be
allowed to destroy the table) and surface it as a transient instead.

HONEST-STATUS: manifest/download failure -> transient (data kept, vintage not advanced). A parse
yielding 0 rows -> structural. A suspiciously small snapshot -> transient, not published.
"""
from __future__ import annotations
import json
import os
import tempfile
import zipfile

import pyarrow.parquet as pq
import requests

from ... import config, blob
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_gleif as ig   # reuse the memory-bounded LEI2 XML/CSV parser

SOURCE = "gleif"
MANIFEST = "https://leidata.gleif.org/api/v1/concatenated-files/lei2"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
PARQUET = "lei_records.parquet"
SIDECAR = "_bulk_vintages.json"
SHRINK_FLOOR = 0.90                 # refuse a snapshot smaller than 90% of what we hold
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def _latest(raise_transient: bool):
    """Newest manifest entry -> (token, download_url, record_count) or None."""
    try:
        r = requests.get(MANIFEST, headers=UA, timeout=120)
    except Exception as e:
        if raise_transient:
            raise TransientError(f"gleif: manifest fetch failed: {e}")
        return None
    if r.status_code != 200:
        if raise_transient:
            raise TransientError(f"gleif: manifest HTTP {r.status_code}")
        return None
    try:
        data = (r.json() or {}).get("data") or []
    except ValueError:
        if raise_transient:
            raise TransientError("gleif: manifest body is not JSON")
        return None
    if not data:
        if raise_transient:
            raise TransientError("gleif: manifest carried no entries")
        return None
    e = data[0]
    token = f"{e.get('id','')}|{e.get('content_date','')}|{e.get('record_count','')}"
    return token, e.get("file") or "", e.get("record_count") or 0


def current_vintage(unit) -> str | None:
    got = _latest(raise_transient=False)
    return f"gleif:{got[0]}" if got else None


def _load_sidecar(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_sidecar(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def _download(url, dest, tries=3):
    for a in range(tries):
        try:
            with requests.get(url, headers=UA, stream=True, timeout=1800) as r:
                if r.status_code in _TRANSIENT_HTTP:
                    if a == tries - 1:
                        raise TransientError(f"gleif: download HTTP {r.status_code}")
                    continue
                if r.status_code != 200:
                    raise TransientError(f"gleif: download HTTP {r.status_code}")
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            f.write(chunk)
            return
        except (requests.Timeout, requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            if a == tries - 1:
                raise TransientError(f"gleif: download {e}")
    raise TransientError("gleif: download retry budget exhausted")


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, PARQUET)
    tally = Tally()

    token, url, _rc = _latest(raise_transient=True)
    sidecar = _load_sidecar(out_dir)
    before = blob.row_count(path) if blob.exists(path) else 0

    if sidecar.get("lei2") == token and before:
        return finalize(tally, before, since or None, source=SOURCE)   # current -> no_change
    if not url:
        raise TransientError("gleif: manifest entry had no download URL")

    fd, tmp_zip = tempfile.mkstemp(prefix="gleif_", suffix=".zip"); os.close(fd)
    fd, tmp_pq = tempfile.mkstemp(prefix="gleif_", suffix=".parquet"); os.close(fd)
    try:
        try:
            _download(url, tmp_zip)
        except TransientError:
            tally.transient_unit()
            return finalize(tally, before, since or None, source=SOURCE)

        # stream-parse (memory-bounded, 200k-row batches) into the temp parquet
        try:
            z = zipfile.ZipFile(tmp_zip)
            members = z.namelist()
            xmls = [m for m in members if m.lower().endswith(".xml")]
            csvs = [m for m in members if m.lower().endswith(".csv")]
            if xmls:
                with z.open(xmls[0]) as f:
                    n_new = ig._parse_xml(f, tmp_pq)
            elif csvs:
                with z.open(csvs[0]) as f:
                    n_new = ig._parse_csv(f, tmp_pq)
            else:
                tally.structural_unit()
                return finalize(tally, before, since or None, source=SOURCE)
        except zipfile.BadZipFile:
            tally.structural_unit()
            return finalize(tally, before, since or None, source=SOURCE)
        except Exception:
            tally.transient_unit()
            return finalize(tally, before, since or None, source=SOURCE)

        if not n_new:
            tally.structural_unit()
            return finalize(tally, before, since or None, source=SOURCE)

        # hand-rolled never-shrink: an overwrite has no merge guard behind it
        if before and n_new < before * SHRINK_FLOOR:
            tally.transient_unit()
            return finalize(tally, before, since or None, source=SOURCE)

        # publish the parquet BYTES (no full-table materialisation)
        with open(tmp_pq, "rb") as f:
            blob.write_bytes_atomic(path, f.read())

        sidecar["lei2"] = token
        _save_sidecar(out_dir, sidecar)
        tally.added_unit(max(0, n_new - before))
        res = finalize(tally, n_new, since or None, source=SOURCE)
        if res.status in ("ok", "no_change"):
            res.new_vintage = f"gleif:{token}"
        return res
    finally:
        for p in (tmp_zip, tmp_pq):
            try:
                os.remove(p)
            except OSError:
                pass
