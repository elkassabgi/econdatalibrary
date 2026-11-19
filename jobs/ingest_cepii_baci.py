#!/usr/bin/env python3
"""CEPII BACI bilateral trade database ingest.

BACI provides harmonised bilateral trade flows (value + quantity) for ~200 countries
at the HS 6-digit product level. Multiple HS vintages available.

License: Etalab Open Licence 2.0 (commercial OK, attribution required).
Attribution: "Source: CEPII, BACI (Etalab Open Licence 2.0)"
Source: https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37

Pulls the HS17 (2017 revision) and HS96 vintages for maximum history.
Run: python jobs/ingest_cepii_baci.py            (release read from CEPII's product page)
     python jobs/ingest_cepii_baci.py --release V202601

EVERYTHING IS KEYED ON THE RELEASE (2026-11-19). Until then download() returned ANY cached
data/raw/cepii_baci/<HS>.zip over 1 MB and ingest_zip() returned early whenever
baci_<hs>.parquet existed, while FILES pinned V202401b. Neither the cache nor the skip looked at
the release: a fetcher run that asked for V202601 by URL could get a cached zip of any release
back, skip the ingest because the parquet existed, and book the run as success. (The served
parquets themselves run to 2024, so they are a later release than V202401b - probably V202601,
not proven; only _country_codes.json came from V202401b.) Now:

  * the raw zip lives at data/raw/cepii_baci/<release>/<HS>.zip with a <HS>.zip.json sidecar
    (release, url, bytes, sha256); it is reused only when that sidecar names the release asked.
    A download resumes with HTTP Range from a .part whose own sidecar pins url+length+ETag, and
    the finished file must be exactly Content-Length bytes and a readable zip.
  * the parquet keeps its served name (baci_<hs>.parquet); the release it holds is recorded in
    _baci_<hs>.release.json beside it (underscore + .json: every store reader skips it). The
    ingest is skipped ONLY when that sidecar names the release asked, the zip hash matches and
    the parquet footer still shows the sidecar's row count and schema.
  * a rebuild parses into a part file in a STAGING dir OUTSIDE the store tree (R1949: staging
    inside the served tree gets listed, published and served), verifies it (rows > 0, schema,
    CSV header, every member stamped with the release, unparseable rows bounded, no shrink in
    rows or last year, every country code mapped), and only then os.replace()s it over the old
    parquet. The old parquet is never removed before the new one verifies. A failed replace
    keeps the verified part and its sidecar in staging, so the next run swaps without a re-parse.
  * an older release never replaces a newer one unless allow_downgrade=True.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import hashlib
import io
import json
import os
import sys
import time
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from core.atomic import atomic_replace                                # noqa: E402

OUT = os.path.join(ROOT, "data", "clean_full", "cepii_baci")
TMP = os.path.join(ROOT, "data", "raw", "cepii_baci")
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
CLASSIFICATIONS = ("HS17", "HS96")
URL_FMT = "https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_{hs}_{release}.zip"

SCHEMA = pa.schema([
    ("year",    pa.int16()),
    ("exporter", pa.string()),
    ("importer", pa.string()),
    ("product",  pa.string()),
    ("value",    pa.float64()),  # 1000 USD
    ("quantity", pa.float64()),  # metric tons
])
HEADER = ["t", "i", "j", "k", "v", "q"]       # BACI's trade CSV header, exactly
BATCH = 500_000
# Rows that fail to parse are counted, not silently dropped. Above this fraction the release is
# refused: a partial schema change shows up as a smaller total, not as zero rows.
MAX_SKIP_FRAC = 0.001


class ReleaseRefused(Exception):
    """The release was fetched but failed verification; the old parquet is kept untouched."""


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def release_url(hs: str, release: str) -> str:
    return URL_FMT.format(hs=hs, release=release)


def sidecar_path(name: str, out_dir: str) -> str:
    return os.path.join(out_dir, f"_baci_{name.lower()}.release.json")


def parquet_path(name: str, out_dir: str) -> str:
    return os.path.join(out_dir, f"baci_{name.lower()}.parquet")


def _read_json(p: str):
    try:
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _write_json_atomic(p: str, obj) -> None:
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = f"{p}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=1, sort_keys=True)
    atomic_replace(tmp, p)


def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def _inside(child: str, parent: str) -> bool:
    c = os.path.normcase(os.path.abspath(child))
    p = os.path.normcase(os.path.abspath(parent))
    try:
        return os.path.commonpath([c, p]) == p
    except ValueError:                       # different drives
        return False


def default_stage_dir(out_dir: str) -> str:
    """<store root>/../_staging_cepii_baci — beside clean_full, same volume, outside every reader."""
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(out_dir))),
                        "_staging_cepii_baci")


# --------------------------------------------------------------------------------------------- #
# Download — release-keyed cache, resumable
# --------------------------------------------------------------------------------------------- #

def download(name, url, *, release, raw_dir=TMP, sess=None):
    """Return the local path of `url`'s zip for `release`, downloading only what is missing."""
    if not release:
        raise ValueError("download() needs the release: an unkeyed cache reuses an old vintage")
    sess = sess or requests.Session()
    d = os.path.join(raw_dir, release)
    os.makedirs(d, exist_ok=True)
    dest = os.path.join(d, f"{name}.zip")
    meta_p = dest + ".json"
    meta = _read_json(meta_p)
    cached = (meta and meta.get("release") == release and meta.get("url") == url
              and os.path.exists(dest) and os.path.getsize(dest) == meta.get("bytes"))
    try:
        h = sess.head(url, headers={"User-Agent": UA}, timeout=120, allow_redirects=True)
        h.raise_for_status()
    except Exception as e:                                           # noqa: BLE001
        if cached:                       # offline, but the cache is this release: use it
            log(f"{name}: HEAD failed ({type(e).__name__}); using the cached {release} zip")
            return dest
        raise
    length = int(h.headers.get("Content-Length") or 0)
    etag = h.headers.get("ETag")
    if cached and meta.get("bytes") == length and meta.get("etag") == etag:
        log(f"{name}: {release} already on disk ({meta['bytes']/1e6:.0f} MB)")
        return dest
    if cached:
        log(f"{name}: CEPII re-issued {release} (bytes/ETag changed) - downloading again")
    if length <= 0:
        raise IOError(f"{name}: no Content-Length on {url} - cannot verify a complete download")

    part = dest + ".part"
    pmeta_p = part + ".json"
    pmeta = _read_json(pmeta_p)
    have = os.path.getsize(part) if os.path.exists(part) else 0
    if not (pmeta and pmeta.get("url") == url and pmeta.get("length") == length
            and pmeta.get("etag") == etag and 0 < have < length):
        have = 0                             # a part from another file/length/ETag is not resumable
    _write_json_atomic(pmeta_p, {"url": url, "length": length, "etag": etag, "release": release})

    hdrs = {"User-Agent": UA}
    if have:
        hdrs["Range"] = f"bytes={have}-"
        if etag:
            hdrs["If-Range"] = etag          # a re-upload answers 200 (whole file), never a splice
    log(f"{name}: downloading {url} ({length/1e6:.0f} MB, resume at {have/1e6:.0f} MB)")
    with sess.get(url, headers=hdrs, stream=True, timeout=600) as r:
        r.raise_for_status()
        if have and r.status_code == 206 and str(r.headers.get("Content-Range", "")
                                                   ).startswith(f"bytes {have}-"):
            mode = "ab"
        else:
            mode, have = "wb", 0             # the server ignored the Range: start over
        done = have
        last = time.time()
        with open(part, mode) as f:
            for chunk in r.iter_content(1 << 20):
                if chunk:
                    f.write(chunk)
                    done += len(chunk)
                if time.time() - last > 15:
                    log(f"  {name}: {done/1e6:.0f}/{length/1e6:.0f} MB")
                    last = time.time()
    got = os.path.getsize(part)
    if got != length:
        raise IOError(f"{name}: {got:,} bytes on disk, Content-Length {length:,} - incomplete, "
                      f"kept for resume")
    if not zipfile.is_zipfile(part):
        os.remove(part)
        raise IOError(f"{name}: {url} is not a readable zip - discarded")
    sha = _sha256(part)
    atomic_replace(part, dest)
    _write_json_atomic(meta_p, {"release": release, "url": url, "bytes": length, "sha256": sha,
                                "etag": etag})
    try:
        os.remove(pmeta_p)
    except OSError:
        pass
    log(f"{name}: downloaded {length/1e6:.0f} MB")
    return dest


def quarantine(zip_path: str) -> str:
    """Move a cached zip that failed its CRC aside (never delete), so the next run downloads it."""
    bad = f"{zip_path}.bad-{time.strftime('%Y%m%dT%H%M%S')}"
    os.replace(zip_path, bad)
    if os.path.exists(zip_path + ".json"):
        os.replace(zip_path + ".json", bad + ".json")
    return bad


def _zip_identity(zip_path: str) -> dict:
    meta = _read_json(zip_path + ".json") or {}
    size = os.path.getsize(zip_path)
    sha = meta.get("sha256") if meta.get("bytes") == size else None
    return {"zip_name": os.path.basename(zip_path), "zip_bytes": size,
            "zip_sha256": sha or _sha256(zip_path)}


# --------------------------------------------------------------------------------------------- #
# Ingest — staged, verified, then replaced
# --------------------------------------------------------------------------------------------- #

def _footer(p: str):
    """(rows, schema_ok, max_year) of a parquet, or None when it is absent/unreadable."""
    if not os.path.exists(p):
        return None
    try:
        md = pq.read_metadata(p)
        schema_ok = md.schema.to_arrow_schema().remove_metadata().equals(SCHEMA)
        yi = md.schema.to_arrow_schema().get_field_index("year")
        mx = None
        if yi >= 0:
            for g in range(md.num_row_groups):
                st = md.row_group(g).column(yi).statistics
                if st is not None and st.has_min_max:
                    mx = st.max if mx is None else max(mx, st.max)
        return md.num_rows, schema_ok, mx
    except Exception:                                                # noqa: BLE001
        return None


def parquet_current(name: str, release: str, out_dir: str = OUT) -> bool:
    """True only when the parquet in place IS `release`: sidecar release, footer rows, schema."""
    sc = _read_json(sidecar_path(name, out_dir))
    if not sc or sc.get("release") != release:
        return False
    f = _footer(parquet_path(name, out_dir))
    return bool(f) and f[0] == sc.get("rows") and f[1]


def installed_release(name: str, out_dir: str = OUT):
    sc = _read_json(sidecar_path(name, out_dir))
    return sc.get("release") if sc else None


def legacy_release(out_dir: str):
    """A store from before the release sidecars: the release its _country_codes.json came from."""
    import re
    d = _read_json(os.path.join(out_dir, "_country_codes.json")) or {}
    m = re.search(r"_(V\d{6}[a-z]?)\.csv$", str(d.get("from", "")), re.IGNORECASE)
    return m.group(1) if m else None


def _country_codes(z: zipfile.ZipFile) -> set:
    names = [n for n in z.namelist() if "country_codes" in n.lower()]
    if not names:
        raise ReleaseRefused("no country_codes csv inside the zip")
    with z.open(names[0]) as f:
        rows = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))
    return {r["country_code"].strip() for r in rows if r.get("country_code")}


def _parse_to(z, csvs, part, release):
    """Stream every trade CSV into `part`. Returns (rows, skipped, max_year, country codes seen)."""
    writer = pq.ParquetWriter(part, SCHEMA, compression="zstd")
    total = skipped = 0
    max_year = None
    seen = set()
    try:
        for csv_name in csvs:
            with z.open(csv_name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig", errors="replace"))
                header = [h.strip() for h in next(reader, [])]
                if header != HEADER:
                    raise ReleaseRefused(f"{csv_name}: header {header} != {HEADER}")
                cols = [[] for _ in range(6)]
                for row in reader:
                    try:
                        t, i, j, k, v, q = row
                        yr = int(t)
                        v = v.strip()
                        val = float(v) if v else 0.0          # as before 2026-11-19
                        q = q.strip()
                        qty = float(q) if q and q != "NA" else 0.0
                    except (ValueError, TypeError):
                        skipped += 1
                        continue
                    i = i.strip(); j = j.strip()
                    seen.add(i); seen.add(j)
                    if max_year is None or yr > max_year:
                        max_year = yr
                    for c, x in zip(cols, (yr, i, j, k.strip(), val, qty)):
                        c.append(x)
                    if len(cols[0]) >= BATCH:
                        writer.write_batch(pa.record_batch(
                            [pa.array(c, t_) for c, t_ in zip(cols, SCHEMA.types)], schema=SCHEMA))
                        total += len(cols[0])
                        cols = [[] for _ in range(6)]
                if cols[0]:
                    writer.write_batch(pa.record_batch(
                        [pa.array(c, t_) for c, t_ in zip(cols, SCHEMA.types)], schema=SCHEMA))
                    total += len(cols[0])
            log(f"  {csv_name}: cumulative {total:,} trade flows ({skipped:,} unparseable)")
    finally:
        writer.close()                       # no handle may stay open on the part (Windows)
    return total, skipped, max_year, seen


def _remove(p):
    try:
        if os.path.exists(p):
            os.remove(p)
    except OSError:
        pass


def ingest_zip(name, zip_path, *, release, out_dir=OUT, stage_dir, allow_downgrade=False,
               allow_shrink=False) -> dict:
    """Install `release` of classification `name` from `zip_path` into out_dir/baci_<name>.parquet.

    Returns {"rows", "rebuilt", "release"}. rows == 0 means the zip held no trade rows; nothing was
    replaced. Raises ReleaseRefused when verification fails (old parquet untouched), and any
    other exception (I/O, a held file on replace) as itself - also with the old parquet untouched.
    """
    if not release:
        raise ValueError("ingest_zip() needs the release")
    if _inside(stage_dir, os.path.dirname(os.path.abspath(out_dir))):
        raise ValueError(f"staging dir {stage_dir} is inside the store root "
                         f"{os.path.dirname(os.path.abspath(out_dir))} - readers would serve it")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(stage_dir, exist_ok=True)
    out_path = parquet_path(name, out_dir)
    sc_path = sidecar_path(name, out_dir)
    old_sc = _read_json(sc_path) or {}
    old_release = old_sc.get("release") or legacy_release(out_dir)
    if old_release and release < old_release and not allow_downgrade:
        raise ReleaseRefused(f"{name}: store holds {old_release}, refusing to replace it with the "
                             f"older {release} (allow_downgrade=True to force)")
    ident = _zip_identity(zip_path)
    if parquet_current(name, release, out_dir) and old_sc.get("zip_sha256") == ident["zip_sha256"]:
        log(f"{name}: {release} already ingested ({old_sc['rows']:,} rows)")
        return {"rows": old_sc["rows"], "rebuilt": False, "release": release}

    part = os.path.join(stage_dir, f"baci_{name.lower()}.parquet.part")
    part_meta = part + ".json"
    staged = _read_json(part_meta)
    sf = _footer(part)
    if not (staged and staged.get("release") == release
            and staged.get("zip_sha256") == ident["zip_sha256"]
            and sf and sf[0] == staged.get("rows") and sf[1]):
        # A part without a matching verified sidecar is an interrupted parse or another release.
        _remove(part_meta)
        _remove(part)
        staged = None

    if staged is None:
        log(f"{name}: parsing {release} from {os.path.basename(zip_path)} ...")
        try:
            with zipfile.ZipFile(zip_path) as z:
                members = z.namelist()
                csvs = sorted(m for m in members if m.lower().endswith(".csv")
                              and not any(x in m.lower() for x in ["country", "product", "readme"]))
                stamp = f"_{release.lower()}.csv"            # exact: V202601 must not match V202601b
                unstamped = [m for m in members if m.lower().endswith(".csv")
                             and not m.lower().endswith(stamp)]
                if unstamped:
                    raise ReleaseRefused(f"{name}: zip members not stamped _{release}: "
                                         f"{unstamped[:5]}")
                if not csvs:
                    return {"rows": 0, "rebuilt": False, "release": release}
                codes = _country_codes(z)
                log(f"{name}: {len(csvs)} annual CSV files")
                try:
                    rows, skipped, max_year, seen = _parse_to(z, csvs, part, release)
                except BaseException:
                    _remove(part)
                    raise
        except zipfile.BadZipFile:
            # bit-rot or a bad splice: the cached zip would fail the same way forever
            bad = quarantine(zip_path)
            log(f"{name}: {os.path.basename(zip_path)} is corrupt - moved to {bad}")
            raise
        try:
            f = _footer(part)
            if rows == 0:
                _remove(part)
                return {"rows": 0, "rebuilt": False, "release": release}
            if not f or f[0] != rows or not f[1]:
                raise ReleaseRefused(f"{name}: staged footer {f} disagrees with {rows:,} rows written")
            if skipped > MAX_SKIP_FRAC * (rows + skipped):
                raise ReleaseRefused(f"{name}: {skipped:,} unparseable rows of {rows + skipped:,} "
                                     f"(> {MAX_SKIP_FRAC:.1%}) - schema change?")
            unmapped = sorted(seen - codes)
            if unmapped:
                raise ReleaseRefused(f"{name}: {len(unmapped)} exporter/importer code(s) not in the "
                                     f"release's own country_codes csv: {unmapped[:15]}")
            old = _footer(out_path)
            if old and not allow_shrink:
                if rows < old[0]:
                    raise ReleaseRefused(f"{name}: {release} has {rows:,} rows, the store holds "
                                         f"{old[0]:,} - refusing a shrink (allow_shrink=True)")
                if old[2] is not None and (max_year is None or max_year < old[2]):
                    raise ReleaseRefused(f"{name}: {release} ends {max_year}, the store ends "
                                         f"{old[2]} - refusing (allow_shrink=True)")
        except BaseException:
            _remove(part)
            raise
        staged = {"release": release, "classification": name, **ident, "rows": rows,
                  "skipped_rows": skipped, "max_year": max_year,
                  "schema": [f"{fl.name}:{fl.type}" for fl in SCHEMA],
                  "verified_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")}
        _write_json_atomic(part_meta, staged)

    # THE COMMIT POINT. A reader holding the old parquet makes this raise (Windows); the verified
    # part and its sidecar stay in staging and the next run swaps without re-parsing.
    atomic_replace(part, out_path)
    _write_json_atomic(sc_path, staged)                 # after the bytes, never before
    _remove(part_meta)
    log(f"{name}: DONE {release} {staged['rows']:,} bilateral trade flow records")
    return {"rows": staged["rows"], "rebuilt": True, "release": release}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--release", default=None,
                    help="publisher V-string (default: read from CEPII's product page)")
    ap.add_argument("--allow-downgrade", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true",
                    help="accept a release with fewer rows or an earlier last year than the store")
    a = ap.parse_args(argv)
    if a.release:
        rel = {hs: a.release for hs in CLASSIFICATIONS}
    else:
        from updater.strategies.fetchers.cepii_baci import _releases
        rel = _releases()
        missing = [hs for hs in CLASSIFICATIONS if hs not in rel]
        if missing:
            raise SystemExit(f"CEPII's page does not advertise {missing}; pass --release")
    os.makedirs(OUT, exist_ok=True)
    stage = default_stage_dir(OUT)
    grand = 0
    for name in CLASSIFICATIONS:
        zip_path = download(name, release_url(name, rel[name]), release=rel[name], raw_dir=TMP)
        grand += ingest_zip(name, zip_path, release=rel[name], out_dir=OUT, stage_dir=stage,
                            allow_downgrade=a.allow_downgrade,
                            allow_shrink=a.allow_shrink)["rows"]
    log("NOTE: this job installs the raw files only. The country codes, the pairs projection and "
        "the R2 publish are the fetcher's: run python -m updater.run --source cepii_baci --force")
    log(f"GRAND TOTAL: {grand:,} BACI trade flow records")


if __name__ == "__main__":
    main()
