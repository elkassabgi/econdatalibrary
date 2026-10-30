"""r2_object_fetch - a READ-ONLY GET of a short, named list of served CSV objects to a new local folder.

WHY IT EXISTS (review AR-408, step 3b). The census re-derive needs about 9 served CSVs compared row by row
with a local derive, to see whether 2,174 differing objects changed their header, their column order or
their values. tools/r2_object_census.py LISTs and HEADs only, and by design refuses every body GET (its
ALLOWED_OPERATIONS and tests/test_r2_object_census.py pin that), so a GET does not belong in it: this is a
separate tool with its own lock pair that allows GetObject and nothing else.

  python tools/r2_object_fetch.py --bucket econ-data --ids-file IDS --dest NEW_DIR

IDS holds catalogue ids, one per line ('#' lines and blank lines skipped). Each id becomes its object key
through THE writer's layout (tools.derive_csv_bulk.csv_key, via r2_object_census.id_to_key), so only an
exact `series/census%3A<...>.csv` key can be asked for. The scope is census: SOURCES is a fixed list in
this file, with no flag to widen it (review AR-409 r2); any id of another source is refused before a client
is built. Widening it is a code change, and so a review.

WHAT IT WRITES, only under --dest (a NEW or EMPTY folder):
  raw/<name>     the object's bytes exactly as stored (gzip objects stay gzip)
  csv/<name>     the CSV: the same bytes for a plain object, the gunzipped bytes for a gzip one
  manifest.tsv   one row per id: id, key, status, etag, content_length, content_encoding, csvmd5,
                 last_modified, md5 of raw, md5 of csv, and the checks - md5(raw) == ETag (single-part), and
                 md5(csv) == the csvmd5 metadata when the object carries one
<name> is the key's last segment (e.g. census%3Aeits__m3%23no.csv), the same name the derive's local copies
use. A name with any character outside [A-Za-z0-9%._~-], or two ids giving one name, is refused.

A row that is not 200 with every check passing is a FAILURE, never a skip: the manifest is written and the
exit code is 1. Exit codes: 0 done; 1 failure rows; 2 refused (usage, ids, destination, credentials,
cutover); 4 over the object cap.

READ-ONLY BY CONSTRUCTION, two locks (the same design as r2_object_census.py, with GetObject as the only
operation):
  1. GetOnlyClient offers get_object and raises WriteRefused for every other attribute.
  2. On the real boto3 client it registers, FIRST and on both 'before-call.*.*' and 'before-call.s3', a
     botocore hook that refuses every operation but GetObject. botocore emits before-call before the request
     is signed or sent, so a refused call never leaves the machine.
CREDENTIALS: core.r2_util.client(write=False), as the other reviewed read tools. NOTE: r2_util.creds(
write=False) falls back to the WRITE pair for any read field that is absent, so the key pair may be
write-capable; the safety claim rests on the two operation locks, not on which pair is loaded. This file
never reads a key file, and never prints a credential, the endpoint or an exception's message (only a
code, r2_object_census.error_code). After T0 it refuses before the credentials load (core.cutover).

CAPS: at most 25 objects per run (HARD_MAX_OBJECTS; --max-objects can LOWER it, never raise it), at most
--max-bytes per object (default and ceiling 256 MiB, judged from ContentLength before the body is read and
again while reading), and a gunzip bounded at MAX_DECODED_BYTES. Each GET is one Class B operation; the
client's retry config can send up to ~6 HTTP attempts for one.

DESTINATION: absolute; not on D: or E:; under F:\\ or the system temp directory; not inside this
repository; no path component named clean_full (a store directory); after realpath (a link is followed);
it must not exist (its parent must) or be an empty directory.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import tempfile
import zlib

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from tools.r2_object_census import (  # noqa: E402
    Refused, WriteRefused, _under, error_code, http_status, iso_utc, read_lines, tsv_line, id_to_key,
)

HARD_MAX_OBJECTS = 25
HARD_MAX_BYTES = 256 * 1024 * 1024
MAX_DECODED_BYTES = 1024 * 1024 * 1024
SOURCES = frozenset({"census"})          # the scope; no flag widens it
ALLOWED_OPERATIONS = frozenset({"GetObject"})
_NAME = re.compile(r"^[A-Za-z0-9%._~-]{1,200}$")

EXIT_OK, EXIT_FAILED_ROWS, EXIT_REFUSED, EXIT_CAP = 0, 1, 2, 4
MANIFEST_COLUMNS = ["id", "key", "status", "etag", "content_length", "content_encoding", "csvmd5",
                    "last_modified", "raw_md5", "csv_md5", "etag_check", "csvmd5_check"]


# ---------------------------------------------------------------------------------------------------------
# the two read-only locks
# ---------------------------------------------------------------------------------------------------------
def _refuse_other_operations(model=None, **_kwargs):
    """botocore before-call hook: raise for every operation but GetObject, before it is signed or sent."""
    name = getattr(model, "name", None)
    if name not in ALLOWED_OPERATIONS:
        raise WriteRefused(f"r2_object_fetch is GET-only: refused the operation {name}")


class GetOnlyClient:
    """get_object and nothing else. Any other attribute raises WriteRefused; the wrapped client cannot be
    swapped. A real client (one with meta.events) also gets the before-call hook; a client without events is
    accepted only with allow_unhooked=True, which only the tests' stubs pass."""

    def __init__(self, client, *, allow_unhooked: bool = False):
        events = getattr(getattr(client, "meta", None), "events", None)
        if events is not None:
            events.register_first("before-call.*.*", _refuse_other_operations,
                                  unique_id="r2-object-fetch-get-only-any")
            events.register_first("before-call.s3", _refuse_other_operations,
                                  unique_id="r2-object-fetch-get-only")
        elif not allow_unhooked:
            raise WriteRefused("r2_object_fetch: the client has no botocore event system, so the "
                               "before-call lock cannot be installed - refusing to use it")
        object.__setattr__(self, "_client", client)

    def get_object(self, **kw):
        return self._client.get_object(**kw)

    def __getattr__(self, name):
        raise WriteRefused(f"r2_object_fetch is GET-only: refused the call {name}")

    def __setattr__(self, name, value):
        raise WriteRefused(f"r2_object_fetch is GET-only: refused to set {name}")


def open_client():
    """The read client behind both locks. Refuses after T0 BEFORE the credentials are loaded."""
    from core import r2_util                            # noqa: PLC0415
    from core.cutover import refuse_if_cut_over          # noqa: PLC0415
    refuse_if_cut_over("tools/r2_object_fetch.py (a GET of the frozen cloud copy)")
    return GetOnlyClient(r2_util.client(write=False))


# ---------------------------------------------------------------------------------------------------------
# guards (all run before any client is built)
# ---------------------------------------------------------------------------------------------------------
def check_dest(path: str, *, pathmod=os.path, temp_roots=None, repo_root: str = _REPO,
               check_fs: bool = True) -> str:
    """Return the normalised destination folder, or raise Refused (rules in the module docstring)."""
    if not path:
        raise Refused("--dest is required")
    if not pathmod.isabs(path):
        raise Refused("--dest must be an absolute path")
    norm = pathmod.normpath(path)
    if check_fs:
        norm = os.path.realpath(norm)
    drive = pathmod.splitdrive(norm)[0].rstrip(":").upper()
    if drive in ("D", "E"):
        raise Refused(f"--dest may not be on {drive}: (F: or a temp directory only)")
    if repo_root and _under(norm, repo_root, pathmod):
        raise Refused("--dest may not be inside the repository tree")
    parts = [p.lower() for p in re.split(r"[\\/]+", pathmod.splitdrive(norm)[1]) if p]
    if "clean_full" in parts:
        raise Refused("--dest may not be inside a clean_full (store) directory")
    if temp_roots is None:
        temp_roots = [tempfile.gettempdir()]
    if not (drive == "F" or any(_under(norm, t, pathmod) for t in temp_roots if t)):
        raise Refused("--dest must be under F:\\ or under the system temp directory")
    if check_fs:
        if os.path.exists(norm):
            if not os.path.isdir(norm):
                raise Refused("--dest exists and is not a directory")
            if os.listdir(norm):
                raise Refused("--dest exists and is not empty; choose a new or empty folder")
        elif not os.path.isdir(os.path.dirname(norm)):
            raise Refused("--dest's parent directory does not exist")
    return norm


def plan_keys(ids, sources, max_objects: int) -> list:
    """[(id, key, name)] for the ids, or raise Refused. The cap is checked first, then every id."""
    ids = list(dict.fromkeys(ids))
    if not ids:
        raise Refused("--ids-file holds no ids")
    if len(ids) > max_objects:
        raise _OverCap(f"{len(ids)} objects > the cap of {max_objects} (hard ceiling {HARD_MAX_OBJECTS})")
    out, names = [], {}
    for sid in ids:
        src = sid.split(":", 1)[0] if ":" in sid else ""
        if src not in sources:
            raise Refused(f"id {sid[:80]!r} is not of an allowed source {sorted(sources)}")
        key = id_to_key(sid)
        want = "series/" + _quote_src(src)
        if not key.startswith(want) or not key.endswith(".csv") or key.count("/") != 1:
            raise Refused(f"id {sid[:80]!r} gives the key {key[:100]!r}, not an exact {want}...csv key")
        name = key.split("/", 1)[1]
        if not _NAME.match(name) or name in (".", ".."):
            raise Refused(f"id {sid[:80]!r} gives an unsafe file name")
        if name.lower() in names:
            raise Refused(f"ids {names[name.lower()][:60]!r} and {sid[:60]!r} give one file name")
        names[name.lower()] = sid
        out.append((sid, key, name))
    return out


def _quote_src(src: str) -> str:
    import urllib.parse                                   # noqa: PLC0415
    return urllib.parse.quote(src + ":", safe="")


class _OverCap(Refused):
    pass


# ---------------------------------------------------------------------------------------------------------
# the GETs
# ---------------------------------------------------------------------------------------------------------
def _gunzip_bounded(data: bytes, limit: int) -> bytes:
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out = d.decompress(data, limit)
    if d.unconsumed_tail or not d.eof:
        raise ValueError("gzip body is truncated or decodes past the limit")
    return out


def _write(path: str, data: bytes) -> None:
    tmp = path + ".partial"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


def run_get(client, bucket: str, plan, dest: str, *, max_bytes: int):
    """GET each planned key into dest. Returns (manifest lines, summary)."""
    os.makedirs(os.path.join(dest, "raw"), exist_ok=True)
    os.makedirs(os.path.join(dest, "csv"), exist_ok=True)
    lines = [tsv_line(["#"] + MANIFEST_COLUMNS)]
    ok = failed = 0
    for sid, key, name in plan:
        row = {c: "" for c in MANIFEST_COLUMNS}
        row.update(id=sid, key=key)
        try:
            resp = client.get_object(Bucket=bucket, Key=key)
        except WriteRefused:
            raise
        except Exception as e:                           # noqa: BLE001
            row["status"] = "404" if http_status(e) == 404 else f"err:{error_code(e)}"
            failed += 1
            lines.append(tsv_line([row[c] for c in MANIFEST_COLUMNS]))
            continue
        body = resp.get("Body")
        try:
            etag = (resp.get("ETag") or "").strip('"')
            enc = (resp.get("ContentEncoding") or "").strip().lower()
            meta = {str(k).lower(): v for k, v in (resp.get("Metadata") or {}).items()}
            clen = resp.get("ContentLength")
            row.update(etag=etag, content_length="" if clen is None else clen, content_encoding=enc,
                       csvmd5=meta.get("csvmd5", ""), last_modified=iso_utc(resp.get("LastModified")))
            if isinstance(clen, int) and clen > max_bytes:
                raise _RowFail(f"too_large:{clen}")
            raw = body.read(max_bytes + 1) if body is not None else b""
            if len(raw) > max_bytes:
                raise _RowFail("too_large")
            if isinstance(clen, int) and len(raw) != clen:
                raise _RowFail(f"short_body:{len(raw)}")
            row["raw_md5"] = hashlib.md5(raw).hexdigest()
            if enc == "gzip":
                try:
                    csv = _gunzip_bounded(raw, MAX_DECODED_BYTES)
                except (ValueError, zlib.error):
                    raise _RowFail("gzip_unreadable") from None
            elif enc == "":
                csv = raw
            else:
                raise _RowFail(f"encoding:{enc[:20]}")
            row["csv_md5"] = hashlib.md5(csv).hexdigest()
            if not etag or "-" in etag:
                row["etag_check"] = "unverifiable"
            else:
                row["etag_check"] = "ok" if etag == row["raw_md5"] else "MISMATCH"
            if row["csvmd5"]:
                row["csvmd5_check"] = "ok" if row["csvmd5"].lower() == row["csv_md5"] else "MISMATCH"
            _write(os.path.join(dest, "raw", name), raw)
            _write(os.path.join(dest, "csv", name), csv)
            good = row["etag_check"] == "ok" and row["csvmd5_check"] in ("", "ok")
            row["status"] = "200" if good else "200:check_failed"
        except _RowFail as e:
            row["status"] = f"200:{e}"
            good = False
        except WriteRefused:
            raise
        except Exception as e:                           # noqa: BLE001 - a code only, never the message
            row["status"] = f"200:err:{error_code(e)}"
            good = False
        finally:
            if body is not None and hasattr(body, "close"):
                try:
                    body.close()
                except Exception:                        # noqa: BLE001
                    pass
        ok += good
        failed += not good
        lines.append(tsv_line([row[c] for c in MANIFEST_COLUMNS]))
    lines.append(tsv_line(["# total", f"objects={len(plan)}", f"ok={ok}", f"failed={failed}"]))
    return lines, {"objects": len(plan), "ok": ok, "failed": failed}


class _RowFail(Exception):
    pass


# ---------------------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="r2_object_fetch",
                                 description="Read-only GET of <= 25 named served CSV objects to a new folder.")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--ids-file", required=True, help="catalogue ids, one per line")
    ap.add_argument("--dest", required=True, help="absolute NEW or EMPTY folder under F:\\ or the temp directory")
    ap.add_argument("--max-objects", type=int, default=HARD_MAX_OBJECTS,
                    help=f"object cap; may only LOWER the hard ceiling of {HARD_MAX_OBJECTS}")
    ap.add_argument("--max-bytes", type=int, default=HARD_MAX_BYTES,
                    help=f"per-object byte cap; may only LOWER the ceiling of {HARD_MAX_BYTES:,}")
    return ap


def main(argv=None, *, client_factory=open_client, dest_guard=check_dest) -> int:
    a = build_parser().parse_args(argv)
    try:
        if a.max_objects < 1 or a.max_objects > HARD_MAX_OBJECTS:
            raise _OverCap(f"--max-objects must be 1..{HARD_MAX_OBJECTS}")
        if a.max_bytes < 1 or a.max_bytes > HARD_MAX_BYTES:
            raise Refused(f"--max-bytes must be 1..{HARD_MAX_BYTES}")
        dest = dest_guard(a.dest)
        plan = plan_keys(read_lines(a.ids_file), SOURCES, a.max_objects)
        print(f"planned: GET bucket={a.bucket} - {len(plan)} object(s) = {len(plan)} Class B op(s), "
              f"<= {a.max_bytes:,} bytes each, into {dest}")
        client = client_factory()
        lines, s = run_get(client, a.bucket, plan, dest, max_bytes=a.max_bytes)
        _write(os.path.join(dest, "manifest.tsv"), "".join(lines).encode("utf-8"))
        print(f"get: objects={s['objects']} ok={s['ok']} failed={s['failed']}")
        print(f"wrote {os.path.join(dest, 'manifest.tsv')}")
        if s["failed"]:
            print(f"FAILED: {s['failed']} object(s) are not 200 with every check passing - see manifest.tsv")
            return EXIT_FAILED_ROWS
        return EXIT_OK
    except _OverCap as e:
        print(f"REFUSED: {e}")
        return EXIT_CAP
    except (Refused, WriteRefused) as e:
        print(f"REFUSED: {e}")
        return EXIT_REFUSED
    except Exception as e:                               # noqa: BLE001 - a code only, never the message
        print(f"FAILED: {error_code(e)}")
        return EXIT_REFUSED


if __name__ == "__main__":
    sys.exit(main())
