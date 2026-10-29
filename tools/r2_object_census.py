"""r2_object_census - a READ-ONLY census of R2 objects: LIST with LastModified, and HEAD of a key list.

WHY IT EXISTS. A CSV-debt plan needs, for one key prefix, every object's size and LastModified (to count the
objects older than a job's start), and, for a list of keys, each object's status, ETag, size, LastModified,
ContentEncoding and the `csvmd5` metadata. No reviewed tool gives either: tools/verify_derive_parity.py
returns id SETS only, and tools/probe_csv_freshness.py judges content and ignores LastModified on purpose.

Two modes:

  list --bucket B --prefix P [--ids-file F] --out PATH
      One TSV row per object: key, size, last_modified (UTC ISO), etag [, in_ids].
      Trailer lines start with '#': the page count, the object and byte totals and, with --ids-file, how many
      of those ids were listed and every id whose object is absent ('# absent <id>').
      A listing that read 0 pages, or that failed on ANY page, is VOID: nothing is written, exit 3. It is
      never an empty result. A listing stopped by the --max-ops cap is VOID too (exit 4).

  head --bucket B --keys-file F --out PATH
      One TSV row per distinct key: key, status (200 / 404 / err:<code>), etag, content_length,
      last_modified, csvmd5, content_encoding. An error row (anything but 200 or 404) is a FAILURE, never a
      skip: the file is written and the exit code is 1.

READ-ONLY BY CONSTRUCTION, two locks:
  1. ReadOnlyClient offers list_objects_v2 and head_object and raises WriteRefused for every other attribute
     (put_object, copy_object, delete_object, get_object, get_paginator, upload_file, ...).
  2. On the real boto3 client it registers, FIRST, a botocore before-call hook that refuses every operation
     but ListObjectsV2 and HeadObject. botocore emits before-call before the request is signed or sent, so a
     refused call never leaves the machine. No body GET is allowed at all: nothing here needs one.
  tests/test_r2_object_census.py plants put, delete and copy through both locks and checks no request is
  signed.

CREDENTIALS. Loaded exactly as the other reviewed read tools load them: core.r2_util.client(write=False)
(the same call tools/clear_csv_desktop_owed.py makes). This file never reads a key file itself, and never
prints, logs or writes a credential or the endpoint. Every exception is reduced to a CODE before it is
shown - botocore's connection and timeout errors carry the endpoint URL in their message, so str(error) is
never printed (ledger K2). After T0 the tool refuses before loading the credentials (core.cutover), and
r2_util.client() would refuse every call in any case.

OUTPUT goes only to --out: an absolute, new path under F:\\ or under the system temp directory, never on D:
or E:, never inside this repository. Nothing else is written: no D1, no state, no repo files.

COST GUARD. The planned operation count is printed before any request. A LIST page (up to 1,000 keys) is a
Class A operation; a HEAD is a Class B operation. --max-ops (default 2,000) caps LOGICAL operations - LIST
pages and HEADs - not HTTP requests: core.r2_util's client retries a failed request (botocore standard mode,
max_attempts 5; one HEAD to an unreachable endpoint was measured at 6 signed attempts), so under throttling or
5xx answers the requests sent can reach about 6 x --max-ops. For `head` the count is known before the run
(one HEAD per distinct key) and an over-cap plan is refused before the client is built. For `list` the page
count is known only as the listing proceeds: the listing stops, VOID, before it would request page
--max-ops + 1.

Exit codes: 0 done; 1 head had error rows; 2 refused (usage, output path, ids outside the prefix,
credentials); 3 listing VOID; 4 over the --max-ops cap.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import math
import os
import re
import sys
import tempfile

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

DEFAULT_MAX_OPS = 2000
PAGE_KEYS = 1000

ALLOWED_OPERATIONS = frozenset({"ListObjectsV2", "HeadObject"})

EXIT_OK, EXIT_ERROR_ROWS, EXIT_REFUSED, EXIT_VOID, EXIT_CAP = 0, 1, 2, 3, 4


class WriteRefused(RuntimeError):
    """An operation other than ListObjectsV2 / HeadObject was attempted."""


class Refused(RuntimeError):
    """A guard refused the run before any request (output path, ids, cap, usage)."""


class VoidListing(RuntimeError):
    """The listing is not a census (0 pages read, a page failed, a malformed page, or the cap stopped it)."""

    def __init__(self, msg: str, exit_code: int = EXIT_VOID):
        super().__init__(msg)
        self.exit_code = exit_code


# ---------------------------------------------------------------------------------------------------------
# error -> code. NEVER str(error): botocore errors embed the endpoint URL.
# ---------------------------------------------------------------------------------------------------------
_SAFE_CODE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def error_code(exc: BaseException) -> str:
    """A short code for an exception that cannot carry the endpoint, a key or a secret.

    A botocore ClientError gives its HTTP status or its S3 error code (both server-assigned tokens), kept
    only if it matches [A-Za-z0-9_.-]{1,64}; anything else - including every non-ClientError - is reduced to
    the exception's CLASS NAME, checked the same way. The message text is never read."""
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict):
        status = (resp.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        code = (resp.get("Error") or {}).get("Code")
        if isinstance(code, str) and _SAFE_CODE.match(code):
            return code
        if isinstance(status, int):
            return str(status)
        return "ClientError"
    name = type(exc).__name__
    return name if _SAFE_CODE.match(name) else "Exception"


def http_status(exc: BaseException) -> int | None:
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict):
        s = (resp.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        if isinstance(s, int):
            return s
        code = (resp.get("Error") or {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return 404
    return None


# ---------------------------------------------------------------------------------------------------------
# the two read-only locks
# ---------------------------------------------------------------------------------------------------------
def _refuse_other_operations(model=None, **_kwargs):
    """botocore before-call hook: raise for every operation but ListObjectsV2 and HeadObject. botocore emits it
    before the request is signed or sent, so a refused call never leaves."""
    name = getattr(model, "name", None)
    if name not in ALLOWED_OPERATIONS:
        raise WriteRefused(f"r2_object_census is read-only: refused the operation {name}")


class ReadOnlyClient:
    """list_objects_v2 and head_object, nothing else. Any other attribute raises WriteRefused, and the wrapped
    client cannot be swapped. A real client (one with meta.events) also gets the before-call hook; a client
    without events is accepted only with allow_unhooked=True, which only the tests' stubs pass."""

    def __init__(self, client, *, allow_unhooked: bool = False):
        events = getattr(getattr(client, "meta", None), "events", None)
        if events is not None:
            # Registered FIRST on BOTH the wildcard and the s3 name. botocore runs a responder registered on
            # 'before-call.*.*' (botocore's Stubber uses it) BEFORE a register_first handler on
            # 'before-call.s3' - measured: such a responder answered a put_object and the s3-only lock was
            # never asked. register_first on the wildcard runs ahead of it.
            events.register_first("before-call.*.*", _refuse_other_operations,
                                  unique_id="r2-object-census-read-only-any")
            events.register_first("before-call.s3", _refuse_other_operations,
                                  unique_id="r2-object-census-read-only")
        elif not allow_unhooked:
            raise WriteRefused("r2_object_census: the client has no botocore event system, so the "
                               "before-call lock cannot be installed - refusing to use it")
        object.__setattr__(self, "_client", client)

    def list_objects_v2(self, **kw):
        return self._client.list_objects_v2(**kw)

    def head_object(self, **kw):
        return self._client.head_object(**kw)

    def __getattr__(self, name):                       # only for names not defined above
        raise WriteRefused(f"r2_object_census is read-only: refused the call {name}")

    def __setattr__(self, name, value):
        raise WriteRefused(f"r2_object_census is read-only: refused to set {name}")


def open_client():
    """The read client, loaded the way the other reviewed read tools load it, behind both locks.

    After T0 the cloud copy is frozen and a census of it would describe a copy nobody serves from, so this
    refuses BEFORE the credentials are loaded (r2_util.client() would refuse every call anyway; this says so
    first and loudly)."""
    from core import r2_util                            # noqa: PLC0415
    from core.cutover import refuse_if_cut_over          # noqa: PLC0415
    refuse_if_cut_over("tools/r2_object_census.py (a census of the frozen cloud copy)")
    return ReadOnlyClient(r2_util.client(write=False))


# ---------------------------------------------------------------------------------------------------------
# output path guard
# ---------------------------------------------------------------------------------------------------------
def _under(path: str, root: str, pathmod) -> bool:
    root = pathmod.normcase(pathmod.normpath(root))
    path = pathmod.normcase(path)
    if path == root:
        return True
    return path.startswith(root.rstrip("\\/") + pathmod.sep)


def check_out_path(path: str, *, pathmod=os.path, temp_roots=None, repo_root: str = _REPO,
                   check_fs: bool = True) -> str:
    """Return the normalised output path, or raise Refused.

    Allowed: an ABSOLUTE path, under F:\\ or under a temp root, not on D: or E:, not inside the repo, not an
    existing file, whose parent directory exists. A real run uses os.path with check_fs=True (realpath, so a
    link on F: that points at D: is refused, plus the two file-system checks); the tests also pass ntpath with
    check_fs=False to exercise drive letters on any OS."""
    if not path:
        raise Refused("--out is required")
    if not pathmod.isabs(path):
        raise Refused("--out must be an absolute path")
    norm = pathmod.normpath(path)
    if check_fs:
        norm = os.path.realpath(norm)
    drive = pathmod.splitdrive(norm)[0].rstrip(":").upper()
    if drive in ("D", "E"):
        raise Refused(f"--out may not be on {drive}: (F: or a temp directory only)")
    if repo_root and _under(norm, repo_root, pathmod):
        raise Refused("--out may not be inside the repository tree")
    if temp_roots is None:
        temp_roots = [tempfile.gettempdir()]
    allowed = drive == "F" or any(_under(norm, t, pathmod) for t in temp_roots if t)
    if not allowed:
        raise Refused("--out must be under F:\\ or under the system temp directory")
    # a temp root on D:/E: was already refused above by the drive test
    if check_fs:
        if os.path.exists(norm):
            raise Refused("--out already exists; choose a new path")
        if not os.path.isdir(os.path.dirname(norm)):
            raise Refused("--out's parent directory does not exist")
    return norm


# ---------------------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------------------
def iso_utc(v) -> str:
    """LastModified as UTC ISO 'YYYY-MM-DDTHH:MM:SSZ'. A naive datetime is taken as UTC (botocore returns aware)."""
    if v is None or v == "":
        return ""
    if isinstance(v, _dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return v.strftime("%Y-%m-%dT%H:%M:%SZ")
    raise ValueError("LastModified is not a datetime")


def tsv_line(fields) -> str:
    out = []
    for f in fields:
        s = "" if f is None else str(f)
        if any(c in s for c in "\t\r\n"):
            raise ValueError("a field holds a tab or a line break")
        out.append(s)
    return "\t".join(out) + "\n"


def read_lines(path: str) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        return [ln.strip() for ln in fh if ln.strip() and not ln.lstrip().startswith("#")]


def id_to_key(series_id: str) -> str:
    """A catalogue id '<source>:<series_key>' -> its CSV object key, through THE writer's own definition."""
    from tools.derive_csv_bulk import csv_key           # noqa: PLC0415
    if ":" not in series_id:
        raise Refused(f"ids-file: not a catalogue id (no ':'): line {series_id[:80]!r}")
    src, rest = series_id.split(":", 1)
    return csv_key("series", src, rest)


def _write_atomic(out: str, lines: list[str]) -> None:
    tmp = out + ".partial"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.writelines(lines)
    os.replace(tmp, out)


# ---------------------------------------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------------------------------------
def map_ids(ids, prefix: str) -> dict:
    """{object key: catalogue id}. Refuses an id whose key is outside the prefix - done BEFORE any client is
    built, so a wrong ids file never costs a request."""
    id_by_key = {}
    for sid in ids:
        k = id_to_key(sid)
        if not k.startswith(prefix):
            raise Refused(f"ids-file: an id's object key is outside --prefix (first: {sid[:80]!r})")
        id_by_key[k] = sid
    return id_by_key


def run_list(client, bucket: str, prefix: str, *, max_ops: int, ids=None):
    """Return (lines, summary). Raises VoidListing when the listing is not a census."""
    id_by_key = map_ids(ids, prefix) if ids is not None else {}
    pages, objects, total_bytes = 0, [], 0
    token = None
    while True:
        if pages >= max_ops:
            raise VoidListing(f"listing stopped at the --max-ops cap ({max_ops} LIST pages) before it was "
                              f"complete; raise --max-ops to list the whole prefix", EXIT_CAP)
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": PAGE_KEYS}
        if token:
            kw["ContinuationToken"] = token
        try:
            resp = client.list_objects_v2(**kw)
        except WriteRefused:
            raise
        except Exception as e:                           # noqa: BLE001
            raise VoidListing(f"LIST page {pages + 1} failed ({error_code(e)}); {pages} page(s) read - "
                              f"VOID, not a result") from None
        pages += 1
        for o in resp.get("Contents") or []:
            key, size, lm, etag = o.get("Key"), o.get("Size"), o.get("LastModified"), o.get("ETag")
            if not isinstance(key, str) or not isinstance(size, int):
                raise VoidListing(f"LIST page {pages} carried an object without a key or size - VOID")
            objects.append((key, size, iso_utc(lm), (etag or "").strip('"')))
            total_bytes += size
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                raise VoidListing(f"LIST page {pages} was truncated without a continuation token - VOID")
            continue
        break
    if pages == 0:                                       # unreachable by the loop above; kept as the rule
        raise VoidListing("0 pages read - VOID, never an empty result")
    lines = []
    listed = set()
    for key, size, lm, etag in objects:
        row = [key, size, lm, etag]
        if ids is not None:
            row.append(1 if key in id_by_key else 0)
        listed.add(key)
        lines.append(tsv_line(row))
    summary = {"pages": pages, "objects": len(objects), "bytes": total_bytes}
    lines.append(tsv_line(["# total", f"pages={pages}", f"objects={len(objects)}", f"bytes={total_bytes}"]))
    if ids is not None:
        absent = sorted(sid for k, sid in id_by_key.items() if k not in listed)
        summary.update(ids=len(id_by_key), ids_listed=len(id_by_key) - len(absent), ids_absent=len(absent))
        lines.append(tsv_line(["# ids", f"given={len(id_by_key)}", f"listed={len(id_by_key) - len(absent)}",
                               f"absent={len(absent)}"]))
        lines.extend(tsv_line(["# absent", sid]) for sid in absent)
    return lines, summary


# ---------------------------------------------------------------------------------------------------------
# head
# ---------------------------------------------------------------------------------------------------------
HEAD_COLUMNS = ["key", "status", "etag", "content_length", "last_modified", "csvmd5", "content_encoding"]
LIST_COLUMNS = ["key", "size", "last_modified", "etag"]


def run_head(client, bucket: str, keys: list[str]):
    """Return (lines, summary). One row per key; an error is a row with status err:<code>, never a skip."""
    lines = [tsv_line(["#"] + HEAD_COLUMNS)]
    n200 = n404 = nerr = 0
    for key in keys:
        try:
            h = client.head_object(Bucket=bucket, Key=key)
        except WriteRefused:
            raise
        except Exception as e:                           # noqa: BLE001
            if http_status(e) == 404:
                n404 += 1
                lines.append(tsv_line([key, "404", "", "", "", "", ""]))
            else:
                nerr += 1
                lines.append(tsv_line([key, f"err:{error_code(e)}", "", "", "", "", ""]))
            continue
        meta = {str(k).lower(): v for k, v in (h.get("Metadata") or {}).items()}
        n200 += 1
        lines.append(tsv_line([key, "200", (h.get("ETag") or "").strip('"'), h.get("ContentLength", ""),
                               iso_utc(h.get("LastModified")), meta.get("csvmd5", ""),
                               h.get("ContentEncoding") or ""]))
    summary = {"keys": len(keys), "ok200": n200, "absent404": n404, "errors": nerr}
    lines.append(tsv_line(["# total", f"keys={len(keys)}", f"200={n200}", f"404={n404}", f"error={nerr}"]))
    return lines, summary


# ---------------------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------------------
HELP_EPILOG = """\
cost: a LIST page (up to 1,000 keys) is a Class A operation; a HEAD is a Class B operation.
More than --max-ops (default 2,000) LIST pages or HEADs is refused unless --max-ops is raised. The cap counts
operations, not HTTP requests: with retries (up to ~6 attempts each) the requests sent can exceed it.
exit codes: 0 done; 1 head error rows; 2 refused; 3 listing VOID; 4 over the --max-ops cap.
"""


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="r2_object_census", description="Read-only R2 census: LIST and HEAD.",
                                 epilog=HELP_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--bucket", required=True)
    common.add_argument("--out", required=True, help="absolute new path under F:\\ or the temp directory")
    common.add_argument("--max-ops", type=int, default=DEFAULT_MAX_OPS,
                        help="cap on LIST pages (Class A) or HEADs (Class B); default 2,000")
    pl = sub.add_parser("list", parents=[common], epilog=HELP_EPILOG,
                        formatter_class=argparse.RawDescriptionHelpFormatter,
                        help="LIST a prefix: key, size, LastModified (UTC), ETag (each page = 1 Class A op)")
    pl.add_argument("--prefix", required=True)
    pl.add_argument("--ids-file", help="catalogue ids, one per line: mark which are listed, name the absent")
    ph = sub.add_parser("head", parents=[common], epilog=HELP_EPILOG,
                        formatter_class=argparse.RawDescriptionHelpFormatter,
                        help="HEAD each key of a file (each key = 1 Class B op)")
    ph.add_argument("--keys-file", required=True, help="object keys, one per line")
    return ap


def main(argv=None, *, client_factory=open_client, out_guard=check_out_path) -> int:
    a = build_parser().parse_args(argv)
    try:
        if a.max_ops < 1:
            raise Refused("--max-ops must be at least 1")
        out = out_guard(a.out)
        if a.mode == "list":
            if not a.prefix:
                raise Refused("--prefix may not be empty (a whole-bucket listing is not a census of anything)")
            ids = read_lines(a.ids_file) if a.ids_file else None
            if ids is not None:
                ids = list(dict.fromkeys(ids))
                map_ids(ids, a.prefix)                   # refuse a bad ids file before any client exists
            floor = max(1, math.ceil(len(ids) / PAGE_KEYS)) if ids else 1
            print(f"planned: LIST bucket={a.bucket} prefix={a.prefix} - at least {floor} page(s), at most "
                  f"{a.max_ops} (each page = 1 Class A op, <= {PAGE_KEYS} keys)")
            if floor > a.max_ops:
                print(f"REFUSED: the ids alone need at least {floor} LIST pages > --max-ops {a.max_ops}")
                return EXIT_CAP
            client = client_factory()
            lines, s = run_list(client, a.bucket, a.prefix, max_ops=a.max_ops, ids=ids)
            header = tsv_line(["#"] + LIST_COLUMNS + (["in_ids"] if ids is not None else []))
            _write_atomic(out, [header] + lines)
            msg = f"list: pages={s['pages']} objects={s['objects']} bytes={s['bytes']}"
            if ids is not None:
                msg += f" ids={s['ids']} listed={s['ids_listed']} absent={s['ids_absent']}"
            print(msg)
            print(f"wrote {out}")
            return EXIT_OK
        keys = list(dict.fromkeys(read_lines(a.keys_file)))
        print(f"planned: HEAD bucket={a.bucket} - {len(keys)} distinct key(s) = {len(keys)} Class B op(s)")
        if not keys:
            raise Refused("--keys-file holds no keys")
        if len(keys) > a.max_ops:
            print(f"REFUSED: {len(keys)} HEADs > --max-ops {a.max_ops}; raise --max-ops explicitly")
            return EXIT_CAP
        client = client_factory()
        lines, s = run_head(client, a.bucket, keys)
        _write_atomic(out, lines)
        print(f"head: keys={s['keys']} 200={s['ok200']} 404={s['absent404']} error={s['errors']}")
        print(f"wrote {out}")
        if s["errors"]:
            print(f"FAILED: {s['errors']} key(s) answered neither 200 nor 404 - see the err: rows")
            return EXIT_ERROR_ROWS
        return EXIT_OK
    except Refused as e:
        print(f"REFUSED: {e}")
        return EXIT_REFUSED
    except VoidListing as e:
        print(f"VOID: {e}")
        return e.exit_code
    except WriteRefused as e:
        print(f"REFUSED: {e}")
        return EXIT_REFUSED
    except Exception as e:                               # noqa: BLE001 - a code only, never the message
        print(f"FAILED: {error_code(e)}")
        return EXIT_REFUSED


if __name__ == "__main__":
    sys.exit(main())
