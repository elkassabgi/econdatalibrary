"""served_receipt - prove what USERS get from the public API, one series at a time.

WHAT IT CHECKS. For each id it GETs /v1/series/<id>.csv?raw=1 through the public worker, exactly as a
user's client does (X-API-Key header), and GETs the R2 object the worker serves that route from
(series/<quoted id>.csv, through the writer's own key layout, tools.derive_csv_bulk.csv_key). It reports,
per id: the served newest obs_date, the R2 newest obs_date, and whether the two bodies are EQUAL.

  python tools/served_receipt.py                          # the default sample below
  python tools/served_receipt.py --ids "a:b:c d:e:f"      # ids separated by spaces or commas

EXIT CODES: 0 every id equal; 1 any id different, or any id that could not be fetched from either side;
2 refused before any data request (missing secret, missing R2 credentials, bad ids, or a worker that
forwards to the self-hosted origin, where R2 is no longer what users get). Never 0 on a skipped id.

WHY ?raw=1. Without it the body starts with a citation header built from the catalogue, which R2 does not
hold. With it the worker returns the stored CSV unchanged (series.ts, step 6, and the streamed paths).
The comparison is on the data lines: lines that start with '#' (the in-band completeness marker a
streamed answer may carry) are set aside on both sides before the bytes are compared.

WHICH BACKEND. The worker serves .csv from R2 only while its FORWARD flag is off. When it is on, the
data comes from the workstation's origin and an R2 comparison would prove nothing about users. So the
first request is GET /v1/edge-status (public, no key, no storage read); forward=true refuses with exit 2.

CACHE. The worker sets `cache-control: public, max-age=300` on CSV answers and does not cache them at
its own edge (the Cache API is used only for catalogue, stats and other public routes). The worker
documents no cache-busting parameter, so none is added; the request sends `Cache-Control: no-cache` for
any shared cache on the path. A shared cache could still hold an answer for up to 300 s: an R2 object
replaced in the last five minutes can show as "different" once. Re-run before you act on it.

THE KEY. ECON_RECEIPT_API_KEY is read from the environment, in this process, and only placed in the
X-API-Key request header. It is never printed, logged, written to a file or put in a URL. Any text this
tool prints passes through _redact() first, so even an exception message that quoted it would print
"***". Masking it in the job log (::add-mask::) is done by the workflow, not here: the mask command
itself would put the key on stdout. A missing secret is exit 2 with an owner-step message, never a pass.

R2 ACCESS. core.r2_util.client(write=False), wrapped in tools.r2_object_fetch.GetOnlyClient: every S3
operation but GetObject is refused before it is signed or sent. In CI the read client falls back to the
R2_WRITE_* pair (the only pair stored as secrets), so the read-only claim rests on that lock, not on the
key pair. Refuses after T0 (core.cutover) before credentials load. At most HARD_MAX_IDS ids per run.

Hermetic tests: tests/test_served_receipt.py (fake HTTP layer and fake S3; no network).
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import zlib
from typing import Callable

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

API_BASE = "https://econdl-api.elkassabgi.workers.dev"
BUCKET = "econ-data"
KEY_ENV = "ECON_RECEIPT_API_KEY"
MISSING_KEY_MSG = f"secret {KEY_ENV} not set - owner step"
HARD_MAX_IDS = 25
MAX_BODY_BYTES = 256 * 1024 * 1024
MAX_DECODED_BYTES = 1024 * 1024 * 1024
TIMEOUT_S = 120

DEFAULT_IDS = (
    "imf_imts_direct:IMTS:USA.M.XG_FOB_USD",
    "fhfa:hpi_at_3zip_quarterly:010",
    "fhfa:hpi_at_3zip_quarterly:100",
    "noaa:gsom:CA004047080:TAVG",
    "bis:WS_CBPOL:US",
)

EXIT_OK, EXIT_DIFF, EXIT_REFUSED = 0, 1, 2
_ID = re.compile(r"^[a-z0-9_]+:\S{1,400}$")
_DATE = re.compile(r"^\d{4}(-\d{2}){0,2}$")


class Refused(RuntimeError):
    pass


# ---------------------------------------------------------------------------------------------------------
# redaction: every line this tool prints goes through it
# ---------------------------------------------------------------------------------------------------------
def _redact(text: str, secrets: tuple[str, ...]) -> str:
    for s in secrets:
        if s:
            text = text.replace(s, "***")
    return text


# ---------------------------------------------------------------------------------------------------------
# HTTP (the seam the tests replace)
# ---------------------------------------------------------------------------------------------------------
HttpGet = Callable[[str, dict], "tuple[int, dict, bytes]"]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):          # a redirect could carry the key header to another host
        return None


def http_get(url: str, headers: dict) -> tuple[int, dict, bytes]:
    """GET -> (status, lower-cased headers, body bytes as sent on the wire). Never raises on an HTTP status."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=TIMEOUT_S) as r:
            body = r.read(MAX_BODY_BYTES + 1)
            return r.status, {k.lower(): v for k, v in r.headers.items()}, body
    except urllib.error.HTTPError as e:
        body = e.read(64 * 1024) if e.fp else b""
        return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, body


def gunzip_bounded(data: bytes, limit: int = MAX_DECODED_BYTES) -> bytes:
    """Every gzip member, at most `limit` bytes out. ONE decoder for both sides, so an encoding detail
    (a multi-member object) can never make the two sides decode differently."""
    out, rest = [], data
    total = 0
    while rest:
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        chunk = d.decompress(rest, limit - total + 1)
        total += len(chunk)
        if total > limit or d.unconsumed_tail:
            raise ValueError("gzip body decodes past the limit")
        if not d.eof:
            raise ValueError("gzip body is truncated")
        out.append(chunk)
        rest = d.unused_data
    return b"".join(out)


def decode_body(headers: dict, body: bytes) -> bytes:
    """The bytes a user's client ends up with: gunzipped ONLY when the answer says content-encoding gzip.
    Gzip bytes without that header are what a plain client receives, so they stay as they are (and then
    cannot compare equal with a CSV)."""
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("body larger than the cap")
    if (headers.get("content-encoding") or "").lower() == "gzip":
        return gunzip_bounded(body)
    return body


# ---------------------------------------------------------------------------------------------------------
# the comparison
# ---------------------------------------------------------------------------------------------------------
def data_lines(csv_bytes: bytes) -> list[bytes]:
    """The CSV's lines without '#' lines and without line-end differences (CRLF vs LF)."""
    return [ln.rstrip(b"\r") for ln in csv_bytes.split(b"\n") if ln.strip() and not ln.startswith(b"#")]


def newest_date(lines: list[bytes]) -> str:
    """max(obs_date) over the data rows; 'n/a' when the header has no obs_date column (a wide source)."""
    if not lines:
        return "n/a"
    rows = csv.reader(ln.decode("utf-8", "replace") for ln in lines)
    header = next(rows)
    if "obs_date" not in header:
        return "n/a"
    col = header.index("obs_date")
    best = ""
    for parts in rows:
        if col < len(parts):
            d = parts[col].strip()
            if _DATE.match(d) and d > best:
                best = d
    return best or "n/a"


def compare(served: bytes, stored: bytes) -> dict:
    a, b = data_lines(served), data_lines(stored)
    return {"served_newest": newest_date(a), "r2_newest": newest_date(b),
            "served_rows": max(len(a) - 1, 0), "r2_rows": max(len(b) - 1, 0),
            # two empty sides are not a receipt: equal needs at least one data row on each
            "equal": a == b and len(a) >= 2}


# ---------------------------------------------------------------------------------------------------------
# the two sides
# ---------------------------------------------------------------------------------------------------------
def object_key(series_id: str) -> str:
    from tools.derive_csv_bulk import csv_key            # noqa: PLC0415 - THE writer's layout
    src, rest = series_id.split(":", 1)
    return csv_key("series", src, rest)


def served_url(base: str, series_id: str) -> str:
    return f"{base}/v1/series/{urllib.parse.quote(series_id, safe='')}.csv?raw=1"


def fetch_served(get: HttpGet, base: str, series_id: str, key: str) -> bytes:
    status, headers, body = get(served_url(base, series_id),
                                {"X-API-Key": key, "Accept-Encoding": "gzip", "Cache-Control": "no-cache",
                                 "User-Agent": "econdl-served-receipt"})
    if status != 200:
        raise RuntimeError(f"served HTTP {status}")
    return decode_body(headers, body)


def fetch_r2(s3, series_id: str) -> bytes:
    try:
        r = s3.get_object(Bucket=BUCKET, Key=object_key(series_id))
    except Exception as e:  # noqa: BLE001 - reduced to a code that cannot carry a key or the endpoint
        from tools.r2_object_census import error_code      # noqa: PLC0415
        raise RuntimeError(f"R2 {error_code(e)}") from None
    raw = r["Body"].read(MAX_BODY_BYTES + 1)
    if len(raw) > MAX_BODY_BYTES:
        raise RuntimeError("R2 object larger than the cap")
    if (r.get("ContentEncoding") or "").lower() == "gzip" or raw[:2] == b"\x1f\x8b":
        return gunzip_bounded(raw)
    return raw


def check_forward(get: HttpGet, base: str) -> None:
    import json                                         # noqa: PLC0415
    status, headers, body = get(f"{base}/v1/edge-status", {"User-Agent": "econdl-served-receipt"})
    if status != 200:
        raise Refused(f"/v1/edge-status answered HTTP {status}; cannot tell which backend serves .csv")
    try:
        forward = json.loads(decode_body(headers, body).decode("utf-8")).get("forward")
    except Exception:  # noqa: BLE001
        raise Refused("/v1/edge-status is not the expected JSON; cannot tell which backend serves .csv") from None
    if forward is not False:
        raise Refused(f"the worker reports forward={forward!r}: .csv is served by the self-hosted origin, "
                      "so an R2 comparison does not prove what users get")


def open_r2():
    """The GET-only R2 client. Refuses after T0 before credentials load."""
    from core import r2_util                            # noqa: PLC0415
    from core.cutover import CutoverRefused, refuse_if_cut_over   # noqa: PLC0415
    from tools.r2_object_fetch import GetOnlyClient      # noqa: PLC0415
    try:
        refuse_if_cut_over("tools/served_receipt.py (a GET of the cloud copy)")
    except CutoverRefused as e:
        raise Refused(f"{e}; after T0 this receipt needs a successor that compares with the origin store") from None
    if r2_util.creds(write=False) is None:
        raise Refused("R2 read credentials are not set (the workflow passes the R2_* repo secrets)")
    return GetOnlyClient(r2_util.client(write=False))


def parse_ids(text: str | None) -> list[str]:
    ids = [t for t in re.split(r"[\s,]+", text or "") if t] or list(DEFAULT_IDS)
    if len(ids) > HARD_MAX_IDS:
        raise Refused(f"{len(ids)} ids; at most {HARD_MAX_IDS} per run")
    bad = [i for i in ids if not _ID.match(i)]
    if bad:
        raise Refused(f"not a catalogue id: {bad[0][:80]!r}")
    return list(dict.fromkeys(ids))


# ---------------------------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------------------------
def run(ids: list[str], *, key: str, get: HttpGet, s3_factory, base: str, out) -> int:
    check_forward(get, base)
    s3 = s3_factory()
    failures = 0
    out(f"{'id':<48} {'served_newest':<14} {'r2_newest':<14} {'served_rows':>11} {'r2_rows':>9}  result")
    for sid in ids:
        try:
            served = fetch_served(get, base, sid, key)
        except Exception as e:  # noqa: BLE001
            failures += 1
            out(f"{sid:<48} {'-':<14} {'-':<14} {'-':>11} {'-':>9}  UNFETCHED (served: {type(e).__name__}: {e})")
            continue
        try:
            stored = fetch_r2(s3, sid)
        except Exception as e:  # noqa: BLE001
            failures += 1
            out(f"{sid:<48} {'-':<14} {'-':<14} {'-':>11} {'-':>9}  UNFETCHED (r2: {type(e).__name__}: {e})")
            continue
        c = compare(served, stored)
        if not c["equal"]:
            failures += 1
        out(f"{sid:<48} {c['served_newest']:<14} {c['r2_newest']:<14} {c['served_rows']:>11} {c['r2_rows']:>9}  "
            f"{'equal' if c['equal'] else 'DIFFERENT'}")
    out(f"SUMMARY: {len(ids) - failures} of {len(ids)} equal; {failures} different or unfetched")
    return EXIT_DIFF if failures else EXIT_OK


def main(argv=None, *, get: HttpGet = http_get, s3_factory=open_r2, environ=None) -> int:
    env = os.environ if environ is None else environ
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--ids", default="", help="ids separated by spaces or commas (default: the fixed sample)")
    p.add_argument("--base", default=API_BASE, help=argparse.SUPPRESS)
    a = p.parse_args(argv)
    key = (env.get(KEY_ENV) or "").strip()
    secrets = (key,) if key else ()

    def out(line: str) -> None:
        print(_redact(line, secrets), flush=True)

    if not key:
        print(f"REFUSED: {MISSING_KEY_MSG}. Create an API key on the site's account page and add it as the "
              f"repository secret {KEY_ENV}.", file=sys.stderr, flush=True)
        return EXIT_REFUSED
    if not a.base.startswith("https://"):
        print("REFUSED: --base must be https", file=sys.stderr, flush=True)
        return EXIT_REFUSED
    try:
        ids = parse_ids(a.ids)
        return run(ids, key=key, get=get, s3_factory=s3_factory, base=a.base.rstrip("/"), out=out)
    except Refused as e:
        print(_redact(f"REFUSED: {e}", secrets), file=sys.stderr, flush=True)
        return EXIT_REFUSED
    except Exception as e:  # noqa: BLE001 - anything unexpected is a failure, never a pass
        print(_redact(f"FAILED: {type(e).__name__}: {e}", secrets), file=sys.stderr, flush=True)
        return EXIT_DIFF


if __name__ == "__main__":
    sys.exit(main())
