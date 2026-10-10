"""The local router in front of the blue/green origin pair (docs/ECON_SELF_HOSTING_PLAN.md, section 2, change 3).

    tunnel / Workers VPC  ->  router (127.0.0.1:<port>)  ->  the ACTIVE origin instance (blue or green)

A catalogue swap starts the idle instance on the new catalogue copy, health-checks it, and then FLIPS: one
atomic rewrite of the state file. The router reads the state file on every request (a few bytes), so a flip
takes effect on the next request and needs no restart. Requests already in flight finish on the instance
they started on; GET /__router/status (answered by the router itself, never forwarded) reports the active
target and how many requests are in flight on each, so the swap stops the old instance only when its count
is 0 - not after a guessed wait that would cut a long download off.

State file (JSON):  {"active": "blue", "targets": {"blue": "http://127.0.0.1:8801", "green": "http://127.0.0.1:8802"}}

What it does to a request: forward it - when it is one the EDGE can send. A request target reaches an
instance only when forwardable() accepts it: its path (everything before the first `?`) is one the edge
forwards (EDGE_PATHS, EDGE_PREFIX: the same list as api/worker/src/edge.ts), and the target holds nothing
that one of the origin's two URL readers resolves to another path. Any other target is the router's own
404. Of the request headers only FORWARDED_HEADERS go through (the edge's allowlist plus the origin secret;
duplicates kept; one that a Connection header names is dropped); status, headers and the body come back
unchanged (hop-by-hop headers dropped), the body STREAMED as it arrives (read1: a slow filtered answer is
not held back until a buffer fills). WHY BOTH LISTS (review AR-267): the origin's secret gate is in the
WORKER, and `wrangler dev` answers some requests before the worker runs - /cdn-cgi/mf/scheduled, and any
path when the request carries an MF-Original-URL header. Behind a tunnel hostname those would be open to
whoever passes Cloudflare's lock, and to everyone in the minutes before a lock exists. So the router lets
through only the edge's paths and headers, and none of miniflare's control headers.
/__router/status is answered only for a request with no HTTP proxy in front (unproxied): a loopback Host,
no query, and none of the headers a proxy or Cloudflare adds (OUTSIDE_MARKS, OUTSIDE_PREFIXES). For any
other request it is the same 404 as an unknown path. This is NOT "a caller on this machine": a request
that reaches the router with a loopback Host and no such header - from a Worker of the account bound to
the tunnel, or over a private-network route - looks the same. Both are made by an administrator of the
account (review AR-268).
THE HEADER BLOCK IS READ HERE, FROM THE RAW LINES (read_fields), and Python's own header parser is asked
nothing. Two readers of one block disagreed three times (reviews AR-152, AR-268, AR-269): that parser also
ends a line at a bare CR, sets a `From ` line aside, joins a folded line to the one before it, and stops at
a line it cannot read - and each difference hid a Content-Length, so the bytes after the block were read as
a second request. So a request is answered 400, and its connection closed, unless EVERY line of its header
block is `name: value` in the form the edge's chain sends: a token, a colon, a value with no control
character but TAB, and CRLF. This API has no request bodies: a request with any Transfer-Encoding, or a
Content-Length that is not 0, is the same 400. A request line without an HTTP version (HTTP/0.9) is refused
too. What this does not cover: a front proxy that reads a block differently from this grammar; the proxies
in front here (Cloudflare, cloudflared) write their own header lines.
GET, HEAD and OPTIONS are forwarded: the edge sends GET only, and the worker's secret gate runs first on
all three. POST, PUT, DELETE and PATCH are 405 here; any other method is http.server's own 501.
When the active instance does not answer, the router says so with a 502 - it never falls back to the other
instance by itself, because that one may be serving an older catalogue.

Binds 127.0.0.1 only. Run:  python tools/selfhost/router.py --state <state.json> --port 8787
"""
from __future__ import annotations

import argparse
import collections
import http.client
import http.server
import json
import os
import re
import threading
import time
import urllib.parse

CHUNK = 1 << 20
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "transfer-encoding", "upgrade", "proxy-connection"}
STATUS_PATH = "/__router/status"

# The routes the edge forwards (api/worker/src/edge.ts FORWARDED_PATHS and its "/v1/series/" prefix).
# tests/test_selfhost_router.py reads edge.ts and fails when a path, the prefix or a header there differs
# from these lists.
EDGE_PATHS = frozenset({"/", "/v1", "/v1/", "/v1/catalog", "/v1/sources", "/v1/last-updates", "/v1/stats",
                        "/v1/bundle", "/v1/guard-heartbeat"})
EDGE_PREFIX = "/v1/series/"
# The request headers the origin may see: the edge's FORWARDED_REQUEST_HEADERS plus the origin secret.
FORWARDED_HEADERS = frozenset({"accept", "accept-encoding", "accept-language", "user-agent",
                               "x-econ-origin-secret"})
# Headers a reverse proxy or Cloudflare adds. A request that carries one came through an HTTP proxy.
OUTSIDE_MARKS = ("cdn-loop", "forwarded", "via", "x-real-ip")
OUTSIDE_PREFIXES = ("cf-", "x-forwarded-")
# forwardable() models how THIS workerd reads a request target (the version the pinned miniflare runs). A test
# compares it with api/worker/package-lock.json: after a change of version run tools/selfhost/path_fuzz first.
MEASURED_WORKERD = "1.20250718.0"


def forwardable(target: str) -> bool:
    """True when the request target is one the edge forwards - `/v1/series/../../cdn-cgi/mf/scheduled` must
    not pass as a series path. The origin reads a target TWICE and the two readers differ (measured in review
    AR-268 on workerd MEASURED_WORKERD): workerd's HTTP layer takes `#` as an ordinary path character and
    resolves literal `.` and `..` segments, also AFTER a `#` (`/v1/series/a#/../../../cdn-cgi/mf/scheduled`
    arrived as `/cdn-cgi/mf/scheduled`); the WHATWG parser (`new URL(request.url)`) then reads %2e as a dot
    and a backslash as a slash, and ends the path at a `#`. So the path is everything before the first `?`,
    and a target is refused when it holds a `#` anywhere (the edge never sends one: fetch drops a fragment),
    a character outside ASCII or a control character (http.client cannot send either on: the first was a
    dropped connection, the second a 502 that named the origin - reviews AR-268, AR-269), a backslash in its
    path, or a path segment that is `.` or `..` in either spelling."""
    if not target.startswith("/"):
        return False                                     # an absolute-form or authority-form target
    if "#" in target or not target.isascii() or any(c <= " " or c == "\x7f" for c in target):
        return False
    path = target.split("?", 1)[0]
    if "\\" in path:
        return False
    for seg in path.split("/"):
        if seg.lower().replace("%2e", ".") in (".", ".."):
            return False
    return path in EDGE_PATHS or path.startswith(EDGE_PREFIX)


def unproxied(headers) -> bool:
    """True for a request with no HTTP proxy in front: its Host is a loopback name and it carries none of
    the headers a proxy or Cloudflare adds. The tunnel's requests come from 127.0.0.1 too (cloudflared runs
    here), so the peer address says nothing. It cannot tell a caller on this machine from a request that
    arrives with a loopback Host and no such header (see the module text)."""
    hosts = headers.get_all("host") or []
    if len(hosts) != 1:
        return False                                     # none, or two that could be read differently
    host = hosts[0].strip().lower()
    name = host.rsplit(":", 1)[0] if not host.startswith("[") else host.split("]", 1)[0] + "]"
    if name not in ("127.0.0.1", "localhost", "[::1]"):
        return False
    for k in headers.keys():
        low = k.lower()
        if low in OUTSIDE_MARKS or low.startswith(OUTSIDE_PREFIXES):
            return False
    return True


def _dropped(headers) -> set[str]:
    """Hop-by-hop headers plus every header the Connection header names (RFC 9110 section 7.6.1)."""
    names = set(HOP_BY_HOP)
    for value in headers.get_all("connection", []) if hasattr(headers, "get_all") else \
            [v for k, v in headers if k.lower() == "connection"]:
        names.update(t.strip().lower() for t in value.split(",") if t.strip())
    return names


# One header line as the edge's chain sends it (RFC 9110 field syntax, the strict form): a token, a colon,
# optional blanks, a value with no control character but TAB. Bytes of 0x80 and above are allowed in a value
# (a User-Agent may hold them). The line's CRLF is taken off before the match.
_FIELD_LINE = re.compile(rb"([!#$%&'*+\-.^_`|~0-9A-Za-z]+):[ \t]*([^\x00-\x08\x0a-\x1f\x7f]*)")


class Fields:
    """The header fields of one request as THIS module read them (read_fields): names as sent, values
    decoded as ISO-8859-1 with the blanks at their ends removed, in the order sent, duplicates kept."""

    def __init__(self, pairs):
        self._pairs = list(pairs)

    def get_all(self, name: str, default=None):
        found = [v for k, v in self._pairs if k.lower() == name]
        return found or default

    def keys(self) -> list:
        return [k for k, _ in self._pairs]

    def items(self) -> list:
        return list(self._pairs)


def read_fields(raw_lines) -> "Fields | None":
    """The header block from the lines http.server read off the wire (the last one is the empty line that
    ends the block), or None when the block is not made only of clean lines. Not clean: a line that does not
    end in CRLF; a line that is not `token: value` (so: no colon, a space or a tab before the colon, an empty
    name, a line that starts with a blank = a folded line, a `From ` line); a control character in a value -
    a bare CR among them, which Python's parser takes for a line end; a block that did not end with an empty
    line (the connection ended, or the block was cut)."""
    if not raw_lines or raw_lines[-1] != b"\r\n":
        return None
    pairs = []
    for line in raw_lines[:-1]:
        if not line.endswith(b"\r\n"):
            return None
        m = _FIELD_LINE.fullmatch(line[:-2])
        if m is None:
            return None
        pairs.append((m.group(1).decode("ascii"), m.group(2).decode("iso-8859-1").strip(" \t")))
    return Fields(pairs)


class _Tap:
    """Records the lines http.server reads while it parses the header block."""

    def __init__(self, fp):
        self._fp, self.lines = fp, []

    def readline(self, *a):
        line = self._fp.readline(*a)
        self.lines.append(line)
        return line

    def __getattr__(self, name):
        return getattr(self._fp, name)


def _carries_a_body(fields: Fields) -> bool:
    """True when the request names a body: any Transfer-Encoding, or a Content-Length that is not 0 - every
    one of them (review AR-268: `Content-Length: 0` then `Content-Length: 44` let 44 bytes through as a
    second request when only the first was read)."""
    if fields.get_all("transfer-encoding"):
        return True
    return any(v not in ("", "0") for v in fields.get_all("content-length") or [])


class State:
    """The active target. The state file is READ ON EVERY REQUEST (a few bytes; ~1 request a second) and
    parsed again only when its bytes change: a modification time is not enough - on a FAT-family drive two
    writes within its resolution have the same time, and a flip was missed that way in testing. A read that
    lands while a flip replaces the file (Windows: PermissionError) is retried briefly, not answered 503
    (AR-152: 1.4 failed requests per flip under load)."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._raw = None
        self._target = None
        self.target()                                    # a missing or broken state file fails at start

    def _read(self) -> bytes:
        for attempt in range(50):
            try:
                with open(self.path, "rb") as fh:
                    return fh.read()
            except PermissionError:
                if attempt == 49:
                    raise
                time.sleep(0.01)
        raise AssertionError("unreachable")

    def target(self) -> tuple[str, urllib.parse.SplitResult]:
        raw = self._read()
        with self._lock:
            if raw != self._raw:
                d = json.loads(raw)
                name = d["active"]
                url = urllib.parse.urlsplit(d["targets"][name])
                if url.scheme != "http" or url.hostname not in ("127.0.0.1", "localhost"):
                    raise ValueError(f"target {url.geturl()} is not a local http origin")
                if not url.port:                         # also raises ValueError for an out-of-range port
                    raise ValueError(f"target {url.geturl()} has no port")
                self._target, self._raw = (name, url), raw
            return self._target


def flip(state_path: str, active: str) -> None:
    """Make `active` the target: write a temporary file and replace the state file with it in one step."""
    with open(state_path, encoding="utf-8") as fh:
        d = json.load(fh)
    if active not in d["targets"]:
        raise ValueError(f"unknown target {active!r}; known: {sorted(d['targets'])}")
    d["active"] = active
    tmp = f"{state_path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(d, fh)
        fh.flush()
        os.fsync(fh.fileno())
    # Windows refuses to replace a file another process has open without delete-sharing - and the router
    # opens the state file on every request - so a flip that lands during a read is retried, briefly.
    for attempt in range(100):
        try:
            os.replace(tmp, state_path)
            return
        except PermissionError:
            if attempt == 99:
                os.remove(tmp)
                raise
            time.sleep(0.02)


class Inflight:
    """Requests in flight per target name: the swap's drain signal."""

    def __init__(self):
        self._lock = threading.Lock()
        self._n = collections.Counter()

    def add(self, name: str, delta: int) -> None:
        with self._lock:
            self._n[name] += delta
            if self._n[name] == 0:
                del self._n[name]

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._n)


class OriginPool:
    """Idle keep-alive connections to the origin instances, per target (R1218 finding 5: one new connection per
    request left thousands of sockets in TIME_WAIT on the machine that hosts the router). A connection goes back
    only after its response was read to the end and the origin did not ask to close; anything else is closed.
    Bounded per target, and an idle connection older than IDLE_S is closed rather than reused (the origin may
    have dropped it) - a reused one that turns out dead is retried ONCE on a fresh connection, which is safe
    because the router forwards only GET, HEAD and OPTIONS."""
    MAX_IDLE = 32
    # BELOW THE ORIGIN'S OWN IDLE CLOSE (R1229): workerd closes an idle keep-alive connection after ~5 s
    # (measured on the pinned 1.20250718.0), so a pool that kept them 30 s handed out dead ones after every
    # quiet spell - 4 of 8 requests answered 502 after a 7 s lull.
    IDLE_S = 4.0
    TIMEOUT_S = 130

    def __init__(self):
        self._lock = threading.Lock()
        self._idle: dict = {}                               # (host, port) -> [(conn, returned_at)]

    def fresh(self, host: str, port: int):
        return http.client.HTTPConnection(host, port, timeout=self.TIMEOUT_S)

    def get(self, host: str, port: int):
        """(connection, reused?). Idle connections past IDLE_S are closed - for EVERY target, so an old
        target's connections after a flip do not stay open until the next flip back (R1229)."""
        now = time.monotonic()
        stale = []
        found = None
        with self._lock:
            for key, entries in self._idle.items():
                self._idle[key] = [(c, at) for c, at in entries if now - at <= self.IDLE_S]
                stale += [c for c, at in entries if now - at > self.IDLE_S]
            idle = self._idle.get((host, port), [])
            if idle:
                found = idle.pop()[0]
        for s in stale:
            s.close()
        return (found, True) if found is not None else (self.fresh(host, port), False)

    def put(self, host: str, port: int, conn) -> None:
        with self._lock:
            idle = self._idle.setdefault((host, port), [])
            if len(idle) < self.MAX_IDLE:
                idle.append((conn, time.monotonic()))
                return
        conn.close()

    def idle_count(self, host: str, port: int) -> int:
        with self._lock:
            return len(self._idle.get((host, port), []))


def make_handler(state: State, inflight: Inflight, pool: "OriginPool | None" = None):
    pool = pool if pool is not None else OriginPool()
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        timeout = 75                                       # an idle keep-alive connection frees its thread

        def log_message(self, *a):
            pass

        def parse_request(self):
            """http.server's own parse, with the header lines recorded as they are read off the wire.
            self.fields is the router's reading of them (None = not clean, or no block was read)."""
            self.fields = None
            real = self.rfile
            self.rfile = tap = _Tap(real)
            try:
                ok = super().parse_request()
            finally:
                self.rfile = real
            if ok and self.request_version != "HTTP/0.9":
                self.fields = read_fields(tap.lines)
            return ok

        def _refuse(self, status: int, error: str, close: bool = False):
            body = json.dumps({"error": error}).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.send_header("cache-control", "no-store")
            if close:
                self.send_header("connection", "close")
                self.close_connection = True
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _status(self):
            try:
                name, url = state.target()
                d = {"active": name, "target": url.geturl(), "inflight": inflight.snapshot()}
            except Exception as e:                              # noqa: BLE001
                d = {"active": None, "error": f"{type(e).__name__}: {e}", "inflight": inflight.snapshot()}
            body = json.dumps(d).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.send_header("cache-control", "no-store")
            self.end_headers()
            if self.command != "HEAD":                          # a body after a HEAD answer is read as the next answer
                self.wfile.write(body)

        def _forward(self):
            # the header block and the body check come first, for every path: bytes left unread on the
            # connection would be parsed as the next request
            if self.fields is None:                             # HTTP/0.9, or a block read_fields refuses
                return self._refuse(400, "request_headers_not_allowed", close=True)
            if _carries_a_body(self.fields):
                return self._refuse(400, "request_body_not_allowed", close=True)
            if self.path == STATUS_PATH and unproxied(self.fields):
                return self._status()
            if not forwardable(self.path):
                # never forwarded. The answer has no origin mark, so an edge that did get it reports a bad
                # answer from the origin (502), never data
                return self._refuse(404, "not_found")
            try:
                name, t = state.target()
            except Exception:                                   # noqa: BLE001 - the state file broke after start
                return self._refuse(503, "router_state_unreadable")
            inflight.add(name, 1)
            try:
                self._proxy(t)
            finally:
                inflight.add(name, -1)

        def _proxy(self, t):
            drop = _dropped(self.fields) | {"host", "content-length"}
            conn, reused = pool.get(t.hostname, t.port)
            for attempt in (0, 1):
                try:
                    conn.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
                    conn.putheader("host", t.netloc)
                    for k, v in self.fields.items():            # duplicates kept (AR-152)
                        if k.lower() in FORWARDED_HEADERS and k.lower() not in drop:
                            conn.putheader(k, v)
                    conn.endheaders()
                    resp = conn.getresponse()
                    break
                except (OSError, http.client.HTTPException) as e:
                    conn.close()
                    # ONCE MORE, on a NEW connection (not another pooled one, which may be as dead - R1229), and
                    # only when a REUSED connection turned out closed before any answer: never a timeout, which
                    # means the origin had the request and may have run it (R1229: a slow request ran twice)
                    closed = isinstance(e, (ConnectionError, http.client.RemoteDisconnected,
                                            http.client.CannotSendRequest)) and not isinstance(e, TimeoutError)
                    if reused and attempt == 0 and closed:
                        conn = pool.fresh(t.hostname, t.port)
                        continue
                    return self._refuse(502, "origin_instance_unreachable")
            finished = False
            try:
                # send_response_only: the origin's own Date and Server go through, not a second pair
                self.send_response_only(resp.status, resp.reason)
                rdrop = _dropped(resp.getheaders())
                for k, v in resp.getheaders():
                    if k.lower() not in rdrop:
                        self.send_header(k, v)
                length = resp.getheader("content-length")
                bodyless = self.command == "HEAD" or resp.status in (204, 304) or 100 <= resp.status < 200
                chunked = length is None and not bodyless and self.request_version != "HTTP/1.0"
                if chunked:
                    self.send_header("transfer-encoding", "chunked")
                elif length is None and not bodyless:           # HTTP/1.0 client: end the body by closing
                    self.send_header("connection", "close")
                    self.close_connection = True
                self.end_headers()
                if bodyless:
                    resp.read()                                 # b"": lets the connection be reused
                    finished = True
                    return
                while True:
                    buf = resp.read1(CHUNK)                     # what has arrived, not a full buffer (AR-152)
                    if not buf:
                        break
                    if chunked:
                        self.wfile.write(f"{len(buf):x}\r\n".encode() + buf + b"\r\n")
                    else:
                        self.wfile.write(buf)
                    self.wfile.flush()
                if chunked:
                    self.wfile.write(b"0\r\n\r\n")
                finished = True
            except OSError:
                self.close_connection = True                   # the client went away mid-body
            finally:
                # back to the pool only a response read to the end that the origin did not ask to close;
                # anything else is closed - the origin stops work on a closed connection
                # never after a 1xx: http.client skips only 100, so a 103 was forwarded as the answer and the
                # real response left unread on the connection - the next client got it (R1229)
                if finished and resp.isclosed() and not resp.will_close and resp.status >= 200:
                    pool.put(t.hostname, t.port, conn)
                else:
                    conn.close()

        def do_GET(self):
            self._forward()

        def do_HEAD(self):
            self._forward()

        def do_OPTIONS(self):
            self._forward()

        def _not_allowed(self):
            self._refuse(405, "method_not_allowed", close=True)

        do_POST = do_PUT = do_DELETE = do_PATCH = _not_allowed

    return Handler


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128                               # a connection burst from the tunnel is not refused


def serve(state_path: str, port: int) -> _Server:
    pool = OriginPool()
    srv = _Server(("127.0.0.1", port), make_handler(State(state_path), Inflight(), pool))
    srv.pool = pool
    return srv


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--flip", metavar="TARGET", help="make TARGET active and exit")
    a = ap.parse_args()
    if a.flip:
        flip(a.state, a.flip)
        print(f"active -> {a.flip}")
        return 0
    srv = serve(a.state, a.port)
    print(f"router on 127.0.0.1:{a.port} -> {a.state}", flush=True)
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
