"""The local router in front of the blue/green origin pair (docs/ECON_SELF_HOSTING_PLAN.md, section 2, change 3).

    tunnel / Workers VPC  ->  router (127.0.0.1:<port>)  ->  the ACTIVE origin instance (blue or green)

A catalogue swap starts the idle instance on the new catalogue copy, health-checks it, and then FLIPS: one
atomic rewrite of the state file. The router reads the state file on every request (a few bytes), so a flip
takes effect on the next request and needs no restart. Requests already in flight finish on the instance
they started on; GET /__router/status (answered by the router itself, never forwarded) reports the active
target and how many requests are in flight on each, so the swap stops the old instance only when its count
is 0 - not after a guessed wait that would cut a long download off.

State file (JSON):  {"active": "blue", "targets": {"blue": "http://127.0.0.1:8801", "green": "http://127.0.0.1:8802"}}

What it does to a request: forward it. Method, path, query and every header go through (duplicates kept;
hop-by-hop headers and the ones the Connection header names are dropped, both ways); status, headers and
the body come back unchanged, the body STREAMED as it arrives (read1: a slow filtered answer is not held
back until a buffer fills). This API has no request bodies: a request that carries one (Content-Length > 0
or any Transfer-Encoding) is refused with 400 and the connection closed, so it can never desynchronise the
connection to the origin (review AR-152). Only GET, HEAD and OPTIONS exist; anything else is 405 here.
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
import threading
import time
import urllib.parse

CHUNK = 1 << 20
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "transfer-encoding", "upgrade", "proxy-connection"}
STATUS_PATH = "/__router/status"


def _dropped(headers) -> set[str]:
    """Hop-by-hop headers plus every header the Connection header names (RFC 9110 section 7.6.1)."""
    names = set(HOP_BY_HOP)
    for value in headers.get_all("connection", []) if hasattr(headers, "get_all") else \
            [v for k, v in headers if k.lower() == "connection"]:
        names.update(t.strip().lower() for t in value.split(",") if t.strip())
    return names


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
    IDLE_S = 30.0

    def __init__(self):
        self._lock = threading.Lock()
        self._idle: dict = {}                               # (host, port) -> [(conn, returned_at)]

    def get(self, host: str, port: int):
        """(connection, reused?)."""
        now = time.monotonic()
        stale = []
        with self._lock:
            idle = self._idle.get((host, port), [])
            while idle:
                conn, at = idle.pop()
                if now - at <= self.IDLE_S:
                    for s in stale:
                        s.close()
                    return conn, True
                stale.append(conn)
        for s in stale:
            s.close()
        return http.client.HTTPConnection(host, port, timeout=130), False

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
            self.wfile.write(body)

        def _forward(self):
            if self.path == STATUS_PATH:
                return self._status()
            length = self.headers.get("content-length")
            if self.headers.get("transfer-encoding") or (length and length.strip() not in ("", "0")):
                return self._refuse(400, "request_body_not_allowed", close=True)
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
            drop = _dropped(self.headers) | {"host", "content-length"}
            for attempt in (0, 1):
                conn, reused = pool.get(t.hostname, t.port)
                try:
                    conn.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
                    conn.putheader("host", t.netloc)
                    for k, v in self.headers.items():           # duplicates kept (AR-152)
                        if k.lower() not in drop:
                            conn.putheader(k, v)
                    conn.endheaders()
                    resp = conn.getresponse()
                    break
                except (OSError, http.client.HTTPException):
                    conn.close()
                    if reused and attempt == 0:
                        continue                  # a pooled connection the origin had dropped: once more, fresh
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
                if finished and resp.isclosed() and not resp.will_close:
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
