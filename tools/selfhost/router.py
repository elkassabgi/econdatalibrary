"""The local router in front of the blue/green origin pair (docs/ECON_SELF_HOSTING_PLAN.md, section 2, change 3).

    tunnel / Workers VPC  ->  router (127.0.0.1:<port>)  ->  the ACTIVE origin instance (blue or green)

A catalogue swap starts the idle instance on the new catalogue copy, health-checks it, and then FLIPS: one
atomic rewrite of the state file. The router reads the state file on every request (it is a few bytes), so a
flip takes effect on the next request and needs no restart; requests already in flight finish on the old
instance, which is stopped only after that (the swap script's job).

State file (JSON):  {"active": "blue", "targets": {"blue": "http://127.0.0.1:8801", "green": "http://127.0.0.1:8802"}}

What it does to a request: nothing but forward it. Method, path, query and headers go through unchanged
(the edge already stripped the client's credentials and set the origin secret, which the origin checks);
status, headers and the body come back unchanged, the body STREAMED (a download can be gigabytes). Only
GET, HEAD and OPTIONS exist on this API; anything else is 405 here. When the active instance does not
answer, the router says so with a 502 - it never falls back to the other instance by itself, because the
other one may be serving an older catalogue.

Binds 127.0.0.1 only. Run:  python tools/selfhost/router.py --state <state.json> --port 8787
"""
from __future__ import annotations

import argparse
import http.client
import http.server
import json
import os
import threading
import time
import urllib.parse

CHUNK = 1 << 20
HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
              "transfer-encoding", "upgrade"}


class State:
    """The active target. The state file is READ ON EVERY REQUEST (a few bytes; ~1 request a second) and
    parsed again only when its bytes change: a modification time is not enough - on a FAT-family drive two
    writes within its resolution have the same time, and a flip was missed that way in testing."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._raw = None
        self._target = None
        self.target()                                    # a missing or broken state file fails at start

    def target(self) -> urllib.parse.SplitResult:
        with open(self.path, "rb") as fh:
            raw = fh.read()
        with self._lock:
            if raw != self._raw:
                d = json.loads(raw)
                url = urllib.parse.urlsplit(d["targets"][d["active"]])
                if url.scheme != "http" or url.hostname not in ("127.0.0.1", "localhost"):
                    raise ValueError(f"target {url.geturl()} is not a local http origin")
                self._target, self._raw = url, raw
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


def make_handler(state: State):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _refuse(self, status: int, error: str):
            body = json.dumps({"error": error}).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.send_header("cache-control", "no-store")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _forward(self):
            try:
                t = state.target()
            except Exception:                                   # noqa: BLE001 - the state file broke after start
                return self._refuse(503, "router_state_unreadable")
            conn = http.client.HTTPConnection(t.hostname, t.port or 80, timeout=130)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP and k.lower() != "host"}
            headers["host"] = t.netloc
            try:
                conn.request(self.command, self.path, headers=headers)
                resp = conn.getresponse()
            except OSError:
                conn.close()
                return self._refuse(502, "origin_instance_unreachable")
            try:
                self.send_response(resp.status, resp.reason)
                length = resp.getheader("content-length")
                for k, v in resp.getheaders():
                    if k.lower() not in HOP_BY_HOP:
                        self.send_header(k, v)
                chunked = length is None and self.command != "HEAD" and resp.status not in (204, 304)
                if chunked:
                    self.send_header("transfer-encoding", "chunked")
                self.end_headers()
                if self.command == "HEAD":
                    return
                while True:
                    buf = resp.read(CHUNK)
                    if not buf:
                        break
                    if chunked:
                        self.wfile.write(f"{len(buf):x}\r\n".encode() + buf + b"\r\n")
                    else:
                        self.wfile.write(buf)
                if chunked:
                    self.wfile.write(b"0\r\n\r\n")
            except OSError:
                self.close_connection = True                   # the client went away mid-body
            finally:
                conn.close()

        def do_GET(self):
            self._forward()

        def do_HEAD(self):
            self._forward()

        def do_OPTIONS(self):
            self._forward()

        def _not_allowed(self):
            self._refuse(405, "method_not_allowed")

        do_POST = do_PUT = do_DELETE = do_PATCH = _not_allowed

    return Handler


def serve(state_path: str, port: int) -> http.server.ThreadingHTTPServer:
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), make_handler(State(state_path)))
    srv.daemon_threads = True
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
