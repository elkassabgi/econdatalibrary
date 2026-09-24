"""tools/selfhost/router.py - the blue/green router in front of the origin pair (plan change 3). Two stand-in
instances answer with their own name; the router must forward faithfully, stream, flip atomically under
load, refuse request bodies, and never fall back to the other instance by itself (review AR-152)."""
import http.client
import http.server
import json
import os
import socket
import sys
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import router  # noqa: E402

BIG = b"x" * (5 * 1024 * 1024 + 7)


def _instance(name):
    seen = []

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_GET(self):
            seen.append((self.path, self.headers.items()))
            if self.path.startswith("/big"):
                self.send_response(200)
                self.send_header("content-type", "text/csv")
                self.send_header("x-econ-count", "1")
                self.send_header("transfer-encoding", "chunked")        # no length: must be streamed
                self.end_headers()
                for i in range(0, len(BIG), 1 << 20):
                    part = BIG[i:i + (1 << 20)]
                    self.wfile.write(f"{len(part):x}\r\n".encode() + part + b"\r\n")
                self.wfile.write(b"0\r\n\r\n")
                return
            if self.path.startswith("/slow"):                          # 10 bytes every 0.2 s, 8 times
                self.send_response(200)
                self.send_header("transfer-encoding", "chunked")
                self.end_headers()
                for _ in range(8):
                    self.wfile.write(b"a\r\n0123456789\r\n")
                    self.wfile.flush()
                    time.sleep(0.2)
                self.wfile.write(b"0\r\n\r\n")
                return
            if self.path == "/notmodified":
                self.send_response(304)
                self.end_headers()
                return
            if self.path == "/hop":
                self.send_response(200)
                self.send_header("connection", "x-hop")
                self.send_header("x-hop", "private")
                self.send_header("content-length", "2")
                self.end_headers()
                self.wfile.write(b"ok")
                return
            body = json.dumps({"instance": name, "path": self.path}).encode()
            self.send_response(403 if self.path == "/secret" else 200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.send_header("x-econ-origin", "1")
            self.end_headers()
            self.wfile.write(body)

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("content-length", "123")
            self.end_headers()

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, seen


@pytest.fixture
def pair(tmp_path):
    blue, blue_seen = _instance("blue")
    green, green_seen = _instance("green")
    state = tmp_path / "router.json"
    state.write_text(json.dumps({"active": "blue", "targets": {
        "blue": f"http://127.0.0.1:{blue.server_address[1]}", "green": f"http://127.0.0.1:{green.server_address[1]}"}}))
    srv = router.serve(str(state), 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_address[1], state, (blue, blue_seen), (green, green_seen)
    srv.shutdown()
    blue.shutdown()
    green.shutdown()


def _get(port, path, method="GET", headers=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    c.putrequest(method, path, skip_accept_encoding=True)
    for k, v in (headers or []):
        c.putheader(k, v)
    c.endheaders()
    r = c.getresponse()
    body = r.read()
    c.close()
    return r.status, r.getheaders(), body


def _h(headers, name):
    return [v for k, v in headers if k.lower() == name]


def _raw(port, request: bytes, wait=1.0) -> bytes:
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.sendall(request)
    s.settimeout(wait)
    out = b""
    try:
        while True:
            b = s.recv(65536)
            if not b:
                break
            out += b
    except socket.timeout:
        pass
    s.close()
    return out


def test_forwards_to_the_active_instance_with_the_host_rewritten(pair):
    port, state, (blue, blue_seen), _g = pair
    status, headers, body = _get(port, "/v1/sources?x=1", headers=[("x-econ-origin-secret", "s"), ("accept", "a")])
    assert status == 200 and json.loads(body) == {"instance": "blue", "path": "/v1/sources?x=1"}
    assert _h(headers, "x-econ-origin") == ["1"]
    got = dict((k.lower(), v) for k, v in blue_seen[-1][1])
    assert got["x-econ-origin-secret"] == "s", "headers reach the origin (its gate checks them)"
    assert got["host"] == f"127.0.0.1:{blue.server_address[1]}", "Host names the target"
    assert _get(port, "/secret")[0] == 403, "the origin's status passes through"


def test_duplicate_request_headers_are_kept(pair):
    port, _s, (_b, blue_seen), _g = pair
    _get(port, "/x", headers=[("cookie", "a=1"), ("cookie", "b=2")])
    assert [v for k, v in blue_seen[-1][1] if k.lower() == "cookie"] == ["a=1", "b=2"]


def test_connection_listed_headers_are_dropped_both_ways(pair):
    port, _s, (_b, blue_seen), _g = pair
    status, headers, body = _get(port, "/hop", headers=[("connection", "keep-alive, x-priv"), ("x-priv", "1")])
    assert "x-priv" not in [k.lower() for k, _ in blue_seen[-1][1]]
    assert status == 200 and body == b"ok" and not _h(headers, "x-hop")


def test_one_date_and_server_pair(pair):
    headers = _get(pair[0], "/x")[1]
    assert len(_h(headers, "date")) == 1 and len(_h(headers, "server")) == 1


def test_a_large_length_less_body_is_streamed_whole(pair):
    status, headers, body = _get(pair[0], "/big.csv")
    assert status == 200 and body == BIG and _h(headers, "x-econ-count") == ["1"]


def test_bytes_are_passed_on_as_they_arrive(pair):
    """AR-152: resp.read(1 MiB) held a slow answer back for 30 s. The first bytes must arrive at once."""
    s = socket.create_connection(("127.0.0.1", pair[0]), timeout=10)
    s.sendall(b"GET /slow HTTP/1.1\r\nHost: x\r\n\r\n")
    t0 = time.monotonic()
    got = b""
    while b"0123456789" not in got:
        got += s.recv(4096)
    first = time.monotonic() - t0
    s.close()
    assert first < 1.0, f"the first body bytes took {first:.2f} s (the origin sends 10 bytes every 0.2 s)"


def test_a_304_gets_no_body_framing(pair):
    raw = _raw(pair[0], b"GET /notmodified HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    head = raw.split(b"\r\n\r\n", 1)[0].lower()
    assert raw.startswith(b"HTTP/1.1 304") and b"transfer-encoding" not in head


def test_an_http_1_0_client_gets_no_chunked_body(pair):
    raw = _raw(pair[0], b"GET /big.csv HTTP/1.0\r\n\r\n", wait=3.0)
    head, body = raw.split(b"\r\n\r\n", 1)
    assert b"transfer-encoding" not in head.lower() and body == BIG


@pytest.mark.parametrize("request_bytes", [
    b"GET /x HTTP/1.1\r\nHost: x\r\nContent-Length: 38\r\n\r\nGET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n",
    b"GET /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
])
def test_a_request_body_is_refused_and_never_forwarded(pair, request_bytes):
    port, _s, (_b, blue_seen), _g = pair
    raw = _raw(port, request_bytes)
    assert raw.startswith(b"HTTP/1.1 400") and raw.count(b"HTTP/1.1") == 1, "one answer, then the connection closes"
    assert blue_seen == [], "nothing reached the origin"


def test_flips_under_load_never_fail_a_request(pair):
    """AR-152: a read of the state file during a flip's replace raised and answered 503 (1.4 per flip)."""
    port, state = pair[0], pair[1]
    bad, stop, served = [], threading.Event(), []
    # BOUNDED, AND OVER ONE KEEP-ALIVE CONNECTION PER CLIENT (R1218 finding 5): a new connection per request
    # in 8 tight loops left 7,453 sockets in TIME_WAIT from this one test, and overlapping suites exhausted the
    # ephemeral ports (WinError 10048) - on the machine that hosts the live router after T0. The router still
    # opens one origin connection per request, so the cap bounds that side too.
    cap = 200

    def client():
        c, n = None, 0
        while not stop.is_set() and n < cap:
            try:
                if c is None:
                    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
                c.request("GET", "/v1/x")
                r = c.getresponse()
                r.read()
                if r.status != 200:
                    bad.append(r.status)
                if r.getheader("connection", "").lower() == "close":
                    c.close()
                    c = None
            except (OSError, http.client.HTTPException) as e:
                bad.append(type(e).__name__)
                c = None
            n += 1
        served.append(n)
        if c is not None:
            c.close()

    threads = [threading.Thread(target=client) for _ in range(8)]
    for th in threads:
        th.start()
    for i in range(100):
        router.flip(str(state), "green" if i % 2 == 0 else "blue")
        time.sleep(0.002)                      # spread the flips over the requests instead of racing past them
    stop.set()
    for th in threads:
        th.join()
    assert bad == [], f"{len(bad)} failed request(s) during 100 flips: {sorted(set(map(str, bad)))}"
    assert sum(served) >= 200, f"only {sum(served)} requests ran during the flips: the test measured nothing"


def test_the_status_route_reports_in_flight_requests(pair):
    port = pair[0]
    t = threading.Thread(target=lambda: _get(port, "/slow"))
    t.start()
    time.sleep(0.4)
    during = json.loads(_get(port, router.STATUS_PATH)[2])
    t.join()
    after = json.loads(_get(port, router.STATUS_PATH)[2])
    assert during["active"] == "blue" and during["inflight"] == {"blue": 1}
    assert after["inflight"] == {}, "drained"


def test_a_flip_takes_effect_on_the_next_request_and_is_atomic(pair):
    port, state = pair[0], pair[1]
    assert json.loads(_get(port, "/a")[2])["instance"] == "blue"
    router.flip(str(state), "green")
    assert json.loads(_get(port, "/b")[2])["instance"] == "green"
    assert not [p for p in os.listdir(state.parent) if p.endswith(".tmp")], "no temporary file left"
    with pytest.raises(ValueError):
        router.flip(str(state), "purple")
    assert json.loads(state.read_text())["active"] == "green", "a bad flip changes nothing"


def test_a_flip_during_a_read_still_lands(pair):
    state = pair[1]
    fh = open(state, "rb")
    t = threading.Timer(0.3, fh.close)
    t.start()
    router.flip(str(state), "green")
    t.join()
    assert json.loads(state.read_text())["active"] == "green"


def test_a_dead_instance_is_a_502_never_a_silent_fallback(pair):
    port, state, (blue, _), (_green, green_seen) = pair
    blue.shutdown()
    blue.server_close()
    status, _h2, body = _get(port, "/v1/sources")
    assert status == 502 and json.loads(body)["error"] == "origin_instance_unreachable"
    assert green_seen == [], "the other instance was not used behind the operator's back"


def test_a_broken_state_file_is_an_answered_503_with_no_body_on_head(pair):
    port, state = pair[0], pair[1]
    state.write_text("{not json")
    status, _h2, body = _get(port, "/x")
    assert status == 503 and json.loads(body)["error"] == "router_state_unreadable"
    status, _h2, body = _get(port, "/x", method="HEAD")
    assert status == 503 and body == b""


def test_only_read_methods_and_head(pair):
    port = pair[0]
    assert _get(port, "/x", method="POST")[0] == 405
    status, headers, body = _get(port, "/x", method="HEAD")
    assert status == 200 and body == b"" and _h(headers, "content-length") == ["123"]


@pytest.mark.parametrize("target", ["http://example.org:80", "http://127.0.0.1", "http://127.0.0.1:99999"])
def test_a_bad_target_is_refused(tmp_path, target):
    state = tmp_path / "router.json"
    state.write_text(json.dumps({"active": "a", "targets": {"a": target}}))
    with pytest.raises(ValueError):
        router.State(str(state))


def test_the_router_binds_localhost_only(pair):
    srv = router.serve(str(pair[1]), 0)
    try:
        assert srv.server_address[0] == "127.0.0.1"
    finally:
        srv.server_close()
