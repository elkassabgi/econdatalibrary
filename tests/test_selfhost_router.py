"""tools/selfhost/router.py - the blue/green router in front of the origin pair (plan change 3). Two stand-in
instances answer with their own name; the router must forward unchanged, stream, flip atomically, and
never fall back to the other instance by itself."""
import http.client
import http.server
import json
import os
import sys
import threading

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
            seen.append((self.path, dict(self.headers)))
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
    c.request(method, path, headers=headers or {})
    r = c.getresponse()
    body = r.read()
    c.close()
    return r.status, {k.lower(): v for k, v in r.getheaders()}, body


def test_forwards_unchanged_to_the_active_instance(pair):
    port, _state, (_b, blue_seen), _g = pair
    status, headers, body = _get(port, "/v1/sources?x=1", headers={"x-econ-origin-secret": "s", "accept": "a"})
    assert status == 200 and json.loads(body) == {"instance": "blue", "path": "/v1/sources?x=1"}
    assert headers["x-econ-origin"] == "1"
    assert blue_seen[-1][1]["x-econ-origin-secret"] == "s", "headers reach the origin (its gate checks them)"
    status, _h, _b = _get(port, "/secret")
    assert status == 403, "the origin's status passes through"


def test_a_large_length_less_body_is_streamed_whole(pair):
    port = pair[0]
    status, headers, body = _get(port, "/big.csv")
    assert status == 200 and body == BIG and headers["x-econ-count"] == "1"


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
    """Windows refuses os.replace over a file that is open elsewhere; the router opens the state file on every
    request. A flip while a reader holds it must wait for the reader, not fail."""
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
    status, _h, body = _get(port, "/v1/sources")
    assert status == 502 and json.loads(body)["error"] == "origin_instance_unreachable"
    assert green_seen == [], "the other instance was not used behind the operator's back"


def test_only_read_methods_and_head(pair):
    port = pair[0]
    assert _get(port, "/x", method="POST")[0] == 405
    status, headers, body = _get(port, "/x", method="HEAD")
    assert status == 200 and body == b"" and headers["content-length"] == "123"


def test_a_non_local_target_is_refused(tmp_path):
    state = tmp_path / "router.json"
    state.write_text(json.dumps({"active": "a", "targets": {"a": "http://example.org:80"}}))
    with pytest.raises(ValueError, match="not a local"):
        router.State(str(state))


def test_the_router_binds_localhost_only(pair, tmp_path):
    state = pair[1]
    srv = router.serve(str(state), 0)
    try:
        assert srv.server_address[0] == "127.0.0.1"
    finally:
        srv.server_close()
