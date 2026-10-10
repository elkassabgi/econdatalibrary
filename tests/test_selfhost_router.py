"""tools/selfhost/router.py - the blue/green router in front of the origin pair (plan change 3). Two stand-in
instances answer with their own name; the router must forward faithfully, stream, flip atomically under
load, refuse request bodies, and never fall back to the other instance by itself (review AR-152)."""
import http.client
import http.server
import json
import os
import re
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
            if self.path.startswith("/v1/series/big"):
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
            if self.path.startswith("/v1/series/slow"):                          # 10 bytes every 0.2 s, 8 times
                self.send_response(200)
                self.send_header("transfer-encoding", "chunked")
                self.end_headers()
                for _ in range(8):
                    self.wfile.write(b"a\r\n0123456789\r\n")
                    self.wfile.flush()
                    time.sleep(0.2)
                self.wfile.write(b"0\r\n\r\n")
                return
            if self.path == "/v1/series/notmodified":
                self.send_response(304)
                self.end_headers()
                return
            if self.path == "/v1/series/hop":
                self.send_response(200)
                self.send_header("connection", "x-hop")
                self.send_header("x-hop", "private")
                self.send_header("content-length", "2")
                self.end_headers()
                self.wfile.write(b"ok")
                return
            body = json.dumps({"instance": name, "path": self.path}).encode()
            self.send_response(403 if self.path == "/v1/series/secret" else 200)
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
    assert _get(port, "/v1/series/secret")[0] == 403, "the origin's status passes through"


def test_duplicate_request_headers_are_kept(pair):
    port, _s, (_b, blue_seen), _g = pair
    _get(port, "/v1/series/x", headers=[("accept-language", "en"), ("accept-language", "fr")])
    assert [v for k, v in blue_seen[-1][1] if k.lower() == "accept-language"] == ["en", "fr"]


# ---- only what the edge can send reaches an instance (review AR-267) ---------------------------------------

def _edge_ts():
    return open(os.path.join(ROOT, "api", "worker", "src", "edge.ts"), encoding="utf-8").read()


def test_the_router_forwards_the_edges_routes_and_headers_and_no_others():
    """The two lists are copies of api/worker/src/edge.ts. A route or header added there and not here would be
    refused at the workstation (a 404 without the origin mark = the edge's 502); one added here only is a
    hole the edge never uses. Also pinned (review AR-268: both passed unseen): edge.ts has ONE prefix test,
    and originRequest sets no header by hand beyond the allowlist loop, the secret and the two Access
    headers (which the router drops)."""
    import re
    ts = _edge_ts()
    block = ts.split("const FORWARDED_PATHS: ReadonlySet<string> = new Set([", 1)[1].split("]);", 1)[0]
    assert set(re.findall(r'"([^"]*)"', block)) == set(router.EDGE_PATHS)
    assert f'path.startsWith("{router.EDGE_PREFIX}")' in ts
    block = ts.split("const FORWARDED_REQUEST_HEADERS = [", 1)[1].split("];", 1)[0]
    edge_headers = set(re.findall(r'"([^"]*)"', block))
    assert edge_headers and edge_headers | {"x-econ-origin-secret"} == set(router.FORWARDED_HEADERS)
    assert 'ORIGIN_SECRET_HEADER = "x-econ-origin-secret"' in ts
    fn = ts.split("export function isForwardable(", 1)[1].split("\n}", 1)[0]
    assert fn.count("startsWith(") == 1 and fn.count("FORWARDED_PATHS.has(path)") == 1
    fn = ts.split("export function originRequest(", 1)[1].split("\n}", 1)[0]
    assert sorted(re.findall(r"headers\.(?:set|append)\(([^,]+),", fn)) == sorted(
        ["h", "ORIGIN_SECRET_HEADER", '"cf-access-client-id"', '"cf-access-client-secret"'])
    assert fn.count("headers.") == 4 + fn.count("request.headers."), "no other way to add a header"
    assert 'method: "GET"' in fn


def test_the_path_rule_was_measured_on_the_workerd_that_is_pinned():
    """forwardable() models how one workerd version reads a request target (review AR-268 asked the real
    binary about every generated target the rule accepts). Another version may read a target differently.
    When this fails: run tools/selfhost/path_fuzz/fuzz_paths.py against the new binary, read its result,
    and only then change MEASURED_WORKERD."""
    lock = json.load(open(os.path.join(ROOT, "api", "worker", "package-lock.json"), encoding="utf-8"))
    packages = lock["packages"]
    assert packages["node_modules/workerd"]["version"] == router.MEASURED_WORKERD
    # the binary itself is a platform package, and miniflare names the workerd it wants: all must agree
    platform = {k: v["version"] for k, v in packages.items() if k.startswith("node_modules/@cloudflare/workerd-")}
    assert len(platform) >= 3 and set(platform.values()) == {router.MEASURED_WORKERD}, platform
    assert packages["node_modules/miniflare"]["dependencies"]["workerd"] == router.MEASURED_WORKERD
    wants = packages["node_modules/workerd"]["optionalDependencies"]
    assert set(wants.values()) == {router.MEASURED_WORKERD} and set(wants) == {k.split("node_modules/", 1)[1] for k in platform}


@pytest.mark.parametrize("path", [
    "/cdn-cgi/mf/scheduled", "/cdn-cgi/handler/scheduled", "/__scheduled", "/v1/pv", "/v1/public-stats",
    "/v1/edge-status", "/favicon.ico", "/v1/series", "/v1/catalogue", "/V1/sources", "/v1/sources/",
    "/v1/series/../../cdn-cgi/mf/scheduled", "/v1/series/%2e%2e/%2E%2E/cdn-cgi/mf/scheduled",
    "/v1/series/.%2E/x", "/v1/series/..", "/v1/series/..\\..\\cdn-cgi\\mf\\scheduled",
    "/v1/series/a/./b", "/v1/series/x\\y", "/__router/statusx",
    # AR-268: workerd's HTTP layer reads '#' as a path character and resolves the dot segments after it
    # (measured on workerd 1.20250718.0: the first four arrived as /cdn-cgi/mf/scheduled or /v1/pv)
    "/v1/series/a#/../../../cdn-cgi/mf/scheduled", "/v1/catalog#/../../cdn-cgi/mf/scheduled",
    "/#/../cdn-cgi/mf/scheduled", "/v1/series/a#/../../pv?p=/", "/v1/sources#x", "/v1/series/a?x#y",
    "/__router/status?x=1",
])
def test_a_path_the_edge_does_not_forward_never_reaches_an_instance(pair, path):
    port, _s, (_b, blue_seen), (_g, green_seen) = pair
    status, headers, body = _get(port, path)
    assert status == 404 and json.loads(body) == {"error": "not_found"}
    assert not _h(headers, "x-econ-origin"), "the router's own answer never carries the origin's mark"
    assert blue_seen == [] and green_seen == [], "nothing was forwarded"


def test_an_absolute_form_target_is_not_forwarded(pair):
    port, _s, (_b, blue_seen), _g = pair
    out = _raw(port, b"GET http://127.0.0.1/v1/sources HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
    assert out.startswith(b"HTTP/1.1 404") and blue_seen == []


@pytest.mark.parametrize("path", ["/", "/v1", "/v1/", "/v1/catalog?q=gdp&limit=5", "/v1/sources",
                                  "/v1/last-updates", "/v1/stats", "/v1/bundle?ids=a,b", "/v1/guard-heartbeat",
                                  "/v1/series/abs%3ACPI%3A1.10001.10.50.Q.csv?from=2020-01-01",
                                  "/v1/series/ksh%3AKSH%3Ayear%20..%3AMinimum.metadata.json",
                                  "/v1/series/a%2Fb.csv", "/v1/series/"])
def test_every_route_of_the_edge_is_forwarded_with_its_query(pair, path):
    port, _s, (_b, blue_seen), _g = pair
    status, _hd, body = _get(port, path)
    assert status == 200 and json.loads(body)["path"] == path and blue_seen[-1][0] == path


def test_only_the_edges_headers_and_the_secret_reach_an_instance(pair):
    """miniflare reads MF-Original-URL (it REPLACES the request URL), MF-CF-Blob and friends before the worker
    and its secret gate run; a client's credentials and cookies are never the origin's business either."""
    port, _s, (_b, blue_seen), _g = pair
    sent = [("accept", "text/csv"), ("accept-encoding", "gzip"), ("accept-language", "en"), ("user-agent", "t/1"),
            ("x-econ-origin-secret", "s"), ("MF-Original-URL", "http://x/cdn-cgi/mf/scheduled"),
            ("mf-cf-blob", "{"), ("mf-op", "GET"), ("x-api-key", "k"), ("authorization", "Bearer k"),
            ("cookie", "a=1"), ("cf-connecting-ip", "203.0.113.9"), ("x-forwarded-for", "203.0.113.9"),
            ("cf-access-client-secret", "z"), ("range", "bytes=0-9"), ("x-elkassabgi-client", "mcp")]
    assert _get(port, "/v1/series/x", headers=sent)[0] == 200
    got = sorted(k.lower() for k, _v in blue_seen[-1][1])
    assert got == sorted(["accept", "accept-encoding", "accept-language", "user-agent", "x-econ-origin-secret",
                          "host"])


def _status_with(port, headers):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    c.putrequest("GET", router.STATUS_PATH, skip_host=True, skip_accept_encoding=True)
    for k, v in headers:
        c.putheader(k, v)
    c.endheaders()
    r = c.getresponse()
    body = r.read()
    c.close()
    return r.status, body


def test_the_status_route_is_not_answered_through_a_proxy(pair):
    port = pair[0]
    for host in (f"127.0.0.1:{port}", f"localhost:{port}", f"[::1]:{port}", "127.0.0.1", f"LOCALHOST:{port}"):
        status, body = _status_with(port, [("Host", host)])
        assert status == 200 and "inflight" in json.loads(body), host
    outside = [[("Host", "econ-origin.example.com")],                     # the tunnel keeps the public Host
               [("Host", f"127.0.0.1:{port}"), ("Cf-Ray", "8f1-DFW")],
               [("Host", f"127.0.0.1:{port}"), ("CF-Connecting-IP", "203.0.113.9")],
               [("Host", f"127.0.0.1:{port}"), ("X-Forwarded-For", "203.0.113.9")],
               [("Host", f"127.0.0.1:{port}"), ("X-Forwarded-Proto", "https")],
               [("Host", f"127.0.0.1:{port}"), ("Cdn-Loop", "cloudflare")],
               [("Host", f"127.0.0.1:{port}"), ("Via", "1.1 x")],
               [("Host", f"127.0.0.1:{port}"), ("Forwarded", "for=203.0.113.9")],
               [("Host", f"127.0.0.1:{port}"), ("X-Real-IP", "203.0.113.9")],
               [("Host", f"127.0.0.1.example.com:{port}")],
               [("Host", "localhost.example.com")],
               []]                                                        # no Host at all
    for headers in outside:
        status, body = _status_with(port, headers)
        assert status == 404 and json.loads(body) == {"error": "not_found"}, headers
        assert b"inflight" not in body and b"target" not in body


def test_a_target_http_client_cannot_send_is_a_404_not_a_dropped_connection(pair):
    """AR-268 N1: a raw non-ASCII byte in an allowed path made putrequest raise UnicodeEncodeError, which no
    handler caught: the connection was dropped without an answer and a traceback went to the log."""
    port, _s, (_b, blue_seen), _g = pair
    out = _raw(port, b"GET /v1/series/caf\xe9.csv HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    assert out.startswith(b"HTTP/1.1 404") and blue_seen == []
    assert _get(port, "/v1/sources")[0] == 200, "the router still answers"


def test_header_names_are_matched_whatever_their_case(pair):
    """cloudflared sends Canonical-Case names over HTTP/1.1; the allowlist is lower case (AR-268: a
    case-sensitive match would have dropped the secret on every tunnel request with all tests green)."""
    port, _s, (_b, blue_seen), _g = pair
    sent = [("X-Econ-Origin-Secret", "s"), ("Accept-Encoding", "br, gzip"), ("User-Agent", "t/1"),
            ("ACCEPT", "text/csv"), ("Accept-Language", "en"), ("Mf-Original-Url", "http://x/cdn-cgi/mf/scheduled")]
    assert _get(port, "/v1/series/x", headers=sent)[0] == 200
    got = {k.lower(): v for k, v in blue_seen[-1][1]}
    assert got["x-econ-origin-secret"] == "s" and got["accept-encoding"] == "br, gzip" and got["user-agent"] == "t/1"
    assert got["accept"] == "text/csv" and got["accept-language"] == "en" and "mf-original-url" not in got


def test_an_allowed_header_that_connection_names_is_still_dropped(pair):
    """The older test of this rule sends x-priv, which the allowlist now drops by itself (AR-268: the
    Connection rule on the request side was no longer tested)."""
    port, _s, (_b, blue_seen), _g = pair
    _get(port, "/v1/series/x", headers=[("connection", "keep-alive, accept-language"), ("accept-language", "en"),
                                        ("accept", "a")])
    got = [k.lower() for k, _ in blue_seen[-1][1]]
    assert "accept-language" not in got and "accept" in got


def test_a_head_on_the_status_route_has_no_body(pair):
    port = pair[0]
    raw = _raw(port, b"HEAD /__router/status HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
    head, _, body = raw.partition(b"\r\n\r\n")
    assert head.startswith(b"HTTP/1.1 200") and body == b""


def test_a_folded_value_a_bare_cr_and_a_bare_lf_are_refused(pair):
    """What the router sends on must hold no line the origin could read as a header of its own. A value folded
    over lines, a bare CR (a line end for Python's header parser, not for workerd) and a line that ends in LF
    alone are not lines the edge's chain sends: the request is refused whole."""
    port, _s, (_b, blue_seen), _g = pair
    for raw_header in (b"Accept: a\r\n MF-Original-URL: http://x/1", b"Accept: a\r MF-Original-URL: http://x/1",
                       b"Accept: a\n MF-Original-URL: http://x/1", b"Accept: a\r\n\tb",
                       b"Accept: a\rMF-Original-URL: http://x/1", b"Accept: a\nAccept-Language: en",
                       b"Accept: a\x0bb", b"Accept: a\x00b", b"Accept: a\x7f"):
        out = _raw(port, b"GET /v1/series/x HTTP/1.1\r\nHost: x\r\n" + raw_header + b"\r\nConnection: close\r\n\r\n")
        assert out.startswith(b"HTTP/1.1 400") and out.count(b"HTTP/1.1 ") == 1, raw_header
        assert json.loads(out.partition(b"\r\n\r\n")[2]) == {"error": "request_headers_not_allowed"}, raw_header
    assert blue_seen == [], "nothing was forwarded"


_INNER = b"GET /v1/series/smuggled HTTP/1.1\r\nHost: x\r\n\r\n"
_N = str(len(_INNER)).encode()


@pytest.mark.parametrize("header_lines", [
    b"Content-Length: 0\r\nContent-Length: " + _N + b"\r\n",       # two lengths, the harmless one first
    b"Content-Length: " + _N + b"\r\nContent-Length: 0\r\n",
    b"foo bar\r\nContent-Length: " + _N + b"\r\n",                 # a line without a colon ends the header block
    b"Content-Length : " + _N + b"\r\n",                           # a space before the colon
    b"Transfer-Encoding\t: chunked\r\n",                           # a tab before the colon
    b": x\r\nContent-Length: " + _N + b"\r\n",                     # an empty header name
    b"Content-Length: +" + _N + b"\r\n",
    b"Accept: a\r\n Content-Length: " + _N + b"\r\n",              # folded into another header's value
    b"Transfer-Encoding:\r\n",                                     # present, with no value
    # review AR-269: each of these was answered 200 and the bytes after the block went on as a second request -
    # Python's header parser ends a line at a bare CR, so CR CR was its end of the block
    b"Accept: a\r\rContent-Length: " + _N + b"\r\n",
    b"Accept: a\r\n\rContent-Length: " + _N + b"\r\n",
    b"Accept: a\r\r\nContent-Length: " + _N + b"\r\n",
    b"Accept: a\r\rTransfer-Encoding: chunked\r\n",
    b"From Content-Length: " + _N + b"\r\n",                       # a `From ` line is set aside by that parser
    b"Content-Length: 0, " + _N + b"\r\n", b"Content-Length: 00\r\n", b"Content-Length: 0x0\r\n",
    b"content-LENGTH: " + _N + b"\r\n", b"Content-Length:\t" + _N + b" \r\n",
])
def test_a_body_that_the_first_content_length_does_not_show_is_refused(pair, header_lines):
    """AR-268 N4 and AR-269 B2: these let, or could let, the bytes after the header block through as a SECOND
    request, because the check asked Python's header parser what the block held. One answer, a 400, and the
    connection closes."""
    port, _s, (_b, blue_seen), _g = pair
    raw = _raw(port, b"GET /v1/sources HTTP/1.1\r\nHost: x\r\n" + header_lines + b"\r\n" + _INNER)
    assert raw.startswith(b"HTTP/1.1 400") and raw.count(b"HTTP/1.1 ") == 1, "one answer, then the connection closes"
    assert blue_seen == [], "nothing reached the origin"


def test_an_honest_header_block_is_not_taken_for_a_body(pair):
    """The control of the tests above: a request with the header names a proxied request carries (cf-ray,
    cdn-loop, x-forwarded-for - written by hand here, NOT a captured request), a byte outside ASCII in a
    value, an empty value, a TAB inside a value and `Content-Length: 0` twice."""
    port, _s, (_b, blue_seen), _g = pair
    out = _raw(port, b"GET /v1/series/x.csv?from=2020-01-01 HTTP/1.1\r\nHost: econ-origin.example.com\r\n"
                     b"Accept: text/csv, */*;q=0.8\r\nAccept-Encoding: gzip, br\r\nAccept-Language:\r\n"
                     b"User-Agent: caf\xe9/1 (Windows NT 10.0; Win64; x64)\r\nCf-Ray: 8f1-DFW\r\n"
                     b"Cdn-Loop: cloudflare; loops=1\r\nX-Forwarded-For: 203.0.113.9,\t198.51.100.7\r\nContent-Length: 0\r\n"
                     b"Content-Length: 0\r\nX-Econ-Origin-Secret: s\r\nConnection: close\r\n\r\n")
    assert out.startswith(b"HTTP/1.1 200")
    got = {k.lower(): v for k, v in blue_seen[-1][1]}
    assert got["user-agent"] == "caf\xe9/1 (Windows NT 10.0; Win64; x64)" and got["x-econ-origin-secret"] == "s"
    assert sorted(got) == ["accept", "accept-encoding", "accept-language", "host", "user-agent", "x-econ-origin-secret"]


def test_a_request_line_without_a_version_is_refused_and_nothing_is_forwarded(pair):
    """AR-269 B1: for `GET /path` with no HTTP version Python 3.13+ reads NO header block (headers = {}), and
    the body rule raised on it: no answer, a traceback in the log. HTTP/0.9 has no status line, so the
    refusal is the JSON body alone, and the connection closes."""
    port, _s, (_b, blue_seen), _g = pair
    out = _raw(port, b"GET /v1/sources\r\n\r\n")
    assert json.loads(out) == {"error": "request_headers_not_allowed"} and blue_seen == []
    out = _raw(port, b"GET /v1/sources\r\nHost: x\r\n\r\nGET /v1/series/smuggled HTTP/1.1\r\nHost: x\r\n\r\n")
    assert b"200" not in out and blue_seen == [], "the bytes after it are not read as a request"
    assert _get(port, "/v1/sources")[0] == 200, "the router still answers"


@pytest.mark.parametrize("target", [b"/v1/series/a\x01b", b"/v1/series/a\x7f", b"/v1/series/a\x0bb.csv",
                                    b"/v1/sources?x=\x1fy", b"/v1/series/\x00"])
def test_a_control_character_in_a_target_is_refused_and_is_never_a_502(pair, target):
    """AR-269 N1: http.client cannot send such a target on, and the answer was 502 origin_instance_unreachable.
    Now it is the router's 404 - or http.server's own 400 when the byte is one it takes for a blank INSIDE the
    target (0x0b, 0x1f: the request line then has four words)."""
    port, _s, (_b, blue_seen), _g = pair
    out = _raw(port, b"GET " + target + b" HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    assert out[:12] in (b"HTTP/1.1 404", b"HTTP/1.1 400") and blue_seen == []
    assert out.startswith(b"HTTP/1.1 404") or target in (b"/v1/series/a\x0bb.csv", b"/v1/sources?x=\x1fy")


def test_what_goes_on_is_the_target_the_router_judged_never_the_bytes_beside_it(pair):
    """http.server splits the request line as str.split() does, so 0x1f, 0x0b or 0x85 next to the target's
    blanks are blanks too: `GET /v1/sources?x=<0x1f> HTTP/1.1` is the target `/v1/sources?x=`. The router
    judges THAT string and sends THAT string on; the byte beside it reaches no instance."""
    port, _s, (_b, blue_seen), _g = pair
    for line, seen in ((b"GET /v1/sources?x=\x1f HTTP/1.1", "/v1/sources?x="),
                       (b"GET \x0b/v1/series/a.csv\x1c HTTP/1.1", "/v1/series/a.csv")):
        out = _raw(port, line + b"\r\nHost: x\r\nConnection: close\r\n\r\n")
        assert out.startswith(b"HTTP/1.1 200")
        assert blue_seen[-1][0] == seen


def test_a_value_with_the_byte_0x85_goes_on_as_one_value(pair):
    """str.splitlines() ends a line at 0x85 (NEL); a header reader built on it would see a second header here.
    The router's reader and the stand-in (Python's http.server) both read ONE Accept value, and the sweep in
    test_whatever_read_fields_accepts_pythons_parser_reads_the_same_way holds such values too. 0x85 is also a
    byte of ordinary UTF-8 text (the second byte of an A with a ring), so it is not refused."""
    port, _s, (_b, blue_seen), _g = pair
    out = _raw(port, b"GET /v1/series/x HTTP/1.1\r\nHost: x\r\nAccept: a\x85\x85Content-Length: 45\r\n"
                     b"Connection: close\r\n\r\n")
    assert out.startswith(b"HTTP/1.1 200") and out.count(b"HTTP/1.1 ") == 1
    assert [(k.lower(), v) for k, v in blue_seen[-1][1] if k.lower() != "host"] == [("accept", "a\x85\x85Content-Length: 45")]


def test_the_body_check_comes_before_the_status_route_and_a_refusal_has_no_body_on_head(pair):
    port = pair[0]
    out = _raw(port, b"GET /__router/status HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\n\r\nhello")
    assert out.startswith(b"HTTP/1.1 400") and b"inflight" not in out
    out = _raw(port, b"HEAD /nowhere HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    head, _, body = out.partition(b"\r\n\r\n")
    assert head.startswith(b"HTTP/1.1 404") and body == b"", "read from the socket: http.client would hide a body"


def test_the_status_route_needs_exactly_one_host(pair):
    port = pair[0]
    out = _raw(port, b"GET /__router/status HTTP/1.1\r\nHost: 127.0.0.1\r\nHost: econ-origin.example.com\r\n"
                     b"Connection: close\r\n\r\n")
    assert out.startswith(b"HTTP/1.1 404")
    out = _raw(port, b"GET /__router/status HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
    assert out.startswith(b"HTTP/1.1 200")


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "PATCH"])
def test_a_method_that_writes_is_a_405_and_is_never_forwarded(pair, method):
    port, _s, (_b, blue_seen), _g = pair
    assert _get(port, "/v1/series/x", method=method)[0] == 405 and blue_seen == []


# ---- read_fields: the router's own reading of a header block (no sockets) -------------------------------

def _lines(block: bytes) -> list:
    """The lines as http.server reads them off the wire: split after every LF, up to the first empty line."""
    out = []
    for line in re.findall(rb"[^\n]*\n|[^\n]+$", block):
        out.append(line)
        if line in (b"\r\n", b"\n"):
            break
    return out


def test_read_fields_takes_clean_lines_and_keeps_order_case_and_duplicates():
    f = router.read_fields(_lines(b"Host: a\r\nACCEPT:text/csv\r\nX-Y:  v  w \t\r\nAccept: b\r\nEmpty:\r\n"
                                  b"User-Agent: caf\xe9 \x85 x\r\n\r\n"))
    assert f.items() == [("Host", "a"), ("ACCEPT", "text/csv"), ("X-Y", "v  w"), ("Accept", "b"), ("Empty", ""),
                         ("User-Agent", "caf\xe9 \x85 x")]
    assert f.get_all("accept") == ["text/csv", "b"] and f.get_all("nothing") is None and f.get_all("nothing", []) == []
    assert router.read_fields([b"\r\n"]).items() == []


@pytest.mark.parametrize("block", [
    b"", b"Host: a\r\n", b"Host: a\r\n\n", b"Host: a\n\r\n", b"Host a\r\n\r\n", b"Host : a\r\n\r\n",
    b"Host\t: a\r\n\r\n", b": a\r\n\r\n", b" Host: a\r\n\r\n", b"Host: a\r\n b\r\n\r\n", b"From x\r\n\r\n",
    b"Host: a\rb\r\n\r\n", b"Host: a\r\r\n\r\n", b"H\xf6st: a\r\n\r\n", b"Host: a\x00\r\n\r\n", b"Host: a\x1c\r\n\r\n",
    b"Ho(st: a\r\n\r\n", b"Host: a\r\n\r\r\n",
])
def test_read_fields_refuses_a_block_that_is_not_made_of_clean_lines(block):
    assert router.read_fields(_lines(block)) is None


def test_whatever_read_fields_accepts_pythons_parser_reads_the_same_way():
    """A seeded sweep over header blocks built from the bytes the tricks are made of. For every block the
    router's reader ACCEPTS, Python's own header parser (which http.server still uses for its keep-alive
    decision) must see the same names in the same order with the same values, no defect and nothing left
    over - so no accepted block is one the two readers split differently. And the sweep must accept some
    blocks and refuse some, or it shows nothing."""
    import io
    import random
    rnd = random.Random(20261010)
    names = [b"Host", b"Accept", b"Content-Length", b"Transfer-Encoding", b"X-A", b"From", b"", b"a b", b"Cf-Ray"]
    seps = [b": ", b":", b" : ", b":\t", b" ", b"\t: "]
    values = [b"a", b"0", b"45", b"chunked", b"", b"a b", b"caf\xe9", b"x\x85y", b"a\tb", b"a\rb", b"a\x0bb",
              b"a\x1cb", b"a\x00b", b" lead", b"x\xe2\x80\xa8y", b"a\x7fb"]
    ends = [b"\r\n"] * 12 + [b"\n", b"\r", b"\r\r", b"\r\n ", b"\r\n\t", b"\x85", b""]
    accepted = refused = 0
    for _ in range(20000):
        block = b"".join(rnd.choice(names) + rnd.choice(seps) + rnd.choice(values) + rnd.choice(ends)
                         for _ in range(rnd.randint(0, 5))) + b"\r\n"
        mine = router.read_fields(_lines(block))
        if mine is None:
            refused += 1
            continue
        accepted += 1
        fp = io.BytesIO(block + b"REST")
        theirs = http.client.parse_headers(fp)
        assert fp.read() == b"REST", block
        assert not theirs.defects and not theirs.get_payload() and not theirs.get_unixfrom(), block
        assert [(k, v.strip(" \t")) for k, v in theirs.items()] == mine.items(), block
    assert accepted > 500 and refused > 5000, (accepted, refused)


def test_a_refused_path_keeps_the_connection(pair):
    """The tunnel reuses its connections: a 404 for one caller must not close the connection under the next."""
    port, _s, (_b, blue_seen), _g = pair
    out = _raw(port, b"GET /cdn-cgi/mf/scheduled HTTP/1.1\r\nHost: x\r\n\r\n"
                     b"GET /v1/sources HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    assert out.startswith(b"HTTP/1.1 404") and out.count(b"HTTP/1.1 200") == 1
    assert [p for p, _h2 in blue_seen] == ["/v1/sources"]


def test_a_request_body_on_an_unknown_path_is_still_a_400_that_closes(pair):
    """The body check comes before the path check: a 404 that left the body unread would have it parsed as
    the next request on the connection."""
    port, _s, (_b, blue_seen), _g = pair
    inner = b"GET /v1/series/smuggled HTTP/1.1\r\nhost: x\r\n\r\n"
    out = _raw(port, b"GET /nowhere HTTP/1.1\r\nhost: x\r\ncontent-length: " + str(len(inner)).encode()
               + b"\r\n\r\n" + inner)
    assert out.startswith(b"HTTP/1.1 400") and out.count(b"HTTP/1.1 ") == 1 and blue_seen == []


def test_connection_listed_headers_are_dropped_both_ways(pair):
    port, _s, (_b, blue_seen), _g = pair
    status, headers, body = _get(port, "/v1/series/hop", headers=[("connection", "keep-alive, x-priv"), ("x-priv", "1")])
    assert "x-priv" not in [k.lower() for k, _ in blue_seen[-1][1]]
    assert status == 200 and body == b"ok" and not _h(headers, "x-hop")


def test_one_date_and_server_pair(pair):
    headers = _get(pair[0], "/v1/series/x")[1]
    assert len(_h(headers, "date")) == 1 and len(_h(headers, "server")) == 1


def test_a_large_length_less_body_is_streamed_whole(pair):
    status, headers, body = _get(pair[0], "/v1/series/big.csv")
    assert status == 200 and body == BIG and _h(headers, "x-econ-count") == ["1"]


def test_bytes_are_passed_on_as_they_arrive(pair):
    """AR-152: resp.read(1 MiB) held a slow answer back for 30 s. The first bytes must arrive at once."""
    s = socket.create_connection(("127.0.0.1", pair[0]), timeout=10)
    s.sendall(b"GET /v1/series/slow HTTP/1.1\r\nHost: x\r\n\r\n")
    t0 = time.monotonic()
    got = b""
    while b"0123456789" not in got:
        got += s.recv(4096)
    first = time.monotonic() - t0
    s.close()
    assert first < 1.0, f"the first body bytes took {first:.2f} s (the origin sends 10 bytes every 0.2 s)"


def test_a_304_gets_no_body_framing(pair):
    raw = _raw(pair[0], b"GET /v1/series/notmodified HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    head = raw.split(b"\r\n\r\n", 1)[0].lower()
    assert raw.startswith(b"HTTP/1.1 304") and b"transfer-encoding" not in head


def test_an_http_1_0_client_gets_no_chunked_body(pair):
    raw = _raw(pair[0], b"GET /v1/series/big.csv HTTP/1.0\r\n\r\n", wait=3.0)
    head, body = raw.split(b"\r\n\r\n", 1)
    assert b"transfer-encoding" not in head.lower() and body == BIG


@pytest.mark.parametrize("request_bytes", [
    b"GET /v1/series/x HTTP/1.1\r\nHost: x\r\nContent-Length: 38\r\n\r\nGET /v1/series/smuggled HTTP/1.1\r\nHost: x\r\n\r\n",
    b"GET /v1/series/x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
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
                c.request("GET", "/v1/series/vx")
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
    # FLIP UNTIL EVERY CLIENT HAS MADE ALL ITS REQUESTS (and at least 100 times): a fixed 100 flips 2 ms apart
    # is a ~0.2 s window, and on CI's faster runner only 64 requests fell inside it (the test measured nothing)
    flips, deadline = 0, time.monotonic() + 120
    while (flips < 100 or any(th.is_alive() for th in threads)) and time.monotonic() < deadline:
        router.flip(str(state), "green" if flips % 2 == 0 else "blue")
        flips += 1
        time.sleep(0.002)                      # spread the flips over the requests instead of racing past them
    stop.set()
    for th in threads:
        th.join()
    assert bad == [], f"{len(bad)} failed request(s) during {flips} flips: {sorted(set(map(str, bad)))}"
    assert sum(served) == 8 * cap, f"only {sum(served)} of {8 * cap} requests ran during the flips"
    assert flips >= 100


def test_the_status_route_reports_in_flight_requests(pair):
    port = pair[0]
    t = threading.Thread(target=lambda: _get(port, "/v1/series/slow"))
    t.start()
    time.sleep(0.4)
    during = json.loads(_get(port, router.STATUS_PATH)[2])
    t.join()
    after = json.loads(_get(port, router.STATUS_PATH)[2])
    assert during["active"] == "blue" and during["inflight"] == {"blue": 1}
    assert after["inflight"] == {}, "drained"


def test_a_flip_takes_effect_on_the_next_request_and_is_atomic(pair):
    port, state = pair[0], pair[1]
    assert json.loads(_get(port, "/v1/series/a")[2])["instance"] == "blue"
    router.flip(str(state), "green")
    assert json.loads(_get(port, "/v1/series/b")[2])["instance"] == "green"
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
    status, _h2, body = _get(port, "/v1/series/x")
    assert status == 503 and json.loads(body)["error"] == "router_state_unreadable"
    status, _h2, body = _get(port, "/v1/series/x", method="HEAD")
    assert status == 503 and body == b""


def test_only_read_methods_and_head(pair):
    port = pair[0]
    assert _get(port, "/v1/series/x", method="POST")[0] == 405
    status, headers, body = _get(port, "/v1/series/x", method="HEAD")
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


# ---- the origin connection pool (R1218 finding 5) -------------------------------------------------------------
def _counting_instance():
    """An origin that records every TCP connection it accepts, and can drop idle keep-alive connections."""
    conns = []

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def setup(self):
            super().setup()
            conns.append(self.client_address)

        def do_GET(self):
            if self.path == "/v1/series/drop-after":
                self.send_response(200)
                self.send_header("content-length", "2")
                self.end_headers()
                self.wfile.write(b"ok")
                self.wfile.flush()
                self.close_connection = True                   # the origin drops the kept-alive connection
                return
            if self.path == "/v1/series/slow":
                self.send_response(200)
                self.send_header("transfer-encoding", "chunked")
                self.end_headers()
                for _ in range(8):
                    self.wfile.write(b"a" + CRLF + b"0123456789" + CRLF)
                    self.wfile.flush()
                    time.sleep(0.2)
                self.wfile.write(b"0" + CRLF + CRLF)
                return
            if self.path == "/v1/series/close":
                self.send_response(200)
                self.send_header("content-length", "2")
                self.send_header("connection", "close")
                self.end_headers()
                self.wfile.write(b"ok")
                return
            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("content-length", "123")
            self.end_headers()

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, conns


CRLF = bytes([13, 10])


@pytest.fixture
def pooled(tmp_path):
    origin, conns = _counting_instance()
    state = tmp_path / "router.json"
    state.write_text(json.dumps({"active": "blue", "targets": {
        "blue": f"http://127.0.0.1:{origin.server_address[1]}", "green": f"http://127.0.0.1:{origin.server_address[1]}"}}))
    srv = router.serve(str(state), 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_address[1], conns, origin, srv
    srv.shutdown()
    origin.shutdown()


def test_sequential_requests_reuse_one_origin_connection(pooled):
    port, conns, _o, _srv = pooled
    for _ in range(20):
        assert _get(port, "/v1/series/vx")[0] == 200            # a NEW client connection each time
    assert len(conns) == 1, f"{len(conns)} origin connections for 20 requests: the pool is not reused"


def test_head_responses_go_back_to_the_pool(pooled):
    port, conns, _o, _srv = pooled
    for _ in range(5):
        assert _get(port, "/v1/series/vx", method="HEAD")[0] == 200
    assert len(conns) == 1


def test_a_connection_the_origin_asked_to_close_is_not_reused(pooled):
    port, conns, origin, srv = pooled
    for _ in range(3):
        assert _get(port, "/v1/series/close")[0] == 200
    assert len(conns) == 3
    assert srv.pool.idle_count("127.0.0.1", origin.server_address[1]) == 0, "a closed connection is not kept"


def test_the_pool_never_hands_out_an_aged_connection_and_ages_out_every_target():
    """Unit level: through the router, http.client silently reopens a closed connection, so a mutant that kept
    aged entries still passed the request test."""
    p = router.OriginPool()
    a, b = p.fresh("127.0.0.1", 1), p.fresh("127.0.0.1", 2)
    p.put("127.0.0.1", 1, a)
    p.put("127.0.0.1", 2, b)
    p.IDLE_S = 0.0
    time.sleep(0.1)                   # well past time.monotonic's ~15.6 ms step on Windows (0.01 flaked, R1235)
    conn, reused = p.get("127.0.0.1", 1)
    assert conn is not a and reused is False
    assert p.idle_count("127.0.0.1", 2) == 0, "the other target's aged connection went too (R1229: after a flip)"


def test_the_idle_limit_is_below_the_origins_own_idle_close():
    """R1229: workerd (pinned 1.20250718.0) closes an idle keep-alive connection after ~5 s. Above that, every
    lull hands out dead connections (answered only because the retry takes a fresh one)."""
    assert router.OriginPool.IDLE_S < 5.0


def test_an_idle_connection_past_its_age_is_not_reused(pooled):
    port, conns, origin, srv = pooled
    assert _get(port, "/v1/series/vx")[0] == 200
    srv.pool.IDLE_S = 0.0                                     # every idle connection is now too old
    time.sleep(0.05)
    assert _get(port, "/v1/series/vx")[0] == 200
    assert len(conns) == 2, "the aged connection was closed, a fresh one made"


def test_a_pooled_connection_the_origin_dropped_is_retried_once_fresh(pooled):
    port, conns, _o, _srv = pooled
    assert _get(port, "/v1/series/drop-after")[0] == 200          # the origin closes after answering, without saying so
    time.sleep(0.2)
    assert _get(port, "/v1/series/vx")[0] == 200, "the dead pooled connection was retried, not answered 502"
    assert len(conns) == 2


def test_a_client_that_leaves_mid_body_does_not_return_the_connection(pooled):
    """The origin must see a closed connection to stop work (the pre-pool rule): an unfinished response is never
    pooled."""
    port, conns, origin, srv = pooled
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(b"GET /v1/series/slow HTTP/1.1" + CRLF + b"Host: x" + CRLF + CRLF)
    s.recv(64)
    s.close()
    time.sleep(2.5)
    assert srv.pool.idle_count("127.0.0.1", origin.server_address[1]) == 0
    assert _get(port, "/v1/series/vx")[0] == 200
    assert len(conns) == 2, "the abandoned connection was not reused"


# ---- R1229: an origin that closes idle connections, a timeout, a 1xx -----------------------------------------
def _serve_origin(handler_cls):
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _router_for(tmp_path, origin_port):
    state = tmp_path / "router.json"
    state.write_text(json.dumps({"active": "blue", "targets": {
        "blue": f"http://127.0.0.1:{origin_port}", "green": f"http://127.0.0.1:{origin_port}"}}))
    srv = router.serve(str(state), 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def test_after_the_origin_closes_idle_connections_every_request_still_succeeds(tmp_path):
    """R1229: workerd closes an idle keep-alive connection after ~5 s. This origin does it after 1 s. A burst,
    a lull longer than that (and shorter than the pool's IDLE_S), and a burst again: no 502 - the retry takes a
    NEW connection, not another dead pooled one."""
    ran = []

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        timeout = 1                                            # closes an idle kept-alive connection after 1 s

        def log_message(self, *a):
            pass

        def do_GET(self):
            ran.append(self.path)
            time.sleep(0.2)                                    # so the burst needs several connections
            body = b"ok"
            self.send_response(200)
            self.send_header("content-length", "2")
            self.end_headers()
            self.wfile.write(body)
    origin = _serve_origin(H)
    srv = _router_for(tmp_path, origin.server_address[1])
    try:
        for burst in range(2):
            out = []
            threads = [threading.Thread(target=lambda: out.append(_get(srv.server_address[1], "/v1/series/vx")[0]))
                       for _ in range(8)]
            for th in threads:
                th.start()
            for th in threads:
                th.join()
            assert out == [200] * 8, f"burst {burst}: {out}"
            if burst == 0:
                assert srv.pool.idle_count("127.0.0.1", origin.server_address[1]) >= 2, "precondition: pooled"
                time.sleep(1.8)                                # past the origin's close, inside IDLE_S (4 s)
    finally:
        srv.shutdown()
        origin.shutdown()


def test_a_timeout_is_not_retried(tmp_path):
    """R1229: a timeout is an OSError, and was retried - the origin ran the slow request twice."""
    ran = []

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_GET(self):
            ran.append(self.path)
            if self.path == "/v1/series/slow-answer":
                time.sleep(2.0)
            self.send_response(200)
            self.send_header("content-length", "2")
            self.end_headers()
            self.wfile.write(b"ok")
    origin = _serve_origin(H)
    srv = _router_for(tmp_path, origin.server_address[1])
    try:
        srv.pool.TIMEOUT_S = 1
        assert _get(srv.server_address[1], "/v1/series/vx")[0] == 200                # pools a connection (1 s timeout)
        assert _get(srv.server_address[1], "/v1/series/slow-answer")[0] == 502
        time.sleep(2.5)
        assert ran.count("/v1/series/slow-answer") == 1, f"the origin ran it {ran.count('/v1/series/slow-answer')} times"
    finally:
        srv.shutdown()
        origin.shutdown()


def test_a_1xx_answer_never_leaves_its_connection_in_the_pool(tmp_path):
    """R1229: http.client skips only 100; a 103 was forwarded as the answer and the connection pooled with the
    real response unread - the NEXT client got it."""
    import socketserver

    class Raw(socketserver.StreamRequestHandler):
        def handle(self):
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                path = line.split()[1].decode()
                while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                    pass
                body = ("SECRET-FOR " + path).encode()
                self.wfile.write(b"HTTP/1.1 103 Early Hints\r\nlink: </x>\r\n\r\n")
                self.wfile.write(b"HTTP/1.1 200 OK\r\ncontent-length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
                self.wfile.flush()

    origin = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Raw)
    threading.Thread(target=origin.serve_forever, daemon=True).start()
    srv = _router_for(tmp_path, origin.server_address[1])
    try:
        _get(srv.server_address[1], "/v1/series/a")
        assert srv.pool.idle_count("127.0.0.1", origin.server_address[1]) == 0
        body = _get(srv.server_address[1], "/v1/series/b")[2]
        assert b"SECRET-FOR /v1/series/a" not in body, "client B got client A's response"
    finally:
        srv.shutdown()
        origin.shutdown()
        origin.server_close()


def _scripted_origin(answers):
    """A raw origin that gives the Nth request it reads answers[N] (bytes to send, or None = close the connection
    without a byte), counting connections and requests."""
    import socketserver
    seen = {"connections": 0, "requests": 0}

    class Raw(socketserver.StreamRequestHandler):
        def handle(self):
            seen["connections"] += 1
            while True:
                if not self.rfile.readline():
                    return
                while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                    pass
                n = seen["requests"]
                seen["requests"] += 1
                answer = answers[min(n, len(answers) - 1)]
                if answer is None:
                    return                                  # closed before any byte of an answer
                self.wfile.write(answer)
                self.wfile.flush()

    origin = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Raw)
    threading.Thread(target=origin.serve_forever, daemon=True).start()
    return origin, seen


OK_ANSWER = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok"


def test_a_failure_on_a_fresh_connection_is_not_retried(tmp_path):
    """R1235 mutant R6: only a REUSED connection that the origin had closed is retried - a fresh connection that
    fails is a real failure, answered 502 after ONE attempt (not sent twice to an origin that is failing)."""
    origin, seen = _scripted_origin([None])
    srv = _router_for(tmp_path, origin.server_address[1])
    try:
        assert _get(srv.server_address[1], "/v1/series/a")[0] == 502
        assert seen == {"connections": 1, "requests": 1}
    finally:
        srv.shutdown()
        origin.shutdown()
        origin.server_close()


def test_a_malformed_answer_on_a_reused_connection_is_not_retried(tmp_path):
    """R1235 mutant R7: a retry is for a connection closed under us, not for any error - an origin that answered
    garbage has seen the request, and must not get it twice."""
    origin, seen = _scripted_origin([OK_ANSWER, b"NOT HTTP AT ALL\r\n\r\n"])
    srv = _router_for(tmp_path, origin.server_address[1])
    try:
        assert _get(srv.server_address[1], "/v1/series/a")[0] == 200
        assert srv.pool.idle_count("127.0.0.1", origin.server_address[1]) == 1, "precondition: pooled"
        assert _get(srv.server_address[1], "/v1/series/b")[0] == 502
        assert seen["requests"] == 2, f"the malformed answer's request was sent again: {seen}"
    finally:
        srv.shutdown()
        origin.shutdown()
        origin.server_close()
