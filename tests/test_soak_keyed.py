"""tools/selfhost/soak_keyed.py against stand-ins on loopback: an ORIGIN (router + instance with an instance.log
in wrangler's line shape), a soak EDGE that asks for the key and forwards with the origin secret, and PRODUCTION,
which serves the same stored objects by key.

THE SOURCE OF THE STAND-IN'S SHAPES is api/worker/test/downloadShapes.test.ts, which measures them on the worker's
own route handler. `serve_object` below is that table and nothing else:
  string       small object: content-length; `#` lines first, or the contract header with raw=1; ETag only bare+whole.
  passthrough  large stored-gzip object, no window: the stored bytes, content-encoding gzip, exact length,
               `x-econdl-citation-omitted`.
  pump         large PLAIN object WITH OR WITHOUT a window (and a large gzip object with a window): no
               content-length, `x-econdl-bytes-at-rest`, last line `# econdl-complete rows=N`; `#` lines first
               unless raw=1; ETag only bare+whole.
The first version of this file answered a large plain object as a passthrough, as the script then expected: 48
green tests and 34 caught hand-breaks, all against the wrong worker (review AR-303, ledger R1545).

Nothing goes to the network. The stand-ins are kinder than the real system in known ways: the edge copies the
origin's answer headers one to one (a real hop can drop content-length), the edge reads the origin's whole answer
before it sends a byte (so the origin stand-in never sees an early client close), the log line is written before
the answer, production reads the same bytes as the origin unless a switch says otherwise, and no download log
exists (the read-back is not this script's).

What is pinned: BROKEN lists one switch per behaviour and the row that must then not PASS. A behaviour that is
not in that list is not claimed.
"""
import getpass
import gzip
import http.client
import http.server
import json
import os
import random
import socket
import sys
import threading
import urllib.parse
import warnings

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
for cand in (os.path.join(os.path.dirname(HERE), "tools", "selfhost"), HERE):
    if os.path.isfile(os.path.join(cand, "soak_keyed.py")):
        sys.path.insert(0, cand)
        break

import soak_keyless as K  # noqa: E402
import soak_keyed as D  # noqa: E402

SECRET = "cd" * 32
KEY = "soakkey-" + "k9" * 14
GATED = "gatedsrc:X.1"
SMALL, LGZ, LPL = "s001:SMALL.1", "s001:LARGE.GZ", "s001:LARGE.PLAIN"
SOURCES = [{"source": f"s{i:03d}"} for i in range(120)]
CITATION = b"# Source: a test series\n# Licence: a test licence\n"
ALL_ROWS = {"from": ["1000-01-01"], "to": ["2999-12-31"]}
_rng = random.Random(7)


def _csv(sid, rows):
    lines = ["series_id,obs_date,value"]
    for i in range(rows):
        lines.append(f"{sid},{1990 + i // 12:04d}-{i % 12 + 1:02d}-01,{_rng.getrandbits(96):x}")
    return ("\n".join(lines) + "\n").encode()


TEXT = {SMALL: _csv(SMALL, 240), LGZ: _csv(LGZ, 30000), LPL: _csv(LPL, 9000)}
STORED = {SMALL: TEXT[SMALL], LGZ: gzip.compress(TEXT[LGZ], mtime=0), LPL: TEXT[LPL]}
assert len(STORED[LGZ]) >= D.STREAM_MIN_BYTES and len(STORED[LPL]) >= D.STREAM_MIN_BYTES > len(STORED[SMALL])


def _window(text, lo, hi):
    out = [ln for ln in text.decode().splitlines()[1:] if lo <= ln.split(",")[1] <= hi]
    return out, len(out)


def serve_object(sid, q, b):
    """(status, headers, body, chunked) as the worker answers a .csv request (downloadShapes.test.ts);
    `b` = the switches that are on."""
    if sid not in STORED:
        return 404, [], json.dumps({"error": "not_found"}).encode(), False
    raw = q.get("raw") == ["1"]
    lo, hi = (q.get("from") or [None])[0], (q.get("to") or [None])[0]
    windowed = lo is not None or hi is not None
    etag = [("ETag", '"etag-of-' + sid + '"')]
    cite = b"" if raw else CITATION
    if sid == SMALL:                                                        # STRING
        if windowed:
            lines, _n = _window(TEXT[sid], lo or "0000", hi or "9999")
            body = cite + ("series_id,obs_date,value\n" + "\n".join(lines) + "\n").encode()
            return 200, (etag if "window_has_etag" in b else []), body, False
        body = (b"" if "citation_missing" in b else cite) + TEXT[sid]
        heads = etag if raw and "no_etag" not in b else []
        if "small_is_large" in b:
            heads = heads + [("x-econdl-citation-omitted", "large-object")]
        return 200, heads, body, False
    if sid == LGZ and not windowed:                                         # PASSTHROUGH
        heads = ([] if "no_mark" in b else [("x-econdl-citation-omitted", "large-object")]) + (etag if raw else [])
        body = STORED[sid] if "gzip_bad_magic" not in b else b"xx" + STORED[sid][2:]
        if "gzip_not_marked" not in b:
            heads.append(("Content-Encoding", "gzip"))
        return 200, heads, body, "gzip_no_length" in b
    if sid == LPL and "plain_is_passthrough" in b and not windowed:         # the shape the first version expected
        return 200, [("x-econdl-citation-omitted", "large-object")], STORED[sid], False
    lines, n = _window(TEXT[sid], lo or "0000", hi or "9999")               # PUMP
    tail = b"" if "pump_no_complete_line" in b else f"# econdl-complete rows={0 if 'pump_zero_rows' in b else n}\n".encode()
    body = cite + ("series_id,obs_date,value\n" + "\n".join(lines) + "\n").encode() + tail
    heads = [] if "pump_no_at_rest" in b else [("x-econdl-bytes-at-rest", str(len(STORED[sid])))]
    if "pump_marked_omitted" in b:
        heads.append(("x-econdl-citation-omitted", "large-object"))
    if (raw and not windowed) or ("pump_window_has_etag" in b and windowed):
        heads = heads + etag
    return 200, heads, body, "pump_has_length" not in b


class Quiet(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send(self, status, body, headers=(), chunked=False, short=False):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        for k, v in headers:
            self.send_header(k, v)
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
        else:
            self.send_header("Content-Length", str(len(raw) + (50 if short else 0)))
        if short:
            self.close_connection = True
        self.end_headers()
        try:
            for i in range(0, len(raw), 32768):
                part = raw[i:i + 32768]
                self.wfile.write(b"%x\r\n" % len(part) + part + b"\r\n" if chunked else part)
            if chunked:
                self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except OSError:
            self.server.aborted.append(self.path)
            self.close_connection = True


class Origin(Quiet):
    def do_GET(self):                                                          # noqa: N802
        srv = self.server
        u = urllib.parse.urlsplit(self.path)
        p = u.path
        if p == "/__router/status":
            return self.send(200, {"active": "green", "target": "x"})
        chunked = False
        heads = []
        if self.headers.get(K.SECRET_HEADER) != SECRET:
            status, body = 403, json.dumps({"error": "forbidden"}).encode()
        elif p == "/v1/sources":
            status, body = 200, json.dumps({"sources": SOURCES, "total": len(SOURCES)}).encode()
        elif p.startswith("/v1/series/") and p.endswith(".metadata.json"):
            sid = urllib.parse.unquote(p[len("/v1/series/"):-len(".metadata.json")])
            status, body = (200, json.dumps({"series_id": sid}).encode()) if sid in STORED else (404, json.dumps({"error": "not_found"}).encode())
        elif p.startswith("/v1/series/") and p.endswith(".csv"):
            sid = urllib.parse.unquote(p[len("/v1/series/"):-len(".csv")])
            status, heads, body, chunked = serve_object(sid, urllib.parse.parse_qs(u.query), srv.broken)
        else:
            status, body = 404, json.dumps({"error": "not_found"}).encode()
        with srv.lock:
            with open(srv.log_path, "a", encoding="utf-8") as fh:
                for _ in range(2 if p.endswith(".csv") and "two_log_lines" in srv.broken else 1):
                    fh.write(f"[wrangler:inf] GET {p} {status} OK (1ms)\n")
        if "slow_first_byte" in srv.broken and p.endswith(".csv"):
            threading.Event().wait(0.4)
        self.send(status, body, headers=heads + [(K.MARK_HEADER, "1")], chunked=chunked,
                  short="short_body" in srv.broken and p.endswith(".csv") and not chunked)


class Edge(Quiet):
    """The soak worker: edge-status is its own; the gate, then the key for a .csv, then the forward with the secret."""

    def do_GET(self):                                                          # noqa: N802
        srv = self.server
        u = urllib.parse.urlsplit(self.path)
        p = u.path
        srv.keys_seen.append(self.headers.get("X-API-Key"))
        if p == "/v1/edge-status":
            return self.send(200, {"commit": "c0ffee00" * 5, "soak": True, "forward": True, "edge_state": "users", "origin_configured": True})
        if p.startswith("/v1/series/"):
            sid = urllib.parse.unquote(p[len("/v1/series/"):].rsplit(".", 2 if p.endswith(".metadata.json") else 1)[0])
            if sid == GATED:
                return self.send(451, {"error": "not_redistributable"})
            if p.endswith(".csv") and self.headers.get("X-API-Key") != KEY:
                return self.send(401, {"error": "invalid_key" if self.headers.get("X-API-Key") else "auth_required"})
            if p.endswith(".csv") and "edge_answers_itself" in srv.broken:      # a copy kept at the edge: the origin is not asked
                status, heads, body, chunked = serve_object(sid, urllib.parse.parse_qs(u.query), set())
                return self.send(status, body, headers=heads, chunked=chunked)
        c = http.client.HTTPConnection("127.0.0.1", srv.origin_port, timeout=30)
        c.request("GET", self.path, headers={K.SECRET_HEADER: SECRET})
        r = c.getresponse()
        raw = r.read()
        heads = [(k, v) for k, v in r.getheaders()
                 if k.lower() in ("etag", "content-encoding", "x-econdl-citation-omitted", "x-econdl-bytes-at-rest")]
        chunked = r.getheader("Transfer-Encoding") == "chunked"
        c.close()
        self.send(r.status, raw, headers=heads, chunked=chunked)


class Prod(Quiet):
    """Production: the same stored objects, by key, with no origin behind it."""

    def do_GET(self):                                                          # noqa: N802
        srv = self.server
        u = urllib.parse.urlsplit(self.path)
        srv.keys_seen.append(self.headers.get("X-API-Key"))
        if self.headers.get("X-API-Key") != KEY:
            return self.send(401, {"error": "auth_required"})
        sid = urllib.parse.unquote(u.path[len("/v1/series/"):-len(".csv")])
        status, heads, body, chunked = serve_object(sid, urllib.parse.parse_qs(u.query), set())
        if "prod_other_etag" in srv.broken:
            heads = [(k, '"another"' if k == "ETag" else v) for k, v in heads]
        if "prod_differs" in srv.broken:
            body = body[:-2] + b"9\n"
        self.send(status, body, headers=heads, chunked=chunked)


def _serve(handler, **attrs):
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    srv.daemon_threads = True
    srv.aborted = []
    for k, v in attrs.items():
        setattr(srv, k, v)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


# ---------------------------------------------------------------- no test may reach the standing origin
ORIGIN_PORTS = {8787, 8798, 8801, 8802}              # the router, the blob sidecar, the two instances
_REAL_CONNECT = socket.socket.connect


@pytest.fixture(autouse=True)
def never_the_standing_origin(monkeypatch):
    """On the workstation these four loopback ports are the STANDING ORIGIN's. The tool's default router is one
    of them. Two tests of main() once used that default (ledger R1544). Every connect of every test in this
    file passes here first, whatever code makes it."""
    def connect(self, address):
        if isinstance(address, tuple) and len(address) >= 2 and address[1] in ORIGIN_PORTS:
            raise AssertionError(f"this test would have connected to the standing origin's port {address[1]}")
        return _REAL_CONNECT(self, address)
    monkeypatch.setattr(socket.socket, "connect", connect)


def test_the_guard_against_the_standing_origin_can_fail(tmp_path, monkeypatch):
    monkeypatch.setattr(D, "ALLOW_PLAIN_HTTP", True)
    run = D.KeyedRun("http://127.0.0.1:9", "http://127.0.0.1:9", str(tmp_path))        # no router given: the default
    assert run.router == K.ROUTER and K.ROUTER[1] in ORIGIN_PORTS
    with pytest.raises(AssertionError, match="standing origin's port 8787"):
        run.loop("/__router/status")
    for port in sorted(ORIGIN_PORTS):
        with pytest.raises(AssertionError, match=f"standing origin's port {port}"):
            socket.create_connection(("127.0.0.1", port), timeout=1)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    monkeypatch.setattr(K, "SETTLE_SCALE", 0.02)
    monkeypatch.setattr(D, "ALLOW_PLAIN_HTTP", True)
    root = tmp_path / "origin"
    gen = root / "work" / "gen-green"
    gen.mkdir(parents=True)
    (root / "code" / "api" / "worker").mkdir(parents=True)
    (root / "code" / "api" / "worker" / ".dev.vars").write_text(f"ORIGIN_SECRET={SECRET}\n", encoding="ascii")
    (root / "work" / "instances.json").write_text(json.dumps({"green": {"gen": str(gen), "port": 1, "pid": 1}}), encoding="utf-8")
    log_path = gen / "instance.log"
    log_path.write_text("", encoding="utf-8")
    broken = set()
    origin = _serve(Origin, log_path=str(log_path), lock=threading.Lock(), broken=broken)
    port = origin.server_address[1]
    edge = _serve(Edge, origin_port=port, broken=broken, keys_seen=[])
    prod = _serve(Prod, broken=broken, keys_seen=[])
    ids = tmp_path / "ids.json"
    ids.write_text(json.dumps({"small": SMALL, "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01", "to": "1999-12-01"}), encoding="utf-8")

    class Rig:
        pass

    g = Rig()
    g.root, g.edge, g.prod, g.origin, g.broken, g.ids, g.tmp = root, edge, prod, origin, broken, ids, tmp_path
    g.soak_url = f"http://127.0.0.1:{edge.server_address[1]}"
    g.prod_url = f"http://127.0.0.1:{prod.server_address[1]}"
    g.router = ("127.0.0.1", port)

    def new():
        run = D.KeyedRun(g.soak_url, g.prod_url, str(root), router=g.router)
        run.set_key(KEY)
        return run
    g.new = new
    yield g
    for s in (origin, edge, prod):
        s.shutdown()
        s.server_close()


def _go(rig, ids=None, abort=100_000):
    run = rig.new()
    D.run_keyed(run, ids or D.read_ids(str(rig.ids)), abort)
    return run


def _verdicts(run):
    return {r["check"]: r["verdict"] for r in run.rows}


def _spellings(*ids):
    return [f for i in ids for f in (i, urllib.parse.quote(i, safe=""))]


W = "large plain object, whole: a length-less download read to the end (the pump)"
WF = "large plain object with a date window (the pump, bare)"
X = "length-less download, closed by the client"
GZ = "large stored-gzip object (passthrough)"
A, AP = "small CSV, bare (string path)", "small CSV, bare: the same bytes as production"
READBACK = "for the read-back of the download log (the author's step, 60 s or more after this run)"


# ---------------------------------------------------------------- the stand-in is the measured table
def test_the_stand_in_answers_in_the_shapes_the_worker_test_measures():
    """One line per case of api/worker/test/downloadShapes.test.ts. If that test changes, this one must."""
    def shape(sid, q):
        st, heads, body, chunked = serve_object(sid, urllib.parse.parse_qs(q), set())
        names = {k.lower() for k, _v in heads}
        kind = "PUMP" if "x-econdl-bytes-at-rest" in names else "PASSTHROUGH" if "x-econdl-citation-omitted" in names else "STRING"
        return st, kind, not chunked, "etag" in names, body[:1] == b"#"
    win = "from=1000-01-01&to=2999-12-31"
    #                                  status  shape          length  etag   `#` first
    assert shape(SMALL, "") == (200, "STRING", True, False, True)
    assert shape(SMALL, "raw=1") == (200, "STRING", True, True, False)
    assert shape(SMALL, "raw=1&from=1995-01-01&to=1999-12-01") == (200, "STRING", True, False, False)
    assert shape(LGZ, "") == (200, "PASSTHROUGH", True, False, False)
    assert shape(LGZ, "raw=1") == (200, "PASSTHROUGH", True, True, False)
    assert shape(LGZ, "raw=1&" + win) == (200, "PUMP", False, False, False)
    assert shape(LPL, "") == (200, "PUMP", False, False, True)
    assert shape(LPL, "raw=1") == (200, "PUMP", False, True, False)
    assert shape(LPL, "raw=1&" + win) == (200, "PUMP", False, False, False)
    src = open(os.path.join(os.path.dirname(HERE), "api", "worker", "test", "downloadShapes.test.ts"), encoding="utf-8").read()
    for words in ("PUMP: a large PLAIN object of the contract layout is the pump WITH OR WITHOUT a window",
                  "PASSTHROUGH: a large stored-gzip object without a window goes out as stored",
                  "STRING: a small object has a content-length; the ETag only bare and whole"):
        assert words in src, "the worker test that this stand-in copies has changed: " + words


# ---------------------------------------------------------------- the healthy case
def test_the_keyed_run_passes_on_healthy_stand_ins_and_shows_no_key_and_no_id(rig, capsys):
    run = _go(rig)
    v = _verdicts(run)
    not_pass = {k: x for k, x in v.items() if x != "PASS"}
    assert not_pass == {READBACK: "SEEN"}, not_pass
    assert len(run.rows) == 24, len(run.rows)
    assert {W, WF, X, GZ, A, AP} <= set(v)
    assert K.finish(run, "keyed") == 0
    out = capsys.readouterr().out
    files = list((rig.root / "logs").iterdir())
    assert len(files) == 1 and files[0].name.startswith("soak_keyed_")
    text = files[0].read_text(encoding="utf-8")
    for hidden in [KEY, SECRET] + _spellings(SMALL, LGZ, LPL):
        assert hidden not in out and hidden not in text, hidden
    assert "+key" in text and "<the small id>" in text and "<the large_gzip id>" in text and "<the large_plain id>" in text


def test_the_key_goes_to_the_soak_worker_and_to_production_and_nowhere_else(rig):
    _go(rig)
    assert KEY in rig.edge.keys_seen and KEY in rig.prod.keys_seen
    assert rig.prod.keys_seen == [KEY, KEY], "production is asked twice: the whole small series and its window"
    assert rig.edge.keys_seen.count(KEY) == 8, "eight keyed downloads through the soak address"
    assert rig.edge.keys_seen.count(None) == 6, "edge-status, two path probes and three id checks carry no key"


def test_the_closed_download_is_closed_before_the_end(rig):
    run = _go(rig, abort=60_000)
    row = next(r for r in run.rows if r["check"] == X)
    assert row["verdict"] == "PASS" and "stopped=True" in row["got"]
    last = run.rows[-1]
    assert last["verdict"] == "SEEN" and "closed after bytes>=60000" in last["got"] and "completed bytes=" in last["got"]
    assert "above the row id that was noted before the run" in last["note"]


def test_a_download_that_ends_before_the_client_closes_it_is_not_a_closed_download(rig):
    """The stop point is beyond the whole answer: the client reads to the end, so nothing was closed early."""
    run = _go(rig, abort=50_000_000)
    row = next(r for r in run.rows if r["check"] == X)
    assert row["verdict"] == "FAIL" and "stopped=False" in row["got"]


# ---------------------------------------------------------------- one switch, one row that must not pass
BROKEN = [
    ("no_etag", A, "FAIL"),
    ("small_is_large", A, "FAIL"),
    ("short_body", A, "FAIL"),
    ("two_log_lines", A, "FAIL"),
    ("edge_answers_itself", A, "FAIL"),
    ("slow_first_byte", A, "FAIL"),
    ("prod_differs", AP, "FAIL"),
    ("prod_other_etag", AP, "SEEN"),
    ("prod_other_etag", "date window: the same bytes as production", "SEEN"),
    ("citation_missing", "small CSV with its citation header", "FAIL"),
    ("window_has_etag", "small CSV with a date window", "FAIL"),
    ("gzip_not_marked", GZ, "FAIL"),
    ("gzip_bad_magic", GZ, "FAIL"),
    ("no_mark", GZ, "FAIL"),
    ("gzip_no_length", "large stored-gzip object: the length after the hop", "SEEN"),
    ("plain_is_passthrough", W, "FAIL"),             # the shape the first version of the script expected
    ("plain_is_passthrough", X, "FAIL"),
    ("pump_has_length", W, "FAIL"),
    ("pump_has_length", WF, "FAIL"),
    ("pump_has_length", X, "FAIL"),
    ("pump_no_complete_line", W, "FAIL"),
    ("pump_no_complete_line", WF, "FAIL"),
    ("pump_zero_rows", W, "FAIL"),
    ("pump_no_at_rest", W, "FAIL"),
    ("pump_marked_omitted", W, "FAIL"),
    ("pump_window_has_etag", WF, "FAIL"),
    ("two_log_lines", W, "FAIL"),
    ("slow_first_byte", W, "FAIL"),
]


@pytest.mark.parametrize("switch,row,want", BROKEN, ids=[f"{s}:{w}:{i}" for i, (s, _r, w) in enumerate(BROKEN)])
def test_a_broken_behaviour_is_not_a_pass(rig, monkeypatch, switch, row, want):
    rig.broken.add(switch)
    if switch == "slow_first_byte":
        monkeypatch.setattr(D, "EDGE_WAIT_S", 0.2)
    v = _verdicts(_go(rig))
    assert v[row] == want, (switch, row, v[row])


def test_a_dropped_length_on_the_gzip_object_is_seen_and_the_object_still_passes(rig):
    rig.broken.add("gzip_no_length")
    v = _verdicts(_go(rig))
    assert v[GZ] == "PASS"
    assert v["large stored-gzip object: the length after the hop"] == "SEEN"


def test_a_skipped_comparison_is_not_a_failure_and_not_a_pass(rig):
    rig.broken.add("prod_other_etag")
    run = _go(rig)
    v = _verdicts(run)
    assert v[AP] == "SEEN" and "SKIPPED" in next(r["note"] for r in run.rows if r["check"] == AP)
    assert K.finish(run, "keyed") == 0
    assert rig.prod.keys_seen == [KEY], "the window is not asked of production when the whole series did not match"


# ---------------------------------------------------------------- an id that is refused
@pytest.mark.parametrize("bad", [GATED, "s001:NO.SUCH"], ids=["the licence gate (451)", "not in the catalogue (404)"])
def test_a_refused_id_stops_the_run_before_any_keyed_request_and_is_not_named(rig, capsys, bad):
    ids = {"small": bad, "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01", "to": "1999-12-01"}
    run = _go(rig, ids=ids)
    v = _verdicts(run)
    assert v["the small id is served (no key)"] == "FAIL" and v["the keyed checks"] == "NOTRUN"
    assert KEY not in rig.edge.keys_seen and rig.prod.keys_seen == []
    assert K.finish(run, "keyed") == 1
    out = capsys.readouterr().out
    text = next((rig.root / "logs").iterdir()).read_text(encoding="utf-8")
    for hidden in _spellings(bad, LGZ, LPL):
        assert hidden not in out and hidden not in text, hidden
        assert hidden not in run.clean("a later row that names " + hidden), "both spellings are in the hidden list"


def test_an_id_inside_another_id_is_hidden_whole(rig):
    ids = {"small": "s001:A", "large_gzip": "s001:A.B", "large_plain": "s001:A.B.C", "from": "1995-01-01", "to": "1999-12-01"}
    run = rig.new()
    D.hide_ids(run, ids)
    assert run.clean("GET /v1/series/s001%3AA.B.C.csv and s001:A.B and s001:A") == \
        "GET /v1/series/<the large_plain id>.csv and <the large_gzip id> and <the small id>"


# ---------------------------------------------------------------- where the key may go
def test_the_key_is_refused_for_any_other_address(rig):
    run = rig.new()
    with pytest.raises(RuntimeError, match="neither the soak worker nor production"):
        run.fetch("http://127.0.0.1:9", "/v1/series/x.csv")
    with pytest.raises(RuntimeError, match="neither the soak worker nor production"):
        run.fetch(rig.soak_url + ".example", "/v1/series/x.csv")
    with pytest.raises(RuntimeError, match="the key in a URL"):
        run.fetch(rig.soak_url, "/v1/series/x.csv?api_key=" + KEY)
    run.secret()
    with pytest.raises(RuntimeError, match="the origin secret in a URL"):
        run.fetch(rig.soak_url, "/v1/series/x.csv?s=" + SECRET)


def test_plain_http_and_other_addresses_are_refused_outside_the_tests(rig, monkeypatch):
    run = rig.new()
    monkeypatch.setattr(D, "ALLOW_PLAIN_HTTP", False)
    with pytest.raises(RuntimeError, match="not https"):
        run.fetch(rig.soak_url, "/v1/series/x.csv")
    with pytest.raises(SystemExit, match="soak worker and to production only"):
        D.KeyedRun("https://econdl-api-soak.example.workers.dev", D.PROD, str(rig.root))
    with pytest.raises(SystemExit, match="soak worker and to production only"):
        D.KeyedRun(K.SOAK, "https://econdl-api.example.workers.dev", str(rig.root))
    D.KeyedRun(K.SOAK, D.PROD, str(rig.root))            # the two real addresses are accepted (nothing is sent)


def test_the_key_is_in_the_hidden_list_so_a_row_can_never_show_it(rig, capsys):
    """No row of the script names the key. The hidden list is the second barrier: a row that did would be
    cleaned, and finish() refuses to write a file that holds a hidden value."""
    run = rig.new()
    assert run.clean("X-API-Key: " + KEY) == "X-API-Key: <key>"
    run.row("a row that names the key", "GET x +" + KEY, "got " + KEY, "SEEN", "note " + KEY)
    assert K.finish(run, "keyed") == 0
    text = next((rig.root / "logs").iterdir()).read_text(encoding="utf-8")
    assert KEY not in text and KEY not in capsys.readouterr().out and text.count("<key>") == 3


def test_a_keyed_request_before_the_key_is_refused(rig):
    run = D.KeyedRun(rig.soak_url, rig.prod_url, str(rig.root), router=rig.router)
    with pytest.raises(RuntimeError, match="before the key was given"):
        run.fetch(rig.soak_url, "/v1/series/x.csv")


@pytest.mark.parametrize("bad", ["", "short", "has a space in the key 123", "k" * 15, "é" * 20])
def test_a_key_that_cannot_be_right_is_refused(rig, bad):
    run = D.KeyedRun(rig.soak_url, rig.prod_url, str(rig.root), router=rig.router)
    with pytest.raises(SystemExit):
        run.set_key(bad)


# ---------------------------------------------------------------- the key is typed at a real console only
class _Stdin:
    def __init__(self, tty):
        self._tty = tty

    def isatty(self):
        return self._tty


def test_the_key_is_not_asked_when_the_input_is_not_a_terminal(monkeypatch):
    """Review AR-303 C4: with a pipe or a file as input, getpass falls back to a prompt that shows the text."""
    asked = []
    monkeypatch.setattr(D.getpass, "getpass", lambda prompt: asked.append(prompt) or KEY)
    monkeypatch.setattr(D.sys, "stdin", _Stdin(False))
    with pytest.raises(SystemExit, match="not a terminal"):
        D.ask_key()
    assert asked == []


def test_a_console_that_cannot_hide_the_key_is_refused(monkeypatch):
    def loud(prompt):
        warnings.warn("Can not control echo on the terminal.", getpass.GetPassWarning, stacklevel=2)
        return KEY
    monkeypatch.setattr(D.getpass, "getpass", loud)
    monkeypatch.setattr(D.sys, "stdin", _Stdin(True))
    with pytest.raises(SystemExit, match="cannot hide"):
        D.ask_key()


def test_the_key_is_asked_once_at_a_terminal(monkeypatch):
    asked = []
    monkeypatch.setattr(D.getpass, "getpass", lambda prompt: asked.append(prompt) or KEY)
    monkeypatch.setattr(D.sys, "stdin", _Stdin(True))
    assert D.ask_key() == KEY and len(asked) == 1 and "not shown" in asked[0]


# ---------------------------------------------------------------- the ids file and the command line
@pytest.mark.parametrize("ids", [
    {"small": SMALL, "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01"},                          # a field is missing
    {"small": SMALL, "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01", "to": "1994-01-01"},      # from after to
    {"small": SMALL, "large_gzip": LGZ, "large_plain": LPL, "from": "1995", "to": "1999-01-01"},
    {"small": SMALL, "large_gzip": SMALL, "large_plain": LPL, "from": "1995-01-01", "to": "1999-01-01"},    # one id twice
    {"small": SMALL, "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01", "to": "1999-01-01", "key": "x" * 20},
    {"small": "", "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01", "to": "1999-01-01"},
    [SMALL, LGZ, LPL],
])
def test_an_ids_file_of_another_shape_is_refused(tmp_path, ids):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps(ids), encoding="utf-8")
    with pytest.raises(SystemExit):
        D.read_ids(str(p))


def test_main_runs_the_whole_thing_and_takes_no_address_from_the_command_line(rig, capsys):
    code = D.main(["--ids-file", str(rig.ids), "--root", str(rig.root), "--abort-after-bytes", "80000"], key=KEY,
                  soak=rig.soak_url, prod=rig.prod_url, router=rig.router)
    assert code == 0
    assert KEY not in capsys.readouterr().out
    for flag in ("--soak", "--prod", "--key", "--router"):
        with pytest.raises(SystemExit):
            D.main(["--ids-file", str(rig.ids), flag, "x"], key=KEY, soak=rig.soak_url, prod=rig.prod_url, router=rig.router)


def test_main_asks_for_the_key_through_ask_key_only(rig, monkeypatch, capsys):
    asked = []
    monkeypatch.setattr(D, "ask_key", lambda: asked.append(1) or KEY)
    assert D.main(["--ids-file", str(rig.ids), "--root", str(rig.root), "--abort-after-bytes", "80000"],
                  soak=rig.soak_url, prod=rig.prod_url, router=rig.router) == 0
    assert asked == [1] and KEY not in capsys.readouterr().out
    src = open(D.__file__, encoding="utf-8").read()
    assert "input(" not in src and "environ" not in src and src.count("getpass.getpass(") == 1
