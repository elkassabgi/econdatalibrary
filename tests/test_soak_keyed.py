"""tools/selfhost/soak_keyed.py against stand-ins on loopback: an ORIGIN (router + instance with an instance.log
in wrangler's line shape), a soak EDGE that asks for the key and forwards with the origin secret, and PRODUCTION,
which serves the same stored objects by key. The three download shapes are the worker's (api/worker/src/
series.ts): string (content-length, ETag when bare and whole), passthrough (the stored bytes, their exact length,
`x-econdl-citation-omitted`), pump (no content-length, `x-econdl-bytes-at-rest`, `# econdl-complete rows=N`).

Nothing goes to the network. The stand-ins are kinder than the real system in known ways: the edge copies the
origin's answer headers one to one (a real hop can drop content-length), the log line is written before the
answer, production reads the same bytes as the origin unless a switch says otherwise, and no download log
exists (the read-back is not this script's).

What is pinned: BROKEN lists one switch per behaviour and the row that must then not PASS. A behaviour that is
not in that list is not claimed.
"""
import gzip
import http.client
import http.server
import json
import os
import random
import sys
import threading
import urllib.parse

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
    out, rows = [], 0
    for ln in text.decode().splitlines()[1:]:
        if lo <= ln.split(",")[1] <= hi:
            out.append(ln)
            rows += 1
    return out, rows


def serve_object(sid, q, b):
    """(status, headers, body, chunked) as the worker answers a .csv request; `b` = the switches that are on."""
    if sid not in STORED:
        return 404, [], json.dumps({"error": "not_found"}).encode(), False
    raw = q.get("raw") == ["1"]
    lo, hi = (q.get("from") or [None])[0], (q.get("to") or [None])[0]
    windowed = lo is not None or hi is not None
    etag = [("ETag", '"etag-of-' + sid + '"')]
    if sid == SMALL:
        if windowed:
            lines, _n = _window(TEXT[sid], lo or "0000", hi or "9999")
            body = ("series_id,obs_date,value\n" + "\n".join(lines) + "\n").encode()
            return 200, (etag if "window_has_etag" in b else []), body, False
        body = TEXT[sid] if raw else (b"" if "citation_missing" in b else b"# Source: a test series\n") + TEXT[sid]
        heads = etag if raw and "no_etag" not in b else []
        if "small_is_large" in b:
            heads = heads + [("x-econdl-citation-omitted", "large-object")]
        return 200, heads, body, False
    if not windowed:                                                        # passthrough
        heads = [] if "no_mark" in b else [("x-econdl-citation-omitted", "large-object")]
        if sid == LGZ:
            body = STORED[sid] if "gzip_bad_magic" not in b else b"xx" + STORED[sid][2:]
            if "gzip_not_marked" not in b:
                heads.append(("Content-Encoding", "gzip"))
            return 200, heads, body, "gzip_no_length" in b
        if "plain_encoded" in b:
            heads.append(("Content-Encoding", "gzip"))
        return 200, heads, STORED[sid], False
    lines, n = _window(TEXT[sid], lo or "0000", hi or "9999")               # the pump
    tail = b"" if "pump_no_complete_line" in b else f"# econdl-complete rows={0 if 'pump_zero_rows' in b else n}\n".encode()
    body = ("series_id,obs_date,value\n" + "\n".join(lines) + "\n").encode() + tail
    heads = [] if "pump_no_at_rest" in b else [("x-econdl-bytes-at-rest", str(len(STORED[sid])))]
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


_REAL_LOOP = K.Run.loop


@pytest.fixture(autouse=True)
def never_the_real_router(monkeypatch):
    """The tool's default router is the STANDING ORIGIN's (127.0.0.1:8787). A test that forgets to give its own
    stand-in must fail here, not ask the real one (ledger R1544: two tests of main() did, 2026-10-12)."""
    def loop(self, target, with_secret=False):
        assert self.router[1] != K.ROUTER[1], "this test would have asked the standing origin's real router"
        return _REAL_LOOP(self, target, with_secret)
    monkeypatch.setattr(K.Run, "loop", loop)


def test_the_guard_against_the_real_router_can_fail(tmp_path, monkeypatch):
    monkeypatch.setattr(D, "ALLOW_PLAIN_HTTP", True)
    run = D.KeyedRun("http://127.0.0.1:9", "http://127.0.0.1:9", str(tmp_path))        # no router given: the default
    assert run.router == K.ROUTER
    with pytest.raises(AssertionError, match="real router"):
        run.loop("/__router/status")


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
    g.router = ("127.0.0.1", port)
    g.prod_url = f"http://127.0.0.1:{prod.server_address[1]}"

    def new():
        run = D.KeyedRun(g.soak_url, g.prod_url, str(root), router=("127.0.0.1", port))
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


# ---------------------------------------------------------------- the healthy case
def test_the_keyed_run_passes_on_healthy_stand_ins_and_shows_no_key(rig, capsys):
    run = _go(rig)
    v = _verdicts(run)
    not_pass = {k: x for k, x in v.items() if x != "PASS"}
    assert not_pass == {"for the read-back of the download log (the author's step, 60 s or more after this run)": "SEEN"}, not_pass
    assert len(run.rows) == 24, len(run.rows)
    assert K.finish(run, "keyed") == 0
    out = capsys.readouterr().out
    files = list((rig.root / "logs").iterdir())
    assert len(files) == 1 and files[0].name.startswith("soak_keyed_")
    text = files[0].read_text(encoding="utf-8")
    for hidden in (KEY, SECRET):
        assert hidden not in out and hidden not in text
    assert "+key" in text


def test_the_key_goes_to_the_soak_worker_and_to_production_and_nowhere_else(rig):
    _go(rig)
    assert KEY in rig.edge.keys_seen and KEY in rig.prod.keys_seen
    assert rig.prod.keys_seen == [KEY, KEY], "production is asked twice: the whole small series and its window"
    assert rig.edge.keys_seen.count(None) >= 6, "the keyless requests (edge-status, the path probes, the three id checks) carry no key"


def test_the_closed_download_is_closed_before_the_end(rig):
    run = _go(rig, abort=60_000)
    row = next(r for r in run.rows if r["check"] == "length-less download, closed by the client")
    assert row["verdict"] == "PASS" and "stopped=True" in row["got"]
    last = run.rows[-1]
    assert last["verdict"] == "SEEN" and "closed after bytes>=60000" in last["got"] and "completed bytes=" in last["got"]


def test_a_download_that_ends_before_the_client_closes_it_is_not_a_closed_download(rig):
    """The stop point is beyond the whole answer: the client reads to the end, so nothing was closed early."""
    run = _go(rig, abort=50_000_000)
    row = next(r for r in run.rows if r["check"] == "length-less download, closed by the client")
    assert row["verdict"] == "FAIL" and "stopped=False" in row["got"]


# ---------------------------------------------------------------- one switch, one row that must not pass
A, AP = "small CSV, bare (string path)", "small CSV, bare: the same bytes as production"
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
    ("gzip_not_marked", "large stored-gzip object (passthrough)", "FAIL"),
    ("gzip_bad_magic", "large stored-gzip object (passthrough)", "FAIL"),
    ("no_mark", "large stored-gzip object (passthrough)", "FAIL"),
    ("no_mark", "large plain object (passthrough)", "FAIL"),
    ("gzip_no_length", "large stored-gzip object: the length after the hop", "SEEN"),
    ("plain_encoded", "large plain object (passthrough)", "FAIL"),
    ("pump_has_length", "length-less download, read to the end (the pump)", "FAIL"),
    ("pump_no_complete_line", "length-less download, read to the end (the pump)", "FAIL"),
    ("pump_zero_rows", "length-less download, read to the end (the pump)", "FAIL"),
    ("pump_no_at_rest", "length-less download, read to the end (the pump)", "FAIL"),
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
    assert v["large stored-gzip object (passthrough)"] == "PASS"
    assert v["large stored-gzip object: the length after the hop"] == "SEEN"


def test_a_skipped_comparison_is_not_a_failure_and_not_a_pass(rig):
    rig.broken.add("prod_other_etag")
    run = _go(rig)
    v = _verdicts(run)
    assert v[AP] == "SEEN" and "SKIPPED" in next(r["note"] for r in run.rows if r["check"] == AP)
    assert K.finish(run, "keyed") == 0
    assert rig.prod.keys_seen == [KEY], "the window is not asked of production when the whole series did not match"


# ---------------------------------------------------------------- an id the gate refuses
def test_a_gated_id_stops_the_run_before_any_keyed_request_and_is_not_named(rig, capsys):
    ids = {"small": GATED, "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01", "to": "1999-12-01"}
    run = _go(rig, ids=ids)
    v = _verdicts(run)
    assert v["the small id is served (no key)"] == "FAIL" and v["the keyed checks"] == "NOTRUN"
    assert KEY not in rig.edge.keys_seen and rig.prod.keys_seen == []
    assert K.finish(run, "keyed") == 1
    out = capsys.readouterr().out
    text = next((rig.root / "logs").iterdir()).read_text(encoding="utf-8")
    for hidden in (GATED, urllib.parse.quote(GATED, safe="")):
        assert hidden not in out and hidden not in text
        assert hidden not in run.clean("a later row that names " + hidden), "both spellings are in the hidden list"


def test_an_id_the_origin_does_not_know_stops_the_run(rig):
    ids = {"small": "s001:NO.SUCH", "large_gzip": LGZ, "large_plain": LPL, "from": "1995-01-01", "to": "1999-12-01"}
    v = _verdicts(_go(rig, ids=ids))
    assert v["the small id is served (no key)"] == "FAIL" and v["the keyed checks"] == "NOTRUN"
    assert KEY not in rig.edge.keys_seen


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
    run = D.KeyedRun(rig.soak_url, rig.prod_url, str(rig.root))
    with pytest.raises(RuntimeError, match="before the key was given"):
        run.fetch(rig.soak_url, "/v1/series/x.csv")


@pytest.mark.parametrize("bad", ["", "short", "has a space in the key 123", "k" * 15, "é" * 20])
def test_a_key_that_cannot_be_right_is_refused(rig, bad):
    run = D.KeyedRun(rig.soak_url, rig.prod_url, str(rig.root))
    with pytest.raises(SystemExit):
        run.set_key(bad)


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


def test_main_asks_for_the_key_without_echo(rig, monkeypatch, capsys):
    asked = []
    monkeypatch.setattr(D.getpass, "getpass", lambda prompt: asked.append(prompt) or KEY)
    assert D.main(["--ids-file", str(rig.ids), "--root", str(rig.root), "--abort-after-bytes", "80000"],
                  soak=rig.soak_url, prod=rig.prod_url, router=rig.router) == 0
    assert len(asked) == 1 and KEY not in capsys.readouterr().out
    src = open(D.__file__, encoding="utf-8").read()
    assert "input(" not in src and "environ" not in src and "argv" not in src.replace("argv=None", "").replace("parse_args(argv)", "")
