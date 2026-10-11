"""tools/selfhost/soak_keyless.py against stand-ins on loopback: an ORIGIN (router + instance, with an
instance.log in wrangler's line shape, written AFTER the answer when `late` is set), an EDGE that answers in
the order of api/worker/src/index.ts and edge.ts (404, 503, the gate, the key, then the forward with the secret
added, a cache for cacheable 200s, the origin's marks removed), and the origin's PUBLIC name behind Access.

Nothing goes to the network. The stand-ins are kinder than the real system in known ways: the edge's cache is
written at once (real: after the answer, per data centre), /v1/public-stats never asks the origin (real: once
an hour), no client header is forwarded, the stand-in router answers /__router/status to anyone (real: only to
a loopback Host), the log line is written BEFORE the answer unless `late` is set (real: always after), and the
public name knows only the answers listed in `PublicOrigin`.

What is pinned: BROKEN lists one switch per behaviour and the row that must then FAIL; LOCK_CASES lists what
each answer of the public name must read as. A behaviour that is not in those lists is not claimed.
"""
import http.client
import http.server
import json
import os
import socket
import sys
import threading
import urllib.parse

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
for cand in (os.path.join(os.path.dirname(HERE), "tools", "selfhost"), HERE):
    if os.path.isfile(os.path.join(cand, "soak_keyless.py")):
        sys.path.insert(0, cand)
        break

import soak_keyless as K  # noqa: E402

SECRET = "ab" * 32
GATED = "gatedsrc:X.1"
SOURCES = [{"source": f"s{i:03d}"} for i in range(120)]
IDS1 = [f"s001:UNEMP.{i}" for i in range(5)]
IDS2 = [f"s001:UNEMP.{i}" for i in range(5, 10)]
FORWARDED = ("/", "/v1", "/v1/catalog", "/v1/sources", "/v1/last-updates", "/v1/stats", "/v1/bundle", "/v1/guard-heartbeat")


class Quiet(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send(self, status, body, headers=()):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)
        self.wfile.flush()


class Origin(Quiet):
    """The router and the instance in one: /__router/status is the router's; everything else is the worker's and
    adds one line to instance.log - before the answer, or `late` seconds after it (wrangler writes it after)."""

    def do_GET(self):                                                          # noqa: N802
        srv = self.server
        u = urllib.parse.urlsplit(self.path)
        if u.path == "/__router/status":
            return self.send(200, {"active": "green", "target": "x"})
        if self.headers.get(K.SECRET_HEADER) != SECRET:
            status, body = 403, {"error": "forbidden"}
        else:
            status, body = self.answer(u)

        def write():
            with srv.lock:
                with open(srv.log_path, "a", encoding="utf-8") as fh:
                    for _ in range(2 if "__soak_" in u.path and srv.double_probe else 1):
                        fh.write(f"[wrangler:inf] GET {u.path} {status} OK (1ms)\n")
        if srv.late:
            self.send(status, body, headers=[(K.MARK_HEADER, "1"), ("x-econ-instance", "gen-test")])
            threading.Timer(srv.late, write).start()
        else:
            write()
            self.send(status, body, headers=[(K.MARK_HEADER, "1"), ("x-econ-instance", "gen-test")])

    def answer(self, u):
        q = urllib.parse.parse_qs(u.query)
        p = u.path
        if p in ("/", "/v1"):
            return 200, {"endpoints": ["/v1/catalog"]}
        if p == "/v1/sources":
            srcs = SOURCES[:5] if self.server.few_sources else SOURCES
            return 200, {"sources": srcs, "total": len(srcs)}
        if p == "/v1/catalog":
            ids = IDS2 if q.get("offset") == ["5"] else IDS1
            return 200, {"results": [{"series_id": i} for i in ids], "total": 10}
        if p == "/v1/last-updates":
            return 200, {"datasets": [{"source": "s001", "last_updated": "2026-10-10T00:00:00Z"}, {"source": "s002", "last_updated": None}]}
        if p == "/v1/stats":
            return 200, {"catalog_entries": 12345}
        if p == "/v1/guard-heartbeat":
            return 200, {"utc": "2026-10-10T00:00:00Z"}
        if p == "/v1/bundle":
            return 200, {"manifest": q.get("ids")}
        if p.startswith("/v1/series/") and p.endswith(".metadata.json"):
            sid = urllib.parse.unquote(p[len("/v1/series/"):-len(".metadata.json")])
            return (200, {"series_id": sid}) if sid in IDS1 else (404, {"error": "not_found"})
        return 404, {"error": "not_found"}


class Edge(Quiet):
    """The order of index.ts: page views and public-stats are the edge's own; then 404 for a path that is not
    forwarded, 503 without the origin values, the gate (451), the key (401), then the forward."""

    def do_GET(self):                                                          # noqa: N802
        srv = self.server
        b = srv.broken
        u = urllib.parse.urlsplit(self.path)
        p = u.path
        srv.paths.append(self.path)
        if self.headers.get("User-Agent") == "curl/8.5.0" and "blocks_curl" in b:
            return self.send(403, b"<html>blocked</html>")
        if p == "/v1/edge-status":
            return self.send(200, {"commit": "c0ffee00" * 5, "soak": "soak_flag_off" not in b, "forward": True, "edge_state": "users",
                                   "origin_configured": srv.configured and "status_unconfigured" not in b})
        if p == "/v1/pv":
            return self.send(200 if "pv_on" in b else 404, {"error": "not_found"})
        if p == "/v1/pv/report":
            return self.send(200 if "pv_report_on" in b else 404, {"error": "not_found"})
        if p == "/v1/public-stats":
            if "public_stats_silent" in b:                              # the connection ends with no answer at all
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return None
            return self.send(500 if "public_stats_500" in b else 200, {"visitors": 1})
        if not (p in FORWARDED or p.startswith("/v1/series/")):
            return self.send(404, {"error": "not_found"})
        if not srv.configured:
            return self.send(503, {"error": "other_word" if "words_5xx" in b else "origin_not_configured"})
        if p.startswith("/v1/series/__soak_") and "probe_at_edge" in b:      # an edge that answers the probe itself
            return self.send(404, {"error": "not_found"})
        if p.startswith("/v1/series/"):
            sid = urllib.parse.unquote(p[len("/v1/series/"):].rsplit(".", 2 if p.endswith(".metadata.json") else 1)[0])
            if sid == GATED and "gate_off" not in b:
                return self.send(451, {"error": "other_word" if "wrong_words" in b else "not_redistributable"})
            if p.endswith(".csv") and "csv_open" not in b:
                key = self.headers.get("X-API-Key")
                return self.send(401, {"error": "other_word" if "wrong_words" in b else "invalid_key" if key else "auth_required"})
        if not srv.access and "access_open" not in b:
            return self.send(502, {"error": "other_word" if "words_5xx" in b else "origin_bad_answer"})
        if f"empty:{p}" in b or ("empty:series" in b and p.endswith(".metadata.json") and "__soak_" not in p):
            return self.send(200, {})
        if p == "/v1/sources" and "stale_cache" in b and srv.stale is not None:   # a cache that forgets the query
            return self.send(200, srv.stale)
        cacheable = p in ("/v1/catalog", "/v1/stats", "/v1/sources", "/v1/last-updates") or p.endswith(".metadata.json")
        if cacheable and "no_cache" not in b and self.path in srv.cache:
            status, raw = srv.cache[self.path]
            return self.send(status, raw + (b" " if "second_body_differs" in b else b""))
        c = http.client.HTTPConnection("127.0.0.1", srv.origin_port, timeout=30)
        c.request("GET", self.path, headers={K.SECRET_HEADER: SECRET})
        r = c.getresponse()
        raw = r.read()
        c.close()
        if p == "/v1/sources" and "short_sources" in b:
            raw = json.dumps({"sources": SOURCES[:100], "total": 100}).encode()
        if p == "/v1/sources" and "long_total" in b:
            raw = json.dumps({"sources": SOURCES[:100], "total": len(SOURCES)}).encode()
        if p == "/v1/stats" and "other_stats" in b:
            raw = json.dumps({"catalog_entries": 1}).encode()
        if p == "/v1/catalog" and "other_ids" in b:
            raw = json.dumps({"results": [{"series_id": "zzz:1"}]}).encode()
        if p == "/v1/sources" and srv.stale is None:
            srv.stale = raw
        if cacheable and r.status == 200:
            srv.cache[self.path] = (r.status, raw)
        extra = ([(K.MARK_HEADER, "1")] if "leaks_mark" in b else []) + ([("x-econ-instance", "gen-test")] if "leaks_instance" in b else [])
        self.send(r.status, raw, headers=extra)


class PublicOrigin(Quiet):
    """The tunnel's public name. `mode` says what stands in front of the origin."""

    def do_GET(self):                                                          # noqa: N802
        srv = self.server
        m = srv.mode
        if m == "open":                                     # no lock: the request reaches the origin (without a valid secret)
            c = http.client.HTTPConnection("127.0.0.1", srv.origin_port, timeout=30)
            c.request("GET", self.path, headers={K.SECRET_HEADER: "0" * 64})
            r = c.getresponse()
            raw = r.read()
            c.close()
            return self.send(r.status, raw, headers=[(K.MARK_HEADER, "1")])
        if m == "open_behind_403":                          # the origin is reached, and the client is shown a bare 403
            c = http.client.HTTPConnection("127.0.0.1", srv.origin_port, timeout=30)
            c.request("GET", self.path, headers={K.SECRET_HEADER: "0" * 64})
            c.getresponse().read()
            c.close()
            return self.send(403, b"")
        if m == "access302":
            return self.send(302, b"", headers=[("Location", "https://team-example.cloudflareaccess.com/cdn-cgi/access/login/x")])
        if m == "access403":
            return self.send(403, b"<html><title>Forbidden</title>Cloudflare Access</html>", headers=[("Server", "cloudflare"), ("CF-RAY", "abc-XYZ")])
        if m == "redirect_elsewhere":
            return self.send(302, b"", headers=[("Location", "https://elsewhere.example/login")])
        if m == "lookalike":
            return self.send(302, b"", headers=[("Location", "https://cloudflareaccess.com.elsewhere.example/login")])
        if m == "challenge":
            return self.send(403, b"<html>Just a moment...</html>", headers=[("cf-mitigated", "challenge")])
        if m == "bare403":
            return self.send(403, b"error code: 1010")
        if m == "basic401":                                 # another product's login: the word "access" in a header is no mark
            return self.send(401, b"", headers=[("WWW-Authenticate", 'Basic realm="Restricted access"')])
        if m == "denied403":                                # a firewall page that holds both words, not together
            return self.send(403, b"<html><title>Access denied</title>Cloudflare Ray ID: 1</html>", headers=[("Server", "cloudflare")])
        if m == "three_marked":                             # Access's page for three requests, a bare 403 for the fourth
            if self.headers.get("cf-access-client-id"):
                return self.send(403, b"")
            return self.send(403, b"<html><title>Forbidden</title>Cloudflare Access</html>")
        if m == "bare401":
            return self.send(401, b"")
        if m == "parked200":
            return self.send(200, b"<html>hello</html>")
        return self.send(int(m), b"<html>error code: 1033</html>")


def _serve(handler, **attrs):
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    srv.daemon_threads = True
    for k, v in attrs.items():
        setattr(srv, k, v)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


@pytest.fixture
def rig(tmp_path, monkeypatch):
    monkeypatch.setattr(K, "SETTLE_SCALE", 0.02)
    root = tmp_path / "origin"
    gen = root / "work" / "gen-green"
    gen.mkdir(parents=True)
    (root / "code" / "api" / "worker").mkdir(parents=True)
    (root / "code" / "api" / "worker" / ".dev.vars").write_text(f"ORIGIN_SECRET={SECRET}\n", encoding="ascii")
    (root / "work" / "instances.json").write_text(json.dumps({"green": {"gen": str(gen), "port": 1, "pid": 1}}), encoding="utf-8")
    log_path = gen / "instance.log"
    log_path.write_text("", encoding="utf-8")
    origin = _serve(Origin, log_path=str(log_path), lock=threading.Lock(), late=0.0, double_probe=False, few_sources=False)
    port = origin.server_address[1]
    edge = _serve(Edge, origin_port=port, broken=set(), configured=True, access=True, cache={}, paths=[], stale=None, seen_once=set())
    public = _serve(PublicOrigin, origin_port=port, mode="access403")

    class Rig:
        pass

    g = Rig()
    g.root, g.edge, g.origin, g.public, g.log_path, g.gen = root, edge, origin, public, log_path, gen
    g.new = lambda: K.Run(f"http://127.0.0.1:{edge.server_address[1]}", str(root), router=("127.0.0.1", port))
    g.public_url = f"http://127.0.0.1:{public.server_address[1]}"
    yield g
    for s in (origin, edge, public):
        s.shutdown()
        s.server_close()


def _verdicts(run):
    return {r["check"]: r["verdict"] for r in run.rows}


def _failed(run):
    return sorted(r["check"] for r in run.rows if r["verdict"] == "FAIL")


def _suite(rig, gated=GATED):
    run = rig.new()
    K.mode_suite(run, gated)
    return run


def _lock(rig):
    run = rig.new()
    K.mode_lock(run, rig.public_url)
    return run


# ---------------------------------------------------------------- the healthy case
def test_the_suite_passes_on_healthy_stand_ins_and_shows_no_secret(rig, capsys):
    run = _suite(rig)
    assert _failed(run) == [] and set(_verdicts(run).values()) == {"PASS"}, [r for r in run.rows if r["verdict"] != "PASS"]
    assert len(run.rows) >= 34
    assert K.finish(run, "suite") == 0
    out = capsys.readouterr().out
    files = list((rig.root / "logs").iterdir())
    assert len(files) == 1
    text = files[0].read_text(encoding="utf-8")
    for hidden in (SECRET, GATED, urllib.parse.quote(GATED, safe="")):
        assert hidden not in out and hidden not in text


def test_the_suite_passes_when_the_log_line_is_written_after_the_answer(rig):
    """AR-291 B3: wrangler writes its line after the answer. A count that starts before the line of the request
    before it has landed reads one line too many on a healthy system."""
    rig.origin.late = 0.1
    K.SETTLE_SCALE = 0.1                                             # (monkeypatched in the fixture; restored after the test)
    for mode in (lambda r: K.mode_suite(r, GATED), lambda r: K.mode_lock(r, rig.public_url)):
        run = rig.new()
        mode(run)
        assert _failed(run) == [] and "NOTRUN" not in _verdicts(run).values(), [r for r in run.rows if r["verdict"] not in ("PASS", "SEEN")]
    rig.edge.access = False
    run = rig.new()
    K.mode_phase1(run)
    assert _failed(run) == [] and _verdicts(run)["phase 1: the request did not reach the instance"] == "PASS"


def test_every_forwarded_request_of_the_suite_carries_a_nonce_and_hidden_values_stay_hidden(rig, capsys):
    """A request without a nonce can be answered by the edge's cache: it would show nothing about the origin."""
    run = _suite(rig)
    forwarded = [p for p in rig.edge.paths if not p.startswith(("/v1/edge-status", "/v1/series/__soak_"))]
    assert len(forwarded) >= 21 and all("_n=" in p for p in forwarded), [p for p in forwarded if "_n=" not in p]
    assert "/v1/pv/report" in " ".join(forwarded)
    capsys.readouterr()
    run.row("x " + SECRET, "sent " + GATED, "got " + SECRET, "SEEN", "note " + GATED)   # the second net: clean() on every field
    shown = capsys.readouterr().out + json.dumps(run.rows[-1])
    assert SECRET not in shown and GATED not in shown and "<secret>" in shown and "<the gated id>" in shown


def test_without_a_gated_id_the_suite_is_not_a_pass(rig):
    run = _suite(rig, gated=None)
    assert _failed(run) == [] and _verdicts(run)["a gated id"] == "NOTRUN"
    assert K.finish(run, "suite") == 2


def test_without_the_loopback_control_nothing_is_counted_as_passed(rig):
    rig.origin.shutdown()
    rig.origin.server_close()
    run = _suite(rig)
    v = _verdicts(run)
    assert v["suite"] == "NOTRUN" and "FAIL" in v.values() and K.finish(run, "suite") == 1
    assert any("client_" in r["got"] for r in run.rows), "a connection failure is shown with its type"


def test_an_origin_that_answers_with_a_handful_of_sources_fails_the_control(rig):
    """The control's floor: an origin whose catalogue is nearly empty answers 200 too. Every floor of the suite is
    compared with this answer, so it must itself be a real one (at least 100 entries, total equal to the list)."""
    rig.origin.few_sources = True
    run = _suite(rig)
    assert "before loopback: sources with the secret" in _failed(run) and run.rows[-1]["verdict"] == "NOTRUN"


def test_a_log_that_nothing_writes_to_fails_the_control(rig):
    """AR-291 B2: instances.json names a folder that the instance does not write to. A count of zero there would
    prove nothing: the control's own request must be seen in the log."""
    dead = rig.root / "work" / "gen-dead"
    dead.mkdir()
    (dead / "instance.log").write_text("", encoding="utf-8")
    (rig.root / "work" / "instances.json").write_text(json.dumps({"green": {"gen": str(dead)}}), encoding="utf-8")
    rig.public.mode = "open_behind_403"
    run = _lock(rig)
    assert "before loopback: this request is in the instance log" in _failed(run)
    assert _verdicts(run)["lock"] == "NOTRUN" and "lock: nothing reached the instance" not in _verdicts(run)
    assert _suite(rig).rows[-1]["verdict"] == "NOTRUN"


# ---------------------------------------------------------------- each pinned behaviour can fail
BROKEN = [
    ("no_cache", "the edge cache"), ("stale_cache", "the edge cache"), ("second_body_differs", "the edge cache"),
    ("stale_cache", "User-Agent: curl"), ("blocks_curl", "User-Agent: curl"), ("short_sources", "User-Agent: a crawler"),
    ("leaks_mark", "sources"), ("leaks_instance", "sources"), ("short_sources", "sources"), ("long_total", "sources"),
    ("other_stats", "stats"), ("other_ids", "catalogue search"),
    ("empty:/", "root"), ("empty:/v1/catalog", "catalogue browse"), ("empty:/v1/catalog", "catalogue page 2"),
    ("empty:/v1/last-updates", "last-updates"), ("empty:/v1/guard-heartbeat", "guard-heartbeat"),
    ("empty:series", "metadata"), ("empty:/v1/bundle", "bundle"),
    ("pv_on", "page views are off on the soak worker: /v1/pv"), ("pv_report_on", "page views are off on the soak worker: /v1/pv/report"),
    ("public_stats_500", "public-stats"), ("public_stats_silent", "public-stats"),
    ("csv_open", "a CSV without a key"), ("csv_open", "the two refused CSV requests did not reach the instance"),
    ("wrong_words", "a CSV with a key that does not exist"), ("wrong_words", "a gated id"), ("gate_off", "a gated id"),
    ("probe_at_edge", "start: path probe"), ("probe_at_edge", "end: path probe"),
    ("soak_flag_off", "edge-status"), ("status_unconfigured", "edge-status"),
]


@pytest.mark.parametrize("broken, check", BROKEN)
def test_a_broken_edge_fails_the_row_that_is_about_it(rig, broken, check):
    rig.edge.broken.add(broken)
    run = _suite(rig)
    assert check in _failed(run), (broken, _failed(run))


def test_a_cache_that_does_not_work_is_seen_in_the_log_count_not_in_the_answers(rig):
    rig.edge.broken.add("no_cache")
    row = next(r for r in _suite(rig).rows if r["check"] == "the edge cache")
    assert "200 and 200, same body=True" in row["got"] and "lines=2" in row["got"] and row["verdict"] == "FAIL"
    rig.edge.broken.clear()
    rig.edge.broken.add("stale_cache")                               # the opposite fault: no request reaches the origin
    row = next(r for r in _suite(rig).rows if r["check"] == "the edge cache")
    assert "lines=0" in row["got"] and row["verdict"] == "FAIL"


def test_a_path_probe_needs_exactly_one_line_and_the_origins_own_answer(rig):
    rig.origin.double_probe = True
    run = _suite(rig)
    assert "start: path probe" in _failed(run) and "lines with the nonce=2" in next(r["got"] for r in run.rows if r["check"] == "start: path probe")


def test_the_control_of_the_path_shape_stands_beside_the_zero(rig):
    run = _suite(rig)
    row = next(r for r in run.rows if r["check"].startswith("control: a path with this id"))
    assert row["verdict"] == "PASS" and "new lines of that path=1" in row["got"]
    rig.edge.broken.add("empty:series")                              # the edge answers the metadata itself: no line, no control
    run = _suite(rig)
    assert row["check"] in _failed(run) and "the two refused CSV requests did not reach the instance" in _failed(run)


# ---------------------------------------------------------------- the phases
def test_phase0_passes_only_while_no_origin_value_is_set(rig):
    rig.edge.configured = False
    run = rig.new()
    K.mode_phase0(run)
    assert _failed(run) == [] and len(run.rows) == 5
    rig.edge.broken.add("words_5xx")                                # a 503 with another word is not "no value yet"
    run = rig.new()
    K.mode_phase0(run)
    assert len(_failed(run)) == 4
    rig.edge.broken.clear()
    rig.edge.configured = True
    run = rig.new()
    K.mode_phase0(run)
    assert len(_failed(run)) >= 2                                   # edge-status says configured, and the routes answer
    rig.edge.access = False                                         # another 5xx (502) is not the 503 of "no value yet"
    run = rig.new()
    K.mode_phase0(run)
    rows = [r for r in run.rows if r["check"].startswith("phase 0")]
    assert len(rows) == 4 and all(r["verdict"] == "FAIL" and r["got"].startswith("502") for r in rows)


def test_phase1_needs_the_502_and_a_request_that_did_not_reach_the_instance(rig):
    rig.edge.access = False
    run = rig.new()
    K.mode_phase1(run)
    v = _verdicts(run)
    assert _failed(run) == [], run.rows
    assert v["phase 1: the request did not reach the instance"] == "PASS"
    four = next(r for r in run.rows if r["check"].startswith("phase 1 counts as"))
    assert four["verdict"] == "SEEN", "two of the four conditions are not shown by this run: never a PASS"
    assert K.finish(run, "phase1") == 0
    rig.edge.broken.add("words_5xx")                                # a 502 with another word
    run = rig.new()
    K.mode_phase1(run)
    assert "phase 1: the origin values without the Access token" in _failed(run)
    assert next(r for r in run.rows if r["check"].startswith("phase 1 counts as"))["verdict"] == "FAIL"
    rig.edge.broken.clear()
    rig.edge.access = True                                          # the token is there already: 200, and the instance is reached
    run = rig.new()
    K.mode_phase1(run)
    f = _failed(run)
    assert "phase 1: the origin values without the Access token" in f and "phase 1: the request did not reach the instance" in f
    rig.edge.access = False
    rig.edge.broken.add("access_open")                              # no token and yet the origin answers: the lock is open
    run = rig.new()
    K.mode_phase1(run)
    assert "phase 1: the request did not reach the instance" in _failed(run)


# ---------------------------------------------------------------- the lock
LOCK_CASES = [
    # what the public name answers       the four rows   the summary row   reached   exit
    ("access403", "PASS", "PASS", "PASS", 0),
    ("access302", "PASS", "PASS", "PASS", 0),
    ("bare403", "SEEN", "NOTRUN", "PASS", 2),             # a firewall or bot page reads the same: not told apart
    ("bare401", "SEEN", "NOTRUN", "PASS", 2),
    ("basic401", "SEEN", "NOTRUN", "PASS", 2),            # AR-294 K1: "access" in WWW-Authenticate read as Access
    ("denied403", "SEEN", "NOTRUN", "PASS", 2),           # the two words apart in a page are not the mark
    ("challenge", "FAIL", "FAIL", "PASS", 1),             # cf-mitigated: a challenge is not Access
    ("redirect_elsewhere", "FAIL", "FAIL", "PASS", 1),    # a 302 that does not go to Access
    ("lookalike", "FAIL", "FAIL", "PASS", 1),             # a host that only CONTAINS the Access domain
    ("parked200", "FAIL", "FAIL", "PASS", 1),
    ("404", "FAIL", "FAIL", "PASS", 1),                   # no route
    ("530", "FAIL", "FAIL", "PASS", 1),                   # the tunnel is down
    ("open", "FAIL", "FAIL", "FAIL", 1),                  # the origin itself answers
    ("open_behind_403", "SEEN", "NOTRUN", "FAIL", 1),     # a bare 403 in front, and the instance is reached
]


@pytest.mark.parametrize("mode, rows, summary, reached, code", LOCK_CASES)
def test_the_lock_mode_reads_each_answer_of_the_public_name(rig, capsys, mode, rows, summary, reached, code):
    rig.public.mode = mode
    run = _lock(rig)
    asked = [r for r in run.rows if r["check"].startswith(("lock: no token", "lock: a token"))]
    assert len(asked) == 4 and {r["verdict"] for r in asked} == {rows}, [(r["verdict"], r["got"]) for r in asked]
    v = _verdicts(run)
    assert v["lock: the refusals are Access's own"] == summary and v["lock: nothing reached the instance"] == reached
    assert K.finish(run, "lock") == code
    shown = capsys.readouterr().out + "".join(p.read_text(encoding="utf-8") for p in (rig.root / "logs").iterdir())
    host = f"127.0.0.1:{rig.public.server_address[1]}"
    assert rig.public_url not in shown and host not in shown, "the origin's name is never shown"
    assert run.clean("redirect to " + host + "/login") == "redirect to <origin host>/login"


def test_one_bare_refusal_among_marked_ones_is_not_a_pass(rig):
    """The summary needs ALL four refusals to carry the mark."""
    rig.public.mode = "three_marked"
    run = _lock(rig)
    asked = [r["verdict"] for r in run.rows if r["check"].startswith(("lock: no token", "lock: a token"))]
    assert asked == ["PASS", "PASS", "PASS", "SEEN"]
    assert _verdicts(run)["lock: the refusals are Access's own"] == "NOTRUN" and K.finish(run, "lock") == 2


def test_a_lock_row_keeps_what_a_person_needs_to_find_the_request_in_the_access_log(rig):
    """AR-294 K3: the cf-ray value, the header names, the scheme of WWW-Authenticate and a cleaned page title."""
    run = _lock(rig)                                                 # the default public answer: Access's 403 page
    got = next(r["got"] for r in run.rows if r["check"] == "lock: no token")
    assert "cf-ray=abc-XYZ" in got and "title=Forbidden" in got and "header names=[" in got and "'cf-ray'" in got
    rig.public.mode = "basic401"
    got = next(r["got"] for r in _lock(rig).rows if r["check"] == "lock: no token")
    assert "www-authenticate scheme=Basic" in got and "Restricted" not in got


def test_an_open_lock_is_named_in_its_own_row_when_the_log_line_is_late(rig):
    """AR-294 K2: with the line written AFTER the answer, the requests that reached the instance must be counted in
    the row that is about them, not in the control that comes after."""
    rig.origin.late = 0.1
    K.SETTLE_SCALE = 0.1
    rig.public.mode = "open"
    run = _lock(rig)
    row = next(r for r in run.rows if r["check"] == "lock: nothing reached the instance")
    assert row["verdict"] == "FAIL" and "new lines=4" in row["got"], row
    assert "after loopback: this request is in the instance log" not in _failed(run)
    rig.edge.access = False
    rig.edge.broken.add("access_open")
    run = rig.new()
    K.mode_phase1(run)
    row = next(r for r in run.rows if r["check"] == "phase 1: the request did not reach the instance")
    assert row["verdict"] == "FAIL" and "new lines=2" in row["got"], row
    assert "after loopback: this request is in the instance log" not in _failed(run)


def test_the_lock_count_has_its_own_positive_control(rig):
    run = _lock(rig)
    row = next(r for r in run.rows if r["check"] == "lock: nothing reached the instance")
    assert row["verdict"] == "PASS" and "new lines=1" in row["got"], "exactly the line of the control after the requests"
    assert [r["check"] for r in run.rows].count("after loopback: this request is in the instance log") == 1


def test_the_origin_address_file_is_one_line_in_our_zone(tmp_path):
    p = tmp_path / "u.txt"
    p.write_text("https://tunnel-example.econdatalibrary.com\n", encoding="utf-8")
    assert K.read_origin(str(p)) == "https://tunnel-example.econdatalibrary.com"
    for bad in ("https://tunnel-example.econdatalibrary.org", "https://a.b.econdatalibrary.com", "http://tunnel-example.econdatalibrary.com",
                "https://tunnel-example.econdatalibrary.com/x", ""):
        p.write_text(bad + "\n", encoding="utf-8")
        with pytest.raises(SystemExit):
            K.read_origin(str(p))


# ---------------------------------------------------------------- the probe
def _probe_lines(rig):
    return (rig.root / "logs" / "soak_probe.log").read_text(encoding="ascii").splitlines()


def test_the_probe_writes_one_line_per_run_and_fails_when_the_edge_does(rig):
    assert K.mode_probe(rig.new()) == 0
    assert "path_probe=404/not_found/1" in _probe_lines(rig)[-1] and "total=120" in _probe_lines(rig)[-1]
    for switch in ("short_sources", "long_total", "soak_flag_off", "status_unconfigured", "probe_at_edge"):
        rig.edge.broken.clear()
        rig.edge.broken.add(switch)
        want = 0 if switch == "short_sources" else 1                 # 100 entries with total 100: the floor holds
        assert K.mode_probe(rig.new()) == want, switch
    rig.edge.broken.clear()
    rig.edge.access = False
    assert K.mode_probe(rig.new()) == 1
    rig.edge.access = True
    rig.edge.configured = False
    assert K.mode_probe(rig.new()) == 1
    lines = _probe_lines(rig)
    assert [ln.split()[1] for ln in lines] == ["PASS", "PASS", "FAIL", "FAIL", "FAIL", "FAIL", "FAIL", "FAIL"]
    assert SECRET not in "".join(lines)
    asked = [p for p in rig.edge.paths if p.startswith("/v1/sources")]
    assert asked and all("_n=" in p for p in asked), "the probe's sources request carries a nonce"


def test_the_probe_does_not_read_the_secret_and_fails_without_the_router(rig):
    os.remove(rig.root / "code" / "api" / "worker" / ".dev.vars")
    assert K.mode_probe(rig.new()) == 0
    soak = f"http://127.0.0.1:{rig.edge.server_address[1]}"
    run = K.Run(soak, str(rig.root))                                 # the default router address: nothing listens for this test
    run.router = ("127.0.0.1", 9)
    assert K.mode_probe(run) == 1 and "path_probe=no_router_or_log" in _probe_lines(rig)[-1]


@pytest.mark.parametrize("answer", [
    {"total": "7\n2026-10-10T00:00:00Z PASS forged", "sources": []},
    {"total": 120, "sources": "not a list"},
    {"total": "éè", "sources": [1]},
    "[]",
])
def test_whatever_the_edge_answers_the_probe_writes_one_fail_line(rig, monkeypatch, answer):
    """AR-291 C6: an answer with a line break wrote two lines, one a forged PASS; other answers ended the run
    with no line at all."""
    real = K.Run.edge

    def edge(self, target, **kw):
        if target.startswith("/v1/sources"):
            return 200, {}, (answer if isinstance(answer, str) else json.dumps(answer)).encode("utf-8"), 0.0
        return real(self, target, **kw)

    monkeypatch.setattr(K.Run, "edge", edge)
    assert K.mode_probe(rig.new()) == 1
    lines = _probe_lines(rig)
    assert len(lines) == 1 and lines[0].split()[1] == "FAIL" and "forged" not in lines[0]


def test_the_probe_line_is_one_line_whatever_its_fields_hold(rig, monkeypatch):
    """The last net: even if a field with a line break reached the line, the file gets ONE line."""
    monkeypatch.setattr(K, "_probe_fields", lambda run: (True, "edge-status=200\n2026-10-10T00:00:00Z PASS forged\ttab é"))
    assert K.mode_probe(rig.new()) == 0
    lines = _probe_lines(rig)
    assert len(lines) == 1 and lines[0].count("PASS") == 2 and "\t" not in lines[0] and lines[0].isascii()


def test_an_exception_in_the_probe_is_a_fail_line(rig, monkeypatch):
    def boom(run):
        raise ValueError("x\nPASS forged")

    monkeypatch.setattr(K, "_probe_fields", boom)
    assert K.mode_probe(rig.new()) == 1
    lines = _probe_lines(rig)
    assert len(lines) == 1 and lines[0].endswith("FAIL error=ValueError")


def test_the_whole_probe_command_runs_without_the_secret_file(rig, monkeypatch):
    os.remove(rig.root / "code" / "api" / "worker" / ".dev.vars")
    monkeypatch.setattr(K, "ROUTER", ("127.0.0.1", rig.origin.server_address[1]))
    soak = f"http://127.0.0.1:{rig.edge.server_address[1]}"
    monkeypatch.setattr(K.Run, "__init__", lambda self, s, r, router=None: _init(self, s, r, ("127.0.0.1", rig.origin.server_address[1])))
    assert K.main(["--mode", "probe", "--soak", soak, "--root", str(rig.root)]) == 0


_REAL_INIT = K.Run.__init__


def _init(self, soak, root, router):
    _REAL_INIT(self, soak, root, router)


# ---------------------------------------------------------------- the secret never leaves on the public side
def test_a_request_to_a_public_address_with_the_secret_is_refused(rig):
    run = rig.new()
    run.secret()
    with pytest.raises(RuntimeError):
        run.edge("/v1/sources", headers=[("X-Anything", SECRET)])
    with pytest.raises(RuntimeError):
        run.edge("/v1/sources?x=" + SECRET)
    with pytest.raises(RuntimeError):
        run.edge("/v1/sources", headers=[(K.SECRET_HEADER, "anything")])
    seen = []
    orig = Edge.do_GET

    def spy(self):
        seen.append({k.lower(): v for k, v in self.headers.items()})
        return orig(self)

    Edge.do_GET = spy
    try:
        _suite(rig)
    finally:
        Edge.do_GET = orig
    assert seen and all(K.SECRET_HEADER not in h and SECRET not in " ".join(h.values()) for h in seen)


def test_a_bad_dev_vars_file_stops_the_run_without_showing_it(rig):
    (rig.root / "code" / "api" / "worker" / ".dev.vars").write_text("ORIGIN_SECRET=short\n", encoding="ascii")
    with pytest.raises(SystemExit) as e:
        rig.new().secret()
    assert "short" not in str(e.value)
