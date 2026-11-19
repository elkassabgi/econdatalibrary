"""The probe child's sandbox (updater/release_watch.py run_child / child_main): release-watch design Revision 5, tests
B8.4 to B8.7 (C2), plus the env-order check.

Every case starts a REAL child (`python -I -c ...`) with a STUB probe written into tmp_path, so nothing here reaches
the network: the one HTTP case replaces requests' transport adapter inside the child. Planted secrets live only in
tmp_path; each case asserts the sandbox refused the route (COULD NOT CHECK, kind "sandbox") AND that the planted
value appears nowhere in the outcome."""
from __future__ import annotations

import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import release_watch as RW  # noqa: E402

SID, UID = RW.PHASE1_UNITS[0]
PLANT = "PLANTED-secret-4f1c"


@pytest.fixture()
def repo(tmp_path):
    """A planted tree: <repo>/.env, <repo>/sub/x.env, <repo>/sub/dir.env/<file>, <repo>/data/<file>."""
    r = tmp_path / "repo"
    (r / "sub" / "dir.env").mkdir(parents=True)
    (r / "data").mkdir()
    (r / ".env").write_text(f"UNCTAD_CLIENT_ID={PLANT}-cid\nUNCTAD_API_KEY={PLANT}-key\n", encoding="utf-8")
    (r / "sub" / "x.env").write_text(f"KEY={PLANT}\n", encoding="utf-8")
    (r / "sub" / "dir.env" / f"{PLANT}.txt").write_text(PLANT, encoding="utf-8")
    (r / "data" / f"{PLANT}.parquet").write_text(PLANT, encoding="utf-8")
    return r


def _probe(tmp_path, repo_root, body, name, **kw):
    """Write `def probe(sid, uid):` with `body` into a stub module and run it in a real sandboxed child."""
    d = tmp_path / "stubs"
    d.mkdir(exist_ok=True)
    src = "import io, os, json, pathlib\nR = %r\n\ndef probe(sid, uid):\n" % str(repo_root)
    src += "".join("    " + l + "\n" for l in body.strip().splitlines())
    (d / f"{name}.py").write_text(src, encoding="utf-8")
    kw.setdefault("token_re", None)          # stubs return free text; the production shape check has its own test
    return RW.run_child(SID, UID, guard_root=str(repo_root), stub=(str(d), name), **kw)


def _refused(o):
    assert o["p"] is None and o["kind"] == "sandbox", o
    assert PLANT not in json.dumps(o), o


# ------------------------------------------------------------------ B8.4 every bypass route
ROUTES = {
    "builtins_open": "return open(os.path.join(R, '.env')).read()",
    "io_open": "return io.open(os.path.join(R, 'sub', 'x.env')).read()",
    "os_open": "fd = os.open(os.path.join(R, '.env'), os.O_RDONLY)\nreturn os.read(fd, 200).decode()",
    "path_read_text": "return pathlib.Path(R, 'sub', 'x.env').read_text()",
    "os_scandir": "return ','.join(e.name for e in os.scandir(os.path.join(R, 'sub', 'dir.env')))",
    "os_scandir_data": "return ','.join(e.name for e in os.scandir(os.path.join(R, 'data')))",
    "os_path_exists": "return 'exists=' + str(os.path.exists(os.path.join(R, '.env')))",
    "os_stat": "return 'size=' + str(os.stat(os.path.join(R, 'sub', 'x.env')).st_size)",
    "creds": "import jobs.ingest_unctad_ds as j\nj.ROOT = R\nreturn '|'.join(j.creds())",
}


@pytest.mark.parametrize("route", sorted(ROUTES))
def test_b8_4_every_bypass_route_reads_could_not_check_sandbox(tmp_path, repo, route):
    _refused(_probe(tmp_path, repo, ROUTES[route], f"stub_{route}"))


def test_b8_4_control_the_same_stub_on_an_unguarded_file_succeeds(tmp_path, repo):
    """Negative control: the routes above fail because of the guard, not because the stub cannot read files. The
    file sits in the stub directory, which is on the read allow-list."""
    (tmp_path / "stubs").mkdir(exist_ok=True)
    (tmp_path / "stubs" / "plain.txt").write_text("readable", encoding="utf-8")
    o = _probe(tmp_path, repo, "return open(%r).read()" % str(tmp_path / "stubs" / "plain.txt"), "stub_control")
    assert o == {"p": "readable", "kind": "ok", "error": None}


# The routes the first review found open (each returned the planted value before the audit hook).
AUDIT_ROUTES = {
    "subprocess": "import subprocess\n"
                  "cmd = ['cmd', '/c', 'type', os.path.join(R, '.env')] if os.name == 'nt' else "
                  "['cat', os.path.join(R, '.env')]\n"
                  "return subprocess.run(cmd, capture_output=True, text=True).stdout",
    "shutil_copy2": "import shutil\nd = os.environ['HOME']\n"
                    "shutil.copy2(os.path.join(R, '.env'), os.path.join(d, 'c.txt'))\n"
                    "return open(os.path.join(d, 'c.txt')).read()",
    "_io_open": "import _io\nreturn _io.open(os.path.join(R, '.env')).read()",
    "io_fileio": "return io.FileIO(os.path.join(R, '.env')).read().decode()",
    "long_path_prefix": "return open('\\\\\\\\?\\\\' + os.path.join(R, 'data', %r)).read()" % (PLANT + ".parquet"),
    "trailing_dot": "return open(os.path.join(R, 'sub', 'x.env.')).read()",
    "env_local": "return open(os.path.join(R, '.env.local')).read()",
    "credentials_file": "return open(os.path.join(R, 'sub', 'credentials.json')).read()",
    "outside_allow_list": "return open(os.path.join(R, 'sub', 'plain.txt')).read()",
    "urllib_post": "import urllib.request\n"
                   "urllib.request.urlopen(urllib.request.Request('https://unctadstat-api.unctad.org/x', data=b'1'))\n"
                   "return 'posted'",
    "ctypes": "import ctypes\nlib = ctypes.CDLL('msvcrt' if os.name == 'nt' else 'libc.so.6')\nreturn str(lib)",
    "other_host": "import socket\nsocket.getaddrinfo('example.org', 443)\nreturn 'resolved'",
}


@pytest.mark.parametrize("route", sorted(AUDIT_ROUTES))
def test_b8_4_the_audit_hook_closes_the_routes_no_patch_sees(tmp_path, repo, route):
    (repo / ".env.local").write_text(f"K={PLANT}\n", encoding="utf-8")
    (repo / "sub" / "credentials.json").write_text(f'{{"k": "{PLANT}"}}', encoding="utf-8")
    (repo / "sub" / "plain.txt").write_text(PLANT, encoding="utf-8")
    _refused(_probe(tmp_path, repo, AUDIT_ROUTES[route], f"stub_a_{route}"))


def test_a_child_token_that_is_not_a_release_token_is_refused(tmp_path, repo):
    """The parent's shape check: whatever a bypass read, it cannot leave through P."""
    o = _probe(tmp_path, repo, "return 'UNCTAD_API_KEY=" + PLANT + "'", "stub_shape", token_re=RW.TOKEN_RE)
    assert o["p"] is None and o["kind"] == "shape" and PLANT not in json.dumps(o)
    o = _probe(tmp_path, repo, "return '10003|2026-07-15T11:40:26'", "stub_shape_ok", token_re=RW.TOKEN_RE)
    assert o == {"p": "10003|2026-07-15T11:40:26", "kind": "ok", "error": None}


def test_the_production_probe_runs_inside_the_sandbox_offline(tmp_path, repo):
    """The REAL chain (registry.to_units, strategies.get, detect_change, _unctad.current_vintage, requests) under
    every guard, with the transport replaced so nothing leaves the machine: the guards must not refuse the probe
    production runs. The URL it asks for is pinned too."""
    body = """
import requests, requests.adapters, requests.models
seen = []
def fake_send(self, request, **kw):
    seen.append((request.method, request.url))
    r = requests.models.Response()
    r.status_code, r.request, r.url = 200, request, request.url
    r._content = b'{"version": 10003, "lastUpdated": "2026-07-15T11:40:26"}'
    return r
requests.adapters.HTTPAdapter.send = fake_send
import updater.release_watch as w
p = w._production_probe(sid, uid)
assert seen == [('GET', 'https://unctadstat-api.unctad.org/api/reportMetadata/US.BiotradeMerch')], seen
return p
"""
    o = _probe(tmp_path, repo, body, "stub_production", token_re=RW.TOKEN_RE)
    assert o == {"p": "10003|2026-07-15T11:40:26", "kind": "ok", "error": None}, o


def test_probe_hosts_is_the_metadata_host():
    from urllib.parse import urlparse
    import jobs.ingest_unctad_ds as j
    assert RW.PROBE_HOSTS == {urlparse(j.META).hostname}


def test_audit_rules_for_raw_http(monkeypatch):
    monkeypatch.setitem(RW._GUARD, "allowed", ())

    class Conn:
        host = "unctadstat-api.unctad.org"
    assert RW._audit_reason("http.client.send", (Conn(), b"GET /api/x HTTP/1.1\r\n")) is None
    assert "other than GET" in RW._audit_reason("http.client.send", (Conn(), b"POST /x HTTP/1.1\r\n"))
    Conn.host = "example.org"
    assert "not a probe host" in RW._audit_reason("http.client.send", (Conn(), b"GET / HTTP/1.1\r\n"))
    assert RW._audit_reason("socket.getaddrinfo", ("unctadstat-api.unctad.org", 443)) is None
    assert RW._audit_reason("socket.getaddrinfo", ("example.org", 443))
    assert RW._audit_reason("urllib.Request", ("https://x", None, {}, "PUT"))
    assert RW._audit_reason("urllib.Request", ("https://x", None, {}, None)) is None


def test_b8_4_a_probe_that_swallows_the_refusal_still_reads_sandbox(tmp_path, repo):
    body = ("try:\n    open(os.path.join(R, '.env')).read()\nexcept BaseException:\n    pass\n"
            "return 'looks fine'")
    _refused(_probe(tmp_path, repo, body, "stub_swallow"))


# ------------------------------------------------------------------ B8.5 netrc / trust_env
def test_b8_5_a_planted_netrc_never_reaches_the_request(tmp_path, repo):
    body = """
import requests, requests.adapters, requests.models
home = os.path.expanduser('~')
for n in ('.netrc', '_netrc'):
    with open(os.path.join(home, n), 'w') as fh:
        fh.write('machine unctadstat-api.unctad.org login PLANTEDUSER password %s\\n')
seen = {}
def fake_send(self, request, **kw):
    seen['auth'] = request.headers.get('Authorization')
    r = requests.models.Response()
    r.status_code, r._content, r.request, r.url = 200, b'{}', request, request.url
    return r
requests.adapters.HTTPAdapter.send = fake_send
requests.get('https://unctadstat-api.unctad.org/api/reportMetadata/US.BiotradeMerch', timeout=5)
s = requests.Session()
s.get('https://unctadstat-api.unctad.org/api/reportMetadata/US.BiotradeMerch', timeout=5)
return 'auth=' + ('present' if seen.get('auth') else 'absent') + ' trust_env=' + str(s.trust_env)
""" % PLANT
    o = _probe(tmp_path, repo, body, "stub_netrc")
    assert o == {"p": "auth=absent trust_env=False", "kind": "ok", "error": None}, o


# ------------------------------------------------------------------ B8.6 the environment is an allow-list
def test_b8_6_unlisted_parent_env_never_reaches_the_child(tmp_path, repo):
    env = dict(os.environ, UNCTAD_API_KEY="planted", FOO_SECRET="planted", GH_TOKEN="planted",
               PYTHONPATH=str(repo))
    body = "return json.dumps([os.environ.get(k) for k in ('UNCTAD_API_KEY', 'FOO_SECRET', 'GH_TOKEN', 'PYTHONPATH')])"
    o = _probe(tmp_path, repo, body, "stub_env", parent_env=env)
    assert o["kind"] == "ok" and json.loads(o["p"]) == [None, None, None, None], o


def test_b8_6_the_child_env_is_exactly_the_allow_list(tmp_path):
    env = RW.child_env(str(tmp_path / "sb"), {"PATH": "p", "SystemRoot": "s", "TEMP": "t", "TMP": "t",
                                              "FOO_SECRET": "x", "R2_SECRET": "x", "HOME": "real-home"})
    assert set(env) == {"PATH", "SYSTEMROOT", "TEMP", "TMP", "AQUEDUCT_BACKEND", "AQUEDUCT_DATA_ROOT",
                        "AQUEDUCT_STATE_DIR", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "ECONDL_CATALOG"}
    assert env["HOME"] != "real-home" and env["AQUEDUCT_BACKEND"] == "local"
    assert not os.path.exists(env["ECONDL_CATALOG"])


def test_env_is_set_before_config_is_imported(tmp_path, repo):
    body = ("from updater import config\n"
            "return json.dumps([config.BACKEND, config.DATA_ROOT == os.path.abspath(os.environ['AQUEDUCT_DATA_ROOT']),"
            " config.STATE_DIR == os.path.abspath(os.environ['AQUEDUCT_STATE_DIR'])])")
    o = _probe(tmp_path, repo, body, "stub_config")
    assert o["kind"] == "ok" and json.loads(o["p"]) == ["local", True, True], o


# ------------------------------------------------------------------ B8.7 no write, no state, no POST
WRITES = {
    "blob_write": "from updater import blob\nblob.write_bytes_atomic(os.path.join(os.environ['TEMP'], 'x.bin'), b'1')\n"
                  "return 'wrote'",
    "blob_read": "from updater import blob\nreturn str(blob.read_bytes(os.path.join(os.environ['TEMP'], 'x.bin')))",
    "blob_class": "from updater import blob\nreturn str(blob.LocalBlob(os.environ['TEMP']).get('k'))",
    "merge": "from updater import merge\nmerge.merge_and_write('x.parquet', None)\nreturn 'merged'",
    "sqlite": "import sqlite3\nsqlite3.connect(':memory:')\nreturn 'connected'",
    "post": "import requests\nrequests.post('https://unctadstat-user-api.unctad.org/x/cur/Facts')\nreturn 'posted'",
    "session_post": "import requests\nrequests.Session().post('https://unctadstat-user-api.unctad.org/x')\n"
                    "return 'posted'",
}


@pytest.mark.parametrize("what", sorted(WRITES))
def test_b8_7_writes_state_and_post_read_could_not_check(tmp_path, repo, what):
    _refused(_probe(tmp_path, repo, WRITES[what], f"stub_w_{what}"))


# ------------------------------------------------------------------ the parent's own mapping
def test_every_failure_kind_is_could_not_check(tmp_path, repo):
    assert _probe(tmp_path, repo, "raise RuntimeError('boom')", "stub_raise")["kind"] == "probe"
    assert _probe(tmp_path, repo, "return None", "stub_none")["kind"] == "probe"
    assert _probe(tmp_path, repo, "import time\ntime.sleep(30)", "stub_slow", timeout=3)["kind"] == "timeout"
    assert _probe(tmp_path, repo, "os._exit(3)", "stub_exit")["kind"] == "exit"
    assert _probe(tmp_path, repo, "os.write(1, b'noise\\n')\nreturn 'x'", "stub_noise")["kind"] == "protocol"
    assert _probe(tmp_path, repo, "print('chatty')\nreturn 'p|1'", "stub_print") == {
        "p": "p|1", "kind": "ok", "error": None}, "a fetcher's print goes to stderr, not the protocol line"


def test_only_phase_1_units_are_probed(tmp_path, repo):
    d = tmp_path / "stubs"
    d.mkdir()
    (d / "stub_any.py").write_text("def probe(sid, uid):\n    return 'p'\n", encoding="utf-8")
    o = RW.run_child("unctad_trademerchtotal", "_all", guard_root=str(repo), stub=(str(d), "stub_any"))
    assert o["kind"] == "probe" and o["error"] == "probe: LookupError" and "not a phase-1 unit" in o["detail"]


def test_the_childs_error_text_never_becomes_the_recorded_error(tmp_path, repo):
    """Review round 2: a probe that raised with a directory listing in its message carried it into last_error, the
    beat and the mail. The recorded error is a fixed phrase and a type from a closed list."""
    o = _probe(tmp_path, repo, "raise RuntimeError('LISTING=' + %r)" % PLANT, "stub_err_text")
    assert o["error"] == "probe: RuntimeError" and PLANT not in o["error"]
    body = "class Leak%s(Exception):\n    pass\nraise Leak%s('x')" % ("x" * 3, "x" * 3)
    assert _probe(tmp_path, repo, body, "stub_err_type")["error"] == "probe: another error"
    forged = "os.write(1, b'{\"p\": null, \"error\": \"WatchSideEffect' + %r.encode() + b'\"}\\n')\nos._exit(0)" % PLANT
    o = _probe(tmp_path, repo, forged, "stub_err_forged")
    assert o["kind"] == "sandbox" and PLANT not in o["error"]


def test_a_raw_socket_to_an_unresolved_address_is_refused(tmp_path, repo):
    """Review round 2: DNS was guarded but connect() to a literal address was not."""
    body = ("import socket\ns = socket.socket()\ns.settimeout(0.5)\n"
            "s.connect(('192.0.2.1', 443))\nreturn 'connected'")
    _refused(_probe(tmp_path, repo, body, "stub_raw_socket"))


def test_listing_outside_the_allow_list_is_refused(tmp_path, repo):
    _refused(_probe(tmp_path, repo, "return ','.join(os.listdir(R))", "stub_list_repo"))


def test_audit_rules_for_sockets(monkeypatch):
    monkeypatch.setitem(RW._GUARD, "ips", {"203.0.113.7"})
    assert RW._audit_reason("socket.connect", (None, ("203.0.113.7", 443))) is None
    assert RW._audit_reason("socket.connect", (None, ("192.0.2.1", 443)))
    assert RW._audit_reason("socket.connect", (None, "/tmp/unix.sock"))
    assert RW._audit_reason("socket.bind", (None, ("0.0.0.0", 0)))
    assert RW._audit_reason("socket.bind", (None, ("::1", 0))) is None, "urllib3's import-time IPv6 test"
    assert RW.main(["--probe", "unctad_trademerchtotal", "_all"]) == 2
