"""The two ISTAT tools take tokens from the ONE shared ISTAT budget (DESIGN 7; review B phase 1, items 4 and 5).

tools/catalog_istat_flows.py: the names GET takes a token per host and does NOT follow redirects (urllib followed
up to 10); a DRY RUN sends nothing. Its main() opens the catalogue for write, so these tests REPLACE
catalog_path.connect with an in-memory catalogue: no test opens a real catalogue (R1940).
tools/probe_istat_hosts.py: every connect to an ISTAT host takes a token (the 127.0.0.1 control none), so
`--every 0` - which used to give about 180 ISTAT connects a minute - is held to the budget.

Nothing leaves this machine: fake connects, a 127.0.0.1 HTTP server, and tests/conftest.py's network guard."""
from __future__ import annotations

import http.server
import json
import os
import sqlite3
import sys
import threading
import urllib.error

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from jobs import _istat_pacer  # noqa: E402
import catalog_istat_flows as cat  # noqa: E402
import probe_istat_hosts as probe  # noqa: E402
from _istat_pacing_fakes import Clock, Recorder, max_in_closed_window  # noqa: E402

real = pytest.mark.istat_pacer_real
NAMES_XML = (b'<m:Structure xmlns:m="m" xmlns:s="s" xmlns:c="c"><s:Dataflow id="101_1015">'
             b'<c:Name xml:lang="en">Crops</c:Name></s:Dataflow></m:Structure>')


# ------------------------------------------------------------------ the catalog tool

class _Opener:
    """A urllib opener stand-in: per URL a body or an HTTPError (a 302 that is NOT followed)."""

    def __init__(self, events, answers):
        self.events, self.answers = events, answers

    def open(self, req, timeout=None):
        url = req.full_url
        self.events.append(("send", _istat_pacer.host_of(url), url))
        a = self.answers(url)
        if isinstance(a, BaseException):
            raise a
        return type("R", (), {"read": lambda self_: a})()


@pytest.fixture
def cat_rig(tmp_path, monkeypatch):
    events = []
    clock = Clock()
    monkeypatch.setattr(cat, "NAMES_CACHE", str(tmp_path / "_dataflow_names.json"))
    monkeypatch.setattr(cat, "STORE", str(tmp_path / "store"))
    monkeypatch.setattr(cat, "time", clock)
    rec = Recorder(clock, events)
    monkeypatch.setattr(_istat_pacer, "_TEST_OVERRIDE", rec)
    opened = []

    def fake_connect(*a, **k):                              # NEVER the real catalogue
        opened.append(k)
        con = sqlite3.connect(":memory:")
        con.execute("create table license(license_id text, reservable int)")
        con.execute("insert into license values(?, 1)", (cat.LICENSE_ID,))
        return con

    monkeypatch.setattr(cat.catalog_path, "connect", fake_connect)
    return events, rec, opened, tmp_path


def test_catalog_dry_run_sends_nothing_even_with_no_names_cache(cat_rig, monkeypatch, capsys):
    events, rec, opened, _tmp = cat_rig
    monkeypatch.setattr(_istat_pacer, "no_redirect_opener",
                        lambda *h: _Opener(events, lambda u: AssertionError("a dry run sent a request")))
    monkeypatch.setattr(sys, "argv", ["catalog_istat_flows.py"])
    rc = cat.main()
    assert rc == 1 and opened, "premise: the fake catalogue was opened and the run reached the split map"
    assert rec.n == 0 and not [e for e in events if e[0] == "send"], events
    assert "a dry run sends nothing" in capsys.readouterr().out


def test_catalog_names_take_a_token_per_host_and_a_302_is_not_followed(cat_rig, monkeypatch):
    events, rec, _opened, tmp = cat_rig

    def answers(url):
        if url.startswith(cat.HOSTS[0]):
            return urllib.error.HTTPError(url, 302, "Found", {"Location": url + "&x=1"}, None)
        return NAMES_XML

    monkeypatch.setattr(_istat_pacer, "no_redirect_opener", lambda *h: _Opener(events, answers))
    names = cat.dataflow_names(refresh=True, send=True)
    assert names == {"101_1015": "Crops"}
    sends = [e for e in events if e[0] == "send"]
    assert len(sends) == 2 and len({s[1] for s in sends}) == 2, sends          # one per host, nothing followed
    tokens = [e for e in events if e[0] == "token"]
    assert [(t[1], t[3], t[4]) for t in tokens] == [("tool:catalog", "http", "dataflow")] * 2
    kinds = [e[0] for e in events if e[0] in ("token", "send")]
    assert kinds == ["token", "send", "token", "send"], kinds
    outs = [e[2] for e in events if e[0] == "outcome"]
    assert outs[0].startswith("302->") and outs[0].endswith(":not-followed") and outs[1] == "200", outs
    assert json.loads((tmp / "_dataflow_names.json").read_text(encoding="utf-8")) == names


def test_catalog_the_real_opener_does_not_follow_a_redirect(cat_rig, monkeypatch):
    """The tool's REAL opener against a 127.0.0.1 server that answers 302: one request reaches the server, not two
    (urllib's default opener follows up to 10)."""
    events, _rec, _opened, _tmp = cat_rig
    seen = []

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.path)
            if "moved" in self.path:
                body = NAMES_XML
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(302)
            self.send_header("Location", "/moved")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    try:
        monkeypatch.setattr(cat, "HOSTS", (f"http://127.0.0.1:{srv.server_address[1]}/SDMXWS/rest/",))
        assert cat.dataflow_names(refresh=True, send=True) == {}
    finally:
        srv.shutdown()
        srv.server_close()
    assert seen == ["/SDMXWS/rest/dataflow/IT1?detail=allstubs"], seen


# ------------------------------------------------------------------ the probe tool

@pytest.fixture
def probe_rig(tmp_path, monkeypatch):
    clock = Clock()
    events = []
    monkeypatch.setattr(probe, "time", clock)
    monkeypatch.setattr(probe, "LOG", str(tmp_path / "logs" / "istat_host_probe.tsv"))

    class _Sock:
        def close(self):
            pass

    def fake_connect(addr, timeout=None):
        events.append(("connect", addr[0], addr[1], clock.time()))
        clock.t += 0.2
        if addr[0] == "127.0.0.1":
            raise ConnectionRefusedError("planted: the negative control refuses")
        return _Sock()

    import socket
    monkeypatch.setattr(socket, "create_connection", fake_connect)
    return clock, events, tmp_path


def test_probe_takes_a_token_per_istat_connect_and_none_for_the_control(probe_rig, monkeypatch):
    clock, events, tmp = probe_rig
    monkeypatch.setattr(_istat_pacer, "_TEST_OVERRIDE", Recorder(clock, events))
    with open(tmp / "sample.tsv", "w+", encoding="utf-8", newline="") as fh:
        got = probe.sample(fh)
        fh.seek(0)
        text = fh.read()
    assert got == {"sdmx": True, "esplora": True, "www": True, "nullport": False}
    toks = [e for e in events if e[0] == "token"]
    assert [t[2] for t in toks] == ["sdmx.istat.it", "esploradati.istat.it", "www.istat.it"], toks
    assert {(t[1], t[3]) for t in toks} == {("tool:probe", "tcp")}
    conns = [e for e in events if e[0] == "connect"]
    assert [c[1] for c in conns] == ["sdmx.istat.it", "esploradati.istat.it", "www.istat.it", "127.0.0.1"]
    rows = [ln.split("\t") for ln in text.splitlines()]
    assert len(rows) == 4 and all(len(r) == 9 for r in rows), rows
    assert rows[3][8] == "" and all(r[8] for r in rows[:3]), rows           # token start only for ISTAT rows


@real
def test_probe_every_0_is_held_to_the_budget(probe_rig, monkeypatch):
    """`--every 0` for 10 fake minutes with the REAL pacer (fail-safe here, as on Linux CI: one token per 64 s).
    Unpaced this loop made about 180 ISTAT connects a minute; now no closed 60 s holds more than the budget, and
    no two ISTAT connects start less than the fail-safe gap apart."""
    clock, events, _tmp = probe_rig
    monkeypatch.setattr(_istat_pacer, "msvcrt", None)
    monkeypatch.setattr(_istat_pacer, "_is_wsl", lambda: False)
    _istat_pacer._reset_process_state()
    probe.run(hours=10 / 60.0, every_min=0.0)
    starts = [c[3] for c in events if c[0] == "connect" and c[1] != "127.0.0.1"]
    assert len(starts) >= 6, starts
    assert max_in_closed_window(starts) <= _istat_pacer.BUDGET_PER_MIN
    assert min(b - a for a, b in zip(starts, starts[1:])) >= _istat_pacer.FAILSAFE_GAP_S - 1e-6


def test_probe_summarise_reads_old_8_column_and_new_9_column_rows(probe_rig, capsys):
    _clock, _events, tmp = probe_rig
    log = tmp / "logs" / "istat_host_probe.tsv"
    log.parent.mkdir(parents=True)
    rows = [probe.HEADER.rstrip("\n")]
    for h in range(13):
        ts = f"2026-11-25T{h:02d}:00:00+00:00"
        for lbl, host, port, role, ok in (("sdmx", "sdmx.istat.it", 443, "subject", 1),
                                          ("esplora", "esploradati.istat.it", 443, "subject", 1),
                                          ("www", "www.istat.it", 443, "control_positive", 1),
                                          ("nullport", "127.0.0.1", 9, "control_negative", 0)):
            extra = "" if h % 2 else f"\t{ts}"                              # odd hours: the old 8-column rows
            rows.append(f"{ts}\t{lbl}\t{host}\t{port}\t{role}\t{ok}\t0.100\t{extra}")
    log.write_text("\n".join(rows) + "\n", encoding="utf-8")
    assert probe.summarise() == 0
    assert "samples: 13" in capsys.readouterr().out
