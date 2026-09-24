"""tools/selfhost/swap.py - the blue/green catalogue swap. The instances are tests/_fake_origin.py (real
processes serving the placed slot files), the router is the real one."""
import contextlib
import http.client
import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import router  # noqa: E402
import swap  # noqa: E402
from test_selfhost_origin_copies import _catalogue  # noqa: E402

FAKE = os.path.join(ROOT, "tests", "_fake_origin.py")
SLOTS = {"CATALOG": "aaaa.sqlite", "CATALOG_CLIMATE": "bbbb.sqlite"}
SECRET = "swap-test-secret"


def _port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _alive(pid):
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _eventually(pred, timeout=20.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.2)
    return pred()


def _get(port, path, secret=SECRET, timeout=30):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        c.request("GET", path, headers={"x-econ-origin-secret": secret})
        r = c.getresponse()
        return r.status, json.loads(r.read() or b"{}")
    finally:
        c.close()


@pytest.fixture
def rig(tmp_path):
    """A catalogue, a worker dir with .dev.vars, a router with blue ACTIVE (a fake instance already running
    on a placed copy, recorded in instances.json) and green idle."""
    cat = tmp_path / "catalog.db"
    _catalogue(cat)
    worker = tmp_path / "worker"
    worker.mkdir()
    (worker / ".dev.vars").write_text(f'ORIGIN_SECRET="{SECRET}"\nOTHER=1\n')
    work = tmp_path / "work"
    work.mkdir()
    ports = {"blue": _port(), "green": _port()}
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"active": "blue", "targets": {n: f"http://127.0.0.1:{p}" for n, p in ports.items()}}))
    procs = []

    def command(extra=()):
        def build(port, persist):
            return [sys.executable, "-B", FAKE, str(port), persist, json.dumps(SLOTS), *extra]
        return build

    # blue: placed and started the way a previous swap would have
    copies = tmp_path / "copies0"
    swap.origin_copies.build(str(cat), str(copies))
    blue_persist = str(work / "persist-blue-20260101T000000Z")
    swap.place(str(copies), blue_persist, SLOTS)
    blue = swap.start(command()(ports["blue"], blue_persist), str(worker), str(work / "blue.log"))
    procs.append(blue)
    swap.save_instances(str(work), {"blue": {"pid": blue.pid, "persist": os.path.abspath(blue_persist),
                                             "port": ports["blue"], "state": "active"}})
    stale = work / "persist-green-20250101T000000Z"          # an older generation nothing uses
    stale.mkdir()
    rt = router.serve(str(state), 0)
    threading.Thread(target=rt.serve_forever, daemon=True).start()
    assert _eventually(lambda: swap.port_in_use(ports["blue"]))
    r = dict(cat=str(cat), worker=str(worker), work=str(work), state=str(state), ports=ports, command=command,
             router_port=rt.server_address[1], router_url=f"http://127.0.0.1:{rt.server_address[1]}",
             blue=blue, stale=str(stale), procs=procs)
    yield r
    rt.shutdown()
    rt.server_close()
    for inst in swap.load_instances(str(work)).values():
        swap.stop(inst["pid"])
    for p in procs:
        swap.stop(p.pid)


def _swap(rig, **kw):
    args = dict(catalogue=rig["cat"], state_path=rig["state"], router_url=rig["router_url"], work=rig["work"],
                worker_dir=rig["worker"], command=rig["command"](), slots=SLOTS, health_timeout=30,
                drain_timeout=10, log=lambda *_: None)
    args.update(kw)
    return swap.swap(**args)


def test_a_swap_flips_to_the_new_copy_and_stops_the_old(rig):
    status, body = _get(rig["router_port"], "/v1/sources")
    assert status == 200 and body["port"] == rig["ports"]["blue"]
    out = _swap(rig)
    assert out["active"] == "green" and out["retired"] == "blue" and out["old_stopped"] is True
    assert out["counts"]["primary"]["series"] + out["counts"]["climate"]["series"] == out["counts"]["catalogue_series"]
    assert set(out["health"]) == {"sources", "primary", "climate"}, "both copies were asked for by id"
    assert json.load(open(rig["state"]))["active"] == "green"
    status, body = _get(rig["router_port"], "/v1/series/noaa:a.metadata.json")
    assert status == 200 and body["port"] == rig["ports"]["green"], "the router now answers from green"
    assert _eventually(lambda: not _alive(rig["blue"].pid)), "blue was stopped after the drain"
    inst = swap.load_instances(rig["work"])
    assert inst["green"]["state"] == "active" and inst["blue"]["state"] == "stopped"
    assert os.path.isdir(inst["blue"]["persist"]), "the retired persist dir is kept for a rollback"
    assert not os.path.exists(rig["stale"]) and out["pruned"] == [rig["stale"]], "older generations go"
    assert not any(d.startswith("copies-") for d in os.listdir(rig["work"])), "the copies folder is cleaned"
    placed = os.listdir(os.path.join(inst["green"]["persist"], swap.SLOT_DIR))
    assert sorted(placed) == sorted(SLOTS.values()), "rollback-journal mode: no -wal / -shm beside the slots"


def test_swapped_slots_fail_the_health_check_and_leave_the_router_alone(rig):
    wrong = {"CATALOG": SLOTS["CATALOG_CLIMATE"], "CATALOG_CLIMATE": SLOTS["CATALOG"]}
    with pytest.raises(swap.SwapRefused, match="not behind the binding|unproven"):
        _swap(rig, slots=wrong)
    assert json.load(open(rig["state"]))["active"] == "blue"
    assert _alive(rig["blue"].pid), "the active instance never stops because of a bad build"
    assert set(swap.load_instances(rig["work"])) == {"blue"}
    assert not any(d.startswith("persist-green-2026") for d in os.listdir(rig["work"])), "the failed dir is removed"
    assert not swap.port_in_use(rig["ports"]["green"]), "the failed instance is stopped"


@pytest.mark.parametrize("extra,match", [(("--unmarked",), "origin mark"), (("--die",), "exited")])
def test_an_unmarked_or_dead_instance_is_refused(rig, extra, match):
    with pytest.raises(swap.SwapRefused, match=match):
        _swap(rig, command=rig["command"](extra))
    assert json.load(open(rig["state"]))["active"] == "blue"


@pytest.mark.parametrize("gate,primary,climate", [
    ("ecb", "noaa_direct:c", "noaa:"),                 # a gated SOURCE is skipped (not listed)
    ("ecb:x,ecb:y", "noaa_direct:c", "noaa:"),         # gated SERIES of a listed source: 451, next one
    ("noaa", "ecb:", None),                            # the climate copy wholly gated: primary alone proves it
])
def test_gated_samples_are_skipped_not_failed(rig, gate, primary, climate):
    out = _swap(rig, command=rig["command"](("--gate", gate)))
    assert out["active"] == "green"
    assert out["health"]["primary"].startswith(primary)
    assert (out["health"]["climate"] is None) if climate is None else out["health"]["climate"].startswith(climate)


def test_a_wholly_gated_primary_is_refused(rig):
    with pytest.raises(swap.SwapRefused, match="unproven"):
        _swap(rig, command=rig["command"](("--gate", "ecb,noaa_direct")))
    assert json.load(open(rig["state"]))["active"] == "blue"


def test_an_answering_idle_port_is_refused(rig):
    s = socket.socket()
    s.bind(("127.0.0.1", rig["ports"]["green"]))
    s.listen()
    try:
        with pytest.raises(swap.SwapRefused, match="already answers"):
            _swap(rig)
    finally:
        s.close()


def test_a_drain_that_does_not_finish_leaves_the_old_instance_running(rig):
    t = threading.Thread(target=lambda: _get(rig["router_port"], "/slow?s=8"), daemon=True)
    t.start()
    assert _eventually(lambda: _get(rig["router_port"], router.STATUS_PATH)[1]["inflight"].get("blue") == 1, 10)
    out = _swap(rig, drain_timeout=1)
    assert out["active"] == "green" and out["old_stopped"] is False
    assert _alive(rig["blue"].pid), "a long request is never cut off to keep a schedule"
    assert swap.load_instances(rig["work"])["blue"]["state"] == "draining"
    assert os.path.isdir(swap.load_instances(rig["work"])["blue"]["persist"])
    t.join(15)


def test_stop_ends_the_whole_process_tree(tmp_path):
    (tmp_path / ".dev.vars").write_text(f"ORIGIN_SECRET={SECRET}\n")
    cat = tmp_path / "c.db"
    _catalogue(cat)
    swap.origin_copies.build(str(cat), str(tmp_path / "cp"))
    persist = str(tmp_path / "persist-x")
    swap.place(str(tmp_path / "cp"), persist, SLOTS)
    pidfile = tmp_path / "child.pid"
    port = _port()
    p = swap.start([sys.executable, "-B", FAKE, str(port), persist, json.dumps(SLOTS), "--child", str(pidfile)],
                   str(tmp_path), str(tmp_path / "log"))
    assert _eventually(lambda: pidfile.exists() and pidfile.read_text().strip() != "")
    child = int(pidfile.read_text())
    assert _alive(child) and _alive(p.pid), "positive control: both are running"
    swap.stop(p.pid)
    assert _eventually(lambda: not _alive(p.pid) and not _alive(child)), "the child (workerd) went too"


def test_place_refuses_an_existing_persist_dir_and_leaves_no_wal(tmp_path):
    cat = tmp_path / "c.db"
    _catalogue(cat)
    con = sqlite3.connect(cat)
    con.execute("PRAGMA journal_mode=WAL")
    con.close()
    swap.origin_copies.build(str(cat), str(tmp_path / "cp"))
    persist = tmp_path / "persist"
    persist.mkdir()
    with pytest.raises(swap.SwapRefused, match="fresh"):
        swap.place(str(tmp_path / "cp"), str(persist), SLOTS)
    swap.place(str(tmp_path / "cp"), str(tmp_path / "persist2"), SLOTS)
    for f in SLOTS.values():
        p = tmp_path / "persist2" / swap.SLOT_DIR / f
        assert open(p, "rb").read(20)[18:20] == b"\x01\x01", "file format bytes say rollback journal, not WAL"


def test_the_secret_comes_from_dev_vars(tmp_path):
    (tmp_path / ".dev.vars").write_text("A=1\nORIGIN_SECRET='quoted value'\n")
    assert swap.read_secret(str(tmp_path)) == "quoted value"
    (tmp_path / ".dev.vars").write_text("ORIGIN_SECRET=\n")
    with pytest.raises(swap.SwapRefused, match="ORIGIN_SECRET"):
        swap.read_secret(str(tmp_path))


def test_a_target_that_is_not_local_is_refused():
    assert swap.port_of("http://127.0.0.1:8801") == 8801
    for bad in ("http://10.0.0.5:8801", "http://127.0.0.1", "https://example.org:443"):
        with pytest.raises(swap.SwapRefused):
            swap.port_of(bad)


def test_after_t0_the_copies_are_made_under_the_writer_lock(tmp_path, monkeypatch):
    from core import catalog_path, cutover
    seen = []

    @contextlib.contextmanager
    def lock():
        seen.append("held")
        yield

    monkeypatch.setattr(catalog_path, "writer_lock", lock)
    monkeypatch.setattr(swap.origin_copies, "build", lambda c, o: seen.append("build") or {"ok": 1})
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    swap.build_copies("c", "o")
    assert seen == ["build"], "before T0 no lock (CI's writer runs elsewhere)"
    seen.clear()
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    swap.build_copies("c", "o")
    assert seen == ["held", "build"]


def test_the_slot_dir_is_the_one_wrangler_uses():
    """d1_slots.mjs and the origin both use <persist>/v3/d1/miniflare-D1DatabaseObject."""
    src = open(os.path.join(ROOT, "tools", "selfhost", "d1_slots.mjs"), encoding="utf-8").read()
    assert '"v3", "d1", "miniflare-D1DatabaseObject"' in src
    assert swap.SLOT_DIR == os.path.join("v3", "d1", "miniflare-D1DatabaseObject")
