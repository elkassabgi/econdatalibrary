"""tools/selfhost/swap.py - the blue/green catalogue swap. The instances are tests/_fake_origin.py (real
processes serving the placed slot files, marked with their instance id), the router is the real one."""
import contextlib
import http.client
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import types

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
    """A free port OUTSIDE the ephemeral range (Windows: 49152-65535). A port the OS hands out for bind(0) is an
    ephemeral one, and a client that connects to a FREE ephemeral port on localhost can be given that same
    port as its source - a TCP self-connect, which reads back its own request: the BadStatusLine
    'GET /v1/sources HTTP/1.1' that failed swap tests under load three times (R1211 finding 5)."""
    import random
    for _ in range(200):
        p = random.randint(20000, 39999)
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", p))
            except OSError:
                continue
            return p
    raise RuntimeError("no free port in 20000-39999")


def _move_green(rig):
    """Green to a fresh port, written to the rig's state file (another process on the machine took the old one)."""
    rig["ports"]["green"] = _port()
    st = json.load(open(rig["state"]))
    st["targets"]["green"] = f"http://127.0.0.1:{rig['ports']['green']}"
    with open(rig["state"], "w") as f:
        json.dump(st, f)


def _hold_green(rig, sock):
    """Bind `sock` to green's port ON PURPOSE. When another process already has that port (WinError 10048 under
    concurrent suites - 2026-09-24 full_main13), move green first: the test is about THIS process holding it."""
    for _ in range(5):
        try:
            sock.bind(("127.0.0.1", rig["ports"]["green"]))
            return rig["ports"]["green"]
        except OSError:
            _move_green(rig)
    raise RuntimeError("could not hold any green port")


def _alive(pid):
    import psutil
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


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


def _fake_cmd(extra=()):
    def build(port, persist, instance):
        return [sys.executable, "-B", FAKE, str(port), persist, json.dumps(SLOTS), "--instance", instance, *extra]
    return build


def _copy_freeze(worker_dir, dest):
    """The tests' stand-in for freeze_worker (which needs a git checkout; it has its own test below)."""
    shutil.copytree(worker_dir, dest)
    return dest, "test-sha"


@pytest.fixture
def rig(tmp_path):
    """A catalogue, a worker dir with .dev.vars, a router with blue ACTIVE (a fake instance already running
    on a placed generation, recorded in instances.json) and green idle."""
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
    gen = work / "gen-blue-20260101T000000Z"
    gen.mkdir()
    swap.origin_copies.build(str(cat), str(gen / "copies"))
    swap.place(str(gen / "copies"), str(gen / "persist"), SLOTS)
    # A random port can be taken between the pick and the bind (other test processes on the machine): a
    # stand-in that exits is started again on a fresh port, and a slow start is given a minute.
    # Every stand-in started here is stopped at teardown - also one abandoned by a retry, and also when the
    # setup itself fails before the yield (a leaked blue once outlived its suite by hours, holding its port).
    started, rt = [], None
    # the REAL stop, captured now: tests patch swap.stop to fail, and whether their patch is undone before this
    # teardown runs depends on fixture order - 8 stand-ins leaked when it was not (R1204 follow-up)
    real_stop = swap.stop
    try:
        for _attempt in range(3):
            blue = swap.start(_fake_cmd()(ports["blue"], str(gen / "persist"), "blue-instance"), str(worker),
                              str(gen / "instance.log"))
            started.append((blue.pid, swap.created(blue.pid)))
            if _eventually(lambda: swap.port_in_use(ports["blue"]) or blue.poll() is not None, 60) \
                    and blue.poll() is None:
                break
            real_stop(blue.pid, started[-1][1])             # slow or dead: never left running behind the retry
            ports["blue"] = _port()
            state.write_text(json.dumps({"active": "blue",
                                         "targets": {n: f"http://127.0.0.1:{p}" for n, p in ports.items()}}))
        swap.save_instances(str(work), {"blue": {"pid": blue.pid, "created": swap.created(blue.pid),
                                                 "gen": str(gen), "port": ports["blue"],
                                                 "instance": "blue-instance", "state": "active"}})
        stale = work / "gen-green-20250101T000000Z"          # an older generation nothing uses
        stale.mkdir()
        rt = router.serve(str(state), 0)
        threading.Thread(target=rt.serve_forever, daemon=True).start()
        assert _eventually(lambda: swap.port_in_use(ports["blue"]), 60), \
            "blue never listened: " + open(gen / "instance.log", encoding="utf-8", errors="replace").read()[-500:]
        r = dict(cat=str(cat), worker=str(worker), work=str(work), state=str(state), ports=ports, gen=str(gen),
                 router_port=rt.server_address[1], router_url=f"http://127.0.0.1:{rt.server_address[1]}",
                 blue=blue, stale=str(stale))
        yield r
    finally:
        if rt is not None:
            rt.shutdown()
            rt.server_close()
        for inst in swap.load_instances(str(work)).values():
            real_stop(inst["pid"], inst.get("created"))
        for pid, created in started:
            real_stop(pid, created)


STOLEN_PORT = re.compile(r"the idle port \d+ (?:was taken while the copies were built|\(.*?\) already answers)")


def _swap(rig, port_retry=True, **kw):
    """One swap. A random idle port can be taken by ANOTHER process on the machine between the rig's pick and
    the swap's check (a concurrent suite or mutant run did it twice, 2026-09-24): swap.py refuses that, as it
    must, and the test then moves green to a fresh port and swaps again. A test that holds the port ON
    PURPOSE passes port_retry=False."""
    args = dict(catalogue=rig["cat"], state_path=rig["state"], router_url=rig["router_url"], work=rig["work"],
                worker_dir=rig["worker"], command=_fake_cmd(), freeze=_copy_freeze, slots=SLOTS, health_timeout=30,
                drain_timeout=10, space_check=False, log=lambda *_: None)
    args.update(kw)
    for attempt in range(3):
        try:
            return swap.swap(**args)
        except swap.SwapRefused as e:
            if not port_retry or attempt == 2 or not STOLEN_PORT.search(str(e)):
                raise
        _move_green(rig)


def _no_green_gen(rig):
    return not any(d.startswith("gen-green-2026") for d in os.listdir(rig["work"]))


# ---- the happy path ------------------------------------------------------------------------------------------
def test_a_swap_flips_to_the_new_copy_and_stops_the_old(rig):
    status, body = _get(rig["router_port"], "/v1/sources")
    assert status == 200 and body["port"] == rig["ports"]["blue"]
    out = _swap(rig)
    assert out["active"] == "green" and out["retired"] == "blue" and out["old_stopped"] is True
    assert out["commit"] == "test-sha"
    assert out["counts"]["primary"]["series"] + out["counts"]["climate"]["series"] == out["counts"]["catalogue_series"]
    assert set(out["health"]) == {"sources", "primary", "climate"} and out["health"]["climate"].startswith("noaa:")
    assert json.load(open(rig["state"]))["active"] == "green"
    status, body = _get(rig["router_port"], "/v1/series/noaa:a.metadata.json")
    assert status == 200 and body["port"] == rig["ports"]["green"], "the router now answers from green"
    assert _eventually(lambda: not _alive(rig["blue"].pid)), "blue was stopped after the drain"
    inst = swap.load_instances(rig["work"])
    assert inst["green"]["state"] == "active" and inst["blue"]["state"] == "stopped"
    assert inst["green"]["commit"] == "test-sha" and len(inst["green"]["instance"]) == 32
    assert os.path.isdir(inst["blue"]["gen"]), "the retired generation is kept for a rollback"
    assert not os.path.exists(rig["stale"]) and out["pruned"] == [rig["stale"]] and out["prune_errors"] == []
    placed = os.listdir(os.path.join(inst["green"]["gen"], "persist", swap.SLOT_DIR))
    assert sorted(placed) == sorted(SLOTS.values()), "rollback-journal mode: no -wal / -shm beside the slots"
    assert not os.path.exists(os.path.join(inst["green"]["gen"], "copies")), "the copies folder is cleaned"


# ---- refusals before the flip: the router is never touched, the new generation is removed --------------------
def _refused_cleanly(rig):
    assert json.load(open(rig["state"]))["active"] == "blue"
    assert _alive(rig["blue"].pid), "the active instance never stops because of a bad build"
    assert set(swap.load_instances(rig["work"])) == {"blue"}
    assert _no_green_gen(rig), "the failed generation is removed"
    assert _eventually(lambda: not swap.port_in_use(rig["ports"]["green"])), "the failed instance is stopped"


def test_swapped_slots_are_refused_by_the_title_check(rig):
    wrong = {"CATALOG": SLOTS["CATALOG_CLIMATE"], "CATALOG_CLIMATE": SLOTS["CATALOG"]}
    with pytest.raises(swap.SwapRefused, match="unproven|not behind the binding"):
        _swap(rig, slots=wrong)
    _refused_cleanly(rig)


def test_a_404_on_a_listed_series_is_refused_with_its_own_message(rig, monkeypatch):
    """R1180 finding 6: the 'not behind the binding' refusal had no test of its own."""
    real = swap.samples
    monkeypatch.setattr(swap, "samples", lambda p, c: {"primary": [("ecb:nosuch", "ecb", "t")] + real(p, c)["primary"],
                                                       "climate": []})
    with pytest.raises(swap.SwapRefused, match=r"primary sample 'ecb:nosuch': metadata answered 404 - the primary "
                                               r"copy is not behind the binding that serves it"):
        _swap(rig)
    _refused_cleanly(rig)


def test_the_title_not_the_echoed_id_is_compared(rig, monkeypatch):
    """The real worker echoes the requested id (metadata.ts), so only a stored field proves the lookup."""
    real = swap.samples

    def wrong_title(p, c):
        s = real(p, c)
        s["primary"] = [(i, src, "a title the copy does not have") for i, src, _t in s["primary"]]
        return s
    monkeypatch.setattr(swap, "samples", wrong_title)
    with pytest.raises(swap.SwapRefused, match="metadata has title"):
        _swap(rig)
    _refused_cleanly(rig)


@pytest.mark.parametrize("extra,match", [(("--unmarked",), "origin mark"), (("--die",), "exited")])
def test_an_unmarked_or_dead_instance_is_refused(rig, extra, match):
    with pytest.raises(swap.SwapRefused, match=match):
        _swap(rig, command=_fake_cmd(extra))
    _refused_cleanly(rig)


def test_any_other_error_after_the_start_also_stops_the_instance(rig):
    """R1180 finding 6 (M05): cleanup must not depend on the error being a SwapRefused."""
    with pytest.raises(ValueError):                                  # json.loads on a garbage body
        _swap(rig, command=_fake_cmd(("--garbage",)))
    _refused_cleanly(rig)


def test_the_metadata_sample_must_come_from_this_instance_too(rig):
    """A surviving R1186 mutant dropped the instance check on the metadata sample."""
    with pytest.raises(swap.SwapRefused, match="primary sample .* came from instance 'someone-else'"):
        _swap(rig, command=_fake_cmd(("--other-instance-on-metadata",)))
    _refused_cleanly(rig)


def test_an_answer_from_another_instance_is_refused(rig, monkeypatch):
    """R1180 finding 1: a process that took the idle port answers, and the swap's own instance does not."""
    for _attempt in range(3):             # a stand-in that exits could not bind: move green and start it again
        hijacker = swap.start(_fake_cmd()(rig["ports"]["green"], os.path.join(rig["gen"], "persist"), "someone-else"),
                              rig["worker"], os.path.join(rig["work"], "hijack.log"))
        if _eventually(lambda: swap.port_in_use(rig["ports"]["green"]) or hijacker.poll() is not None, 60) \
                and hijacker.poll() is None:
            break
        _move_green(rig)
    try:
        assert _eventually(lambda: swap.port_in_use(rig["ports"]["green"]))
        monkeypatch.setattr(swap, "port_in_use", lambda p: False)      # both port checks fooled
        sleeper = lambda port, persist, instance: [sys.executable, "-c", "import time; time.sleep(120)"]  # noqa: E731
        with pytest.raises(swap.SwapRefused, match="another process answers that port"):
            _swap(rig, port_retry=False, command=sleeper)
    finally:
        swap.stop(hijacker.pid, swap.created(hijacker.pid))
    assert json.load(open(rig["state"]))["active"] == "blue" and _no_green_gen(rig)


def test_a_port_taken_during_the_build_is_refused_before_the_start(rig, monkeypatch):
    real = swap.build_copies
    holder = socket.socket()

    def build_then_take(cat, out, **kw):
        r = real(cat, out, **kw)
        try:
            holder.bind(("127.0.0.1", rig["ports"]["green"]))
            holder.listen()
        except OSError:
            pass            # another process already holds it: the port is taken either way, same refusal
        return r
    monkeypatch.setattr(swap, "build_copies", build_then_take)
    try:
        with pytest.raises(swap.SwapRefused, match="taken while the copies were built"):
            _swap(rig, port_retry=False)
    finally:
        holder.close()
    assert json.load(open(rig["state"]))["active"] == "blue" and _no_green_gen(rig)


def test_an_answering_idle_port_is_refused(rig):
    s = socket.socket()
    _hold_green(rig, s)
    s.listen()
    try:
        with pytest.raises(swap.SwapRefused, match="already answers"):
            _swap(rig, port_retry=False)
    finally:
        s.close()


def test_the_rig_moves_off_a_port_another_process_took(rig):
    """The test helper's own retry: swap.py refuses the stolen port, the helper moves green and succeeds."""
    s = socket.socket()
    taken = _hold_green(rig, s)
    s.listen()
    try:
        out = _swap(rig)
    finally:
        s.close()
    assert rig["ports"]["green"] != taken and json.load(open(rig["state"]))["active"] == "green", out


def test_a_router_of_another_state_file_is_refused(rig, tmp_path):
    other = tmp_path / "other.json"
    other.write_text(json.dumps({"active": "green", "targets": json.load(open(rig["state"]))["targets"]}))
    rt2 = router.serve(str(other), 0)
    threading.Thread(target=rt2.serve_forever, daemon=True).start()
    try:
        with pytest.raises(swap.SwapRefused, match="not the router of this state file"):
            _swap(rig, router_url=f"http://127.0.0.1:{rt2.server_address[1]}")
    finally:
        rt2.shutdown()
        rt2.server_close()


def test_two_swaps_at_once_are_refused(rig):
    with swap.swap_lock(rig["work"]):
        with pytest.raises(swap.SwapRefused, match="another swap"):
            _swap(rig)


@pytest.mark.parametrize("gate,primary,climate", [
    ("ecb", "noaa_direct:c", "noaa:"),                 # a gated SOURCE is skipped (not listed)
    ("ecb:x,ecb:y", "noaa_direct:c", "noaa:"),         # gated SERIES of a listed source: 451, next one
    ("noaa", "ecb:", None),                            # the climate copy wholly gated: primary alone proves it
])
def test_gated_samples_are_skipped_not_failed(rig, gate, primary, climate):
    out = _swap(rig, command=_fake_cmd(("--gate", gate)))
    assert out["active"] == "green"
    assert out["health"]["primary"].startswith(primary)
    assert (out["health"]["climate"] is None) if climate is None else out["health"]["climate"].startswith(climate)


def test_a_wholly_gated_primary_is_refused(rig):
    with pytest.raises(swap.SwapRefused, match="unproven"):
        _swap(rig, command=_fake_cmd(("--gate", "ecb,noaa_direct")))
    _refused_cleanly(rig)


# ---- after the flip ------------------------------------------------------------------------------------------
def test_a_drain_that_does_not_finish_leaves_the_old_instance_running(rig):
    t = threading.Thread(target=lambda: _get(rig["router_port"], "/slow?s=8"), daemon=True)
    t.start()
    assert _eventually(lambda: router_inflight(rig).get("blue") == 1, 10)
    out = _swap(rig, drain_timeout=1)
    assert out["active"] == "green" and out["old_stopped"] is False
    assert _alive(rig["blue"].pid), "a long request is never cut off to keep a schedule"
    inst = swap.load_instances(rig["work"])
    assert inst["blue"]["state"] == "draining" and os.path.isdir(inst["blue"]["gen"])
    t.join(15)


def router_inflight(rig):
    return swap.router_status(rig["router_url"])["inflight"]


def test_the_drain_needs_two_zero_readings(monkeypatch):
    readings = iter([{"active": "g", "inflight": {}}, {"active": "g", "inflight": {"b": 1}},
                     {"active": "g", "inflight": {}}, {"active": "g", "inflight": {}}, {"active": "g", "inflight": {}}])
    seen = []
    monkeypatch.setattr(swap, "router_status", lambda u: seen.append(1) or next(readings))
    assert swap.drain("http://r", "b", "g", timeout=30, poll=0.01, settle=0.02) is True
    assert len(seen) >= 4, "a single 0 followed by a request in flight did not end the drain"


def test_the_drain_waits_the_whole_settle_time(monkeypatch):
    """S6 survived: the drain may end only after 0 in flight has held for `settle` seconds, not at the
    second 0 it reads."""
    monkeypatch.setattr(swap, "router_status", lambda u: {"active": "g", "inflight": {}})
    t = time.monotonic()
    assert swap.drain("http://r", "b", "g", timeout=30, poll=0.01, settle=0.5) is True
    assert time.monotonic() - t >= 0.5


def test_the_drain_refuses_a_router_that_did_not_flip(monkeypatch):
    """After the flip this is DrainAborted, never a refusal (R1186 finding 1)."""
    monkeypatch.setattr(swap, "router_status", lambda u: {"active": "b", "inflight": {}})
    with pytest.raises(swap.DrainAborted, match="flipped back or another router"):
        swap.drain("http://r", "b", "g", timeout=5, poll=0.01)
    assert not issubclass(swap.DrainAborted, swap.SwapRefused)


def test_the_drain_rides_out_an_unreadable_state_and_a_router_hiccup(monkeypatch):
    """One {"active": None} reading (the state file being replaced) or a refused connection is not the end."""
    readings = iter([{"active": None, "error": "PermissionError"}, OSError("refused"),
                     http.client.IncompleteRead(b"{"), http.client.BadStatusLine("x"),     # R1191 finding 4
                     {"active": "g", "inflight": {}}, {"active": "g", "inflight": {}}, {"active": "g", "inflight": {}}])

    def status(u):
        r = next(readings)
        if isinstance(r, Exception):
            raise r
        return r
    monkeypatch.setattr(swap, "router_status", status)
    assert swap.drain("http://r", "b", "g", timeout=30, poll=0.01, settle=0.02) is True


def test_the_drain_gives_up_after_its_grace(monkeypatch):
    monkeypatch.setattr(swap, "router_status", lambda u: {"active": None, "error": "gone"})
    with pytest.raises(swap.DrainAborted, match="no usable status"):
        swap.drain("http://r", "b", "g", timeout=30, poll=0.001, grace=3)


def test_a_drain_that_cannot_be_watched_is_reported_as_flipped(rig, monkeypatch, capsys):
    """R1186 finding 1: the router already serves green - main() says so (exit 3), not "REFUSED" (exit 2)."""
    def broken(*a, **k):
        raise swap.DrainAborted("the router at x gave no usable status 11 times")
    monkeypatch.setattr(swap, "drain", broken)
    orig = swap.swap                                   # main() looks `swap` up at call time: wrap it with the rig
    monkeypatch.setattr(swap, "swap", lambda **kw: orig(**{**kw, **dict(
        worker_dir=rig["worker"], command=_fake_cmd(), freeze=_copy_freeze, slots=SLOTS, health_timeout=30,
        space_check=False, log=lambda *_: None)}))
    base = ["--work", rig["work"], "--state", rig["state"], "--router", rig["router_url"], "--catalogue", rig["cat"]]
    assert swap.main(base) == 3
    out = json.loads(capsys.readouterr().out)
    assert out["active"] == "green" and "drain_error" in out and out["old_stopped"] is False and "stop_with" in out
    assert json.load(open(rig["state"]))["active"] == "green" and _alive(rig["blue"].pid)
    assert swap.load_instances(rig["work"])["blue"]["state"] == "draining"


def test_a_failed_stop_is_recorded_as_failed(rig, monkeypatch):
    """R1186 finding 2: a process that survives the kill is not "stopped"."""
    monkeypatch.setattr(swap, "stop", lambda pid, created: {"stopped": False, "alive": [pid],
                                                           "detail": f"STILL ALIVE: [{pid}]"})
    out = _swap(rig)
    assert out["active"] == "green" and out["old_stopped"] is False and "stop_with" in out
    assert swap.load_instances(rig["work"])["blue"]["state"] == "stop-failed"


def _main_with_rig(rig, monkeypatch):
    orig = swap.swap                                   # main() looks `swap` up at call time: wrap it with the rig
    monkeypatch.setattr(swap, "swap", lambda **kw: orig(**{**kw, **dict(
        worker_dir=rig["worker"], command=_fake_cmd(), freeze=_copy_freeze, slots=SLOTS, health_timeout=30,
        space_check=False, log=lambda *_: None)}))
    return ["--work", rig["work"], "--state", rig["state"], "--router", rig["router_url"], "--catalogue", rig["cat"]]


def test_any_error_after_the_flip_is_reported_as_flipped_never_raised(rig, monkeypatch, capsys):
    """R1191 finding 4: an exception after router.flip used to escape main() as a traceback, exit code unset."""
    def boom(pid, created):
        raise RuntimeError("stop blew up")
    monkeypatch.setattr(swap, "stop", boom)
    assert swap.main(_main_with_rig(rig, monkeypatch)) == 3
    out = json.loads(capsys.readouterr().out)
    assert out["active"] == "green" and out["post_flip_error"] == "RuntimeError: stop blew up"
    assert out["old_stopped"] is False and "--stop blue" in out["stop_with"]
    assert json.load(open(rig["state"]))["active"] == "green"


def test_stop_reports_access_denied_and_never_raises(monkeypatch):
    import psutil

    class Denied:
        pid = 4242

        def create_time(self):
            raise psutil.AccessDenied(4242)
    monkeypatch.setattr(psutil, "Process", lambda pid: Denied())
    got = swap.stop(4242, 1.0)
    assert got["stopped"] is False and got["alive"] == [4242] and "access denied" in got["detail"]

    class Unkillable:
        pid = 4243

        def create_time(self):
            return 1.0

        def children(self, recursive=False):
            return []

        def kill(self):
            raise psutil.AccessDenied(4243)
    proc = Unkillable()
    monkeypatch.setattr(psutil, "Process", lambda pid: proc)
    monkeypatch.setattr(psutil, "wait_procs", lambda procs, timeout: ([], list(procs)))
    got = swap.stop(4243, 1.0)
    assert got["stopped"] is False and got["alive"] == [4243]


def test_an_unrecorded_old_instance_that_still_answers_is_not_a_clean_swap(rig):
    """R1191 finding 7: with no record for the active target, main() used to exit 0 while it still served."""
    swap.save_instances(rig["work"], {})
    out = _swap(rig)
    assert out["active"] == "green" and out["old_stopped"] is False
    assert "no record" in out["stop_with"] and _alive(rig["blue"].pid)


def test_two_work_folders_cannot_swap_one_router(rig, tmp_path):
    """R1191 mutant N1: the state-file lock had no test. Another --work folder holding it refuses the swap."""
    with swap.swap_lock(str(tmp_path / "other_work"), os.path.abspath(rig["state"]) + ".swaplock"):
        with pytest.raises(swap.SwapRefused, match="another swap"):
            _swap(rig, port_retry=False)
    assert json.load(open(rig["state"]))["active"] == "blue"


def test_stop_kills_a_child_started_after_its_first_look(monkeypatch):
    """R1191 mutant N2: the second look for late children had no test. A fake tree whose parent has a new
    child on the second look: that child is killed and waited for too."""
    import psutil
    killed = []

    class P:
        def __init__(self, pid):
            self.pid = pid

        def kill(self):
            killed.append(self.pid)

    kid, late = P(11), P(12)

    class Parent(P):
        looks = 0

        def create_time(self):
            return 1.0

        def children(self, recursive=False):
            Parent.looks += 1
            return [kid] if Parent.looks == 1 else [kid, late]
    parent = Parent(10)
    monkeypatch.setattr(psutil, "Process", lambda pid: parent)
    waited = []
    monkeypatch.setattr(psutil, "wait_procs", lambda procs, timeout: (waited.extend(p.pid for p in procs), ([], []))[1])
    got = swap.stop(10, 1.0)
    assert killed == [11, 12, 10] and sorted(waited) == [10, 11, 12] and got["stopped"] is True


def test_a_failed_stop_command_is_a_refusal_and_recorded(rig, monkeypatch, capsys):
    """R1191 mutant N10: --stop whose stop failed must not exit 0."""
    base = ["--work", rig["work"], "--state", rig["state"], "--router", rig["router_url"]]
    assert _swap(rig, drain_timeout=0.01)["active"] == "green"        # blue retired, left running
    monkeypatch.setattr(swap, "stop", lambda pid, created: {"stopped": False, "alive": [pid],
                                                           "detail": f"STILL ALIVE: [{pid}]"})
    assert swap.main(base + ["--stop", "blue", "--force"]) == 2
    assert "STILL ALIVE" in capsys.readouterr().err
    assert swap.load_instances(rig["work"])["blue"]["state"] == "stop-failed"


def test_a_swap_whose_stop_failed_exits_3(rig, monkeypatch, capsys):
    """R1191 mutant N11: a failed stop (no drain error) must exit 3, not 0."""
    monkeypatch.setattr(swap, "stop", lambda pid, created: {"stopped": False, "alive": [pid],
                                                           "detail": f"STILL ALIVE: [{pid}]"})
    assert swap.main(_main_with_rig(rig, monkeypatch)) == 3
    out = json.loads(capsys.readouterr().out)
    assert out["old_stopped"] is False and "drain_error" not in out and "stop_with" in out


def test_the_swap_builds_with_the_state_db(rig, monkeypatch):
    """R1191 mutant N13: nothing checked that swap() passes state_db_path() to the build."""
    seen = {}

    def fake_build(cat, out, **kw):
        seen.update(kw)
        raise swap.SwapRefused("stop here")
    monkeypatch.setattr(swap, "state_db_path", lambda: "SENTINEL-state.db")
    monkeypatch.setattr(swap, "build_copies", fake_build)
    with pytest.raises(swap.SwapRefused, match="stop here"):
        _swap(rig, port_retry=False)
    assert seen.get("state_db") == "SENTINEL-state.db"


def test_the_drain_grace_counts_misses_in_a_row(monkeypatch):
    """R1191 mutant N14: a good reading resets the miss count - grace is misses IN A ROW, not in total."""
    readings = iter([None, None, {"active": "g", "inflight": {"b": 1}}, None, None,
                     {"active": "g", "inflight": {}}, {"active": "g", "inflight": {}}, {"active": "g", "inflight": {}}])

    def status(u):
        r = next(readings)
        if r is None:
            raise OSError("refused")
        return r
    monkeypatch.setattr(swap, "router_status", status)
    assert swap.drain("http://r", "b", "g", timeout=30, poll=0.001, settle=0.001, grace=2) is True


def test_the_stop_command_refuses_when_the_drain_cannot_be_watched(rig, monkeypatch, capsys):
    """R1191 finding 4: DrainAborted escaped --stop as a traceback."""
    base = ["--work", rig["work"], "--state", rig["state"], "--router", rig["router_url"]]
    assert _swap(rig, drain_timeout=0.01)["active"] == "green"        # blue retired, left running

    def broken(*a, **k):
        raise swap.DrainAborted("the router at x gave no usable status 11 times")
    monkeypatch.setattr(swap, "drain", broken)
    assert swap.main(base + ["--stop", "blue"]) == 2
    assert "could not be watched" in capsys.readouterr().err and _alive(rig["blue"].pid)
    assert swap.load_instances(rig["work"])["blue"]["state"] == "draining", "nothing was recorded as stopped"


def test_a_failed_swap_keeps_the_retired_generation_s_record(rig):
    """R1186 finding 3: the idle target's previous record (the rollback) is put back, not dropped."""
    inst = swap.load_instances(rig["work"])
    inst["green"] = {"pid": 1, "created": 1.0, "gen": rig["stale"], "state": "stopped"}
    swap.save_instances(rig["work"], inst)
    with pytest.raises(swap.SwapRefused):
        _swap(rig, command=_fake_cmd(("--unmarked",)))
    assert swap.load_instances(rig["work"])["green"]["gen"] == rig["stale"]
    assert os.path.isdir(rig["stale"]), "and its folder stays"


def test_prune_compares_folders_not_spellings(tmp_path):
    work = tmp_path / "Work"
    (work / "gen-blue-1").mkdir(parents=True)
    (work / "gen-green-0").mkdir()
    keep = [os.path.join(str(work), ".", "gen-blue-1")]                   # os.path keeps the "."
    if os.name == "nt":
        keep = [os.path.join(str(tmp_path), "WORK", ".", "GEN-BLUE-1")]
    assert str(work / "gen-blue-1") not in keep, "precondition: no keep entry equals the folder as text"
    removed, errors = swap.prune(str(work), keep)
    assert (work / "gen-blue-1").is_dir(), "a recorded generation spelled differently is kept"
    assert removed == [str(work / "gen-green-0")] and errors == []


# ---- stop ----------------------------------------------------------------------------------------------------
def test_stop_ends_the_whole_process_tree(tmp_path):
    (tmp_path / ".dev.vars").write_text(f"ORIGIN_SECRET={SECRET}\n")
    cat = tmp_path / "c.db"
    _catalogue(cat)
    swap.origin_copies.build(str(cat), str(tmp_path / "cp"))
    persist = str(tmp_path / "persist-x")
    swap.place(str(tmp_path / "cp"), persist, SLOTS)
    pidfile = tmp_path / "child.pid"
    p = swap.start(_fake_cmd(("--child", str(pidfile)))(_port(), persist, "x"), str(tmp_path), str(tmp_path / "log"))
    assert _eventually(lambda: pidfile.exists() and pidfile.read_text().strip() != "")
    child = int(pidfile.read_text())
    assert _alive(child) and _alive(p.pid), "positive control: both are running"
    st = swap.stop(p.pid, swap.created(p.pid))
    assert st["stopped"] and st["alive"] == [] and st["detail"].startswith(f"stopped pid {p.pid} and 1 child")
    p.wait(15)
    assert _eventually(lambda: not _alive(p.pid) and not _alive(child)), "the child (workerd) went too"


def test_stop_never_kills_a_reused_pid(tmp_path):
    p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        what = swap.stop(p.pid, swap.created(p.pid) - 100.0)          # recorded for an older process
        assert "NOT killed" in what["detail"] and not what["stopped"] and _alive(p.pid)
        what = swap.stop(p.pid, swap.created(p.pid) - 5.0)            # 5 s off: still another process (R1186)
        assert not what["stopped"] and _alive(p.pid), "the tolerance is 1 s, not a minute"
        gone = swap.stop(2 ** 22 + 12345, 1.0)
        assert gone["stopped"] and "gone" in gone["detail"]
    finally:
        p.kill()
        p.wait()


def test_the_stop_command(rig, capsys):
    base = ["--work", rig["work"], "--state", rig["state"], "--router", rig["router_url"]]
    assert swap.main(base + ["--stop", "blue"]) == 2 and "ACTIVE" in capsys.readouterr().err
    assert swap.main(base + ["--stop", "nobody"]) == 2
    out = _swap(rig, drain_timeout=0.01)                              # blue now retired (maybe still running)
    assert out["active"] == "green"
    assert swap.main(base + ["--stop", "blue", "--drain-timeout", "10"]) == 0
    assert swap.load_instances(rig["work"])["blue"]["state"] == "stopped"
    assert _eventually(lambda: not _alive(rig["blue"].pid))
    with pytest.raises(SystemExit):
        swap.main(["--work", rig["work"], "--stop", "blue"])            # --state is required


def test_stop_waits_for_the_drain_unless_forced(rig, capsys):
    """R1180 finding 5: --stop used to kill at once, cutting off the downloads the drain protects."""
    base = ["--work", rig["work"], "--state", rig["state"], "--router", rig["router_url"]]
    t = threading.Thread(target=lambda: _get(rig["router_port"], "/slow?s=6"), daemon=True)
    t.start()
    assert _eventually(lambda: router_inflight(rig).get("blue") == 1, 10)
    _swap(rig, drain_timeout=0.5)                                     # blue retired, still serving
    assert swap.main(base + ["--stop", "blue", "--drain-timeout", "1"]) == 2
    assert "in flight" in capsys.readouterr().err and _alive(rig["blue"].pid)
    assert swap.main(base + ["--stop", "blue", "--force"]) == 0
    assert _eventually(lambda: not _alive(rig["blue"].pid))
    t.join(15)


# ---- the pieces ----------------------------------------------------------------------------------------------
def test_freeze_worker_takes_the_committed_head_only(tmp_path):
    repo = tmp_path / "repo"
    w = repo / "api" / "worker"
    (w / "src").mkdir(parents=True)
    (w / "node_modules" / "pkg").mkdir(parents=True)
    (w / "node_modules" / "pkg" / "index.js").write_text("module.exports = 1\n")
    (w / "src" / "index.ts").write_text("export default 1\n")
    (w / ".dev.vars").write_text("ORIGIN_SECRET=x\n")
    (repo / ".gitignore").write_text("node_modules/\n.dev.vars\n")
    git = lambda *a: subprocess.run(["git", *a], cwd=repo, check=True, capture_output=True)  # noqa: E731
    git("init", "-q")
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "c")
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True).stdout.strip()
    (w / "src" / "index.ts").write_text("export default 2  // UNCOMMITTED\n")
    frozen, got = swap.freeze_worker(str(w), str(tmp_path / "gen" / "code"))
    assert got == sha
    assert open(os.path.join(frozen, "src", "index.ts")).read() == "export default 1\n", "HEAD, not the edit"
    assert os.path.isfile(os.path.join(frozen, "node_modules", "pkg", "index.js"))
    assert open(os.path.join(frozen, ".dev.vars")).read() == "ORIGIN_SECRET=x\n"
    shutil.rmtree(tmp_path / "gen")
    assert (w / "node_modules" / "pkg" / "index.js").is_file(), "removing a generation never touches the checkout"


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


def test_too_little_space_is_refused(tmp_path, monkeypatch):
    cat = tmp_path / "c.db"
    cat.write_bytes(b"x" * 1000)
    (tmp_path / "w" / "node_modules").mkdir(parents=True)
    monkeypatch.setattr(swap.shutil, "disk_usage", lambda p: types.SimpleNamespace(free=1 << 30))
    with pytest.raises(swap.SwapRefused, match="GB free"):
        swap.check_space(str(tmp_path), str(cat), str(tmp_path / "w"))
    monkeypatch.setattr(swap.shutil, "disk_usage", lambda p: types.SimpleNamespace(free=10 << 30))
    swap.check_space(str(tmp_path), str(cat), str(tmp_path / "w"))


def test_the_frozen_worker_counts_in_the_space_needed(tmp_path, monkeypatch):
    """A surviving R1186 mutant dropped the node_modules term: a generation copies it."""
    cat = tmp_path / "c.db"
    cat.write_bytes(b"x" * 1000)
    nm = tmp_path / "w" / "node_modules"
    nm.mkdir(parents=True)
    (nm / "big.bin").write_bytes(b"\0" * (3 << 20))                   # 3 MiB of "node_modules"
    free = int(1000 * 2.0) + (2 << 30) + (1 << 20)                      # enough WITHOUT node_modules, 1 MiB short
    monkeypatch.setattr(swap.shutil, "disk_usage", lambda p: types.SimpleNamespace(free=free))
    with pytest.raises(swap.SwapRefused, match="GB free"):
        swap.check_space(str(tmp_path), str(cat), str(tmp_path / "w"))


def test_after_t0_only_the_build_is_copied_under_the_writer_lock(tmp_path, monkeypatch):
    from core import catalog_path, cutover
    seen = []

    @contextlib.contextmanager
    def lock():
        seen.append("held")
        yield

    build = tmp_path / "catalog.db"
    build.write_bytes(b"")
    monkeypatch.setattr(catalog_path, "writer_lock", lock)
    monkeypatch.setattr(catalog_path, "catalog_path", lambda: str(build))
    def fake_build(c, o, lock=None, state_db=None):
        with (lock() if lock else contextlib.nullcontext()):
            seen.append("build")
            seen.append(("state_db", state_db))
        return {"ok": 1}
    monkeypatch.setattr(swap.origin_copies, "build", fake_build)
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    swap.build_copies("anything", "o")
    assert seen == ["build", ("state_db", None)], "before T0 no lock (CI's writer runs elsewhere)"
    seen.clear()
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    with pytest.raises(swap.SwapRefused, match="copies only the build"):
        swap.build_copies(str(tmp_path / "." / "elsewhere.db"), "o")
    swap.build_copies(str(tmp_path / "." / "catalog.db"), "o")
    assert seen == ["held", "build", ("state_db", os.path.join(catalog_path.LIVE_STATE_DIR, "state.db"))], \
        "after T0 the freshness projection comes from the LIVE state.db (R1186)"


def test_after_t0_a_held_lock_is_waited_for_then_refused(tmp_path, monkeypatch):
    from core import catalog_path, cutover
    build = tmp_path / "catalog.db"
    build.write_bytes(b"")
    tries = []

    @contextlib.contextmanager
    def held():
        tries.append(1)
        raise cutover.CutoverRefused("held by the updater")
        yield  # noqa: unreachable
    monkeypatch.setattr(catalog_path, "writer_lock", held)
    monkeypatch.setattr(catalog_path, "catalog_path", lambda: str(build))
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    monkeypatch.setattr(swap.time, "sleep", lambda s: None)

    def fake_build(c, o, lock=None, state_db=None):
        with lock():
            raise AssertionError("the lock was never free, so the build must not run")
    monkeypatch.setattr(swap.origin_copies, "build", fake_build)
    clock = iter(range(0, 10_000, 10))
    monkeypatch.setattr(swap.time, "monotonic", lambda: next(clock))
    with pytest.raises(swap.SwapRefused, match="stayed held"):
        swap.build_copies(str(build), "o", lock_wait=60)
    assert len(tries) > 1, "it waited and tried again before refusing"


def test_the_slot_dir_is_the_one_wrangler_uses():
    """d1_slots.mjs and the origin both use <persist>/v3/d1/miniflare-D1DatabaseObject."""
    src = open(os.path.join(ROOT, "tools", "selfhost", "d1_slots.mjs"), encoding="utf-8").read()
    assert '"v3", "d1", "miniflare-D1DatabaseObject"' in src
    assert swap.SLOT_DIR == os.path.join("v3", "d1", "miniflare-D1DatabaseObject")


def test_the_real_command_tags_the_instance():
    cmd = swap.wrangler_command(8801, "P", "abc")
    assert cmd[-2:] == ["--var", "INSTANCE_ID:abc"] and "--persist-to" in cmd and "127.0.0.1" in cmd
@pytest.mark.parametrize("answers,listens,held", [(True, False, True), (False, True, True), (False, None, True),
                                                  (False, False, False)])
def test_a_port_is_held_if_it_answers_or_is_listed_or_cannot_be_read(monkeypatch, answers, listens, held):
    """port_held backs the unrecorded-old-instance decision: a busy old instance that missed the 1 s connect
    was reported as gone (a false clean swap). The OS listener table is asked too, and an unreadable table is
    'held' - never a false 'gone'."""
    monkeypatch.setattr(swap, "port_in_use", lambda p: answers)
    monkeypatch.setattr(swap, "listening", lambda p: listens)
    assert swap.port_held(12345) is held


def test_an_unrecorded_old_instance_that_misses_the_connect_is_still_not_a_clean_swap(rig, monkeypatch):
    """The loaded-suite case: the old instance still LISTENS but does not answer the 1 s connect."""
    swap.save_instances(rig["work"], {})
    real = swap.port_in_use
    blue = rig["ports"]["blue"]
    monkeypatch.setattr(swap, "port_in_use", lambda p: False if p == blue else real(p))
    out = _swap(rig)
    assert out["active"] == "green" and out["old_stopped"] is False
    assert "no record" in out["stop_with"]
