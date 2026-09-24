"""The blue/green catalogue swap (docs/ECON_SELF_HOSTING_PLAN.md, section 2: "swap = build N+1, start the idle
instance on its copy, health-check, flip the router, stop the old one").

    python tools/selfhost/swap.py --catalogue <build> --state <router state.json> --router http://127.0.0.1:8787
                                  --work <folder for the instances' persist dirs>

Steps, each one refusing rather than guessing:
  1. The router state names the ACTIVE instance; the other one is IDLE. The idle port must be free - an
     answering idle port is an instance nobody accounted for, and the swap stops.
  2. SLOTS: which local D1 file backs CATALOG and which backs CATALOG_CLIMATE is DISCOVERED by
     tools/selfhost/d1_slots.mjs with the pinned wrangler, never assumed (a wrangler upgrade that changes
     the hashing is found here, not by serving the wrong catalogue from the wrong binding).
  3. COPIES: tools/selfhost/origin_copies.py builds the primary and climate copies with the SQLite backup
     API and checks them. After T0 this runs under the catalogue writer lock, whose holder rolls a hot
     journal back first (AR-153) - so the updater must have released the lock before it calls the swap.
     Each copy is switched to rollback-journal mode and moved into a FRESH persist dir for the idle
     instance (fresh, so the Cache API cannot serve the previous catalogue - plan: "a swap is never hidden
     for 6 h").
  4. START the idle instance (wrangler dev on wrangler.origin.toml, 127.0.0.1 only) and HEALTH-CHECK it
     directly on its own port: /v1/sources answers 200 with the origin mark and at least one source, and a
     series of the primary copy answers its metadata by id - which proves CATALOG holds the primary copy
     (swapped slots would 404 it). The climate copy is asked too when it has a servable series. Samples come
     from LISTED sources and a 451 moves on to the next: a gated id is refused before any lookup.
  5. FLIP the router (one atomic state-file replace), then DRAIN: the old instance is stopped only when the
     router reports 0 requests in flight on it. A drain that does not reach 0 in --drain-timeout leaves the
     old instance RUNNING and says so: a long download is not cut off to keep a schedule.
  6. STOP the old instance with its whole process tree (wrangler dev starts workerd children; on Windows
     `taskkill /T`), and remove persist dirs older than the one just retired - the retired one is kept, so a
     rollback is `router.py --flip <old>` after restarting it on the same dir.

A failed health check stops the idle instance, removes its persist dir and leaves the router untouched: the
active instance never stops serving because of a bad build.

The instances' pid, persist dir and ports are recorded in <work>/instances.json (atomic replace).
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import http.client
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, REPO)

import origin_copies  # noqa: E402
import router  # noqa: E402

WORKER_DIR = os.path.join(REPO, "api", "worker")
CONFIG = "wrangler.origin.toml"
SLOT_DIR = os.path.join("v3", "d1", "miniflare-D1DatabaseObject")
SECRET_HEADER = "x-econ-origin-secret"
MARK_HEADER = "x-econ-origin"


class SwapRefused(RuntimeError):
    """The swap stopped before the router was flipped; the active instance is untouched."""


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---- the instances file -----------------------------------------------------------------------------------
def load_instances(work: str) -> dict:
    p = os.path.join(work, "instances.json")
    if not os.path.exists(p):
        return {}
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


def save_instances(work: str, d: dict) -> None:
    p = os.path.join(work, "instances.json")
    tmp = f"{p}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(d, fh, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, p)


# ---- pieces -----------------------------------------------------------------------------------------------
def read_secret(worker_dir: str = WORKER_DIR) -> str:
    """ORIGIN_SECRET from the worker's .dev.vars - the same file wrangler dev reads. Never printed."""
    with open(os.path.join(worker_dir, ".dev.vars"), encoding="utf-8") as fh:
        for line in fh:
            k, sep, v = line.strip().partition("=")
            if sep and k.strip() == "ORIGIN_SECRET":
                v = v.strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                    v = v[1:-1]
                if v:
                    return v
    raise SwapRefused("no ORIGIN_SECRET in .dev.vars: the origin would refuse every request")


def port_of(url: str) -> int:
    u = urllib.parse.urlsplit(url)
    if u.hostname not in ("127.0.0.1", "localhost") or not u.port:
        raise SwapRefused(f"instance target {url!r} is not a 127.0.0.1 port")
    return u.port


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def discover_slots(worker_dir: str = WORKER_DIR, config: str = CONFIG) -> dict:
    """{"CATALOG": "<file>.sqlite", "CATALOG_CLIMATE": "<file>.sqlite"} from d1_slots.mjs."""
    r = subprocess.run(["node", os.path.join(HERE, "d1_slots.mjs"), config], cwd=worker_dir, capture_output=True,
                       text=True, timeout=300)
    lines = [l for l in r.stdout.splitlines() if l.strip()]
    if r.returncode != 0 or not lines:
        raise SwapRefused(f"d1_slots.mjs failed ({r.returncode}): {r.stderr.strip()[-500:]}")
    slots = json.loads(lines[-1])
    if set(slots) != {"CATALOG", "CATALOG_CLIMATE"} or slots["CATALOG"] == slots["CATALOG_CLIMATE"]:
        raise SwapRefused(f"d1_slots.mjs gave an unusable mapping: {slots}")
    return slots


def build_copies(catalogue: str, out_dir: str) -> dict:
    """origin_copies.build, under the writer lock after T0 (AR-153)."""
    from core import cutover
    lock = contextlib.nullcontext()
    if cutover.is_cut_over():
        from core import catalog_path
        lock = catalog_path.writer_lock()
    with lock:
        return origin_copies.build(catalogue, out_dir)


def samples(primary: str, climate: str) -> dict:
    """Candidates for the health check: one (series_id, source_id) per source present in each copy (an index
    seek per source). The check tries them in order, because a sample from a GATED source answers 451 before
    any catalogue lookup and so proves nothing about which file is behind which binding."""
    out = {}
    for label, path in (("primary", primary), ("climate", climate)):
        con = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
        try:
            found = []
            for (src,) in con.execute("SELECT source_id FROM source ORDER BY source_id").fetchall():
                row = con.execute("SELECT series_id FROM series WHERE source_id=? LIMIT 1", (src,)).fetchone()
                if row:
                    found.append((row[0], src))
        finally:
            con.close()
        out[label] = found
    return out


def place(copies_dir: str, persist: str, slots: dict) -> None:
    """Move the two checked copies into a fresh persist dir, in rollback-journal mode (no -wal/-shm to
    carry), each into the file its binding reads."""
    dest = os.path.join(persist, SLOT_DIR)
    if os.path.exists(persist):
        raise SwapRefused(f"persist dir {persist} already exists: a swap always starts from a fresh one")
    os.makedirs(dest)
    for name, binding in (("primary.sqlite", "CATALOG"), ("climate.sqlite", "CATALOG_CLIMATE")):
        src = os.path.join(copies_dir, name)
        con = sqlite3.connect(src)
        try:
            mode = con.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        finally:
            con.close()
        if mode.lower() != "delete":
            raise SwapRefused(f"{name}: journal_mode stayed {mode!r}")
        for suffix in ("-wal", "-shm", "-journal"):
            if os.path.exists(src + suffix):
                raise SwapRefused(f"{name}{suffix} is still there after the mode change")
        os.replace(src, os.path.join(dest, slots[binding]))


def wrangler_command(port: int, persist: str, config: str = CONFIG) -> list[str]:
    return ["node", os.path.join("node_modules", "wrangler", "bin", "wrangler.js"), "dev", "-c", config, "--local",
            "--ip", "127.0.0.1", "--port", str(port), "--persist-to", persist]


def start(command: list[str], cwd: str, log_path: str) -> subprocess.Popen:
    log = open(log_path, "ab")
    kw: dict = {"cwd": cwd, "stdin": subprocess.DEVNULL, "stdout": log, "stderr": subprocess.STDOUT}
    if os.name == "nt":
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kw["start_new_session"] = True
    try:
        return subprocess.Popen(command, **kw)
    finally:
        log.close()                                      # the child holds its own handle


def stop(pid: int) -> None:
    """The process AND its children (wrangler dev -> workerd). Idempotent: a gone pid is fine."""
    if os.name == "nt":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(pid)], capture_output=True)
    else:
        import signal
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(pid, signal.SIGTERM)


def _get(base: str, path: str, secret: str, timeout: float = 30.0) -> tuple[int, dict, bytes]:
    u = urllib.parse.urlsplit(base)
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=timeout)
    try:
        c.request("GET", path, headers={SECRET_HEADER: secret})
        r = c.getresponse()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read()
    finally:
        c.close()


def health(base: str, secret: str, candidates: dict, proc: subprocess.Popen | None, timeout: float) -> dict:
    """Wait for the instance to answer, then check it. Returns what was checked; raises SwapRefused."""
    deadline = time.monotonic() + timeout
    last = "no answer"
    while True:
        if proc is not None and proc.poll() is not None:
            raise SwapRefused(f"the idle instance exited with {proc.returncode} before it answered")
        try:
            status, headers, body = _get(base, "/v1/sources", secret)
            if status == 200:
                break
            last = f"/v1/sources answered {status}"
        except OSError as e:
            last = f"{type(e).__name__}: {e}"
        if time.monotonic() > deadline:
            raise SwapRefused(f"the idle instance did not answer within {timeout:.0f} s ({last})")
        time.sleep(1)
    if headers.get(MARK_HEADER) != "1":
        raise SwapRefused("/v1/sources came back without the origin mark: something else answers that port")
    sources = json.loads(body).get("sources") or []
    if not sources:
        raise SwapRefused("/v1/sources lists no source")
    listed = {s.get("source") for s in sources}
    out: dict = {"sources": len(sources)}
    for label in ("primary", "climate"):
        for sid, src in candidates.get(label, []):
            if src not in listed:
                continue                                  # a gated source: /v1/sources hides it
            status, headers, body = _get(
                base, "/v1/series/" + urllib.parse.quote(sid, safe=":") + ".metadata.json", secret)
            if status == 451:
                continue                                  # a gated series (a carve-out): proves nothing
            if status != 200 or headers.get(MARK_HEADER) != "1":
                raise SwapRefused(f"{label} sample {sid!r}: metadata answered {status} - the {label} copy is "
                                  "not behind the binding that serves it")
            got = json.loads(body).get("series_id")
            if got != sid:
                raise SwapRefused(f"{label} sample {sid!r}: metadata names {got!r}")
            out[label] = sid
            break
        else:
            if label == "primary":
                raise SwapRefused("no servable series in the primary copy answered: the slot mapping is unproven")
            # The climate copy may be empty or wholly gated. The primary answer already proves the mapping
            # (swapped slots put the climate file behind CATALOG, which holds no primary series).
            out[label] = None
    return out


def drain(router_url: str, name: str, timeout: float, poll: float = 1.0) -> bool:
    """True once the router reports 0 requests in flight on `name`."""
    u = urllib.parse.urlsplit(router_url)
    deadline = time.monotonic() + timeout
    while True:
        c = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
        try:
            c.request("GET", router.STATUS_PATH)
            d = json.loads(c.getresponse().read())
        finally:
            c.close()
        if not d.get("inflight", {}).get(name):
            return True
        if time.monotonic() > deadline:
            return False
        time.sleep(poll)


def prune(work: str, keep: set[str]) -> list[str]:
    """Remove persist dirs under work/ that no instance uses and that are not in `keep`."""
    removed = []
    for d in sorted(os.listdir(work)):
        p = os.path.join(work, d)
        if d.startswith("persist-") and os.path.isdir(p) and os.path.abspath(p) not in keep:
            shutil.rmtree(p)
            removed.append(p)
    return removed


# ---- the swap ---------------------------------------------------------------------------------------------
def swap(*, catalogue: str, state_path: str, router_url: str, work: str, worker_dir: str = WORKER_DIR,
         command=wrangler_command, slots: dict | None = None, health_timeout: float = 300.0,
         drain_timeout: float = 3600.0, log=print) -> dict:
    with open(state_path, encoding="utf-8") as fh:
        st = json.load(fh)
    active = st["active"]
    others = [n for n in st["targets"] if n != active]
    if len(others) != 1:
        raise SwapRefused(f"the router state must name exactly two targets, has {sorted(st['targets'])}")
    idle = others[0]
    idle_url = st["targets"][idle]
    idle_port = port_of(idle_url)
    if port_in_use(idle_port):
        raise SwapRefused(f"the idle port {idle_port} ({idle}) already answers: an instance nobody accounted "
                          "for - stop it first")
    os.makedirs(work, exist_ok=True)
    secret = read_secret(worker_dir)
    slots = slots or discover_slots(worker_dir)
    log(f"active={active} idle={idle} slots={slots}")

    stamp = _now()
    copies_dir = os.path.join(work, f"copies-{stamp}")
    persist = os.path.abspath(os.path.join(work, f"persist-{idle}-{stamp}"))
    counts = build_copies(catalogue, copies_dir)
    log(f"copies checked: {json.dumps(counts)}")
    ids = samples(os.path.join(copies_dir, "primary.sqlite"), os.path.join(copies_dir, "climate.sqlite"))
    if not ids["primary"]:
        shutil.rmtree(copies_dir, ignore_errors=True)
        raise SwapRefused("the primary copy holds no series")
    try:
        place(copies_dir, persist, slots)
    finally:
        shutil.rmtree(copies_dir, ignore_errors=True)

    proc = start(command(idle_port, persist), worker_dir, os.path.join(work, f"{idle}-{stamp}.log"))
    instances = load_instances(work)
    instances[idle] = {"pid": proc.pid, "persist": persist, "port": idle_port, "started_utc": stamp,
                       "catalogue": os.path.abspath(catalogue), "counts": counts, "state": "starting"}
    save_instances(work, instances)
    try:
        checked = health(idle_url, secret, ids, proc, health_timeout)
    except BaseException:
        stop(proc.pid)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=30)
        shutil.rmtree(persist, ignore_errors=True)
        instances.pop(idle, None)
        save_instances(work, instances)
        raise
    log(f"{idle} healthy: {json.dumps(checked)}")

    router.flip(state_path, idle)
    instances[idle]["state"] = "active"
    old = instances.get(active)
    if old:
        old["state"] = "draining"
    save_instances(work, instances)
    log(f"flipped the router to {idle}")

    result = {"active": idle, "retired": active, "counts": counts, "health": checked, "old_stopped": False}
    if old:
        if drain(router_url, active, drain_timeout):
            stop(old["pid"])
            old["state"] = "stopped"
            result["old_stopped"] = True
            log(f"{active} drained and stopped (pid {old['pid']})")
        else:
            log(f"{active} still has requests in flight after {drain_timeout:.0f} s: LEFT RUNNING (pid "
                f"{old['pid']}); stop it with: python tools/selfhost/swap.py --stop {active} --work {work}")
        save_instances(work, instances)
    keep = {i["persist"] for i in instances.values()}
    result["pruned"] = prune(work, keep)
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--work", required=True, help="folder for persist dirs, logs and instances.json")
    ap.add_argument("--catalogue")
    ap.add_argument("--state")
    ap.add_argument("--router", default="http://127.0.0.1:8787")
    ap.add_argument("--health-timeout", type=float, default=300.0)
    ap.add_argument("--drain-timeout", type=float, default=3600.0)
    ap.add_argument("--stop", metavar="TARGET", help="stop a recorded instance (after a drain that timed out)")
    a = ap.parse_args()
    if a.stop:
        instances = load_instances(a.work)
        if a.stop not in instances:
            raise SystemExit(f"no recorded instance {a.stop!r}")
        if not a.state:
            ap.error("--stop needs --state, so the ACTIVE instance can never be the one stopped")
        with open(a.state, encoding="utf-8") as fh:
            if json.load(fh)["active"] == a.stop:
                raise SystemExit(f"{a.stop} is the ACTIVE instance; flip the router first")
        stop(instances[a.stop]["pid"])
        instances[a.stop]["state"] = "stopped"
        save_instances(a.work, instances)
        return 0
    if not (a.catalogue and a.state):
        ap.error("--catalogue and --state are required for a swap")
    print(json.dumps(swap(catalogue=a.catalogue, state_path=a.state, router_url=a.router, work=a.work,
                          health_timeout=a.health_timeout, drain_timeout=a.drain_timeout), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
