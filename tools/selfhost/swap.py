"""The blue/green catalogue swap (docs/ECON_SELF_HOSTING_PLAN.md, section 2: "swap = build N+1, start the idle
instance on its copy, health-check, flip the router, stop the old one").

    python tools/selfhost/swap.py --work <folder> --catalogue <build> --state <router state.json>
                                  [--router http://127.0.0.1:8787]
    python tools/selfhost/swap.py --work <folder> --state <state.json> --stop <target> [--force]

Each swap is ONE GENERATION: <work>/gen-<target>-<UTC stamp>/ holding the catalogue copies (persist/), a
FROZEN copy of the worker (code/: `git archive` of the committed HEAD plus its node_modules - wrangler dev
hot-reloads whatever it runs from, so it never runs from a checkout someone edits; review R1180 finding 3)
and the instance's log. Steps, each one refusing rather than guessing:
  0. One swap at a time: an exclusive lock on <work>/swap.lock. The router at --router must report the
     same active target as the state file (it is the router this swap flips).
  1. The router state names the ACTIVE instance; the other one is IDLE. Free disk is checked first.
  2. SLOTS: which local D1 file backs CATALOG and which backs CATALOG_CLIMATE is DISCOVERED by
     tools/selfhost/d1_slots.mjs with the pinned wrangler, never assumed.
  3. COPIES: tools/selfhost/origin_copies.py builds and checks the primary and climate copies with the
     SQLite backup API. After T0 the catalogue must be THE build (core.catalog_path) and the copy runs
     under the writer lock (which rolls a hot journal back first); a lock held by the updater is waited
     for, up to --lock-wait. The copies go into rollback-journal mode, into the generation's persist dir.
  4. START the idle instance from the frozen code with a per-swap INSTANCE_ID (wrangler dev --var), after
     checking again that its port is free. HEALTH-CHECK it on its own port: every answer must carry
     x-econ-instance = that id (so an answer from any other process on the port is refused - R1180
     finding 1), /v1/sources lists sources, and a series of the primary copy answers its metadata with
     the TITLE stored in the copy (the id is echoed by the worker and proves nothing); the climate copy is
     asked too when it has a servable series. Samples come from listed sources; a 451 moves on to the
     next candidate (a gated id is refused before any lookup); a 404 is a refusal.
  5. FLIP the router (one atomic state-file replace) and check the router reports the new target. DRAIN:
     the old instance is stopped only after two readings of 0 in flight, 2 s apart; a drain that does not
     finish in --drain-timeout leaves it RUNNING and says so.
  6. STOP the old instance and its whole process tree - only if the pid still belongs to the process that
     was started (its creation time is recorded; a reused pid is never killed). Generations older than
     the retired one are pruned; the retired one is kept, so a rollback is: start it again from its own
     gen dir, then `router.py --flip <old>`.
Anything that fails before the flip stops the idle instance, removes its generation and leaves the router
alone: the active instance never stops serving because of a bad build.
Recorded in <work>/instances.json (atomic replace): pid, creation time, generation, commit, instance id.

THE SECRET: each generation's frozen worker holds a COPY of api/worker/.dev.vars (the origin secret), so
rotating the secret means: change .dev.vars, then swap (the new generation starts with it) and give the edge
the new value - the running instance keeps the old one until it is replaced. Kept generations (the retired
one) hold the old copy until they are pruned. The folder under --work must be as private as .dev.vars.
The slots are discovered and the secret read from the checkout the swap runs from; the instance runs that
checkout's COMMITTED HEAD (frozen) - both come from the same node_modules and the same .dev.vars file.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import http.client
import json
import os
import secrets as _secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import zipfile

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
INSTANCE_HEADER = "x-econ-instance"


class SwapRefused(RuntimeError):
    """The swap stopped before the router was flipped; the active instance is untouched."""


class DrainAborted(RuntimeError):
    """AFTER the flip: the drain could not be watched to its end. The router serves the new instance; the
    old one is left running and recorded as draining. Never reported as a refusal (R1186 finding 1)."""


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _same(a: str, b: str) -> bool:
    """One folder however it is spelled (case, slashes, a relative path) - R1180 finding 2."""
    return os.path.normcase(os.path.realpath(a)) == os.path.normcase(os.path.realpath(b))


# ---- the instances file and the swap lock -------------------------------------------------------------------
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


@contextlib.contextmanager
def swap_lock(work: str, lock_file: str | None = None):
    """One swap (or stop) at a time: <work>/swap.lock, or `lock_file`. Fails at once when held: two swaps
    are refused, not queued."""
    os.makedirs(work, exist_ok=True)
    path = lock_file or os.path.join(work, "swap.lock")
    fh = open(path, "a+b")
    try:
        if os.name == "nt":
            import msvcrt
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        raise SwapRefused(f"another swap holds {path}") from None
    try:
        yield
    finally:
        try:
            if os.name == "nt":
                import msvcrt
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


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


def port_holder(port: int) -> str:
    """Which process listens on 127.0.0.1:port - named in a refusal, so a race can be traced (R1186)."""
    import psutil
    try:
        for c in psutil.net_connections(kind="inet"):
            if c.status == psutil.CONN_LISTEN and c.laddr and c.laddr.port == port:
                try:
                    return f"pid {c.pid} ({' '.join(psutil.Process(c.pid).cmdline())[:120]})"
                except (psutil.Error, TypeError):
                    return f"pid {c.pid}"
    except psutil.Error as e:
        return f"unknown ({type(e).__name__})"
    return "no listener found"


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


def check_space(work: str, catalogue: str, worker_dir: str) -> None:
    """A generation needs about the catalogue's size again (the primary copy is not vacuumed), the climate
    copy, their journals while they are written, and the frozen worker; refuse unless 2 x the catalogue + the
    worker + 2 GB is free (R1185 measured 1.76 x at the peak on a synthetic catalogue)."""
    need = int(os.path.getsize(catalogue) * 2.0) + _tree_size(os.path.join(worker_dir, "node_modules")) + (2 << 30)
    free = shutil.disk_usage(work).free
    if free < need:
        raise SwapRefused(f"{free / 2**30:.1f} GB free in {work}, a generation needs about {need / 2**30:.1f} GB")


def _tree_size(path: str) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(path):
        for f in files:
            with contextlib.suppress(OSError):
                total += os.path.getsize(os.path.join(dirpath, f))
    return total


def state_db_path() -> str | None:
    """The state.db the freshness projection comes from: after T0 the live one, before it this checkout's
    (when there is one - a probe catalogue copied from D1 already carries the tables)."""
    from core import catalog_path, cutover
    if cutover.is_cut_over():
        return os.path.join(catalog_path.LIVE_STATE_DIR, "state.db")
    p = os.path.join(REPO, "data", "_aqueduct", "state.db")
    return p if os.path.isfile(p) else None


def build_copies(catalogue: str, out_dir: str, *, lock_wait: float = 1800.0, state_db: str | None = None) -> dict:
    """origin_copies.build. After T0 the catalogue must be THE build, and its READS run under the writer lock
    (AR-153; only the reads - R1185); the updater holding it is waited for, up to lock_wait seconds (R1180
    finding 7)."""
    from core import cutover
    if not cutover.is_cut_over():
        return origin_copies.build(catalogue, out_dir, state_db=state_db)
    from core import catalog_path
    if not _same(catalogue, catalog_path.catalog_path()):
        raise SwapRefused(f"after T0 the swap copies only the build {catalog_path.catalog_path()}, not {catalogue}")
    return origin_copies.build(catalogue, out_dir, lock=lambda: _waiting_writer_lock(lock_wait),
                               state_db=state_db or state_db_path())


@contextlib.contextmanager
def _waiting_writer_lock(lock_wait: float):
    """The catalogue writer lock, waited for (30 s between tries) up to lock_wait seconds."""
    from core import catalog_path, cutover
    deadline = time.monotonic() + lock_wait
    while True:
        held = catalog_path.writer_lock()
        try:
            held.__enter__()
        except cutover.CutoverRefused:
            if time.monotonic() > deadline:
                raise SwapRefused(f"the catalogue writer lock stayed held for {lock_wait:.0f} s") from None
            time.sleep(30)
            continue
        break
    try:
        yield
    finally:
        held.__exit__(None, None, None)


def samples(primary: str, climate: str) -> dict:
    """Candidates for the health check: one (series_id, source_id, title) per source present in each copy
    (an index seek per source). The check tries them in order, because a sample from a GATED source answers
    451 before any catalogue lookup and so proves nothing about which file is behind which binding."""
    out = {}
    for label, path in (("primary", primary), ("climate", climate)):
        con = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True)
        try:
            found = []
            for (src,) in con.execute("SELECT source_id FROM source ORDER BY source_id").fetchall():
                row = con.execute("SELECT series_id, title FROM series WHERE source_id=? LIMIT 1", (src,)).fetchone()
                if row:
                    found.append((row[0], src, row[1]))
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
        shutil.move(src, os.path.join(dest, slots[binding]))       # move, not replace: may cross drives


def freeze_worker(worker_dir: str, dest: str) -> tuple[str, str]:
    """A frozen, COMMITTED copy of the worker for one generation (R1180 finding 3): `git archive` of HEAD's
    api/worker (uncommitted edits are left out, on purpose), plus the checkout's node_modules (copied, so a
    later npm install cannot change a running generation either) and .dev.vars (the secret; not in git).
    Returns (the frozen worker folder, the commit)."""
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=worker_dir, capture_output=True, text=True,
                         check=True).stdout.strip()
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=worker_dir, capture_output=True, text=True,
                         check=True).stdout.strip()
    rel = os.path.relpath(worker_dir, top).replace(os.sep, "/")
    zpath = dest + ".zip"
    os.makedirs(dest)
    subprocess.run(["git", "archive", "--format=zip", "-o", zpath, sha, rel], cwd=top, check=True,
                   capture_output=True)
    with zipfile.ZipFile(zpath) as z:
        z.extractall(dest)
    os.remove(zpath)
    frozen = os.path.join(dest, *rel.split("/"))
    shutil.copytree(os.path.join(worker_dir, "node_modules"), os.path.join(frozen, "node_modules"), symlinks=True)
    shutil.copy2(os.path.join(worker_dir, ".dev.vars"), os.path.join(frozen, ".dev.vars"))
    return frozen, sha


def wrangler_command(port: int, persist: str, instance: str, config: str = CONFIG) -> list[str]:
    return ["node", os.path.join("node_modules", "wrangler", "bin", "wrangler.js"), "dev", "-c", config, "--local",
            "--ip", "127.0.0.1", "--port", str(port), "--persist-to", persist, "--var", f"INSTANCE_ID:{instance}"]


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


def created(pid: int) -> float | None:
    """The process's creation time, or None when there is no such process."""
    import psutil
    try:
        return psutil.Process(pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def stop(pid: int, created_at: float | None) -> dict:
    """Stop the process AND its children (wrangler dev -> workerd) - only when `pid` still belongs to the
    process started at `created_at` (R1180 finding 5: a reused pid is never killed). Returns
    {"stopped": bool, "alive": [pids], "detail": str}; "stopped" is False when anything of the tree is still
    alive afterwards, so a failed stop is never recorded as a stop (R1186 finding 2)."""
    import psutil
    try:
        p = psutil.Process(pid)
        if created_at is None or abs(p.create_time() - created_at) > 1.0:
            return {"stopped": False, "alive": [pid],
                    "detail": f"pid {pid} is now another process (created {p.create_time()}): NOT killed"}
        tree = p.children(recursive=True) + [p]
    except psutil.NoSuchProcess:
        return {"stopped": True, "alive": [], "detail": f"pid {pid} is gone already"}
    # descendants first, then a second look while the parent still lives (a child it started after the
    # first snapshot is found there), then the parent
    kids = tree[:-1]
    for proc in kids:
        with contextlib.suppress(psutil.NoSuchProcess):
            proc.kill()
    try:
        late = [c for c in p.children(recursive=True) if c.pid not in {k.pid for k in kids}]
    except psutil.NoSuchProcess:
        late = []
    for c in late:
        with contextlib.suppress(psutil.NoSuchProcess):
            c.kill()
    with contextlib.suppress(psutil.NoSuchProcess):
        p.kill()
    everything = kids + late + [p]
    _gone, alive = psutil.wait_procs(everything, timeout=15)
    still = sorted(a.pid for a in alive)
    return {"stopped": not still, "alive": still,
            "detail": f"stopped pid {pid} and {len(everything) - 1} child(ren)"
                      + (f"; STILL ALIVE: {still}" if still else "")}


def _get(base: str, path: str, secret: str, timeout: float = 30.0) -> tuple[int, dict, bytes]:
    u = urllib.parse.urlsplit(base)
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=timeout)
    try:
        c.request("GET", path, headers={SECRET_HEADER: secret})
        r = c.getresponse()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read()
    finally:
        c.close()


def _ours(headers: dict, instance: str, what: str) -> None:
    if headers.get(MARK_HEADER) != "1":
        raise SwapRefused(f"{what} came back without the origin mark: something else answers that port")
    if headers.get(INSTANCE_HEADER) != instance:
        raise SwapRefused(f"{what} came from instance {headers.get(INSTANCE_HEADER)!r}, not the one this swap "
                          f"started ({instance!r}): another process answers that port")


def health(base: str, secret: str, candidates: dict, proc: subprocess.Popen | None, timeout: float,
           instance: str) -> dict:
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
    _ours(headers, instance, "/v1/sources")
    sources = json.loads(body).get("sources") or []
    if not sources:
        raise SwapRefused("/v1/sources lists no source")
    listed = {s.get("source") for s in sources}
    out: dict = {"sources": len(sources)}
    for label in ("primary", "climate"):
        for sid, src, title in candidates.get(label, []):
            if src not in listed:
                continue                                  # a gated source: /v1/sources hides it
            status, headers, body = _get(
                base, "/v1/series/" + urllib.parse.quote(sid, safe=":") + ".metadata.json", secret)
            if status == 451:
                continue                                  # a gated series (a carve-out): proves nothing
            if status != 200:
                raise SwapRefused(f"{label} sample {sid!r}: metadata answered {status} - the {label} copy is "
                                  "not behind the binding that serves it")
            _ours(headers, instance, f"{label} sample {sid!r}")
            got = json.loads(body).get("title")
            if got != title:
                raise SwapRefused(f"{label} sample {sid!r}: metadata has title {got!r}, the copy {title!r}")
            out[label] = sid
            break
        else:
            if label == "primary":
                raise SwapRefused("no servable series in the primary copy answered: the slot mapping is unproven")
            # The climate copy may be empty or wholly gated. The primary answer already proves the mapping
            # (swapped slots put the climate file behind CATALOG, which holds no primary series).
            out[label] = None
    return out


def router_status(router_url: str) -> dict:
    u = urllib.parse.urlsplit(router_url)
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
    try:
        c.request("GET", router.STATUS_PATH)
        return json.loads(c.getresponse().read())
    finally:
        c.close()


def drain(router_url: str, name: str, now_active: str, timeout: float, poll: float = 1.0,
          settle: float = 2.0, grace: int = 10) -> bool:
    """True once the router - the one that now reports `now_active` - has shown 0 in flight on `name` for
    `settle` seconds (a request can sit between the router's target read and its count; R1180 finding 8).
    Up to `grace` readings in a row may fail or show the state as unreadable (active None - the file being
    replaced; the router restarting); a router that keeps reporting another target, or never answers,
    raises DrainAborted (R1186 finding 1: that happens AFTER the flip and is not a refusal)."""
    deadline = time.monotonic() + timeout
    zero_since, misses, last = None, 0, ""
    while True:
        try:
            d = router_status(router_url)
        except (OSError, ValueError) as e:
            d, last = None, f"{type(e).__name__}: {e}"
        if d is None or d.get("active") is None:
            misses += 1
            last = last if d is None else f"state unreadable: {d.get('error')}"
            if misses > grace:
                raise DrainAborted(f"the router at {router_url} gave no usable status {misses} times ({last})")
            time.sleep(poll)
            continue
        misses = 0
        if d.get("active") != now_active:
            raise DrainAborted(f"the router at {router_url} reports active {d.get('active')!r}, not "
                               f"{now_active!r} - flipped back or another router")
        if not d.get("inflight", {}).get(name):
            if zero_since is None:
                zero_since = time.monotonic()
            elif time.monotonic() - zero_since >= settle:
                return True
        else:
            zero_since = None
        if time.monotonic() > deadline:
            return False
        time.sleep(poll)


def _remove_tree(path: str) -> str | None:
    try:
        shutil.rmtree(path)
        return None
    except OSError as e:
        return f"{path}: {e}"


def prune(work: str, keep: list[str]) -> tuple[list[str], list[str]]:
    """Remove generation folders under work/ that are not in `keep` (compared as folders, whatever the
    spelling). Returns (removed, errors) - never raises after a flip."""
    removed, errors = [], []
    for d in sorted(os.listdir(work)):
        p = os.path.join(work, d)
        if d.startswith("gen-") and os.path.isdir(p) and not any(_same(p, k) for k in keep):
            err = _remove_tree(p)
            (errors.append(err) if err else removed.append(p))
    return removed, errors


# ---- the swap ---------------------------------------------------------------------------------------------
def swap(*, catalogue: str, state_path: str, router_url: str, work: str, worker_dir: str = WORKER_DIR,
         command=wrangler_command, freeze=freeze_worker, slots: dict | None = None, health_timeout: float = 300.0,
         drain_timeout: float = 3600.0, lock_wait: float = 1800.0, space_check: bool = True, log=print) -> dict:
    # two locks: the work folder AND the router state file (two --work folders on one router - R1186 f7)
    state_dir = os.path.dirname(os.path.abspath(state_path))
    with swap_lock(work), swap_lock(state_dir, os.path.abspath(state_path) + ".swaplock"):
        return _swap(catalogue, state_path, router_url, work, worker_dir, command, freeze, slots, health_timeout,
                     drain_timeout, lock_wait, space_check, log)


def _swap(catalogue, state_path, router_url, work, worker_dir, command, freeze, slots, health_timeout,
          drain_timeout, lock_wait, space_check, log) -> dict:
    with open(state_path, encoding="utf-8") as fh:
        st = json.load(fh)
    active = st["active"]
    others = [n for n in st["targets"] if n != active]
    if len(others) != 1:
        raise SwapRefused(f"the router state must name exactly two targets, has {sorted(st['targets'])}")
    idle = others[0]
    idle_url = st["targets"][idle]
    idle_port = port_of(idle_url)
    rs = router_status(router_url)
    if rs.get("active") != active:
        raise SwapRefused(f"the router at {router_url} reports active {rs.get('active')!r} but {state_path} says "
                          f"{active!r}: it is not the router of this state file")
    if port_in_use(idle_port):
        raise SwapRefused(f"the idle port {idle_port} ({idle}) already answers ({port_holder(idle_port)}): an "
                          "instance nobody accounted for - stop it first")
    if space_check:
        check_space(work, catalogue, worker_dir)
    secret = read_secret(worker_dir)
    slots = slots or discover_slots(worker_dir)
    log(f"active={active} idle={idle} slots={slots}")

    stamp = _now()
    gen = os.path.abspath(os.path.join(work, f"gen-{idle}-{stamp}"))
    copies_dir, persist = os.path.join(gen, "copies"), os.path.join(gen, "persist")
    instance = _secrets.token_hex(16)
    instances = load_instances(work)
    previous = instances.get(idle)                       # the record a failure must put back (R1186 f3)
    proc = None
    try:
        os.makedirs(gen)
        counts = build_copies(catalogue, copies_dir, lock_wait=lock_wait, state_db=state_db_path())
        log(f"copies checked: {json.dumps(counts)}")
        ids = samples(os.path.join(copies_dir, "primary.sqlite"), os.path.join(copies_dir, "climate.sqlite"))
        if not ids["primary"]:
            raise SwapRefused("the primary copy holds no series")
        place(copies_dir, persist, slots)
        shutil.rmtree(copies_dir, ignore_errors=True)
        code, sha = freeze(worker_dir, os.path.join(gen, "code"))
        if port_in_use(idle_port):                        # checked again: the build took minutes (R1180 f1)
            raise SwapRefused(f"the idle port {idle_port} was taken while the copies were built "
                              f"({port_holder(idle_port)})")
        proc = start(command(idle_port, persist, instance), code, os.path.join(gen, "instance.log"))
        instances[idle] = {"pid": proc.pid, "created": created(proc.pid), "gen": gen, "port": idle_port,
                           "started_utc": stamp, "commit": sha, "instance": instance,
                           "catalogue": os.path.abspath(catalogue), "counts": counts, "state": "starting"}
        save_instances(work, instances)
        checked = health(idle_url, secret, ids, proc, health_timeout, instance)
        log(f"{idle} healthy: {json.dumps(checked)}")
        router.flip(state_path, idle)
    except BaseException:
        if proc is not None:
            log(stop(proc.pid, created(proc.pid))["detail"])
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=30)
        err = _remove_tree(gen) if os.path.exists(gen) else None
        if err:
            log(f"could not remove the failed generation: {err}")
        if previous is not None:
            instances[idle] = previous                   # the retired generation's record stays (rollback)
        else:
            instances.pop(idle, None)
        save_instances(work, instances)
        raise

    instances[idle]["state"] = "active"
    old = instances.get(active)
    if old:
        old["state"] = "draining"
    save_instances(work, instances)
    log(f"flipped the router to {idle}")

    result = {"active": idle, "retired": active, "commit": sha, "counts": counts, "health": checked,
              "old_stopped": False}
    how_to_stop = (f"python tools/selfhost/swap.py --work {work} --state {state_path} --router {router_url} "
                   f"--stop {active}")
    if old:
        # FROM HERE THE ROUTER SERVES THE NEW INSTANCE: nothing below is a refusal (R1186 finding 1)
        try:
            drained = drain(router_url, active, idle, drain_timeout)
        except DrainAborted as e:
            drained, result["drain_error"] = False, str(e)
            log(f"FLIPPED to {idle}, but the drain of {active} could not be watched: {e}; {active} LEFT RUNNING")
        if drained:
            st = stop(old["pid"], old.get("created"))
            result["stop"] = st["detail"]
            old["state"] = "stopped" if st["stopped"] else "stop-failed"
            result["old_stopped"] = st["stopped"]
            log(f"{active} drained: {st['detail']}")
        elif "drain_error" not in result:
            log(f"{active} still has requests in flight after {drain_timeout:.0f} s: LEFT RUNNING (pid "
                f"{old['pid']}); stop it with: {how_to_stop}")
        if not result["old_stopped"]:
            result["stop_with"] = how_to_stop
        save_instances(work, instances)
    keep = [i["gen"] for i in instances.values() if i.get("gen")]
    result["pruned"], result["prune_errors"] = prune(work, keep)
    return result


def stop_recorded(work: str, state_path: str, target: str, router_url: str, *, force: bool = False,
                  drain_timeout: float = 3600.0) -> str:
    """--stop: the recorded instance `target`, never the active one; drained first unless force."""
    with swap_lock(work):
        instances = load_instances(work)
        if target not in instances:
            raise SwapRefused(f"no recorded instance {target!r}")
        with open(state_path, encoding="utf-8") as fh:
            active = json.load(fh)["active"]
        if active == target:
            raise SwapRefused(f"{target} is the ACTIVE instance; flip the router first")
        if not force and not drain(router_url, target, active, drain_timeout):
            raise SwapRefused(f"{target} still has requests in flight; wait, or pass --force to cut them off")
        st = stop(instances[target]["pid"], instances[target].get("created"))
        instances[target]["state"] = "stopped" if st["stopped"] else "stop-failed"
        save_instances(work, instances)
        if not st["stopped"]:
            raise SwapRefused(f"{target}: {st['detail']}")
        return st["detail"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--work", required=True, help="folder for the generations, the lock and instances.json")
    ap.add_argument("--state", required=True, help="the router's state file")
    ap.add_argument("--router", default="http://127.0.0.1:8787")
    ap.add_argument("--catalogue")
    ap.add_argument("--health-timeout", type=float, default=300.0)
    ap.add_argument("--drain-timeout", type=float, default=3600.0)
    ap.add_argument("--lock-wait", type=float, default=1800.0, help="after T0: how long to wait for the writer lock")
    ap.add_argument("--stop", metavar="TARGET", help="stop a recorded, non-active instance (drained first)")
    ap.add_argument("--force", action="store_true", help="with --stop: do not wait for the drain")
    a = ap.parse_args(argv)
    try:
        if a.stop:
            print(stop_recorded(a.work, a.state, a.stop, a.router, force=a.force, drain_timeout=a.drain_timeout))
            return 0
        if not a.catalogue:
            ap.error("--catalogue is required for a swap")
        out = swap(catalogue=a.catalogue, state_path=a.state, router_url=a.router, work=a.work,
                   health_timeout=a.health_timeout, drain_timeout=a.drain_timeout, lock_wait=a.lock_wait)
        print(json.dumps(out, indent=1))
        # 0: flipped and the old one stopped (or there was none); 3: FLIPPED, the old one still runs
        return 0 if out["old_stopped"] or "stop_with" not in out else 3
    except SwapRefused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
