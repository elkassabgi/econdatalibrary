"""CLI runner for the Aqueduct orchestrator + the R2 state round-trip.

  python -m updater.run --dry                      # show what's due, do nothing
  python -m updater.run --source bcrp              # update one source
  python -m updater.run --strategy extend_by_date  # all S2 sources
  python -m updater.run --cadence daily            # everything on the daily cadence
  python -m updater.run --source treasury --force  # force a run regardless of cadence
  python -m updater.run --pull-state               # R2 state.db.zst -> local state.db (+ record ETag)
  python -m updater.run --push-state               # VACUUM INTO + zstd + CAS upload (+ dated backup)

State round-trip (UPDATER_BUILD_PLAN.md §1.2; decision O-8: single file +
VACUUM INTO + zstd, no table split): between CI runs the source of truth for
data/_aqueduct/state.db is R2 at ``_aqueduct/state.db.zst``. ``--pull-state``
records the downloaded object's ETag in ``data/_aqueduct/.state_etag``;
``--push-state`` HEADs the remote object first and ABORTS with exit 2 if that
ETag changed — another writer won, and a blind PUT would silently destroy its
state. This compare-and-swap is the actual cross-writer guard: the ``leases``
table lives INSIDE state.db, so two writers each holding their own downloaded
copy can never arbitrate through it. The GitHub Actions concurrency group is
the serializer; the CAS here is the backstop that turns a lost race into a
loud red run instead of silent state corruption. Every successful push also
writes a dated backup under ``_aqueduct/backups/``.
"""
from __future__ import annotations
import argparse
import datetime
import os
import sqlite3
import sys
import uuid

from . import blob as blobmod
from . import config

# R2 object keys for the state round-trip. The state ALWAYS lives in R2
# regardless of AQUEDUCT_BACKEND — these flags exist precisely so a run on any
# machine (CI runner, laptop drain) round-trips the same authoritative copy.
STATE_KEY = "_aqueduct/state.db.zst"
BACKUP_KEY_FMT = "_aqueduct/backups/state-{stamp}-{runid}.db.zst"
ETAG_PATH = os.path.join(config.STATE_DIR, ".state_etag")


def _zstd():
    """Import zstandard lazily so plain local runs never require it."""
    try:
        import zstandard
    except ImportError:
        print("[state] ERROR: --pull-state/--push-state need the 'zstandard' package "
              "(pip install zstandard).", file=sys.stderr)
        sys.exit(1)
    return zstandard


def _stored_etag() -> str | None:
    if not os.path.exists(ETAG_PATH):
        return None
    with open(ETAG_PATH, encoding="utf-8") as f:
        return f.read().strip() or None


def _record_etag(etag: str) -> None:
    os.makedirs(config.STATE_DIR, exist_ok=True)
    with open(ETAG_PATH, "w", encoding="utf-8") as f:
        f.write(etag)


def _live_pids() -> set[int]:
    """PIDs currently running, or an empty set if we cannot enumerate them."""
    import subprocess
    try:
        if os.name == "nt":
            out = subprocess.run(["tasklist", "/FO", "CSV", "/NH"],
                                 capture_output=True, text=True, encoding="utf-8",
                                 errors="replace", timeout=30).stdout
            return {int(p[1].strip('"')) for p in
                    (ln.split(",") for ln in out.splitlines() if "," in ln)
                    if p[1].strip('"').isdigit()}
        out = subprocess.run(["ps", "-e", "-o", "pid="], capture_output=True,
                             text=True, timeout=30).stdout
        return {int(t) for t in out.split() if t.isdigit()}
    except Exception:                                        # noqa: BLE001
        return set()


def _state_db_holders() -> list[str]:
    """Name the likely holders of state.db: leases whose owner PID is still alive.

    A lease is `orch-<pid>`; one that is past its expiry while its owner still
    runs is a runaway, and a runaway is what blocks the replace (R406).
    """
    lines: list[str] = []
    try:
        con = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True, timeout=10)
        rows = list(con.execute("select key, owner, expires_utc from leases"))
        con.close()
    except Exception as e:                                   # noqa: BLE001
        return [f"(could not read leases: {str(e)[:80]})"]
    live = _live_pids()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for key, owner, expires in rows:
        pid = None
        if isinstance(owner, str) and owner.startswith("orch-"):
            tail = owner[5:]
            pid = int(tail) if tail.isdigit() else None
        if pid is not None and pid in live:
            state = "EXPIRED but owner ALIVE - RUNAWAY" if (
                expires or "") < now else "held, not yet expired"
            lines.append(f"pid {pid} holds lease {key} (expires {expires}) - {state}")
    if not lines:
        lines.append("no live lease owner found; check for any other process with "
                     "state.db open (orphaned `updater.run`, a reader tool, a shell)")
    return lines


def pull_state() -> int:
    """Download R2 state.db.zst -> data/_aqueduct/state.db; record the ETag."""
    zstandard = _zstd()
    r2 = blobmod.R2Blob()
    print(f"[pull-state] GET r2://{r2.bucket}/{STATE_KEY}")
    etag_before = r2.etag(STATE_KEY)
    data = r2.get(STATE_KEY)
    if data is None:
        print(f"[pull-state] ERROR: r2://{r2.bucket}/{STATE_KEY} does not exist. "
              f"Seed it once from the machine holding the authoritative "
              f"{config.STATE_DB} via: python -m updater.run --push-state",
              file=sys.stderr)
        return 1
    # Re-HEAD after the GET: if the ETag moved mid-download we would otherwise
    # record a NEWER etag against OLDER bytes and a later --push-state would
    # pass CAS while silently reverting the other writer's state.
    etag_after = r2.etag(STATE_KEY)
    if etag_before != etag_after or etag_after is None:
        print(f"[pull-state] ERROR: remote object changed during download "
              f"(etag {etag_before} -> {etag_after}); another writer is active. "
              f"Re-run --pull-state.", file=sys.stderr)
        return 1

    raw = zstandard.ZstdDecompressor().decompress(data)
    os.makedirs(config.STATE_DIR, exist_ok=True)

    # R407. os.replace swaps the .db but leaves -wal/-shm beside it, so a WAL
    # belonging to the OLD database gets replayed against the freshly downloaded
    # one: `PRAGMA integrity_check` came back "Rowid out of order" and every
    # push-state then died on `VACUUM INTO`. Worse, that WAL can hold local runs
    # that were never pushed, so deleting it blind DESTROYS work. Back the pair
    # up first, then clear them, and say so.
    wal, shm = config.STATE_DB + "-wal", config.STATE_DB + "-shm"
    if os.path.exists(wal):
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        keep = os.path.join(config.STATE_DIR, "_superseded")
        os.makedirs(keep, exist_ok=True)
        print(f"[pull-state] WARNING: a write-ahead log is present ({os.path.getsize(wal):,} B). "
              f"It belongs to the LOCAL database being replaced and may hold runs that were "
              f"never pushed. Backing the local state up to {keep} before overwriting.",
              file=sys.stderr)
        for src in (config.STATE_DB, wal, shm):
            if os.path.exists(src):
                try:
                    os.replace(src, os.path.join(keep, os.path.basename(src) + "." + stamp))
                except OSError as e:
                    print(f"[pull-state] ERROR: could not set aside {src}: {e}", file=sys.stderr)
                    return 1

    tmp = f"{config.STATE_DB}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        with open(tmp, "wb") as f:
            f.write(raw)
        try:
            os.replace(tmp, config.STATE_DB)  # atomic publish of the local copy
        except PermissionError:
            # Windows only: SQLite opens without FILE_SHARE_DELETE, so ANY live
            # connection to state.db makes the replace fail. R406 - two orphaned
            # manual updater.run processes held it for 30 h and every local-heavy
            # launch aborted here, silently skipping all 18 heavy sources. A bare
            # traceback hid that in a 5-minute retry loop, so name the holder.
            print(f"[pull-state] ERROR: cannot replace {config.STATE_DB} - another "
                  f"process holds it open.", file=sys.stderr)
            for line in _state_db_holders():
                print(f"[pull-state]   {line}", file=sys.stderr)
            print("[pull-state] End those processes (an expired lease whose owner is "
                  "still running is a runaway) and re-run --pull-state.",
                  file=sys.stderr)
            return 1
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    _record_etag(etag_after)
    print(f"[pull-state] ok: {len(data):,} B compressed -> {len(raw):,} B at "
          f"{config.STATE_DB} (etag {etag_after})")
    return 0


# Compressed bytes above which the remote state store counts as authoritative and
# is protected from being overwritten by a shrunken local copy (R407). The real
# store is ~10 MB compressed; test fixtures and genuine first seeds are a few KB.
_SUBSTANTIAL_REMOTE = 200_000


def push_state() -> int:
    """VACUUM INTO + zstd-compress state.db, compare-and-swap upload to R2.

    Exit codes: 0 = pushed (+ dated backup); 2 = CAS refusal (another writer
    won — state NOT overwritten); 1 = other error.
    """
    zstandard = _zstd()
    if not os.path.exists(config.STATE_DB):
        print(f"[push-state] ERROR: {config.STATE_DB} does not exist; nothing to push.",
              file=sys.stderr)
        return 1
    r2 = blobmod.R2Blob()

    # --- Compare-and-swap gate (plan §1.2): never blind last-writer-wins. ---
    stored = _stored_etag()
    remote = r2.etag(STATE_KEY)
    if remote != stored:
        # Both-absent (no remote object, no recorded etag) is the ONE allowed
        # mismatch-free seed case and compares equal above. Everything else stops here.
        print("[push-state] ABORT: compare-and-swap failed — another writer won.",
              file=sys.stderr)
        print(f"  remote ETag now : {remote or '<object absent>'}", file=sys.stderr)
        print(f"  ETag we pulled  : {stored or '<never pulled>'}  ({ETAG_PATH})",
              file=sys.stderr)
        print("  The remote state.db changed since --pull-state (or was never pulled "
              "on this machine). NOT overwriting — that would silently destroy the "
              "other writer's state. Re-run the whole job starting from --pull-state.",
              file=sys.stderr)
        return 2
    if remote is None:
        print(f"[push-state] seeding: r2://{r2.bucket}/{STATE_KEY} does not exist yet; "
              f"this push creates it.")

    # --- R407 SHRINK GUARD. CAS only proves nobody else moved the remote; it does
    # --- NOT prove the local copy is still the state we pulled. If state.db has
    # --- been replaced by an empty database (a lost/moved file makes SQLite mint a
    # --- fresh 4 KB one), CAS passes and this push WIPES the authoritative store.
    # --- Refuse on an implausible local state; --allow-shrink is the escape hatch.
    # The guard's real question is "am I about to destroy something SUBSTANTIAL?",
    # and only the remote can answer it. The first version asked "is the local file
    # small?" instead, which refused every legitimate seed (a fresh machine's
    # state.db has no source_state table at all) and turned the `tests` workflow
    # red. Gate on the size of what would be overwritten: a trivial remote is not
    # worth protecting, a real one is.
    _remote_bytes = r2.size(STATE_KEY) or 0
    if _remote_bytes >= _SUBSTANTIAL_REMOTE and not os.environ.get("AQUEDUCT_ALLOW_SHRINK"):
        local_bytes = os.path.getsize(config.STATE_DB) if os.path.exists(config.STATE_DB) else 0
        try:
            _c = sqlite3.connect(f"file:{config.STATE_DB}?mode=ro", uri=True, timeout=30)
            n_src = _c.execute("SELECT COUNT(*) FROM source_state").fetchone()[0]
            n_run = _c.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            _c.close()
        except Exception as e:                                   # noqa: BLE001
            print(f"[push-state] REFUSING: cannot read {config.STATE_DB} to sanity-check "
                  f"it before pushing ({type(e).__name__}: {str(e)[:100]}).", file=sys.stderr)
            return 3
        if local_bytes < 1_000_000 or n_src < 50:
            print(f"[push-state] REFUSING to push an implausibly small state: "
                  f"{local_bytes:,} B, {n_src} source_state rows, {n_run} runs. "
                  f"The authoritative remote is not going to be overwritten with this. "
                  f"Restore the real state.db (see {os.path.join(config.STATE_DIR, '_superseded')} "
                  f"and --pull-state) and re-run. Set AQUEDUCT_ALLOW_SHRINK=1 only if you "
                  f"genuinely intend to shrink the state store.", file=sys.stderr)
            return 3
        print(f"[push-state] sanity ok: local {local_bytes:,} B / {n_src} sources / "
              f"{n_run} runs")

    # --- VACUUM INTO a temp copy: compact + a consistent snapshot even if some ---
    # --- other local process still holds the db open.                          ---
    tmp_db = os.path.join(config.STATE_DIR,
                          f"state.vacuum.{os.getpid()}.{uuid.uuid4().hex[:8]}.db")
    try:
        con = sqlite3.connect(config.STATE_DB)
        try:
            con.execute("VACUUM INTO ?", (tmp_db,))
        finally:
            con.close()
        with open(tmp_db, "rb") as f:
            raw = f.read()
    finally:
        if os.path.exists(tmp_db):
            try:
                os.remove(tmp_db)
            except OSError:
                pass

    # Level 9: the ~207 MB db compresses once per run but transfers twice
    # (state + backup) — worth the extra CPU for the smaller round-trip.
    comp = zstandard.ZstdCompressor(level=9).compress(raw)
    print(f"[push-state] {len(raw):,} B vacuumed -> {len(comp):,} B zstd")

    r2.put_atomic(STATE_KEY, comp)
    new_etag = r2.etag(STATE_KEY)
    if new_etag is None:  # cannot happen after a successful PUT; stay loud anyway
        print("[push-state] ERROR: uploaded object has no ETag on re-HEAD.",
              file=sys.stderr)
        return 1
    _record_etag(new_etag)  # a second push in the same job must pass its own CAS

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    runid = os.environ.get("GITHUB_RUN_ID", "local")
    backup_key = BACKUP_KEY_FMT.format(stamp=stamp, runid=runid)
    r2.put_atomic(backup_key, comp)
    print(f"[push-state] ok: r2://{r2.bucket}/{STATE_KEY} (etag {new_etag}) "
          f"+ backup {backup_key}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Aqueduct continuous-update runner")
    ap.add_argument("--source", action="append", help="limit to source_id (repeatable)")
    ap.add_argument("--strategy", action="append", help="limit to strategy (repeatable)")
    ap.add_argument("--cadence", action="append", help="limit to cadence (repeatable)")
    ap.add_argument("--force", action="store_true", help="ignore cadence + change-detection")
    ap.add_argument("--dry", "--dry-run", dest="dry", action="store_true",
                    help="report what's due, make no changes")
    ap.add_argument("--pull-state", action="store_true",
                    help="download R2 _aqueduct/state.db.zst to local state.db, "
                         "record its ETag, then exit")
    ap.add_argument("--push-state", action="store_true",
                    help="VACUUM+zstd local state.db, compare-and-swap upload to R2 "
                         "plus dated backup, then exit (exit 2 = another writer won)")
    a = ap.parse_args()

    # STAMP THE STACK. Dev and CI are not on the same major pandas: this workstation runs 2.3.3
    # and the runner resolves `pandas>=2.2` (uncapped, deliberately — see requirements-updater.txt)
    # to pandas-3.0.5-cp311. That is not academic. pandas 3.0 parses datetimes to non-nanosecond
    # resolution by default, which is why SEC's "15-NOV-0006" survived coercion on the runner,
    # produced a timestamp[us] column, and made sec_edgar fail every run it ever had while the
    # same input returned NaT on the laptop the fetcher was written on.
    #
    # That difference at least RAISED. The dangerous ones are quiet — dtype defaults, NA handling,
    # pandas 3.0's str-by-default — and would read as data rather than as a failure. One line in
    # every log makes a behaviour change attributable to the stack instead of to the publisher.
    try:
        import pandas as _pd
        import pyarrow as _pa
        print(f"[run] python {sys.version.split()[0]}  pandas {_pd.__version__}  "
              f"pyarrow {_pa.__version__}", flush=True)
    except Exception:                                              # noqa: BLE001
        pass          # a version banner must never be the reason a run does not start

    if a.pull_state and a.push_state:
        ap.error("--pull-state and --push-state are separate steps; pass one at a time")
    if a.pull_state:
        sys.exit(pull_state())
    if a.push_state:
        sys.exit(push_state())

    # Imported here, not at module top: the state steps must work (and fail
    # loudly on their own terms) without loading the full strategy stack.
    from . import orchestrate

    res = orchestrate.run_once(sources=a.source, strategies=a.strategy, cadences=a.cadence,
                               force=a.force, dry=a.dry)
    print(f"\n=== {len(res)} unit(s) processed ===")
    for k, s in res:
        print(f"  {s:16} {k}")


if __name__ == "__main__":
    main()
