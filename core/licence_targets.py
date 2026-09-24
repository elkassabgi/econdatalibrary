"""WHERE the licence tools remove a source from (docs/ECON_SELF_HOSTING_PLAN.md, section 3, change 5: "The
licence tools get their local backend FIRST, so licence enforcement never has a gap").

tools/retire_source.py, delist_source_rows.py, delist_timeless_tables.py and purge_unpermitted_r2.py decide
WHAT to remove. This module owns WHERE, so the answer changes in one place at T0:

  before T0 (today, unchanged): objects in the econ R2 bucket, the checkout's catalogue, and D1 through wrangler (the
      remote execute in core.d1_remote);
  after T0 (core/cutover.py): series CSVs in the self-hosted blob store (updater.blob.SelfhostBlob), store files
      under the production store root's data/ folder, the ONE catalogue build (core.catalog_path) under the
      single-writer lock - and NO D1. D1 is the frozen copy: nothing may write to it, and a rollback onto it
      must first put anything removed after T0 on the edge denylist (plan, step 6 fallback). skip_d1() says so.

Every key is re-checked against the caller's terminated prefixes before it is removed (the R112/R129 trap:
'imf_fsi' must never sweep 'imf_fsi_direct'), whichever backend holds it.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import urllib.parse

from core.cutover import is_cut_over

BUCKET = "econ-data"
D1_NAME = "econ-catalog"
# Every table that names a source by source_id: /v1/sources needs series + source; source_counts is what
# /v1/catalog serves as `total` and /v1/stats sums (R709); unit_state / source_state / source_data_through
# are the freshness projection /v1/last-updates reads with no join to `source`.
SOURCE_TABLES = ("series", "source", "source_counts", "unit_state", "source_state", "source_data_through")
# After T0, one JSON line per removal, in the state folder (Targets.record_removal).
REMOVAL_LOG = "licence_removals.jsonl"


def csv_prefix(source: str) -> str:
    """The TERMINATED series-CSV prefix of a source: 'series/<source>%3A'."""
    return "series/" + urllib.parse.quote(f"{source}:", safe="")


def removed_csv_prefixes() -> list[str]:
    """Every series-CSV prefix a post-T0 licence removal took out of the store, from the durable removal log
    (record_removal): each logged source's prefix, and any prefix a delisting named. A tool that could put an
    object back (tools/selfhost/import_from_r2.py) refuses these - R1220 finding 1: it restored a retired
    source's CSVs from the frozen R2 copy. No log = nothing removed; a log that cannot be read is an error."""
    import json                                                                    # noqa: PLC0415
    from core.catalog_path import LIVE_STATE_DIR                                   # noqa: PLC0415
    path = os.path.join(LIVE_STATE_DIR, REMOVAL_LOG)
    if not os.path.exists(path):
        return []
    out = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            rec = json.loads(line)                    # a torn line raises: never read as "nothing removed"
            named = [w for w in str(rec.get("what", "")).split() if w.startswith("series/")]
            # a purge of ONE prefix names it, and blocks only it (R1225: it blocked the whole source); a
            # retirement or a delisting names no prefix, and blocks all of the source's CSVs
            out.update(named or [csv_prefix(rec["source"])])
    return sorted(out)


def store_prefix(source: str) -> str:
    """The TERMINATED store prefix of a source: 'clean_full/<source>/'."""
    return f"clean_full/{source}/"


def _check(keys, allowed: tuple[str, ...]) -> None:
    bad = [k for k in keys if not k.startswith(allowed)]
    if bad:
        raise ValueError(f"refusing to remove keys outside {allowed}: {bad[:3]}")


class Targets:
    """The places a licence removal acts on. Build ONE per run, before any change: the T0 answer is taken
    once, so a run never mixes the two backends."""

    def __init__(self, apply: bool = True):
        self.selfhosted = is_cut_over()
        self._r2_read = self._r2_write = self._blob = None
        if self.selfhosted and apply:                   # a dry run only reads, from any checkout (R1220)
            # up front, before the run changes anything: a removal from a worktree deleted catalogue rows and
            # archived parquets, then had its CSV delete refused - the source gone from the catalogue while
            # its CSVs stayed served (R1217 finding 2). The lock is the caller's (catalogue writes need it).
            from updater.blob import refuse_unless_live_checkout                  # noqa: PLC0415
            refuse_unless_live_checkout("a licence removal")

    # -- objects ----------------------------------------------------------------------------------------
    def _read(self):
        if self._r2_read is None:
            from core import r2_util                                               # noqa: PLC0415
            self._r2_read = r2_util.client()
        return self._r2_read

    def _write(self):
        if self._r2_write is None:
            from core import r2_util                                               # noqa: PLC0415
            self._r2_write = r2_util.client(write=True)
        return self._r2_write

    def _blobs(self):
        if self._blob is None:
            from updater.blob import SelfhostBlob                                  # noqa: PLC0415
            self._blob = SelfhostBlob()
        return self._blob

    def _store_path(self, key: str) -> str:
        from core.catalog_path import LIVE_STORE_ROOT                              # noqa: PLC0415
        return os.path.join(LIVE_STORE_ROOT, "data", *key.split("/"))

    def list(self, prefix: str) -> list[tuple[str, int]]:
        """(key, size) under a prefix."""
        if not self.selfhosted:
            out, tok = [], None
            while True:
                kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
                if tok:
                    kw["ContinuationToken"] = tok
                r = self._read().list_objects_v2(**kw)
                out += [(o["Key"], o["Size"]) for o in r.get("Contents", [])]
                if not r.get("IsTruncated"):
                    return out
                tok = r.get("NextContinuationToken")
        if prefix.startswith("series/"):
            b = self._blobs()
            return [(k, b.size(k) or 0) for k in b.list_keys(prefix)]
        base = self._store_path(prefix.rstrip("/"))
        out = []
        if os.path.isdir(base):
            for dirpath, _dirs, files in os.walk(base):
                for f in files:
                    p = os.path.join(dirpath, f)
                    rel = os.path.relpath(p, self._store_path("")).replace(os.sep, "/")
                    if rel.startswith(prefix):
                        out.append((rel, os.path.getsize(p)))
        return sorted(out)

    def archive(self, key: str, dst: str) -> None:
        """Keep a copy of `key` at `dst` before it is removed (cheap insurance)."""
        if not self.selfhosted:
            self._write().copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=dst)
            return
        if key.startswith("series/"):
            self._blobs().copy(key, dst)                     # R2's copy_object keeps all of these (AR-153)
            return
        target = self._store_path(dst)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy2(self._store_path(key), target)

    def delete(self, keys: list[str], allowed: tuple[str, ...]) -> int:
        """Remove keys, each re-checked against the terminated prefixes. Returns how many."""
        keys = list(keys)
        _check(keys, allowed)
        if not self.selfhosted:
            for i in range(0, len(keys), 1000):
                batch = keys[i:i + 1000]
                _check(batch, allowed)
                self._write().delete_objects(Bucket=BUCKET,
                                             Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
            return len(keys)
        dirs = set()
        for k in keys:
            if k.startswith("series/"):
                self._blobs().delete(k)
            else:
                p = self._store_path(k)
                if os.path.exists(p):
                    os.remove(p)
                dirs.add(os.path.dirname(p))
        # the now-empty folders go too (AR-153), deepest first, never above the store's data folder
        stop = os.path.normcase(os.path.abspath(self._store_path("")))
        for d in sorted(dirs, key=len, reverse=True):
            while os.path.normcase(os.path.abspath(d)) != stop and os.path.isdir(d) and not os.listdir(d):
                os.rmdir(d)
                d = os.path.dirname(d)
        return len(keys)

    # -- the catalogue ----------------------------------------------------------------------------------
    def catalogue(self, write: bool = True) -> sqlite3.Connection:
        """A connection to the catalogue through the one resolver (core.catalog_path): the checkout's
        catalogue before T0 (the same file as today), the one build after T0. A WRITE connection after T0
        needs core.catalog_path.writer_lock() held by the caller; a dry run reads (write=False) and so
        needs no lock (AR-153)."""
        from core.catalog_path import connect                                       # noqa: PLC0415
        con = connect(write=write, timeout=120)
        con.execute("PRAGMA busy_timeout=120000")
        return con

    def remove_source_rows(self, con: sqlite3.Connection, source: str) -> int:
        """Delete a source's catalogue rows; returns the residual `series` rows (must be 0).

        Before T0: series + source, as the tools always did locally (the other tables live only in D1).
        After T0 the build ALSO carries the tables D1 carried - source_counts, unit_state, source_state,
        source_data_through - and D1 is skipped, so they are deleted here: leaving them would keep the
        source in /v1/catalog totals and /v1/last-updates (R709; 15 ids served that way on 2026-09-21)."""
        tables = SOURCE_TABLES if self.selfhosted else SOURCE_TABLES[:2]
        present = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in tables:
            if table in present:
                con.execute(f"DELETE FROM {table} WHERE source_id=?", (source,))
        if self.selfhosted and "series_fts" in present:
            # the search index rows too (AR-153: orphans broke the swap's fts = series check). One range scan
            # of the FTS table; 'src:' <= id < 'src;' never reaches a 'src_direct:' neighbour.
            con.execute("DELETE FROM series_fts WHERE series_id >= ? AND series_id < ?", (f"{source}:", f"{source};"))
        con.commit()
        return con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (source,)).fetchone()[0]

    # -- D1 ---------------------------------------------------------------------------------------------
    @staticmethod
    def d1_statements(source: str) -> list[str]:
        """The D1 deletes for one source, every table that names it (R709 and the freshness projection)."""
        return [f"DELETE FROM {table} WHERE source_id='{source}';" for table in SOURCE_TABLES]

    @staticmethod
    def d1_plan(source: str) -> list[tuple[str, list[str]]]:
        """(database, statements) for one source. A source routed to the climate shard
        (core.sync_state_d1.CATALOG_SHARD_FOR) has its series and source_counts rows THERE, so they are
        deleted there too - the tools used to delete only on the primary (AR-153 finding 8)."""
        from core.sync_state_d1 import CATALOG_SHARD_FOR                          # noqa: PLC0415
        plan = [(D1_NAME, Targets.d1_statements(source))]
        shard = CATALOG_SHARD_FOR.get(source)
        if shard:
            plan.append((shard, [f"DELETE FROM {t} WHERE source_id='{source}';" for t in ("series", "source_counts")]))
        return plan

    def skip_d1(self) -> bool:
        """True after T0: D1 is the frozen copy and is not written. Prints what a rollback then needs."""
        if self.selfhosted:
            print("  D1: skipped - econ is self-hosted; D1 is the frozen T0 copy. Before any rollback onto it, "
                  "every removal in the licence-removal log must be on the edge denylist (plan, step 6 fallback).")
        return self.selfhosted

    def d1_execute(self, source: str) -> bool:
        """Before T0: run the source's D1 plan (core.d1_remote.execute_wrangler, the one D1 chokepoint),
        stopping at the first failure. After T0: never called for writes (skip_d1 first); it refuses."""
        if self.selfhosted:
            raise RuntimeError("d1_execute after T0: D1 is frozen; call skip_d1() first")
        from core.d1_remote import execute_wrangler                                # noqa: PLC0415
        return all(execute_wrangler(db, stmts) for db, stmts in self.d1_plan(source))

    def record_removal(self, tool: str, source: str, what: str) -> None:
        """After T0, append the removal to the durable licence-removal log in the state folder (AR-153
        finding 2): a rollback onto the frozen D1/R2 copy within the fallback period must deny every one of
        these at the edge first, and console output is not a record. Before T0 D1 itself is updated, so
        there is nothing to record."""
        if not self.selfhosted:
            return
        import datetime as dt                                                      # noqa: PLC0415
        import json                                                                # noqa: PLC0415
        from core.catalog_path import LIVE_STATE_DIR                               # noqa: PLC0415
        os.makedirs(LIVE_STATE_DIR, exist_ok=True)
        line = json.dumps({"at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                           "tool": tool, "source": source, "what": what})
        with open(os.path.join(LIVE_STATE_DIR, REMOVAL_LOG), "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())
