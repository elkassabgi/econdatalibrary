"""merge_and_write — the extension invariant.

A write either ADVANCES last_obs_date / obs_count for a unit, or it is a no-op.
It NEVER replaces good data with fewer or zero rows. This guards two bug classes:
  - "skip series if key already present" -> existing series freeze (67 sources)
  - "silently write a 0-row group, then mark done" (bea, fred_releases)

mode='merge'     : union new rows with the existing parquet, dedup on dedup_keys
                   (new rows win on revision), sort, publish atomically.
mode='overwrite' : new_table IS the full content (whole-table refresh); still
                   never-shrink checked before publish.

Cloud portability (UPDATER_BUILD_PLAN.md §1.3): merge_and_write accepts an
optional blob= handle. With blob=None (the default) behavior is byte-identical
to the original local-filesystem path. With a Blob (e.g. blob.R2Blob in CI),
out_path is the object KEY and the same read-modify-write + invariants run
against R2: GET existing object -> concat/dedup/never-shrink -> single atomic
PUT. The invariants themselves are one shared code path — only the I/O layer
switches — so local and cloud publishes can never drift apart.
"""
from __future__ import annotations

import datetime as dt
import io

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import blob as fsblob
from .errors import DefinitiveError

DEDUP_KEYS = ("series_key", "obs_date")


def _max_obs_date(table) -> str | None:
    if table.num_rows == 0 or "obs_date" not in table.column_names:
        return None
    m = pc.max(table.column("obs_date")).as_py()
    return str(m) if m is not None else None


# Promote a string column PRE-EMPTIVELY once it gets near Arrow's 2 GiB int32-offset
# ceiling. 1 GiB leaves room for the concat that follows to double it without crossing.
_LARGE_STRING_TRIGGER = 1 << 30


def _needs_large_string(table) -> bool:
    """True if any 32-bit `string` column is close enough to 2 GiB to be worth promoting."""
    for name in table.column_names:
        col = table.column(name)
        if col.type == pa.string() and col.nbytes >= _LARGE_STRING_TRIGGER:
            return True
    return False


def _dedup(table, keys):
    keys = [k for k in keys if k in table.column_names]
    if not keys or table.num_rows == 0:
        return table

    # PROMOTE BEFORE GROUPING, NOT AFTER FAILING. _concat and _sort both recover from the
    # 2 GiB ceiling by catching pa.ArrowInvalid("offset overflow") and retrying on
    # large_string. group_by DOES NOT RAISE — it dereferences past the overflowed offsets
    # and takes the process down: measured on bis/LBS.parquet (36,379,671 rows whose
    # series_key column holds 13,203,140,215 bytes, 6.6x the ceiling) it exits
    # 0xC0000005 ACCESS_VIOLATION on Windows and SIGABRT/134 on Linux via
    # `std::length_error: vector::_M_default_append`. Neither is catchable from Python, so a
    # reactive guard here is not possible — the promotion has to happen first.
    #
    # This is what actually killed the daily updater, and it is NOT a memory problem: the
    # same crash occurred on a 382 GB workstation with 337 GB free. Verified in isolation —
    # group_by on the raw table dies, while sort_by on the SAME table cast to large_string
    # completes all 36,379,671 rows.
    if _needs_large_string(table):
        table = _promote_large_string(table)[0]

    # SORT-BASED, NOT HASH-BASED. This used to be
    #     grouped = t.group_by(keys).aggregate([("__i", "max")])
    #     mask    = pc.is_in(t.column("__i"), value_set=grouped.column("__i_max"))
    # and group_by is what crashed. Measured on bis/LBS.parquet: it dies on a `string`
    # column (0xC0000005 ACCESS_VIOLATION) AND on the same data cast to `large_string`
    # (0xC0000409), so promoting the type is necessary but NOT sufficient — the hash
    # aggregation itself cannot handle this size. sort_by on the identical table completes
    # all 36,379,671 rows, so the dedup is expressed with sort + vector comparisons only.
    #
    # Same semantics as before: sorting by (keys..., __i) puts each key-combo's rows in
    # ORIGINAL order, so the last row of each run is the one group_by's max(__i) chose.
    # New data is appended after existing, so last still means "new wins".
    t = table.append_column("__i", pa.array(range(table.num_rows), type=pa.int64()))
    t = _sort(t, tuple(keys) + ("__i",))
    n = t.num_rows
    if n == 1:
        return t.drop_columns(["__i"])

    # NULL == NULL MUST MEAN "SAME KEY" HERE. Arrow's equal() returns NULL when either side
    # is null; that NULL survives and_(), invert() turns it into NULL, and filter() DROPS
    # null-mask rows. So a dedup key that is null silently discarded every row but the last:
    # measured on treasury's four DATELESS endpoints, where obs_date is null BY DESIGN and
    # identity is the dimension columns — fbp_dpai_account_summary deduped 185 distinct rows
    # down to 1, every run since 2026-07-23.
    #
    # It only surfaced because the never-shrink ratio refused the write (185 -> 1 is far
    # under 97%), and that is the dangerous part: a source with SOME null keys would lose
    # only those rows, stay above the ratio, and publish as a clean merge. Silent data loss
    # that happened to be caught by a different guard.
    #
    # These are GROUPING semantics, not SQL comparison semantics: two rows whose key is null
    # in the same position ARE the same key. fill_null(False) makes a null comparison "not
    # equal", then both-null is added back explicitly.
    same_as_next = None
    for k in keys:
        col = t.column(k).combine_chunks()
        lhs, rhs = col.slice(0, n - 1), col.slice(1, n - 1)
        eq = pc.or_(pc.fill_null(pc.equal(lhs, rhs), False),
                    pc.and_(pc.is_null(lhs), pc.is_null(rhs)))
        same_as_next = eq if same_as_next is None else pc.and_(same_as_next, eq)

    # keep a row when the NEXT row begins a different key-combo; the final row always ends
    # its own run
    keep = pa.concat_arrays([
        pc.invert(same_as_next).cast(pa.bool_()).combine_chunks()
        if hasattr(same_as_next, "combine_chunks") else pc.invert(same_as_next).cast(pa.bool_()),
        pa.array([True], pa.bool_()),
    ])
    return t.filter(keep).drop_columns(["__i"])


def _sort(table, keys):
    keys = [k for k in keys if k in table.column_names]
    if not keys:
        return table
    try:
        return table.sort_by([(k, "ascending") for k in keys])
    except pa.ArrowInvalid as e:
        if "offset overflow" not in str(e):
            raise
        # sort_by() materialises the result via take(), which re-concatenates the string
        # columns and so hits Arrow's 2 GiB 32-bit `string` offset ceiling — the same wall
        # as concat, just reached later. Retry on the 64-bit type. Real case: ons_uk's
        # 200+ char colon-joined series_keys over ~3.9M rows.
        return _promote_large_string(table)[0].sort_by([(k, "ascending") for k in keys])


def _promote_large_string(*tables):
    """Cast every `string` column to `large_string` across the given tables.

    Arrow's 32-bit `string` type caps a column's total bytes at 2 GiB; concatenating past
    that raises ArrowInvalid("offset overflow while concatenating arrays"). Sources with long
    identifiers hit this for real — ons_uk builds colon-joined `dim=value` series_keys of
    200+ chars (it stores both code AND label, e.g. `sex=female:Sex=Female`), so ~3.9M rows
    overflow. `large_string` is the 64-bit variant and is written back to parquet as ordinary
    UTF-8, so this changes nothing on disk for readers.
    """
    out = []
    for t in tables:
        fields = [f.with_type(pa.large_string()) if f.type == pa.string() else f
                  for f in t.schema]
        out.append(t.cast(pa.schema(fields)) if fields != list(t.schema) else t)
    return out


def _concat(existing, new_table):
    if existing.schema.equals(new_table.schema):
        try:
            return pa.concat_tables([existing, new_table])
        except pa.ArrowInvalid as e:
            if "offset overflow" not in str(e):
                raise
            # 2 GiB string-offset ceiling: retry with the 64-bit string type rather than
            # failing the source. Keeps ALL rows — the alternative would be silent loss.
            big_existing, big_new = _promote_large_string(existing, new_table)
            return pa.concat_tables([big_existing, big_new])
    # NEVER drop a column that exists in the published data (silent column-level loss).
    # If the new table is missing any existing column, that's a schema regression -> refuse.
    missing = [c for c in existing.column_names if c not in new_table.column_names]
    if missing:
        raise DefinitiveError(
            f"new data is missing column(s) {missing} present in the published file "
            f"(existing={existing.column_names}, new={new_table.column_names}); refusing to "
            f"drop historical columns — keeping old data, surfacing as partial")
    # New may add columns; permissive union preserves ALL columns and null-fills the rest.
    return pa.concat_tables([existing, new_table], promote_options="permissive")


def _table_from_bytes(data: bytes):
    return pq.read_table(io.BytesIO(data))


def _table_to_bytes(table, compression: str = "zstd") -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)
    return buf.getvalue()


# No published statistical series reaches this far. The longest real horizon in this fleet is
# UN WPP at 2101, and health.py documents the other genuine projections (ABS to 2046 and 2071,
# IMF WEO to 2031). 2200 sits far above all of them, so anything past it is a parse artifact,
# not data — the point is to be unarguable, not tight.
_IMPOSSIBLE_AFTER = dt.date(2200, 1, 1)


# IMPOSSIBLE DATES SEEN THIS UNIT. The guard below has always returned its count and the
# caller has always thrown it away, so the only record was a printed line — and a warning that
# never blocks and never AGGREGATES is indistinguishable from silence. It was printed on every
# affected run for weeks while 273,980 rows across six sources sat published with dates between
# 2999-12-31 and 9999-12-31, and it was found by finally running a standalone audit, not by
# anyone reading a log (ledger R320).
#
# Accumulating it here lets the orchestrator report a per-source total next to the row counts,
# where a number that grows is noticeable. Still does NOT block a publish: dropping rows on a
# heuristic would be data loss, and that judgement is unchanged.
_impossible_seen: dict = {"rows": 0, "files": 0, "worst": None}


def impossible_reset() -> None:
    """Called by the orchestrator before each unit runs."""
    _impossible_seen["rows"] = 0
    _impossible_seen["files"] = 0
    _impossible_seen["worst"] = None


def impossible_report() -> dict:
    """Snapshot of impossible-date rows written since the last reset."""
    return dict(_impossible_seen)


def _report_impossible_dates(table, out_path) -> int:
    """Count and ANNOUNCE observations dated beyond any possible publication horizon.

    WHY THIS EXISTS. Every instrument in this system measures RECENCY — is the newest
    observation old? A fabricated FUTURE date passes that trivially; it makes a source look
    maximally fresh. The health gate even filters forward-dated periods out of its recency
    signal, correctly, so that real projections do not cry wolf — which means the same
    mechanism hides fabricated dates. Nothing anywhere asked whether a value was POSSIBLE.

    Measured 2026-08-03: cso had been serving 434,408 rows (0.887% of 48,960,271, across 11
    files) dated beyond 2100 — 272,445 in Census 2016 at 9998-12-31, because a classification
    dimension whose codes are CSO sentinels (3001, 9998, 9999) was being read as the time axis.
    It reached users. No check in the fleet could see it, and it was found only by reading the
    store by hand.

    REPORTS, NEVER DROPS. Silently discarding rows would be data loss decided by a heuristic,
    and this runs on the path EVERY fetcher takes — the blast radius of a wrong bound is the
    whole fleet. Refusing the publish would be worse still: it would reject a good pull over a
    handful of bad cells. So it prints, loudly, into the run log that is already read daily,
    and the humans decide.

    Cheap by construction: one Arrow comparison over a column already materialised, next to a
    sort and a serialise that dominate this function's cost.
    """
    try:
        if "obs_date" not in table.column_names:
            return 0
        col = table.column("obs_date").combine_chunks()
        bad = pc.greater(col, _IMPOSSIBLE_AFTER)
        n_bad = pc.sum(pc.cast(bad, "int64")).as_py() or 0
        if not n_bad:
            return 0
        _impossible_seen["rows"] += int(n_bad)
        _impossible_seen["files"] += 1
        sample = table.filter(bad).slice(0, 1)
        key = (sample.column("series_key")[0].as_py()
               if "series_key" in sample.column_names else "?")
        when = sample.column("obs_date")[0].as_py()
        print(f"[merge] IMPOSSIBLE DATES: {n_bad:,} of {table.num_rows:,} row(s) at {out_path} "
              f"are dated after {_IMPOSSIBLE_AFTER.year} — e.g. {str(key)[:80]} -> {when}. "
              f"Published anyway (dropping would be data loss decided by a heuristic), but a "
              f"time axis is almost certainly being read off a non-time dimension.", flush=True)
        if (_impossible_seen["worst"] is None
                or when > _impossible_seen["worst"][1]):
            _impossible_seen["worst"] = (str(key)[:80], when)
        return n_bad
    except Exception:                                        # noqa: BLE001
        return 0                                             # never fail a good publish


def merge_and_write(out_path, new_table, *, mode="merge", dedup_keys=DEDUP_KEYS,
                    min_ratio=0.97, allow_empty=False, blob=None):
    """Publish new_table to out_path under the never-shrink invariant.

    Returns (rows_written, last_obs_date).
    Raises DefinitiveError if: a published column would be dropped; the dedup keys
    are absent (dedup would silently break); the result is empty (and not
    allow_empty); or the result would shrink below min_ratio of the existing row
    count. In every refusal the existing file is left untouched (caller keeps good
    data and surfaces the unit as partial). `min_ratio` defaults to 0.97 so a
    truncated/partial upstream pull can't silently overwrite good data; sources
    that legitimately shrink more must pass an explicit lower min_ratio.

    blob=None publishes to the local filesystem exactly as before. Passing a
    Blob handle (blob.LocalBlob/blob.R2Blob) treats out_path as the object key
    and runs the identical invariants against that backend.
    """
    if blob is None:
        # Local filesystem — the original code path, byte-identical behavior.
        old_rows = fsblob.row_count(out_path)
        existing = (fsblob.read_table(out_path)
                    if mode == "merge" and fsblob.exists(out_path) else None)
    else:
        # Blob handle: one GET serves both the never-shrink baseline and the merge
        # base (an R2 GET is the expensive step — never fetch the object twice).
        raw = blob.get(out_path)
        prior = _table_from_bytes(raw) if raw is not None else None
        old_rows = prior.num_rows if prior is not None else 0
        existing = prior if mode == "merge" else None

    if existing is not None:  # merge mode with a published object to extend
        combined = _concat(existing, new_table)  # raises if a published column would vanish
        missing_keys = [k for k in dedup_keys if k not in combined.column_names]
        if dedup_keys and missing_keys:
            raise DefinitiveError(
                f"dedup key(s) {missing_keys} absent from columns {combined.column_names} at "
                f"{out_path}; refusing to merge (dedup would silently break)")
        final = _dedup(combined, dedup_keys)
    elif mode == "merge":
        final = _dedup(new_table, dedup_keys)
    else:  # overwrite — new_table is the full content; never-shrink still guards it
        final = new_table

    n = final.num_rows
    if n == 0 and not allow_empty:
        raise DefinitiveError(f"refusing to publish 0 rows to {out_path} (existing={old_rows})")
    if old_rows and n < old_rows * min_ratio:
        raise DefinitiveError(
            f"refusing shrink {old_rows}->{n} at {out_path} (< {min_ratio:.0%} of existing)")

    _report_impossible_dates(final, out_path)

    final = _sort(final, dedup_keys)
    if blob is None:
        fsblob.write_table_atomic(out_path, final)
    else:
        blob.put_atomic(out_path, _table_to_bytes(final))
    last = _max_obs_date(final)

    # RETURN THE WORKING SET TO THE OS BEFORE THE NEXT CALL (2026-07-30).
    # This function is the hot loop of every batched fetcher: read the whole existing
    # parquet, concat, dedup (which allocates an index column, a group-by hash table and
    # an is_in value set), sort, serialise. Dropping the references is not enough — Arrow
    # keeps freed blocks in its pool, so across many calls RSS only climbs.
    #
    # THIS KILLED A RUN. bis streams LBS.parquet (36,379,671 rows) in BATCH=500,000
    # chunks, i.e. 73 merges over a growing table, and memory went 1,516MB -> 15,700MB in
    # under seven minutes (~2,100 MB/min, seven times abs's rate) before Arrow aborted the
    # process: `std::length_error: vector::_M_default_append`, exit 134 (SIGABRT). That is
    # a THIRD way this class evades the workflow's rc=137/143 OOM branch, after a destroyed
    # runner reporting "cancelled" and a plain unbounded fold.
    #
    # Placed here rather than in bis so all ~26 bulk_snapshot_if_changed sources and every
    # other batched merger get the bound, instead of one fetcher at a time.
    del final, existing, new_table
    try:
        pa.default_memory_pool().release_unused()
    except Exception:                                        # noqa: BLE001
        pass                                                 # never fail a good publish
    return n, last
