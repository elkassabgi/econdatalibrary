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


def _dedup(table, keys):
    keys = [k for k in keys if k in table.column_names]
    if not keys or table.num_rows == 0:
        return table
    t = table.append_column("__i", pa.array(range(table.num_rows), type=pa.int64()))
    grouped = t.group_by(keys).aggregate([("__i", "max")])  # keep last row per key-combo
    keep = grouped.column("__i_max")
    mask = pc.is_in(t.column("__i"), value_set=keep)
    return t.filter(mask).drop_columns(["__i"])


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

    final = _sort(final, dedup_keys)
    if blob is None:
        fsblob.write_table_atomic(out_path, final)
    else:
        blob.put_atomic(out_path, _table_to_bytes(final))
    return n, _max_obs_date(final)
