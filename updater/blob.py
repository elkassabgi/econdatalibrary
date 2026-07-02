"""Blob (object) accessor — filesystem now, R2 later.

The atomic-publish primitive: write to <path>.tmp then os.replace (atomic on the
same filesystem). On R2 this becomes: write a versioned key, then flip a pointer.
Strategy adapters call only these functions, never open()/boto directly, so the
cloud swap is isolated here.
"""
from __future__ import annotations
import os
import uuid

import pyarrow.parquet as pq


def exists(path: str) -> bool:
    return os.path.exists(path)


def read_table(path: str):
    return pq.read_table(path)


def write_table_atomic(path: str, table, compression: str = "zstd") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Per-process+per-call unique temp name so two concurrent writers (different
    # runners/threads) can never clobber each other's in-progress temp file.
    tmp = f"{path}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        pq.write_table(table, tmp, compression=compression)
        os.replace(tmp, path)  # atomic publish — readers never see a half-written file
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def row_count(path: str) -> int:
    if not os.path.exists(path):
        return 0
    try:
        return pq.read_metadata(path).num_rows
    except Exception:
        return 0
