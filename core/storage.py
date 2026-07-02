"""Per-series Parquet writer/reader -- LOCAL stand-in for R2.

Same layout we'll use on R2:  data/clean/<source>/<series>.parquet
(on R2 the object key is the identical path). Swapping to R2 later = replacing
the open()/write with a boto3 put_object; nothing else changes.
"""
from __future__ import annotations
import os

import pyarrow as pa
import pyarrow.parquet as pq

DATA = os.path.join(os.path.dirname(__file__), "..", "data", "clean")


def _safe(series_id: str) -> str:
    return series_id.replace(":", "__").replace("/", "_")


def series_path(series_id: str, base: str | None = None) -> str:
    base = os.path.abspath(base or DATA)
    source = series_id.split(":")[0]
    return os.path.join(base, source, _safe(series_id) + ".parquet")


def write_series_parquet(series_id, observations, base: str | None = None) -> str:
    rows = sorted(observations, key=lambda o: o.obs_date)
    table = pa.table({
        "obs_date": pa.array([o.obs_date for o in rows], type=pa.date32()),
        "value":    pa.array([o.value for o in rows], type=pa.float64()),
        "version":  pa.array([o.version for o in rows], type=pa.string()),
    })
    path = series_path(series_id, base)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path)
    return path


def read_series(series_id, base: str | None = None) -> list[dict]:
    return pq.read_table(series_path(series_id, base)).to_pylist()
