"""derive_csv_bulk must serve ONLY uniform-long parquets from a mixed store dir.

cepii_baci's store holds raw vintage parquets (year/exporter/importer/product/value/quantity)
BESIDE the tidy pairs projection that actually serves. Before the schema filter, the tool's
DISTINCT-series_key scan hit the raw file first and died with a DuckDB BinderException — and
a subtler failure was possible: a raw file that HAPPENED to carry a series_key column would
have polluted the id universe silently. The filter must (a) keep the tidy file, (b) name what
it skipped, (c) refuse a dir with nothing servable. Each proven here, including that the
guard can FAIL (R346): remove the filter and the first test dies exactly as production did.
"""
from __future__ import annotations

import os
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "derive_csv_bulk.py")


def _mixed_store(root, source="mixsrc"):
    d = os.path.join(root, "data", "clean_full", source)
    os.makedirs(d, exist_ok=True)
    pq.write_table(pa.table({
        "year": pa.array([2020], pa.int64()),
        "exporter": pa.array(["4"], pa.string()),
        "value": pa.array([1.0], pa.float64()),
    }), os.path.join(d, "native_raw.parquet"))
    pq.write_table(pa.table({
        "series_key": pa.array(["K:AAA:BBB", "K:AAA:CCC"], pa.string()),
        "obs_date": pa.array([__import__("datetime").date(2020, 12, 31)] * 2, pa.date32()),
        "value": pa.array([1.0, 2.0], pa.float64()),
    }), os.path.join(d, "tidy_pairs.parquet"))
    return d


def _run(source, tmp_root):
    env = dict(os.environ, ECONDL_ROOT=str(tmp_root), PYTHONIOENCODING="utf-8")
    return subprocess.run(
        [sys.executable, TOOL, "--source", source, "--dry-run", "--verify", "0"],
        capture_output=True, text=True, cwd=ROOT, env=env, timeout=180)


def test_mixed_store_skips_raw_loudly_and_serves_tidy(tmp_path):
    _mixed_store(str(tmp_path))
    r = _run("mixsrc", tmp_path)
    out = r.stdout + r.stderr
    assert "skipping 1 non-uniform parquet" in out and "native_raw.parquet" in out, out
    assert "BinderException" not in out, "the raw file still reached the key scan:\n" + out
    # "N distinct series" only prints under --verify; the dry-run's own proof is the stream
    # count and the per-key PUT plan — both keys from the tidy file, none from the raw one.
    assert "done: 2 series streamed" in out, out
    assert out.count("would PUT") == 2, out


def test_store_with_nothing_servable_refuses(tmp_path):
    d = os.path.join(str(tmp_path), "data", "clean_full", "rawonly")
    os.makedirs(d)
    pq.write_table(pa.table({"year": pa.array([2020], pa.int64())}),
                   os.path.join(d, "native_raw.parquet"))
    r = _run("rawonly", tmp_path)
    assert r.returncode == 2, r.stdout + r.stderr
    assert "nothing here can serve" in (r.stdout + r.stderr)
