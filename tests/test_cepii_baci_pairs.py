"""cepii_baci's pair-grain projection: correctness on a known fixture, guards proven to fire.

The projection turns 243M raw HS6 rows into ~74k pair-total series. The failure modes that
matter are not the happy path: double-counting (summing HS17 AND HS96), a silently dropped
country (unmapped numeric code), and building from a guessed crosswalk instead of the
vintage's own. Each has a test that FAILS when the guard is removed (R346).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.errors import DefinitiveError                       # noqa: E402
from updater.strategies.fetchers import cepii_baci as F          # noqa: E402


def _fixture_store(tmp_path, monkeypatch):
    """A tiny baci_hs96.parquet with KNOWN totals, plus the codes sidecar."""
    d = tmp_path / "cepii_baci"
    d.mkdir()
    # 4 raw rows: pair (4->12) has two products in 2020 (values 1.5+2.5, qty 3.0+null),
    # pair (12->4) one product in 2020 and one in 2021.
    tbl = pa.table({
        "year":     pa.array([2020, 2020, 2020, 2021], pa.int64()),
        "exporter": pa.array(["4", "4", "12", "12"], pa.string()),
        "importer": pa.array(["12", "12", "4", "4"], pa.string()),
        "product":  pa.array(["010101", "020202", "010101", "010101"], pa.string()),
        "value":    pa.array([1.5, 2.5, 10.0, 20.0], pa.float64()),
        "quantity": pa.array([3.0, None, 7.0, 8.0], pa.float64()),
    })
    pq.write_table(tbl, str(d / "baci_hs96.parquet"))
    # an HS17 file that would DOUBLE the totals if the projection wrongly read it
    pq.write_table(tbl, str(d / "baci_hs17.parquet"))
    (d / "_country_codes.json").write_text(
        json.dumps({"from": "fixture", "codes": {"4": "AFG", "12": "DZA"}}), encoding="utf-8")
    monkeypatch.setattr(F.config, "source_dir", lambda s: str(d))
    return d


def test_projection_totals_are_exact_and_hs96_only(tmp_path, monkeypatch):
    d = _fixture_store(tmp_path, monkeypatch)
    n = F.build_pairs_projection(str(d))
    out = pq.read_table(str(d / F.PAIRS_BASENAME))
    got = {(k, str(o)): v for k, o, v in zip(out.column("series_key").to_pylist(),
                                             out.column("obs_date").to_pylist(),
                                             out.column("value").to_pylist())}
    # exact totals — and if HS17 were wrongly included, every one of these would double
    assert got[("BACI:tv:AFG:DZA", "2020-12-31")] == pytest.approx(4.0)
    assert got[("BACI:tq:AFG:DZA", "2020-12-31")] == pytest.approx(3.0)   # null-safe sum
    assert got[("BACI:tv:DZA:AFG", "2020-12-31")] == pytest.approx(10.0)
    assert got[("BACI:tv:DZA:AFG", "2021-12-31")] == pytest.approx(20.0)
    assert n == out.num_rows
    # schema is the uniform-long serving contract
    assert set(out.column_names) == {"series_key", "obs_date", "value"}


def test_unmapped_country_code_fails_listing_it(tmp_path, monkeypatch):
    d = _fixture_store(tmp_path, monkeypatch)
    (d / "_country_codes.json").write_text(
        json.dumps({"from": "fixture", "codes": {"4": "AFG"}}), encoding="utf-8")  # 12 missing
    with pytest.raises(DefinitiveError) as e:
        F.build_pairs_projection(str(d))
    assert "12" in str(e.value), "the failing code must be NAMED, not just counted"


def test_missing_sidecar_refuses_to_guess(tmp_path, monkeypatch):
    d = _fixture_store(tmp_path, monkeypatch)
    (d / "_country_codes.json").unlink()
    with pytest.raises(DefinitiveError):
        F.build_pairs_projection(str(d))


def test_extract_country_codes_from_a_real_zip_shape(tmp_path):
    z = tmp_path / "HS96.zip"
    rows = "country_code,country_name,country_iso2,country_iso3\n" + "\n".join(
        f"{i},Name{i},X{i % 10},IS{i % 10}" for i in range(1, 240))
    with zipfile.ZipFile(z, "w") as f:
        f.writestr("country_codes_V209901.csv", rows)
        f.writestr("BACI_HS96_Y2020_V209901.csv", "ignored")
    out = tmp_path / "out"
    out.mkdir()
    codes = F.extract_country_codes(str(z), str(out))
    assert codes["1"] == "IS1" and len(codes) == 239
    side = json.loads((out / F.CODES_SIDECAR).read_text(encoding="utf-8"))
    assert side["codes"] == codes

    # and the shrunken-mapping guard FIRES (a 10-row csv is a schema change, not a small world)
    small = tmp_path / "small.zip"
    with zipfile.ZipFile(small, "w") as f:
        f.writestr("country_codes_V209902.csv",
                   "country_code,country_name,country_iso2,country_iso3\n1,A,AA,AAA\n")
    with pytest.raises(DefinitiveError):
        F.extract_country_codes(str(small), str(out))


def test_resolver_targets_the_pairs_file_only(tmp_path, monkeypatch):
    sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
    from econdl import _resolve as R
    d = _fixture_store(tmp_path, monkeypatch)
    F.build_pairs_projection(str(d))
    res = R.resolve("cepii_baci:BACI:tv:AFG:DZA", root=str(tmp_path))
    assert os.path.basename(str(res.parquet_path)) == F.PAIRS_BASENAME, (
        "the resolver must pin the tidy projection, never glob the raw vintage files")
    t = R.read_native(res)
    assert t.num_rows == 1 and t.column("value").to_pylist() == [pytest.approx(4.0)]
