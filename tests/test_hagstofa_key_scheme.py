"""hagstofa: a re-fetch whose key scheme the table has never stored is refused, not merged (2026-09-23).

Hagstofa's 2026-09-15 republications renamed variable codes (SJA04903: Tegund/Land/Afurdaflokkur/Eining
-> Species/Country/Product category/Unit). A renamed key never collides with its stored twin, so the
merge published both schemes in one table: on R2 SJA04903 holds 4,251 Icelandic-coded rows frozen at
2024 beside 2 English-coded rows with 2025 (R519's shape: nothing 404s, never-shrink cannot see it).
The real update() and merge run; _fetch_table is faked.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402

PATH = "sjavarutvegur/utf/SJA04903.px"
PREFIX = "ICE:Atvinnuvegir:sjavarutvegur:utf:SJA04903.px"
OLD = f"{PREFIX}:Tegund=0:Land=0:Eining=0"
NEW = f"{PREFIX}:Species=0:Country=0:Unit=0"


def _run(tmp_path, monkeypatch, stored_keys, fetched_keys):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": "Atvinnuvegir", "path": PATH, "id": "SJA04903.px",
                                                     "text": "x"}])
    if stored_keys:
        pq.write_table(pa.table({"series_key": stored_keys,
                                 "obs_date": pa.array([dt.date(2024, 12, 31)] * len(stored_keys)),
                                 "value": [1.0] * len(stored_keys)}), str(tmp_path / "Atvinnuvegir.parquet"))
    monkeypatch.setattr(H, "_fetch_table", lambda sess, db, path, prefix, since, **k:
                        ([(k, dt.date(2025, 12, 31), 2.0) for k in fetched_keys], "data"))
    unit = types.SimpleNamespace(config={}, key="hagstofa/_all")
    try:
        res = H.update(unit, None)
    except H.DefinitiveError as e:          # a structural sub-unit raises from finalize()
        res = types.SimpleNamespace(status="structural", error=str(e))
    keys = sorted(set(pq.read_table(str(tmp_path / "Atvinnuvegir.parquet")).column("series_key").to_pylist())) \
        if (tmp_path / "Atvinnuvegir.parquet").exists() else []
    return res, keys


def test_a_renamed_scheme_is_refused_and_named_not_merged(tmp_path, monkeypatch):
    res, keys = _run(tmp_path, monkeypatch, [OLD], [NEW])
    assert res.status == "structural" and "RESTRUCTURED" in res.error and PATH in res.error, res.error
    assert keys == [OLD], "nothing merged: the store keeps one id scheme"


def test_negative_control_the_stored_scheme_merges(tmp_path, monkeypatch):
    res, keys = _run(tmp_path, monkeypatch, [OLD], [OLD])
    assert res.status in ("ok", "no_change"), res.error
    assert keys == [OLD]


def test_a_first_landing_has_no_scheme_to_disagree_with(tmp_path, monkeypatch):
    res, keys = _run(tmp_path, monkeypatch, [f"ICE:Atvinnuvegir:other:T.px:X=0"], [NEW])
    assert res.status in ("ok", "no_change"), res.error
    assert NEW in keys


def test_a_mixed_table_refuses_the_minority_scheme(tmp_path, monkeypatch):
    """REVERSED 2026-09-23 (R1139). This used to pin "a table already holding both schemes is not blocked",
    on the assumption that its updates come in the table's main scheme. SJA04903 disproved it: 4,226
    Icelandic-scheme series and ONE stray English one, and dry runs 4-6 merged 4,241 English-scheme series
    for 2025 beside them - into a table catalogued as one id. The incoming scheme must DOMINATE the store."""
    stored = [f"{PREFIX}:Tegund={i}:Land=0:Eining=0" for i in range(5)] + [NEW]
    res, keys = _run(tmp_path, monkeypatch, stored, [NEW, f"{PREFIX}:Species=1:Country=0:Unit=0"])
    assert res.status == "structural" and "RESTRUCTURED" in res.error and "stray" in res.error, res.error
    assert keys == sorted(stored), "nothing merged"


def test_negative_control_a_mixed_table_still_takes_its_dominant_scheme(tmp_path, monkeypatch):
    stored = [f"{PREFIX}:Tegund={i}:Land=0:Eining=0" for i in range(5)] + [NEW]
    res, keys = _run(tmp_path, monkeypatch, stored, stored[:5])
    assert res.status in ("ok", "no_change"), res.error


def test_a_tie_between_two_stored_schemes_is_refused(tmp_path, monkeypatch):
    res, keys = _run(tmp_path, monkeypatch, [OLD, NEW], [NEW])
    assert res.status == "structural" and "RESTRUCTURED" in res.error, res.error


def test_the_key_scheme_is_the_dimension_names_after_the_prefix():
    assert H._key_scheme(OLD, PREFIX) == ("Tegund", "Land", "Eining")
    assert H._key_scheme(PREFIX, PREFIX) == ()


def test_a_colon_inside_a_value_code_is_not_a_dimension():
    """Review R1112: THJ11002, UTA05000, THJ05551, MAN10001 carry ':' inside value codes; the fragment
    ' 01' is not a dimension name, or every new such code would read as a restructure."""
    p = "ICE:Efnahagur:vinnumagn:THJ11002.px"
    assert H._key_scheme(f"{p}:Atvinnugrein=A: 01:Mælikvarði=0:Starfandi=1", p) == \
        ("Atvinnugrein", "Mælikvarði", "Starfandi")
