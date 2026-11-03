"""The derive's id list follows $ECONDL_CATALOG, the same file its resolver reads.

Why it exists: a targeted re-derive (`--only`) must not open the production catalogue in place (ledger
R1646, R1772, R1780: even a mode=ro open writes the -shm). The resolver (econdl._catalog.default_db)
already honoured $ECONDL_CATALOG, but _catalog_ids read the checkout's data/catalog.db regardless, so a
run pointed at a copy still opened the production file - and took its ids from a different catalogue
than the one it resolved them with.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.derive_csv as m  # noqa: E402


def _cat(path, rows):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE series (series_id TEXT, source_id TEXT)")
    conn.executemany("INSERT INTO series VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    return str(path)


@pytest.fixture
def pre_t0(monkeypatch):
    from econdl import _catalog
    monkeypatch.setattr(_catalog, "_cut_over", lambda: False)
    return _catalog


def test_the_override_is_read_and_the_checkout_is_not(tmp_path, monkeypatch, pre_t0):
    checkout = _cat(tmp_path / "checkout.db", [("census:a", "census")])
    copy = _cat(tmp_path / "copy.db", [("census:b", "census")])
    monkeypatch.setattr(m, "CATALOG", checkout)
    monkeypatch.setenv("ECONDL_CATALOG", copy)
    assert m._catalog_ids(None, ["census"]) == [("census:b", "census")]


def test_unset_reads_the_checkout_as_before(tmp_path, monkeypatch, pre_t0):
    checkout = _cat(tmp_path / "checkout.db", [("census:a", "census")])
    monkeypatch.setattr(m, "CATALOG", checkout)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    assert m._catalog_ids(None, ["census"]) == [("census:a", "census")]


def test_ids_and_resolver_read_the_same_file(tmp_path, monkeypatch, pre_t0):
    copy = _cat(tmp_path / "copy.db", [("census:b", "census")])
    monkeypatch.setattr(m, "CATALOG", _cat(tmp_path / "checkout.db", []))
    monkeypatch.setenv("ECONDL_CATALOG", copy)
    assert os.path.samefile(m._catalog_path(), pre_t0.default_db())


def test_after_t0_an_override_naming_another_file_is_refused(tmp_path, monkeypatch):
    from econdl import _catalog
    monkeypatch.setattr(_catalog, "_cut_over", lambda: True)
    monkeypatch.setenv("ECONDL_CATALOG", _cat(tmp_path / "copy.db", [("census:b", "census")]))
    with pytest.raises(RuntimeError, match="refused"):
        m._catalog_ids(None, ["census"])


def test_a_missing_override_is_an_error_not_an_empty_catalogue(tmp_path, monkeypatch, pre_t0):
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "absent.db"))
    with pytest.raises(FileNotFoundError):
        m._catalog_ids(None, ["census"])
    assert not (tmp_path / "absent.db").exists()
