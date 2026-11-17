"""noaa's renamed Canadian stations through the bulk re-derive lane (tools/derive_csv_bulk.py), plus the shard
key cache's live half and mtime.

The bulk tool is the route health names for noaa's full_rederive_owed debt. It streams STORE keys and never
calls the resolver, so before AR-282 it wrote `series/noaa:gsom:CAN....csv` (or skipped it under
--only-catalogued) while the catalogued CA0 object stayed on the old vintage, and its zero-error stamp then
cleared the debt. It now writes a renamed key under its catalogued CA0 id, never as an uncatalogued CAN
object, and refuses the stamp while a catalogued CA0 id it streamed was left unwritten.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import urllib.parse

import pyarrow.parquet as pq
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (HERE, ROOT, os.path.join(ROOT, "clients", "python")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from econdl import _resolve as R  # noqa: E402
from test_noaa_canada_station_forms import CA_ROWS, _read, _shard  # noqa: E402


# --- the shard key cache: live half and mtime ----------------------------------------------------

def test_a_can_key_only_in_the_live_half_is_found(tmp_path):
    R._NOAA_KEYS.clear()
    _shard(tmp_path, "gsom__CA", [("gsom:CAW00064433:TAVG", "2020-01-01", 1.0)])
    live = os.path.join(str(tmp_path), "noaa", "_live")
    os.makedirs(live)
    src = _shard(tmp_path / "x", "gsom__CA", [("gsom:CAN04047080:TAVG", "2026-03-01", 7.0)])
    pq.write_table(pq.read_table(src), os.path.join(live, "gsom__CA.parquet"))
    _, t = _read("noaa:gsom:CA004047080:TAVG", tmp_path)
    assert t.column("value").to_pylist() == [7.0]


def test_a_same_size_rewrite_with_a_new_mtime_is_read_again(tmp_path):
    R._NOAA_KEYS.clear()
    p = _shard(tmp_path, "gsom__CA", [("gsom:CA004047080:TAVG", "2020-01-01", 1.0)])
    _, t = _read("noaa:gsom:CA004047080:TAVG", tmp_path)
    assert t.column("value").to_pylist() == [1.0]
    st = os.stat(p)
    _shard(tmp_path, "gsom__CA", [("gsom:CAN04047080:TAVG", "2020-01-01", 1.0)])   # same-length key
    assert os.path.getsize(p) == st.st_size, "precondition: the rewrite must keep the size"
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
    _, t = _read("noaa:gsom:CA004047080:TAVG", tmp_path)
    assert set(t.column("series_key").to_pylist()) == {"gsom:CAN04047080:TAVG"}


# --- the bulk re-derive lane -------------------------------------------------------------------

class _Store:
    bucket = "econ-data"

    def __init__(self):
        self.put = {}

    def list_modified(self, prefix):
        return []

    def put_atomic(self, key, data):
        self.put[key] = data


def _q(sid):
    return "series/" + urllib.parse.quote(sid, safe="") + ".csv"


BULK_CATALOGUE = ["noaa:gsom:CA004047080:TAVG", "noaa:gsom:CA004047080:PRCP",
                  "noaa:gsom:CA1AB000001:PRCP", "noaa:gsom:CAW00064433:TAVG",
                  "noaa:gsom:CA004047081:TAVG", "noaa:gsom:USW00094728:TAVG"]
# CA_ROWS also holds gsom:CAN1AB00001:PRCP: a CAN station catalogued in neither form.


@pytest.fixture()
def bulk(tmp_path, monkeypatch):
    from tools import derive_csv_bulk as dcb
    from updater import blob
    root = tmp_path / "root"
    croot = root / "data" / "clean_full"
    R._NOAA_KEYS.clear()
    _shard(croot, "gsom__CA", CA_ROWS)
    _shard(croot, "gsom__US", [("gsom:USW00094728:TAVG", "2026-01-01", 1.5)])
    con = sqlite3.connect(str(root / "data" / "catalog.db"))
    con.execute("create table series (series_id text primary key, source_id text)")
    con.executemany("insert into series values (?, 'noaa')", [(s,) for s in BULK_CATALOGUE])
    con.commit()
    con.close()
    monkeypatch.setattr(dcb, "ROOT", str(root))
    import core.derive_csv as cdc
    monkeypatch.setattr(cdc, "_mirror_behind_store", lambda sources, sample=0: [])
    reg = tmp_path / "registry.yaml"
    reg.write_text("- source_id: noaa\n", encoding="utf-8")
    from updater import config as ucfg
    monkeypatch.setattr(ucfg, "REGISTRY", str(reg))
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setattr(ucfg, "STATE_DIR", str(state_dir))
    monkeypatch.setattr(ucfg, "STATE_DB", str(state_dir / "state.db"))
    cleared = []
    monkeypatch.setattr(dcb, "_durable_clear", lambda source, **kw: cleared.append(source) or True)
    monkeypatch.setenv("ECONDL_DATA", str(croot))

    def go(*extra):
        store = _Store()
        monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
        from core import cutover
        monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
        monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: store)
        monkeypatch.setattr(sys, "argv", ["derive_csv_bulk.py", "--source", "noaa", "--bucket", "econ-data",
                                          "--allow-stale-mirror", "--verify", "0",
                                          "--failed-keys-file", str(tmp_path / "failed.tsv"), *extra])
        return dcb.main(), store
    go.cleared = cleared
    go.dcb = dcb
    return go


@pytest.mark.parametrize("flag", [(), ("--only-catalogued",)])
def test_bulk_writes_the_ca0_object_and_no_can_object(bulk, flag):
    rc, store = bulk(*flag)
    assert rc == 0, rc
    keys = set(store.put)
    assert _q("noaa:gsom:CA004047080:TAVG") in keys and _q("noaa:gsom:CA004047080:PRCP") in keys
    assert _q("noaa:gsom:CA004047081:TAVG") in keys           # the neighbour, under its own CA0 id
    assert not [k for k in keys if "CAN" in k], sorted(keys)  # never an uncatalogued CAN object
    assert store.put[_q("noaa:gsom:CA004047080:TAVG")].decode() == (
        "series_id,obs_date,value\n"
        "gsom:CAN04047080:TAVG,2026-01-01,-12.5\n"
        "gsom:CAN04047080:TAVG,2026-02-01,-9.0\n")
    assert _q("noaa:gsom:CA1AB000001:PRCP") in keys and _q("noaa:gsom:USW00094728:TAVG") in keys
    assert bulk.cleared == ["noaa"]


def test_bulk_bytes_equal_the_resolver(bulk):
    _, store = bulk("--only-catalogued")
    from core import derive_csv as D
    for sid in ("noaa:gsom:CA004047080:TAVG", "noaa:gsom:CA004047080:PRCP", "noaa:gsom:CA004047081:TAVG"):
        assert store.put[_q(sid)] == D._series_csv_bytes(sid), sid


def test_bulk_verify_gate_passes_through_the_catalogued_id(bulk):
    rc, store = bulk("--only-catalogued", "--verify", "50")
    assert rc == 0 and _q("noaa:gsom:CA004047080:TAVG") in store.put


def test_bulk_does_not_pay_the_debt_when_a_ca0_id_was_skipped(bulk, monkeypatch):
    # a mapping that regressed to the native CAN id: the catalogue-side guard must refuse the clear
    dcb = bulk.dcb
    real = dcb.target_ids
    monkeypatch.setattr(dcb, "target_ids",
                        lambda s, k, ca: [f"{s}:{k}"] if s == "noaa" else real(s, k, ca))
    rc, store = bulk("--only-catalogued")
    assert _q("noaa:gsom:CA004047080:TAVG") not in store.put
    assert rc == 1 and bulk.cleared == [], "a CA0 id left unwritten must keep the debt standing"


@pytest.mark.parametrize("flag", [(), ("--only-catalogued",)])
def test_bulk_verify_reads_the_catalogued_ca0_id(bulk, monkeypatch, capsys, flag):
    """The byte-exactness gate must check the object the campaign WRITES: a renamed CAN key is compared
    through its catalogued CA0 id, never through the raw store id (AR-282 round-2 mutant M18)."""
    import core.derive_csv as cdc
    real, asked = cdc._series_csv_bytes, []

    def rec(sid, *a, **k):
        asked.append(sid)
        return real(sid, *a, **k)
    monkeypatch.setattr(cdc, "_series_csv_bytes", rec)
    rc, _ = bulk(*flag, "--verify", "1000")                    # larger than the key count: every key
    assert rc == 0, capsys.readouterr().out
    assert "noaa:gsom:CA004047080:TAVG" in asked and "noaa:gsom:CA004047080:PRCP" in asked
    assert "noaa:gsom:CA004047081:TAVG" in asked
    assert not [s for s in asked if s.startswith("noaa:gsom:CAN0")], asked
    out = capsys.readouterr().out
    assert f"verify: {len(asked)}/{len(asked)} byte-identical" in out, out


def test_bulk_refuses_both_forms_in_the_store(bulk, tmp_path):
    croot = tmp_path / "root" / "data" / "clean_full"
    R._NOAA_KEYS.clear()
    _shard(croot, "gsom__CA", CA_ROWS + [("gsom:CA004047080:TAVG", "1990-01-01", 0.5)])
    with pytest.raises(SystemExit, match="BOTH"):
        bulk("--only-catalogued")
    assert bulk.cleared == []


def test_bulk_refuses_noaa_when_the_catalogue_cannot_be_read(bulk, tmp_path):
    os.remove(str(tmp_path / "root" / "data" / "catalog.db"))
    rc, store = bulk()
    assert rc == 2 and store.put == {} and bulk.cleared == []


def test_noaa_ca0_missed_is_computed_from_the_catalogue_side():
    from tools import derive_csv_bulk as dcb
    ca = {"noaa:gsom:CA004047080:TAVG", "noaa:gsom:CA009999999:TAVG", "noaa:gsom:CA1AB000001:PRCP"}
    streamed = {"gsom:CAN04047080:TAVG", "gsom:CA1AB000001:PRCP"}
    assert dcb.noaa_ca0_missed(ca, streamed, set()) == ["noaa:gsom:CA004047080:TAVG"]
    assert dcb.noaa_ca0_missed(ca, streamed, {"noaa:gsom:CA004047080:TAVG"}) == []


def test_target_ids_for_other_sources_is_unchanged():
    from tools import derive_csv_bulk as dcb
    assert dcb.target_ids("other", "gsom:CAN04047080:TAVG", None) == ["other:gsom:CAN04047080:TAVG"]
    assert dcb.target_ids("noaa", "gsom:CAN04047080:TAVG", set()) == []
    assert dcb.target_ids("noaa", "gsom:CAN04047080:TAVG", {"noaa:gsom:CA004047080:TAVG"}) == [
        "noaa:gsom:CA004047080:TAVG"]
    assert dcb.target_ids("noaa", "gsom:USW00094728:TAVG", set()) == ["noaa:gsom:USW00094728:TAVG"]


def test_health_remedy_for_noaa_names_the_mapped_route(monkeypatch):
    from updater import health
    from updater.state import StateStore
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: {"sources": [
        {"source_id": "noaa", "strategy": "bulk_snapshot_if_changed", "cadence": "monthly", "live": True},
        {"source_id": "zzseries", "strategy": "bulk_snapshot_if_changed", "cadence": "monthly", "live": True}]})
    st = StateStore(path=":memory:")
    for s in ("noaa", "zzseries"):
        st.note_full_rederive_owed(s, vintage="v", note="csv coherence unmet: no cursors")
    notes = {r["source"]: r["attention"][0] for r in health.assess(store=st)["sources"]}
    assert "tools/derive_csv_bulk.py --source noaa --only-catalogued" in notes["noaa"], notes["noaa"]
    assert "catalogued CA0 ids" in notes["noaa"]
    assert "run tools/derive_csv_bulk.py --source zzseries; its zero-error" in notes["zzseries"]
