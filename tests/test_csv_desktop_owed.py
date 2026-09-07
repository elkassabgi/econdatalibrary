"""The flow-grain cloud derive path and its per-id debt (design review 2026-09-07, changes 3-5).

Directions held (R414): a too-large flow-grain id is a THIRD outcome — booked, never `failed`,
never queued for retry; a small flow-grain id streams; the series-grain path is untouched;
the debt row is visible in health as ATTENTION; the clear tool clears only rows whose served
object postdates the debt. Hermetic: fakes for the resolver, the streaming derive, the blob and
R2; nothing reads a live store.
"""
import datetime as dt
import gzip
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
sys.path.insert(0, os.path.join(ROOT, "tools"))

from updater import derive, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402


class _Blob:
    def __init__(self):
        self.puts = []

    def put_atomic(self, key, data):
        self.puts.append((key, data))


class _Res:
    def __init__(self, path):
        self.parquet_path = path


def _parquet(tmp_path, name, rows):
    p = tmp_path / name
    pq.write_table(pa.table({"series_key": ["k"] * rows, "obs_date": ["2024-01-01"] * rows,
                             "value": [1.0] * rows}), str(p))
    return str(p)


@pytest.fixture
def flow_env(tmp_path, monkeypatch):
    """Two flow ids: `zz:small` (3 store rows) and `zz:big` (30 store rows); ceiling 10."""
    import core.derive_csv as dc
    from econdl import _resolve
    paths = {"zz:small": _parquet(tmp_path, "SMALL.parquet", 3),
             "zz:big": _parquet(tmp_path, "BIG.parquet", 30)}
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: _Res(paths[sid]))
    monkeypatch.setattr(dc, "resolved_paths", lambda res: [res.parquet_path])
    streamed = []

    def fake_stream(sid, out_path):
        streamed.append(sid)
        with open(out_path, "wb") as fh:
            fh.write(gzip.compress(b"series_id,obs_date,value\nk,2024-01-01,1.0\n", mtime=0))
        return os.path.getsize(out_path)

    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", fake_stream)
    monkeypatch.setattr(derive, "_series_csv_bytes",
                        lambda sid: (_ for _ in ()).throw(AssertionError("in-memory path used")))
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    return streamed


def test_flow_grain_large_id_is_a_third_outcome_not_a_failure(flow_env):
    blob = _Blob()
    out = derive.derive_and_put(["zz:small", "zz:big"], blob, budget_min=0,
                                flow_grain=True, flow_max_rows=10)
    assert out["put"] == 1
    assert out["failed"] == [] and out["deferred_ids"] == []
    assert out["deferred_large"] == {"zz:big": 30}
    assert flow_env == ["zz:small"]                       # the big one was never derived
    (key, data), = blob.puts
    assert key == "series/zz%3Asmall.csv"
    assert data[:2] == b"\x1f\x8b" and gzip.decompress(data).startswith(b"series_id,obs_date,value")


def test_flow_grain_ceiling_defaults_and_env(flow_env, monkeypatch):
    blob = _Blob()
    out = derive.derive_and_put(["zz:big"], blob, budget_min=0, flow_grain=True)
    assert out["deferred_large"] == {} and out["put"] == 1     # 30 < 50,000,000 default
    monkeypatch.setenv("AQUEDUCT_FLOW_DERIVE_MAX_ROWS", "5")
    out = derive.derive_and_put(["zz:big"], _Blob(), budget_min=0, flow_grain=True)
    assert out["deferred_large"] == {"zz:big": 30}


def test_series_grain_path_is_untouched(flow_env, monkeypatch):
    calls = []
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda sid: calls.append(sid) or b"a,b\n")
    blob = _Blob()
    out = derive.derive_and_put(["zz:small", "zz:big"], blob, budget_min=0)   # flow_grain False
    assert calls == ["zz:small", "zz:big"] and flow_env == []
    assert out["put"] == 2 and out["deferred_large"] == {}


def test_flow_grain_caps_the_worker_pool(monkeypatch, flow_env):
    seen = {}
    real = derive.concurrent.futures.ThreadPoolExecutor

    class _Ex(real):
        def __init__(self, max_workers=None, **k):
            seen["w"] = max_workers
            super().__init__(max_workers=max_workers, **k)

    monkeypatch.setattr(derive.concurrent.futures, "ThreadPoolExecutor", _Ex)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "8")
    derive.derive_and_put(["zz:small", "zz:small", "zz:big"], _Blob(), budget_min=0,
                          flow_grain=True, flow_max_rows=100)
    assert seen["w"] == derive.FLOW_DERIVE_WORKERS == 2


def test_state_rows_round_trip(tmp_path):
    st = StateStore(path=str(tmp_path / "state.db"))
    st.note_csv_desktop_owed("eurostat", [("eurostat:hlth_cd_yro", 136_120_337, "too large"),
                                          ("eurostat:migr_asyrescra", 213_650_346, "too large")])
    rows = st.csv_desktop_owed("eurostat")
    assert [r["series_id"] for r in rows] == ["eurostat:hlth_cd_yro", "eurostat:migr_asyrescra"]
    assert rows[0]["rows"] == 136_120_337 and rows[0]["noted_utc"]
    st.note_csv_desktop_owed("eurostat", [("eurostat:hlth_cd_yro", 1, "again")])   # upsert
    assert len(st.csv_desktop_owed()) == 2
    st.clear_csv_desktop_owed(["eurostat:hlth_cd_yro"])
    assert [r["series_id"] for r in st.csv_desktop_owed()] == ["eurostat:migr_asyrescra"]


def test_orchestrator_books_the_debt_and_knows_eurostats_grain(tmp_path, capsys):
    st = StateStore(path=str(tmp_path / "state.db"))
    orchestrate._book_csv_desktop_owed(st, "eurostat", {"eurostat:hlth_cd_yro": 136_120_337})
    assert [r["series_id"] for r in st.csv_desktop_owed("eurostat")] == ["eurostat:hlth_cd_yro"]
    orchestrate._book_csv_desktop_owed(None, "eurostat", {"eurostat:x": 1})     # loud, no crash
    assert "NOT BOOKED" in capsys.readouterr().out
    assert orchestrate._csv_grain("eurostat") == "flow"
    assert orchestrate._csv_grain("norgesbank") == "series"
    assert orchestrate._csv_grain("no_such_source") == "series"


def test_health_holds_a_desktop_owed_source_at_attention(monkeypatch):
    from updater import health
    fake = {"sources": [{"source_id": "zzflow", "strategy": "giant_changed_units",
                         "cadence": "monthly", "live": True, "csv_grain": "flow"}]}
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: fake)
    st = StateStore(path=":memory:")
    st.note_csv_desktop_owed("zzflow", [("zzflow:a", 99, "too large"), ("zzflow:b", 98, "too large")])
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == "zzflow")
    assert row["health"] not in ("OK", "ROTATING")
    assert row["attention"][0].startswith("2 CSV(s) OWED to the desktop derive")
    assert "clear_csv_desktop_owed.py --source zzflow" in row["attention"][0]


def test_clear_tool_clears_only_rows_whose_served_object_postdates_the_debt():
    import clear_csv_desktop_owed as tool
    noted = "2026-09-07T20:00:00+00:00"

    class _S3:
        def head_object(self, Bucket, Key):
            if "old" in Key:
                return {"LastModified": dt.datetime(2026, 9, 7, 19, 0, tzinfo=dt.timezone.utc)}
            if "new" in Key:
                return {"LastModified": dt.datetime(2026, 9, 7, 21, 0, tzinfo=dt.timezone.utc)}
            raise KeyError(Key)

    assert tool.served_after(_S3(), "zz:new", noted)[0] is True
    assert tool.served_after(_S3(), "zz:old", noted)[0] is False
    assert tool.served_after(_S3(), "zz:missing", noted)[0] is False
