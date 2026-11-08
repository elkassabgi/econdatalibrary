"""fed_board Fix B, PR-1 (DESIGN_v2 §1a, §1b, §6; reviews AR-458 r1 FAIL, r2 PROCEED-WITH-CHANGES).

Each test names the mutant it kills. All of them are offline: a fixture release ZIP built here, a
tmp store root, fake blobs and fake credentials (no network: the cutover guard refuses before any
request is sent).

  T1  the sorted rewrite keeps every value BIT and every served CSV byte. The fixture plants a -0.0 and a
      NaN, because pyarrow's Table.equals counts -0.0 equal to 0.0 (AR-458 r2 L3) and a DuckDB
      `COPY ... ORDER BY` turned -0.0 into 0.0 in 10 of 54,156 served CSVs (R1845).
      Mutants: M0 the rewrite done by DuckDB COPY; M1 duplicate rows dropped; M3 the default row groups.
  T2  row-group pruning is real: every key lies in at most 2 row groups by the footer's min/max.
      Mutants: M2 sort by obs_date first; M3.
  T3r a failed rewrite keeps the parse-order file and publishes it; the orchestrator's fence still ends it.
  T4  `workers=` reaches derive_and_put only for a declared source; the R2 store is widened to
      workers + 8 through the GUARDED client (post-T0 PutObject refused); a stub without **kwargs still
      works for another source; the registry read lets the fence through and caches nothing.
      Mutants: M6 keyword always passed; M7 pool left at the default; M7b sibling built unguarded.
  T5  fed_board's cursor cap: a 54,157-key union is reported whole, no cap hit; over the cap the
      Result says cursor_cap_hit; other callers keep 50,000. Mutants: M8 merge_cursors ignores cap=;
      M9 the cap hit only printed.
  T6  health's remedy for a fed_board full_rederive_owed row is fed_board's own (the reviewed heal's
      steps), never ilostat's, even with csv_misses declared. Mutants: M10 the csv_misses branch first;
      M10b the PR-1 v1 text (wrong for derive_csv_bulk as it is).
"""
import datetime as dt
import hashlib
import os
import random
import struct
import sys
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from jobs import ingest_fed_board as ig  # noqa: E402

REL = "ZZ"
N_KEYS = 64
GROUP = 40                      # monkeypatched SORTED_ROW_GROUP: the fixture spans many groups
NEG_ZERO_KEY = "S017.NZ"
NAN_KEY = "S029.NAN"
CROSS_KEYS = ("S005.CROSS", "S041.CROSS")     # cross-listed in two datasets, identical values (G17/H6 shape)


# ----------------------------------------------------------------------------------------------- fixture
def _series_xml(key, obs):
    rows = "".join(f'<frb:Obs OBS_STATUS="A" OBS_VALUE="{v}" TIME_PERIOD="{d}"/>' for d, v in obs)
    return (f'<kf:Series SERIES_NAME="{key}" FREQ="129" UNIT="Percent">'
            f'<frb:Annotations><common:Annotation><common:AnnotationType>Short Description'
            f'</common:AnnotationType><common:AnnotationText>{key}</common:AnnotationText>'
            f'</common:Annotation></frb:Annotations>{rows}</kf:Series>')


def _fixture_zip(path):
    """A release of N_KEYS series in SHUFFLED key order and shuffled observation order, two datasets, two
    cross-listed keys (duplicate (key, date) rows with equal values), one -0.0 and one NaN."""
    rnd = random.Random(458)
    keys = [f"S{i:03d}.X" for i in range(N_KEYS)]
    keys[17], keys[29] = NEG_ZERO_KEY, NAN_KEY
    keys[5], keys[41] = CROSS_KEYS
    # LOW-CARDINALITY values with 0.0 among them, like the rate releases (H15, SLOOS) where it bit: DuckDB keeps a
    # lone -0.0 in a high-cardinality column and drops its sign only when 0.0 shares a low-cardinality column
    # chunk (measured 2026-11-08, duckdb 1.5.4: 3 random values kept it, 10,000 rows of 5 values lost it). A
    # fixture of random uniforms let the DuckDB mutant SURVIVE this test's first version.
    pool = ["0.0", "0.25", "0.5", "1.0", "-1.5", "2.75"]
    obs = {}
    for k in keys:
        n = 3 if k in CROSS_KEYS else rnd.randint(2, 30)        # every key <= GROUP rows (T2's premise)
        months = sorted(rnd.sample(range(0, 120), n))
        obs[k] = [(f"{2010 + m // 12:04d}-{m % 12 + 1:02d}", rnd.choice(pool)) for m in months]
        rnd.shuffle(obs[k])                                       # parse order is not date order
    nz = sorted(rnd.sample(range(0, 120), 3))
    obs[NEG_ZERO_KEY] = [(f"{2010 + m // 12:04d}-{m % 12 + 1:02d}", v) for m, v in zip(nz, ("0.0", "-0.0", "0.0"))]
    obs[NAN_KEY][1] = (obs[NAN_KEY][1][0], "NaN")
    rnd.shuffle(keys)
    ds_a = [k for k in keys if k not in CROSS_KEYS[1:]]
    ds_b = [k for k in keys if k in CROSS_KEYS or rnd.random() < 0.3]
    ds_b = [k for k in ds_b if k not in ds_a or k in CROSS_KEYS]
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<message:MessageGroup xmlns:message="{ig.NS_MSG}" xmlns:common="{ig.NS_COMMON}" '
            f'xmlns:frb="{ig.NS_FRB}" xmlns:kf="urn:test:kf">'
            f'<frb:DataSet id="DS_A">' + "".join(_series_xml(k, obs[k]) for k in ds_a) + '</frb:DataSet>'
            f'<frb:DataSet id="DS_B">' + "".join(_series_xml(k, obs[k]) for k in ds_b) + '</frb:DataSet>'
            '</message:MessageGroup>')
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(f"{REL}_data.xml", body)
    return path


def _ingest(tmp_path, monkeypatch, root_name, rewrite=True):
    """ingest_release of the fixture into <tmp>/<root_name>/fed_board; returns (store root, stats)."""
    root = tmp_path / root_name
    out = root / "fed_board"
    out.mkdir(parents=True)
    monkeypatch.setattr(ig, "OUT", str(out))
    monkeypatch.setattr(ig, "SORTED_ROW_GROUP", GROUP)
    if not rewrite:
        monkeypatch.setattr(ig, "_rewrite_sorted", lambda p: None)
    z = _fixture_zip(str(tmp_path / f"{root_name}.zip"))
    stats = ig.ingest_release(REL, z)
    return str(root), stats


def _bits(v):
    return struct.unpack("<q", struct.pack("<d", v))[0]


def _canon(path):
    """Bit-exact multiset of every row: values compared as int64 bit patterns, never by ==."""
    t = pq.read_table(path).to_pylist()
    return sorted((r["series_key"], r["obs_date"], _bits(r["value"]), r["dataset"], r["obs_status"]) for r in t)


def _csv_bodies(root, keys, monkeypatch):
    from core.derive_csv import _series_csv_bytes
    monkeypatch.setenv("ECONDL_DATA", root)
    return {k: hashlib.md5(_series_csv_bytes(f"fed_board:{REL}:{k}")).hexdigest() for k in keys}


@pytest.fixture
def stores(tmp_path, monkeypatch):
    """The SAME fixture release ingested twice: in parse order (the rewrite switched off) and as shipped."""
    with monkeypatch.context() as m:
        orig_root, _ = _ingest(tmp_path, m, "orig", rewrite=False)
    with monkeypatch.context() as m:
        new_root, stats = _ingest(tmp_path, m, "sorted", rewrite=True)
    return (os.path.join(orig_root, "fed_board", f"{REL}.parquet"),
            os.path.join(new_root, "fed_board", f"{REL}.parquet"), orig_root, new_root, stats)


# ----------------------------------------------------------------------------------------------- T1
def test_t1_the_planted_values_are_really_there(stores):
    """Precondition: a check that cannot see -0.0 or NaN proves nothing (R1845). Both are in the PARSED file."""
    orig, *_ = stores
    vals = pq.read_table(orig).column("value").to_pylist()
    neg_zero = [v for v in vals if v == 0.0 and struct.pack("<d", v)[7] & 0x80]
    assert len(neg_zero) == 1, "the fixture's -0.0 did not survive the parse"
    assert sum(1 for v in vals if v == 0.0 and not struct.pack("<d", v)[7] & 0x80) >= 2, (
        "the -0.0 needs +0.0 beside it in a low-cardinality column, or the DuckDB defect cannot show")
    assert len({v for v in vals if v == v}) <= 8, "the value column must be low-cardinality"
    assert sum(1 for v in vals if v != v) == 1, "the fixture's NaN did not survive the parse"
    assert pq.ParquetFile(orig).metadata.num_rows > 4 * GROUP


def test_t1_rewrite_keeps_every_bit_row_and_schema(stores):
    orig, new, _, _, stats = stores
    assert stats["sorted"] is True
    a, b = pq.read_table(orig), pq.read_table(new)
    assert b.schema.equals(a.schema, check_metadata=True)
    assert b.num_rows == a.num_rows
    assert _canon(new) == _canon(orig), "a value bit, a row or a column changed in the rewrite"
    rows = b.select(["series_key", "obs_date"]).to_pylist()
    pairs = [(r["series_key"], r["obs_date"]) for r in rows]
    assert pairs == sorted(pairs), "the file is not in (series_key, obs_date) order"
    assert not [f for f in os.listdir(os.path.dirname(new)) if f.endswith(".tmp")], "a temp file was left"


def test_t1_rewrite_uses_small_row_groups(stores):
    """M3: the default (one 1M-row group here) would leave nothing to prune."""
    _, new, *_ = stores
    md = pq.ParquetFile(new).metadata
    assert md.num_row_groups >= 2
    assert max(md.row_group(i).num_rows for i in range(md.num_row_groups)) <= GROUP


def test_t1_every_served_csv_is_byte_identical(stores, monkeypatch):
    """The served bytes, for EVERY key of the release (R1845: never a sample), parse order vs sorted."""
    orig, new, orig_root, new_root, _ = stores
    keys = sorted(set(pq.read_table(orig, columns=["series_key"]).column("series_key").to_pylist()))
    assert len(keys) == N_KEYS
    want = _csv_bodies(orig_root, keys, monkeypatch)
    got = _csv_bodies(new_root, keys, monkeypatch)
    differ = [k for k in keys if want[k] != got[k]]
    assert not differ, f"{len(differ)} served CSV(s) changed: {differ[:5]}"
    from core.derive_csv import _series_csv_bytes
    monkeypatch.setenv("ECONDL_DATA", new_root)
    body = _series_csv_bytes(f"fed_board:{REL}:{NEG_ZERO_KEY}").decode()
    assert ",-0.0\n" in body, "the served CSV lost the sign of -0.0"
    assert ",nan\n" in _series_csv_bytes(f"fed_board:{REL}:{NAN_KEY}").decode()


# ----------------------------------------------------------------------------------------------- T2
def test_t2_each_key_lies_in_at_most_two_row_groups(stores):
    """Pruning by statistics: the resolver's pc.equal(series_key) filter reads only groups whose
    [min, max] holds the key. M2 (obs_date first) spreads keys over every group."""
    _, new, *_ = stores
    md = pq.ParquetFile(new).metadata
    col = md.schema.to_arrow_schema().get_field_index("series_key")
    spans = []
    for i in range(md.num_row_groups):
        st = md.row_group(i).column(col).statistics
        assert st is not None and st.has_min_max, "row-group statistics missing: nothing can prune"
        spans.append((st.min, st.max))
    keys = set(pq.read_table(new, columns=["series_key"]).column("series_key").to_pylist())
    worst = max(sum(1 for lo, hi in spans if lo <= k <= hi) for k in keys)
    assert worst <= 2, f"a key lies in {worst} row groups"


# ----------------------------------------------------------------------------------------------- T3r
def test_t3r_a_failed_rewrite_publishes_the_parse_order_file(tmp_path, monkeypatch, capsys):
    """Review F3: a deterministic rewrite failure must not withhold the release forever."""
    def boom(p):
        raise MemoryError("test: no room to sort")
    monkeypatch.setattr(ig, "_rewrite_sorted", boom)
    root, stats = _ingest(tmp_path, monkeypatch, "fail", rewrite=True)
    p = os.path.join(root, "fed_board", f"{REL}.parquet")
    assert stats["sorted"] is False and stats["n_obs"] == pq.ParquetFile(p).metadata.num_rows
    assert "kept in PARSE order" in capsys.readouterr().out


def test_t3r_the_fence_is_never_turned_into_a_fallback(tmp_path, monkeypatch):
    from updater import orchestrate

    def fence(p):
        raise orchestrate.UnitTimeout("test: the unit's fence")
    monkeypatch.setattr(ig, "_rewrite_sorted", fence)
    with pytest.raises(orchestrate.UnitTimeout):
        _ingest(tmp_path, monkeypatch, "fence", rewrite=True)


def test_t3r_the_real_rewrite_cleans_its_temp_on_failure(tmp_path, monkeypatch):
    with monkeypatch.context() as m:
        root, _ = _ingest(tmp_path, m, "keep", rewrite=False)
    p = os.path.join(root, "fed_board", f"{REL}.parquet")
    before = open(p, "rb").read()

    def bad_write(tbl, where, **k):
        open(where, "wb").write(b"half a file")              # a temp that exists when the write fails
        raise OSError("test: disk full")
    monkeypatch.setattr(ig.pq, "write_table", bad_write)
    with pytest.raises(OSError):
        ig._rewrite_sorted(p)
    assert open(p, "rb").read() == before
    assert not [f for f in os.listdir(os.path.dirname(p)) if f.endswith(".tmp")]


# ----------------------------------------------------------------------------------------------- T4
class _Recording:
    def __init__(self):
        self.puts = []

    def put_atomic(self, key, data, plain=False):
        self.puts.append(key)


def test_t4_declared_width_runs_that_many_threads_and_widens_the_r2_pool(monkeypatch):
    """M7: the sibling store's pool. The given R2Blob has the default pool; the threads write through
    an R2Blob on the SAME bucket with pool = workers + 8."""
    from updater import blob, derive
    seen = {}
    real = derive.concurrent.futures.ThreadPoolExecutor

    class _Ex(real):
        def __init__(self, max_workers=None, **k):
            seen["w"] = max_workers
            super().__init__(max_workers=max_workers, **k)

    used = []

    def put_atomic(self, key, data, plain=False):
        used.append((id(self), self.pool, self.bucket, self.write))
    monkeypatch.setattr(derive.concurrent.futures, "ThreadPoolExecutor", _Ex)
    monkeypatch.setattr(blob.R2Blob, "put_atomic", put_atomic)
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda sid: b"series_id,obs_date,value\nk,2024-01-01,1\n")
    given = blob.R2Blob()
    out = derive.derive_and_put([f"zz:{i}" for i in range(30)], given, budget_min=0, workers=24)
    assert out["put"] == 30 and seen["w"] == 24
    assert {u[1:] for u in used} == {(32, given.bucket, True)}, used[:3]
    assert id(given) not in {u[0] for u in used}


def test_t4_an_explicit_env_width_stays_a_ceiling(monkeypatch):
    from updater import derive
    seen = {}
    real = derive.concurrent.futures.ThreadPoolExecutor

    class _Ex(real):
        def __init__(self, max_workers=None, **k):
            seen["w"] = max_workers
            super().__init__(max_workers=max_workers, **k)
    monkeypatch.setattr(derive.concurrent.futures, "ThreadPoolExecutor", _Ex)
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda sid: b"x\n")
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "3")
    derive.derive_and_put([f"zz:{i}" for i in range(10)], _Recording(), budget_min=0, workers=24)
    assert seen["w"] == 3


def test_t4_other_stores_are_never_replaced():
    """R1204: every thread writes the store it was given. Only the exact production R2Blob is widened."""
    from updater import blob, derive
    rec = _Recording()
    assert derive._put_store(rec, 24) is rec
    local = blob.LocalBlob()
    assert derive._put_store(local, 24) is local

    class _Sub(blob.R2Blob):
        pass
    sub = _Sub()
    assert derive._put_store(sub, 24) is sub
    wide = blob.R2Blob(pool=64)
    assert derive._put_store(wide, 24) is wide


@pytest.fixture
def fake_r2_creds(monkeypatch):
    from core import r2_util
    monkeypatch.setattr(r2_util, "creds", lambda write=False: {
        "endpoint": "http://127.0.0.1:9", "key": "k", "secret": "s", "mode": "write" if write else "read"})


def test_t4_the_sibling_store_is_guarded_after_t0(fake_r2_creds, tmp_path, monkeypatch):
    """M7b (AR-458 r2 (d)): the widened client must come from the guarded factory, so the post-T0
    write refusal still holds. The guard is registered FIRST on before-call, so no request is sent."""
    from core import cutover
    from updater import blob, derive
    sib = derive._put_store(blob.R2Blob(), 24)
    assert type(sib) is blob.R2Blob and sib.pool == 32
    c = sib.client
    assert c.meta.config.max_pool_connections == 32
    assert getattr(c, "_econ_cutover_guard", False) and getattr(c, "_econ_cutover_guard_all", False)
    flag = tmp_path / "CUTOVER"
    flag.write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    with pytest.raises(cutover.CutoverRefused):
        c.put_object(Bucket=sib.bucket, Key="series/zz%3Ax.csv", Body=b"x")


class _Unit:
    def __init__(self, sid):
        self.source_id, self.unit_id, self.key, self.strategy = sid, "_all", f"{sid}/_all", "bulk_snapshot_if_changed"
        self.out_paths = []


@pytest.fixture
def tmp_catalog(tmp_path, monkeypatch):
    import sqlite3
    from updater import orchestrate
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    for sid in ("zzwide:a", "zzwide:b", "zzplain:a"):
        con.execute("INSERT INTO series VALUES (?,?)", (sid, sid.split(":")[0]))
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    monkeypatch.setattr(orchestrate, "_csv_grain", lambda s: "series")
    monkeypatch.setattr(orchestrate._csv_derive_workers, "_cache", {"zzwide": 24}, raising=False)
    return p


def test_t4_workers_reach_derive_only_for_a_declared_source(tmp_path, tmp_catalog, monkeypatch):
    """M6: the keyword passed for every source would break every stub without **kwargs (run 34166306918)."""
    from updater import derive, orchestrate
    from updater.state import StateStore
    from updater.strategies.base import Result
    calls = []

    def with_kwargs(ids, blob, **kw):
        calls.append(dict(kw))
        return {"put": len(ids), "failed": [], "deferred": 0, "deferred_ids": [], "failed_reasons": {}}
    monkeypatch.setattr(derive, "derive_and_put", with_kwargs)
    st = StateStore(path=str(tmp_path / "state.db"))
    res = Result(status="ok", obs=2, changed_keys={"a": "2024-01-01", "b": "2024-01-01"})
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(_Unit("zzwide"), res, object(), st)
    assert failed == [] and deferred == [] and calls[-1].get("workers") == 24

    plain_calls = []

    def no_kwargs(ids, blob):                     # the shape of the stubs in eleven existing test files
        plain_calls.append(list(ids))
        return {"put": len(ids), "failed": [], "deferred": 0, "deferred_ids": [], "failed_reasons": {}}
    monkeypatch.setattr(derive, "derive_and_put", no_kwargs)
    res = Result(status="ok", obs=1, changed_keys={"a": "2024-01-01"})
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(_Unit("zzplain"), res, object(), st)
    assert plain_calls == [["zzplain:a"]] and failed == [], (failed, note)


def test_t4_the_real_registry_declares_fed_board_only(monkeypatch):
    from updater import orchestrate, registry
    monkeypatch.setattr(orchestrate._csv_derive_workers, "_cache", None, raising=False)
    assert orchestrate._csv_derive_workers("fed_board") == 24
    assert orchestrate._csv_derive_workers("norgesbank") is None
    assert orchestrate._derive_width_kw("norgesbank") == {}
    declared = {e["source_id"] for e in registry.load()["sources"] if "csv_derive_workers" in e}
    assert declared == {"fed_board"}


@pytest.mark.parametrize("how", ["direct", "disguised"])
def test_t4_the_fence_inside_the_width_read_is_not_swallowed_or_cached(monkeypatch, how):
    """Review F2: swallowed and half-cached, fed_board would run at 8 workers for the rest of the process."""
    from updater import orchestrate, registry
    monkeypatch.setattr(orchestrate._csv_derive_workers, "_cache", None, raising=False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", how == "disguised")

    def load():
        if how == "direct":
            raise orchestrate.UnitTimeout("alarm inside the registry read (test)")
        raise RuntimeError("Query interrupted")
    monkeypatch.setattr(registry, "load", load)
    with pytest.raises(orchestrate.UnitTimeout):
        orchestrate._csv_derive_workers("fed_board")
    assert orchestrate._csv_derive_workers._cache is None


def test_t4_an_unreadable_registry_declares_nothing(monkeypatch):
    from updater import orchestrate, registry
    monkeypatch.setattr(orchestrate._csv_derive_workers, "_cache", None, raising=False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    monkeypatch.setattr(registry, "load", lambda: (_ for _ in ()).throw(ValueError("bad yaml (test)")))
    assert orchestrate._csv_derive_workers("fed_board") is None


@pytest.mark.parametrize("bad", [True, 0, 65, "24", 24.0, None])
def test_t4_registry_validates_the_width(bad):
    from updater import registry
    reg = {"sources": [{"source_id": "zz", "strategy": "bulk_snapshot_if_changed", "cadence": "daily",
                        "live": True, "csv_derive_workers": bad}]}
    assert any("csv_derive_workers" in p for p in registry.validate(reg))
    ok = {"sources": [{**reg["sources"][0], "csv_derive_workers": 24}]}
    assert not [p for p in registry.validate(ok) if "csv_derive_workers" in p]


# ----------------------------------------------------------------------------------------------- T5
def _keys_file(path, rel, n):
    pq.write_table(pa.table({"dataset": ["D"] * n, "series_key": [f"{rel}K{i:06d}" for i in range(n)],
                             "obs_date": pa.array([dt.date(2024, 1, 1)] * n, type=pa.date32()),
                             "value": [1.0] * n, "obs_status": ["A"] * n}, schema=ig.OBS_SCHEMA), path)
    return n


@pytest.fixture
def fake_fetch(tmp_path, monkeypatch):
    """fed_board.update over two fake releases whose union is 54,157 keys (one above the measured 54,156)."""
    from updater import config
    from updater.strategies.fetchers import fed_board as fb
    out = tmp_path / "store" / "fed_board"
    out.mkdir(parents=True)
    sizes = {"A1": 12_441, "Z1": 41_716}
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    monkeypatch.setattr(config, "source_dir", lambda s: str(out))
    monkeypatch.setattr(ig, "OUT", str(out))
    monkeypatch.setattr(fb, "_releases", lambda: sorted(sizes))

    def fetch_hash(rel):
        p = tmp_path / f"{rel}.zip"
        p.write_bytes(b"zip")
        return str(p), f"h-{rel}"
    monkeypatch.setattr(fb, "_fetch_hash", fetch_hash)
    monkeypatch.setattr(ig, "ingest_release",
                        lambda rel, z: {"n_obs": _keys_file(str(out / f"{rel}.parquet"), rel, sizes[rel])})
    return fb


def test_t5_the_whole_measured_universe_is_reported(fake_fetch):
    """M8: merge_cursors ignoring cap= stops at 50,000 and cuts the same Z1 keys every pass (R1844)."""
    res = fake_fetch.update(None, None)
    assert len(res.series_cursors) == 54_157
    assert res.cursor_cap_hit is False


def test_t5_over_the_cap_the_result_says_so(fake_fetch, monkeypatch):
    """M9: a cap hit that is only printed books no debt (the orchestrator reads Result.cursor_cap_hit)."""
    monkeypatch.setattr(fake_fetch, "FED_BOARD_CURSOR_CAP", 100)
    res = fake_fetch.update(None, None)
    assert len(res.series_cursors) == 100 and res.cursor_cap_hit is True


def test_t5_the_cap_is_pinned_above_twice_the_measured_universe():
    """54,156 = 41,716 Z1 + 12,440 other store keys, AR-458 J1 (its q_allrel.log, store_keys summed)."""
    from updater.strategies.fetchers import fed_board as fb
    assert fb.FED_BOARD_CURSOR_CAP >= 2 * 54_156


def test_t5_every_other_caller_keeps_50000(tmp_path):
    from updater.strategies.fetchers import _common
    assert _common.CURSOR_CAP == 50_000
    p = str(tmp_path / "big.parquet")
    _keys_file(p, "B", 50_001)
    assert len(_common.merge_cursors({}, p)) == 50_000
    assert len(_common.merge_cursors({}, p, cap=120_000)) == 50_001
    fetchers = os.path.join(ROOT, "updater", "strategies", "fetchers")
    callers = [f for f in os.listdir(fetchers) if f.endswith(".py")
               and "merge_cursors(" in open(os.path.join(fetchers, f), encoding="utf-8").read()]
    for f in callers:
        src = open(os.path.join(fetchers, f), encoding="utf-8").read()
        if f in ("_common.py", "fed_board.py"):
            continue
        i = 0
        while (i := src.find("merge_cursors(", i)) != -1:
            call = src[i:src.find(")", i) + 1]
            assert "cap=" not in call, f"{f} passes cap= to merge_cursors: {call}"
            i += 1


# ----------------------------------------------------------------------------------------------- T6
@pytest.mark.parametrize("with_misses", [False, True])
def test_t6_health_names_fed_boards_own_remedy(monkeypatch, with_misses):
    """With csv_misses ALSO declared (PR-2's state; AR-458 r2 (b)3 asks for T6 with it on), the csv_bulk
    branch must still be reached first. The text is the reviewed heal's sequence (PR-1 review F1)."""
    from updater import health
    from updater.state import StateStore
    entry = {"source_id": "fed_board", "strategy": "bulk_snapshot_if_changed", "cadence": "daily",
             "live": True, "csv_bulk": "qualify_with_shard"}
    if with_misses:
        entry["csv_misses"] = "desktop_owed"
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: {"sources": [entry]})
    st = StateStore(path=":memory:")
    st.note_full_rederive_owed("fed_board", "fed_board:18:abc", "cursor cap hit - changed-set evidence truncated")
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == "fed_board")
    text = next(a for a in row["attention"] if a.startswith("full re-derive OWED"))
    assert row["health"] not in ("OK", "ROTATING")
    for token in ("--qualify-with-shard --only-catalogued", "--clear-owed-only", "ECONDL_DATA",
                  "AQUEDUCT_REGISTRY", "ECONDL_ROOT", "mirror_sync.py", "never --allow-stale-mirror",
                  "NOT clearing", "catalog.db"):
        assert token in text, f"remedy lacks {token!r}: {text}"
    assert "derive_ilostat_indicators" not in text and "indicator stems" not in text
    assert "success stamp clears" not in text, "the campaign's own stamp cannot clear the row from a new folder"


def test_t6_registry_validates_csv_bulk():
    from updater import registry
    reg = {"sources": [{"source_id": "zz", "strategy": "bulk_snapshot_if_changed", "cadence": "daily",
                        "live": True, "csv_bulk": "qualify-with-shard"}]}
    assert any("csv_bulk" in p for p in registry.validate(reg))
    reg["sources"][0]["csv_bulk"] = "qualify_with_shard"
    assert not [p for p in registry.validate(reg) if "csv_bulk" in p]
