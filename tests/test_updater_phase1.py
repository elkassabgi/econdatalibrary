"""Phase-1 integration gate (UPDATER_BUILD_PLAN.md §6.1).

T-1  merge_and_write invariant suite (0-row refusal, shrink<min_ratio refusal,
     column-drop refusal, dedup keep-last) against the LOCAL Blob backend and —
     when R2 write creds resolve — against a real R2 scratch prefix
     `_aqueduct_test/<uuid>/` (objects deleted afterwards; production keys never
     touched).
T-2  core/r2_util.py credential precedence: env-only, .env-only, env-over-.env
     (monkeypatched ENV path + environ; the real .env is never read or written).
CAS  updater/run.py --push-state ETag compare-and-swap, exercised against a fake
     boto3 client monkeypatched in at the core.r2_util.client boundary:
     matching etag -> upload + dated backup; mismatched -> exit code 2 with the
     remote object untouched; both-absent -> sanctioned first seed; plus the
     pull_state round-trip and missing-remote error path.
D1   core/sync_state_d1.py: emitted SQL is D1-legal (no BEGIN/COMMIT/PRAGMA),
     <=900KB per file, replays into in-memory sqlite row-for-row equal to the
     source (twice — idempotent), handles quote/newline torture rows, refuses a
     zero-row projection, and --dry-run executes nothing.

Run from the repo root:  python -m pytest tests/test_updater_phase1.py -x -q
"""
from __future__ import annotations

import hashlib
import io
import os
import sqlite3
import sys
import uuid
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from updater import blob as blobmod            # noqa: E402
from updater import merge                       # noqa: E402
from updater import run as runmod               # noqa: E402
from updater import config                      # noqa: E402
from updater.errors import DefinitiveError      # noqa: E402
from core import r2_util                        # noqa: E402
from core import sync_state_d1 as d1sync        # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _tbl(series, dates, values, **extra_cols):
    cols = {"series_key": pa.array(series, pa.string()),
            "obs_date": pa.array(dates, pa.string()),
            "value": pa.array(values, pa.float64())}
    for name, vals in extra_cols.items():
        cols[name] = pa.array(vals)
    return pa.table(cols)


def _empty_tbl():
    return _tbl([], [], [])


def _read_blob_table(blob, key):
    data = blob.get(key)
    assert data is not None, f"expected object at {key}"
    return pq.read_table(io.BytesIO(data))


# ---------------------------------------------------------------------------
# T-1: merge_and_write invariants, parametrized over blob backends
# ---------------------------------------------------------------------------

@pytest.fixture(params=["local", "r2"])
def bb(request, tmp_path):
    """Blob backend under test: LocalBlob in a tmp dir, or a real R2 scratch
    prefix `_aqueduct_test/<uuid>/` (skipped when write creds don't resolve).
    Every R2 key minted through .key() is deleted on teardown."""
    if request.param == "local":
        b = blobmod.LocalBlob(root=str(tmp_path))
        yield SimpleNamespace(blob=b, key=lambda name: f"t/{name}", backend="local")
        return
    if r2_util.creds(write=True) is None:
        pytest.skip("R2 write credentials not configured — R2 leg of T-1 skipped")
    b = blobmod.R2Blob()
    prefix = f"_aqueduct_test/{uuid.uuid4().hex}"
    created: list[str] = []

    def key(name: str) -> str:
        k = f"{prefix}/{name}"
        created.append(k)
        return k

    yield SimpleNamespace(blob=b, key=key, backend="r2")
    for k in created:
        try:
            b.client.delete_object(Bucket=b.bucket, Key=k)
        except Exception:
            pass


class TestT1MergeInvariants:
    def test_zero_row_refusal(self, bb):
        key = bb.key("zero.parquet")
        with pytest.raises(DefinitiveError, match="0 rows"):
            merge.merge_and_write(key, _empty_tbl(), blob=bb.blob)
        assert not bb.blob.exists(key), "refused publish must leave nothing behind"

    def test_shrink_refusal_leaves_existing_untouched(self, bb):
        key = bb.key("shrink.parquet")
        n0 = 100
        base = _tbl([f"S{i}" for i in range(n0)], ["2020-01-01"] * n0,
                    [float(i) for i in range(n0)])
        rows, _ = merge.merge_and_write(key, base, blob=bb.blob)
        assert rows == n0
        etag_before = bb.blob.etag(key)
        # 96 < 100*0.97 -> refuse (overwrite mode: new table IS the full content)
        smaller = _tbl([f"S{i}" for i in range(96)], ["2020-01-02"] * 96, [0.0] * 96)
        with pytest.raises(DefinitiveError, match="refusing shrink"):
            merge.merge_and_write(key, smaller, mode="overwrite", blob=bb.blob)
        assert bb.blob.etag(key) == etag_before, "refusal must not touch published data"
        assert _read_blob_table(bb.blob, key).num_rows == n0
        # boundary: exactly min_ratio passes (97 >= 100*0.97)
        ok97 = _tbl([f"S{i}" for i in range(97)], ["2020-01-02"] * 97, [0.0] * 97)
        rows, _ = merge.merge_and_write(key, ok97, mode="overwrite", blob=bb.blob)
        assert rows == 97

    def test_column_drop_refusal(self, bb):
        key = bb.key("coldrop.parquet")
        base = _tbl(["A", "B"], ["2020-01-01", "2020-01-01"], [1.0, 2.0],
                    extra=["x", "y"])
        merge.merge_and_write(key, base, blob=bb.blob)
        etag_before = bb.blob.etag(key)
        new = _tbl(["C"], ["2020-01-02"], [3.0])  # 'extra' column vanished
        with pytest.raises(DefinitiveError, match="missing column"):
            merge.merge_and_write(key, new, blob=bb.blob)
        assert bb.blob.etag(key) == etag_before
        got = _read_blob_table(bb.blob, key)
        assert "extra" in got.column_names and got.num_rows == 2

    def test_dedup_keep_last(self, bb):
        key = bb.key("dedup.parquet")
        base = _tbl(["A", "B"], ["2020-01-01", "2020-01-01"], [1.0, 10.0])
        merge.merge_and_write(key, base, blob=bb.blob)
        # revised value for (A, 2020-01-01) + one genuinely new row
        rev = _tbl(["A", "A"], ["2020-01-01", "2020-01-02"], [2.0, 3.0])
        rows, last = merge.merge_and_write(key, rev, blob=bb.blob)
        assert rows == 3, "dedup must collapse the revised key, keep the new one"
        assert last == "2020-01-02"
        got = _read_blob_table(bb.blob, key).to_pydict()
        by_key = dict(zip(zip(got["series_key"], got["obs_date"]), got["value"]))
        assert by_key[("A", "2020-01-01")] == 2.0, "new row must win on revision"
        assert by_key[("B", "2020-01-01")] == 10.0
        # idempotency: re-merging the same rows is a no-op row-count-wise
        rows2, _ = merge.merge_and_write(key, rev, blob=bb.blob)
        assert rows2 == 3


def test_local_blob_and_none_paths_byte_identical(tmp_path):
    """blob=None (original fs path) and LocalBlob must publish identical parquet
    CONTENT (same rows/schema after the shared invariant path)."""
    base = _tbl(["A", "B"], ["2020-01-01", "2020-01-02"], [1.0, 2.0])
    p_none = str(tmp_path / "none" / "x.parquet")
    merge.merge_and_write(p_none, base)
    b = blobmod.LocalBlob(root=str(tmp_path / "blob"))
    merge.merge_and_write("x.parquet", base, blob=b)
    t1 = pq.read_table(p_none)
    t2 = pq.read_table(str(tmp_path / "blob" / "x.parquet"))
    assert t1.equals(t2)


# ---------------------------------------------------------------------------
# T-2: r2_util credential precedence (env > .env), fully sandboxed
# ---------------------------------------------------------------------------

_W = ("R2_WRITE_ENDPOINT", "R2_WRITE_ACCESS_KEY_ID", "R2_WRITE_SECRET_ACCESS_KEY")


@pytest.fixture()
def clean_r2_env(monkeypatch, tmp_path):
    """No ambient R2_* env vars; ENV points into the tmp dir (real .env untouched)."""
    for k in list(os.environ):
        if k.startswith(("R2_READ_", "R2_WRITE_")):
            monkeypatch.delenv(k, raising=False)
    envfile = tmp_path / ".env"
    monkeypatch.setattr(r2_util, "ENV", str(envfile))
    return envfile


class TestT2CredPrecedence:
    def test_env_only(self, clean_r2_env, monkeypatch):
        assert r2_util.creds(write=True) is None  # nothing configured -> None
        monkeypatch.setenv("R2_WRITE_ENDPOINT", "https://env.example")
        monkeypatch.setenv("R2_WRITE_ACCESS_KEY_ID", "env-key")
        monkeypatch.setenv("R2_WRITE_SECRET_ACCESS_KEY", "env-secret")
        c = r2_util.creds(write=True)
        assert c == {"endpoint": "https://env.example", "key": "env-key",
                     "secret": "env-secret", "mode": "write"}

    def test_dotenv_only(self, clean_r2_env):
        clean_r2_env.write_text(
            "R2_WRITE_ENDPOINT=https://file.example\n"
            "R2_WRITE_ACCESS_KEY_ID=file-key\n"
            "R2_WRITE_SECRET_ACCESS_KEY='file-secret'\n", encoding="utf-8")
        c = r2_util.creds(write=True)
        assert c == {"endpoint": "https://file.example", "key": "file-key",
                     "secret": "file-secret", "mode": "write"}

    def test_env_wins_over_dotenv(self, clean_r2_env, monkeypatch):
        clean_r2_env.write_text(
            "R2_WRITE_ENDPOINT=https://file.example\n"
            "R2_WRITE_ACCESS_KEY_ID=file-key\n"
            "R2_WRITE_SECRET_ACCESS_KEY=file-secret\n", encoding="utf-8")
        monkeypatch.setenv("R2_WRITE_ACCESS_KEY_ID", "env-key")
        c = r2_util.creds(write=True)
        assert c["key"] == "env-key", "real environment must beat .env"
        assert c["endpoint"] == "https://file.example", ".env still fills the unset vars"

    def test_placeholders_are_absent(self, clean_r2_env, monkeypatch):
        monkeypatch.setenv("R2_WRITE_ENDPOINT", "https://env.example")
        monkeypatch.setenv("R2_WRITE_ACCESS_KEY_ID", "...")
        monkeypatch.setenv("R2_WRITE_SECRET_ACCESS_KEY", "env-secret")
        assert r2_util.creds(write=True) is None


# ---------------------------------------------------------------------------
# CAS: --push-state / --pull-state against a fake boto3 client
# ---------------------------------------------------------------------------

def _client_error(op="HeadObject"):
    from botocore.exceptions import ClientError
    return ClientError(
        {"Error": {"Code": "NoSuchKey"},
         "ResponseMetadata": {"HTTPStatusCode": 404}}, op)


class FakeS3:
    """In-memory boto3-shaped client. ETag = md5 hexdigest, exactly what R2
    reports for the single-part PUTs the updater performs."""

    def __init__(self):
        self.objs: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objs[Key] = bytes(Body)
        return {}

    def get_object(self, Bucket, Key):
        if Key not in self.objs:
            raise _client_error("GetObject")
        return {"Body": io.BytesIO(self.objs[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.objs:
            raise _client_error("HeadObject")
        return {"ETag": '"' + hashlib.md5(self.objs[Key]).hexdigest() + '"'}


@pytest.fixture()
def fake_r2(monkeypatch, tmp_path):
    """Monkeypatch the boto3-client boundary (core.r2_util.client) and relocate
    the state dir into tmp so no real state.db / .state_etag is touched."""
    fake = FakeS3()
    monkeypatch.setattr(r2_util, "client", lambda write=False: fake)
    state_dir = str(tmp_path / "_aqueduct")
    state_db = os.path.join(state_dir, "state.db")
    monkeypatch.setattr(config, "STATE_DIR", state_dir)
    monkeypatch.setattr(config, "STATE_DB", state_db)
    monkeypatch.setattr(runmod, "ETAG_PATH", os.path.join(state_dir, ".state_etag"))
    # a fresh R2Blob per call builds its client lazily -> gets the fake
    os.makedirs(state_dir, exist_ok=True)
    con = sqlite3.connect(state_db)
    con.execute("CREATE TABLE t(x)")
    con.execute("INSERT INTO t VALUES (42)")
    con.commit()
    con.close()
    return SimpleNamespace(s3=fake, state_dir=state_dir, state_db=state_db)


class TestStateCAS:
    def test_seed_when_both_absent(self, fake_r2):
        assert runmod.push_state() == 0
        assert runmod.STATE_KEY in fake_r2.s3.objs
        backups = [k for k in fake_r2.s3.objs if k.startswith("_aqueduct/backups/state-")]
        assert len(backups) == 1 and backups[0].endswith("-local.db.zst")
        stored = open(runmod.ETAG_PATH, encoding="utf-8").read().strip()
        assert stored == hashlib.md5(fake_r2.s3.objs[runmod.STATE_KEY]).hexdigest()

    def test_matching_etag_uploads(self, fake_r2):
        assert runmod.push_state() == 0  # seed
        con = sqlite3.connect(fake_r2.state_db)
        con.execute("INSERT INTO t VALUES (43)")
        con.commit()
        con.close()
        assert runmod.push_state() == 0  # stored etag == remote etag -> allowed
        import zstandard
        raw = zstandard.ZstdDecompressor().decompress(fake_r2.s3.objs[runmod.STATE_KEY])
        chk = sqlite3.connect(":memory:")
        chk.deserialize(raw)
        assert chk.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2

    def test_mismatched_etag_aborts_exit2_remote_untouched(self, fake_r2, monkeypatch):
        assert runmod.push_state() == 0  # seed + record etag
        foreign = b"another writer's state"
        fake_r2.s3.objs[runmod.STATE_KEY] = foreign  # remote moved under us
        n_objs = len(fake_r2.s3.objs)
        assert runmod.push_state() == 2
        assert fake_r2.s3.objs[runmod.STATE_KEY] == foreign, "CAS abort must not overwrite"
        assert len(fake_r2.s3.objs) == n_objs, "no backup may be written on abort"
        # and through the CLI: exit code 2 surfaces as SystemExit(2)
        monkeypatch.setattr(sys, "argv", ["run", "--push-state"])
        with pytest.raises(SystemExit) as e:
            runmod.main()
        assert e.value.code == 2

    def test_never_pulled_refuses_push_over_existing_remote(self, fake_r2):
        fake_r2.s3.objs[runmod.STATE_KEY] = b"remote exists"  # but no .state_etag here
        assert runmod.push_state() == 2

    def test_pull_roundtrip_and_missing_remote(self, fake_r2):
        assert runmod.pull_state() == 1  # nothing remote yet -> loud error, exit 1
        assert runmod.push_state() == 0
        os.remove(fake_r2.state_db)
        os.remove(runmod.ETAG_PATH)
        assert runmod.pull_state() == 0
        con = sqlite3.connect(fake_r2.state_db)
        assert con.execute("SELECT x FROM t").fetchone()[0] == 42
        con.close()
        stored = open(runmod.ETAG_PATH, encoding="utf-8").read().strip()
        assert stored == hashlib.md5(fake_r2.s3.objs[runmod.STATE_KEY]).hexdigest()
        # pulled copy pushes cleanly (etags line up end-to-end)
        assert runmod.push_state() == 0


# ---------------------------------------------------------------------------
# D1 sync: emitted SQL validity + idempotency + refusals
# ---------------------------------------------------------------------------

def _scratch_state_db(path: str, n_units: int = 5) -> None:
    from updater.state import DDL
    con = sqlite3.connect(path)
    con.executescript(DDL)
    torture = "O'Brien -- ; DROP TABLE x; \" \n newline\tand unicode: żółć €"
    for i in range(n_units):
        con.execute(
            "INSERT INTO unit_state(source_id,unit_id,strategy,upstream_vintage,"
            "last_success_utc,last_attempt_utc,status,last_obs_date,obs_count,"
            "attempt_count,last_error) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (f"src{i}", "_all", "extend_by_date", f"v{i}", "2026-07-01T00:00:00+00:00",
             "2026-07-02T00:00:00+00:00", "ok", "2026-06-30", 100 + i, 1,
             torture if i == 0 else None))
    con.execute(
        "INSERT INTO source_state(source_id,strategy,cadence,status,last_success_utc,"
        "last_attempt_utc,owner,enabled,note) VALUES ('src0','extend_by_date','daily',"
        "'ok','2026-07-01T00:00:00+00:00','2026-07-02T00:00:00+00:00',NULL,1,?)",
        (torture,))
    con.commit()
    con.close()


class TestD1Sync:
    def test_emitted_sql_is_d1_legal_and_replays_equal(self, tmp_path):
        db = str(tmp_path / "state.db")
        _scratch_state_db(db)
        out = str(tmp_path / "sql")
        os.makedirs(out)
        files, counts = d1sync.emit_sql(db, out)
        assert counts == {"unit_state": 5, "source_state": 1}
        for p in files:
            assert os.path.getsize(p) <= d1sync.MAX_FILE_BYTES
            body = open(p, encoding="utf-8").read()
            for banned in ("BEGIN", "COMMIT", "PRAGMA"):
                assert not any(ln.strip().upper().startswith(banned)
                               for ln in body.splitlines()), f"{banned} in {p}"
        # independent replay (twice = idempotent), row-for-row equality vs source
        mem = sqlite3.connect(":memory:")
        for _ in (1, 2):
            for p in files:
                mem.executescript(open(p, encoding="utf-8").read())
        src = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        for table in ("unit_state", "source_state"):
            q = f"SELECT * FROM {table} ORDER BY source_id"
            assert mem.execute(q).fetchall() == src.execute(q).fetchall()
        src.close()
        # upsert branch: tamper a row in mem, re-apply -> restored
        mem.execute("UPDATE unit_state SET obs_count=-1 WHERE source_id='src0'")
        for p in files:
            mem.executescript(open(p, encoding="utf-8").read())
        assert mem.execute("SELECT obs_count FROM unit_state WHERE source_id='src0'"
                           ).fetchone()[0] == 100
        mem.close()
        # the module's own verifier agrees
        d1sync.verify_replay(db, files, counts)

    def test_dry_run_executes_nothing(self, tmp_path, capsys, monkeypatch):
        db = str(tmp_path / "state.db")
        _scratch_state_db(db)

        def boom(files):  # any wrangler attempt = test failure
            raise AssertionError("execute_remote must not run under --dry-run")
        monkeypatch.setattr(d1sync, "execute_remote", boom)
        d1sync.main(["--dry-run", "--state-db", db])
        out = capsys.readouterr().out
        assert "DRY RUN" in out and "state_delta_000.sql" in out

    def test_zero_row_projection_refused(self, tmp_path):
        db = str(tmp_path / "empty.db")
        from updater.state import DDL
        con = sqlite3.connect(db)
        con.executescript(DDL)
        con.commit()
        con.close()
        out = str(tmp_path / "sql")
        os.makedirs(out)
        with pytest.raises(SystemExit, match="zero"):
            d1sync.emit_sql(db, out)


# ---------------------------------------------------------------------------
# Health gate: --fail-past-2x-sla must actually gate (found as a silent no-op:
# unrecognized flags were ignored and the CI step exited 0 unconditionally)
# ---------------------------------------------------------------------------

from updater import health as healthmod     # noqa: E402


def _health_report(rows):
    return {"generated_utc": "2026-07-03T00:00:00+00:00",
            "sla_tolerance_periods": 2.0, "summary": {}, "sources": rows}


def _health_row(**kw):
    row = {"source": "src", "strategy": "extend_by_date", "cadence": "daily",
           "live": False, "health": "OK", "last_success_age_d": None,
           "newest_obs": None, "newest_obs_age_d": None, "n_series_tracked": 0,
           "n_discontinued": 0, "discontinued_series": [], "attention": []}
    row.update(kw)
    return row


class TestHealthGate:
    def test_unknown_flag_is_rejected_not_ignored(self, monkeypatch):
        called = []
        monkeypatch.setattr(healthmod, "assess", lambda: called.append(1))
        monkeypatch.setattr(sys, "argv", ["health", "--fail-past-2x-slaX"])
        with pytest.raises(SystemExit) as e:
            healthmod.main()
        assert e.value.code == 2
        assert not called, "unknown flag must be refused before any assessment runs"

    def test_gate_judges_only_live_tier(self):
        rows = [_health_row(source="quiet_ok", live=True, health="OK"),
                _health_row(source="red_not_live", live=False, health="RED-SLA")]
        assert healthmod.gate_failures(_health_report(rows)) == []
        rows.append(_health_row(source="red_live", live=True, health="RED-SLA"))
        rows.append(_health_row(source="pending_live", live=True, health="PENDING"))
        fails = healthmod.gate_failures(_health_report(rows))
        assert len(fails) == 2
        assert any("red_live" in f for f in fails)
        assert any("pending_live" in f for f in fails), \
            "a live source with no adapter must fail the gate (mirrors §5.3 run failure)"

    def test_main_exits_1_on_red_live_and_0_when_clean(self, monkeypatch, tmp_path):
        monkeypatch.setattr(healthmod.config, "STATE_DIR", str(tmp_path))
        monkeypatch.setattr(sys, "argv", ["health", "--fail-past-2x-sla"])
        red = _health_report([_health_row(source="s", live=True, health="RED-DATA")])
        monkeypatch.setattr(healthmod, "assess", lambda: red)
        with pytest.raises(SystemExit) as e:
            healthmod.main()
        assert e.value.code == 1
        green = _health_report([_health_row(source="s", live=True, health="OK"),
                                _health_row(source="t", live=False, health="RED-SLA")])
        monkeypatch.setattr(healthmod, "assess", lambda: green)
        with pytest.raises(SystemExit) as e:
            healthmod.main()
        assert e.value.code == 0
