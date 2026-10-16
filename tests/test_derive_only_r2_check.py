"""An --only derive refuses to publish from a file that is not R2's object (review AR-327; ledger R383/R548).

core/derive_csv.py READS through the econdl resolver - this checkout's parquet mirror, or ECONDL_DATA - and
WRITES the served CSV to R2. The whole-source preflight samples at most 64 files of data/clean_full with a
fixed seed, so for an --only run it almost never looked at the file it was about to publish. Measured
2026-10-16: the mirror's eurostat MIGR_ASYUMWITM.parquet was 26,472,276 rows (2026-10-01) while R2 held
26,590,932 (2026-10-06), and the health gate printed exactly the command that would publish the older one.

Now every selected id's file is compared with R2's object of the same key, byte for byte (size + ETag), before
anything is listed or PUT. Every test here runs main() end to end on a FAKE S3 client and a recording CSV
store - no network: tests/conftest.py empties the R2 credentials, and r2_util.client is replaced."""
import datetime as dt
import hashlib
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import derive_csv as dc  # noqa: E402
from core import r2_util  # noqa: E402
from updater import blob  # noqa: E402

UTC = dt.timezone.utc
BODY = b"series_id,obs_date,value\nk,2024-01-01,1\n"
PART = 8 * 1024 * 1024


class Store:
    """A recording CSV store: what R2Blob is to derive_csv."""
    bucket = "econ-data"

    def __init__(self):
        self.put = {}

    def list_keys(self, prefix):
        return []

    def list_modified(self, prefix):
        return []

    def put_atomic(self, key, data, plain=False):
        self.put[key] = data


class FakeS3:
    """head_object over a dict of key -> (bytes or ETag override). Records what was asked."""

    def __init__(self, objects=None, error=None, bodies=None):
        self.objects = dict(objects or {})
        self.error = error
        self.bodies = dict(bodies or {})                             # key -> bytes a GET returns
        self.asked = []
        self.got = []

    def get_object(self, Bucket, Key, IfMatch=None):                 # noqa: N803 - boto3's names
        import io
        self.got.append((Bucket, Key, IfMatch))
        if Key not in self.bodies:
            raise ConnectionError("GET refused")
        return {"Body": io.BytesIO(self.bodies[Key])}

    def head_object(self, Bucket, Key):                              # noqa: N803 - boto3's names
        self.asked.append((Bucket, Key))
        if self.error is not None:
            raise self.error
        if Key not in self.objects:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"},
                               "ResponseMetadata": {"HTTPStatusCode": 404}}, "HeadObject")
        size, etag = self.objects[Key]
        return {"ContentLength": size, "ETag": f'"{etag}"',
                "LastModified": dt.datetime(2026, 10, 6, 15, 55, 52, tzinfo=UTC)}


def _single(data: bytes):
    return len(data), hashlib.md5(data).hexdigest()


def _multipart(data: bytes, part: int = PART):
    parts = [hashlib.md5(data[i:i + part]).digest() for i in range(0, len(data), part)]
    return len(data), hashlib.md5(b"".join(parts)).hexdigest() + f"-{len(parts)}"


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A pre-T0 desktop run: backend unset, no cutover flag, a data root holding zz/A.parquet."""
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
    from econdl import _resolve
    data = tmp_path / "pinned"
    (data / "zz").mkdir(parents=True)
    local = data / "zz" / "A.parquet"
    local.write_bytes(b"LOCAL-MIRROR-VINTAGE" * 100)
    monkeypatch.setenv("ECONDL_DATA", str(data))
    resolved = {"zz:a": str(local), "zz:b": str(local)}

    def resolve(sid, root=None):
        if sid not in resolved:
            raise _resolve.ResolveError(f"{sid}: no such series in this store")
        return types.SimpleNamespace(parquet_path=resolved[sid])
    monkeypatch.setattr(_resolve, "resolve", resolve)
    monkeypatch.setattr(dc, "_catalog_ids", lambda limit, source: [("zz:a", "zz"), ("zz:b", "zz"),
                                                                  ("zz:c", "zz")])
    # as the real one does: resolve first, so an id that does not resolve derives nothing
    monkeypatch.setattr(dc, "_series_csv_bytes", lambda sid: (_resolve.resolve(sid), BODY)[1])
    sampled = []
    monkeypatch.setattr(dc, "_mirror_behind_store", lambda sources, sample=0: sampled.append(sources) or [])
    store = Store()
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: store)
    ids = tmp_path / "ids.txt"
    ids.write_text("zz:a\n", encoding="utf-8")
    r = types.SimpleNamespace(tmp=tmp_path, data=data, local=local, resolved=resolved, store=store,
                              sampled=sampled, ids=str(ids), s3=None)

    def s3(fake):
        r.s3 = fake
        monkeypatch.setattr(r2_util, "client", lambda *a, **k: fake)
        return fake
    r.set_s3 = s3

    def run(*extra, only=True):
        argv = ["derive_csv.py", "--bucket", "econ-data", "--source", "zz"]
        if only:
            argv += ["--only", r.ids]
        monkeypatch.setattr(sys, "argv", argv + list(extra))
        return dc.main()
    r.run = run
    return r


KEY = "clean_full/zz/A.parquet"


def test_an_id_whose_file_differs_from_r2_is_refused_and_nothing_is_put(rig, capsys):
    s3 = rig.set_s3(FakeS3({KEY: _single(b"R2-STORE-VINTAGE" * 200)}))
    with pytest.raises(SystemExit) as e:
        rig.run()
    assert e.value.code == 2
    assert rig.store.put == {}, "a CSV was published from a file R2 does not hold"
    out = capsys.readouterr().out
    assert "REFUSING TO DERIVE" in out and "zz:a" in out
    assert str(rig.local) in out and hashlib.md5(rig.local.read_bytes()).hexdigest() in out, "local side named"
    assert KEY in out and hashlib.md5(b"R2-STORE-VINTAGE" * 200).hexdigest() in out, "R2 side named"
    assert "ECONDL_DATA" in out and "from R2" in out, "the refusal names the safe procedure"
    assert s3.asked == [("econ-data", KEY)]


def test_same_size_different_bytes_is_refused(rig):
    """Size alone is not identity (R549: TEC00115 was size-identical and content-different)."""
    other = bytearray(rig.local.read_bytes())
    other[7] ^= 0xFF
    rig.set_s3(FakeS3({KEY: (len(other), hashlib.md5(bytes(other)).hexdigest())}))
    with pytest.raises(SystemExit):
        rig.run()
    assert rig.store.put == {}


def test_an_id_whose_file_is_r2_s_object_proceeds(rig, capsys):
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    rig.run()
    assert list(rig.store.put) == ["series/zz%3Aa.csv"]
    out = capsys.readouterr().out
    assert "byte for byte" in out
    assert rig.sampled == [], "an --only run reads its own files, not the whole-source sample"


def test_a_multipart_object_is_compared_by_its_part_digests(rig, capsys):
    data = os.urandom(PART + 12_345)                                 # two parts, as upload_file cuts them
    rig.local.write_bytes(data)
    rig.set_s3(FakeS3({KEY: _multipart(data)}))
    rig.run()
    assert list(rig.store.put) == ["series/zz%3Aa.csv"]
    out = capsys.readouterr().out
    assert f"== R2 {KEY}" in out and _multipart(data)[1] in out, "the multipart ETag was proven, not skipped"


@pytest.mark.parametrize("etag", ["wrong-part-size", "same-digest-other-count", "whole-md5-with-suffix"])
def test_a_multipart_etag_that_does_not_reconstruct_is_refused(rig, etag):
    """Only an exact reconstruction passes; anything else fails safe."""
    data = os.urandom(PART + 12_345)
    rig.local.write_bytes(data)
    good = _multipart(data)[1]
    bad = {"wrong-part-size": _multipart(data, 5 * 1024 * 1024)[1],
           "same-digest-other-count": good.split("-")[0] + "-3",
           "whole-md5-with-suffix": hashlib.md5(data).hexdigest() + "-2"}[etag]
    rig.set_s3(FakeS3({KEY: (len(data), bad)}))
    with pytest.raises(SystemExit):
        rig.run()
    assert rig.store.put == {}


@pytest.mark.parametrize("case", ["404", "head-error", "no-client"])
def test_what_cannot_be_proven_is_refused(rig, monkeypatch, case, capsys):
    if case == "404":
        rig.set_s3(FakeS3({}))
    elif case == "head-error":
        rig.set_s3(FakeS3(error=ConnectionError("reset by peer")))
    else:
        def nope(*a, **k):
            raise RuntimeError("R2 read credentials are not set")
        monkeypatch.setattr(r2_util, "client", nope)
    with pytest.raises(SystemExit) as e:
        rig.run()
    assert e.value.code == 2 and rig.store.put == {}
    out = capsys.readouterr().out
    assert {"404": "NO SUCH OBJECT", "head-error": "HEAD FAILED", "no-client": "no R2 client"}[case] in out


def test_a_file_outside_the_store_roots_is_refused(rig, capsys):
    """A resolution that maps to no R2 key cannot be proven equal to anything."""
    stray = rig.tmp / "elsewhere" / "A.parquet"
    stray.parent.mkdir()
    stray.write_bytes(rig.local.read_bytes())
    rig.resolved["zz:a"] = str(stray)
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    with pytest.raises(SystemExit):
        rig.run()
    assert rig.store.put == {}
    assert "NO R2 KEY" in capsys.readouterr().out


def test_allow_stale_mirror_does_not_skip_the_exact_check(rig):
    """The flag skips the whole-source SAMPLE; an --only run that adds it must still be checked (R386)."""
    rig.set_s3(FakeS3({KEY: _single(b"R2-STORE-VINTAGE" * 200)}))
    with pytest.raises(SystemExit):
        rig.run("--allow-stale-mirror")
    assert rig.store.put == {}


@pytest.mark.parametrize("r2", ["other-bytes", "404", "other-size-multipart", "same-size-single-part",
                                "same-size-known-multipart", "same-size-unknown-layout-read"])
def test_the_pin_flag_never_overrides_a_difference(rig, r2, capsys):
    """A copy that IS R2's object passes without the flag, so a flag that let a DIFFERENCE through would
    be a bypass: a stale pinned copy published as if proven (review finding 1). The same-size cases are
    the commonest stale shape (round 2, findings 1, 2 and 5)."""
    n = rig.local.stat().st_size
    other = b"S" * n                                                 # same size, other bytes
    objects = {"other-bytes": {KEY: _single(b"R2-STORE-VINTAGE" * 200)}, "404": {},
               "other-size-multipart": {KEY: (PART * 3, "0" * 32 + "-3")},
               "same-size-single-part": {KEY: (n, hashlib.md5(other).hexdigest())},
               "same-size-known-multipart": {KEY: (n, "0" * 32 + "-1")},      # 1 part fits both layouts
               "same-size-unknown-layout-read": {KEY: (n, "0" * 32 + "-7")}}[r2]
    rig.set_s3(FakeS3(objects, bodies={KEY: other}))
    with pytest.raises(SystemExit) as e:
        rig.run(dc.ONLY_PIN_FLAG)
    assert e.value.code == 2 and rig.store.put == {}
    assert "DIFFERS" in capsys.readouterr().out


@pytest.mark.parametrize("case", ["head-error", "no-client", "unknown-multipart-layout"])
def test_the_pin_flag_overrides_only_what_could_not_be_compared(rig, monkeypatch, case, capsys):
    if case == "head-error":
        rig.set_s3(FakeS3(error=ConnectionError("reset by peer")))
    elif case == "no-client":
        def nope(*a, **k):
            raise RuntimeError("R2 read credentials are not set")
        monkeypatch.setattr(r2_util, "client", nope)
    else:                                                            # 2,000 B in 7 parts fits no layout
        rig.set_s3(FakeS3({KEY: (rig.local.stat().st_size, "0" * 32 + "-7")}))
    with pytest.raises(SystemExit):
        rig.run()                                                    # without the flag: refused
    assert rig.store.put == {}
    capsys.readouterr()
    rig.run(dc.ONLY_PIN_FLAG)
    assert list(rig.store.put) == ["series/zz%3Aa.csv"]
    assert "WARNING" in capsys.readouterr().out


def test_the_pin_flag_without_econdl_data_is_refused(rig, monkeypatch, capsys):
    """Without ECONDL_DATA the run reads the checkout's mirror: the flag would be the R548 run."""
    from econdl import _resolve
    monkeypatch.delenv("ECONDL_DATA")
    monkeypatch.setattr(_resolve, "_DEFAULT_DATA", str(rig.data))
    rig.set_s3(FakeS3({KEY: _single(b"R2-STORE-VINTAGE" * 200)}))
    with pytest.raises(SystemExit) as e:
        rig.run(dc.ONLY_PIN_FLAG)
    assert e.value.code == 2 and rig.store.put == {}
    assert "ECONDL_DATA is not set" in capsys.readouterr().out, "refused for the right reason"


@pytest.mark.parametrize("where", ["the-mirror", "named-clean_full"])
def test_the_pin_flag_pointed_at_a_mirror_is_refused(rig, monkeypatch, where, capsys):
    from econdl import _resolve
    if where == "the-mirror":
        monkeypatch.setattr(_resolve, "_DEFAULT_DATA", str(rig.data))
    else:
        mirror = rig.tmp / "checkout" / "data" / "clean_full"
        (mirror / "zz").mkdir(parents=True)
        (mirror / "zz" / "A.parquet").write_bytes(rig.local.read_bytes())
        rig.resolved["zz:a"] = str(mirror / "zz" / "A.parquet")
        monkeypatch.setenv("ECONDL_DATA", str(mirror))
    rig.set_s3(FakeS3(error=ConnectionError("reset by peer")))     # uncompared, so only the pin decides
    with pytest.raises(SystemExit):
        rig.run(dc.ONLY_PIN_FLAG)
    assert rig.store.put == {}
    assert "a checkout's parquet mirror" in capsys.readouterr().out


def test_the_pin_flag_refuses_a_file_outside_the_pinned_folder(rig, capsys):
    stray = rig.tmp / "elsewhere" / "A.parquet"
    stray.parent.mkdir()
    stray.write_bytes(b"x")
    rig.resolved["zz:a"] = str(stray)
    rig.set_s3(FakeS3(error=ConnectionError("reset by peer")))     # uncompared, so only the pin decides
    with pytest.raises(SystemExit):
        rig.run(dc.ONLY_PIN_FLAG)
    assert rig.store.put == {}
    assert "OUTSIDE ECONDL_DATA" in capsys.readouterr().out


def test_the_pin_flag_needs_only(rig):
    with pytest.raises(SystemExit):
        rig.run(dc.ONLY_PIN_FLAG, only=False)
    assert rig.store.put == {}


def test_a_whole_source_run_is_unchanged(rig, monkeypatch):
    """No --only: the 64-file sample runs as before and the exact check does not."""
    monkeypatch.setattr(dc, "_only_files_differ_from_r2",
                        lambda *a, **k: pytest.fail("the exact check ran on a whole-source run"))
    rig.run(only=False)
    assert rig.sampled == [["zz"]]
    assert sorted(rig.store.put) == ["series/zz%3Aa.csv", "series/zz%3Ab.csv"], "zz:c does not resolve"
    rig.sampled.clear()
    rig.store.put.clear()
    rig.run("--allow-stale-mirror", only=False)
    assert rig.sampled == [], "--allow-stale-mirror still skips the sample on a whole-source run"
    assert len(rig.store.put) == 2


def test_a_dry_run_asks_r2_nothing(rig, monkeypatch, capsys):
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("a dry run contacted R2"))
    rig.run("--dry-run")
    assert "DRY RUN: derived 1" in capsys.readouterr().out


class _Selfhost(blob.SelfhostBlob):
    bucket = "econ-data"

    def __init__(self):
        self.put = {}

    def put_atomic(self, key, data, plain=False):
        self.put[key] = data


def test_after_t0_nothing_is_compared_with_r2(rig, monkeypatch, capsys):
    from core import cutover
    flag = rig.tmp / "CUTOVER"
    flag.write_text("t0")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    st = _Selfhost()
    monkeypatch.setattr(blob, "csv_store", lambda *a, **k: st)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    rig.run()
    assert list(st.put) == ["series/zz%3Aa.csv"]
    assert "after T0" in capsys.readouterr().out


def test_a_self_hosted_probe_before_t0_is_still_checked(rig, monkeypatch):
    """AQUEDUCT_BACKEND=selfhost before T0 writes the CSVs to the self-hosted store, but R2 is still the
    parquet authority - the check must run (review finding 8)."""
    st = _Selfhost()
    monkeypatch.setattr(blob, "csv_store", lambda *a, **k: st)
    rig.set_s3(FakeS3({KEY: _single(b"R2-STORE-VINTAGE" * 200)}))
    with pytest.raises(SystemExit):
        rig.run()
    assert st.put == {}


def test_ids_sharing_a_file_are_compared_once_and_each_is_named(rig, capsys):
    (rig.tmp / "ids.txt").write_text("zz:a\nzz:b\n", encoding="utf-8")
    s3 = rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    rig.run()
    # the check HEADs the shared file once; each id's PUT then re-checks it once (finding 2)
    assert s3.asked == [("econ-data", KEY)] * 3, "one check HEAD per file, then one re-check per PUT"
    assert sorted(rig.store.put) == ["series/zz%3Aa.csv", "series/zz%3Ab.csv"]
    out = capsys.readouterr().out
    assert "[only-check] zz:a:" in out and "[only-check] zz:b:" in out


def test_an_unresolvable_id_is_dropped_so_a_late_file_is_not_published_unchecked(rig, monkeypatch, capsys):
    """zz:c does not resolve at check time; its file then 'appears'. It must not be derived (finding 7)."""
    (rig.tmp / "ids.txt").write_text("zz:a\nzz:c\n", encoding="utf-8")
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    real = dc._only_files_differ_from_r2

    def check_then_appear(*a, **k):
        out = real(*a, **k)
        rig.resolved["zz:c"] = str(rig.local)                       # the file appears after the check
        return out
    monkeypatch.setattr(dc, "_only_files_differ_from_r2", check_then_appear)
    rig.run()
    assert list(rig.store.put) == ["series/zz%3Aa.csv"]
    assert "zz:c: does not resolve here" in capsys.readouterr().out


def test_a_selection_where_nothing_resolves_is_refused(rig):
    (rig.tmp / "ids.txt").write_text("zz:c\n", encoding="utf-8")
    rig.set_s3(FakeS3({}))
    with pytest.raises(SystemExit) as e:
        rig.run()
    assert e.value.code == 2 and rig.store.put == {}


def test_an_id_that_resolves_to_no_file_is_refused(rig):
    empty = rig.data / "zz" / "empty_dir"
    empty.mkdir()
    rig.resolved["zz:a"] = str(empty)
    rig.set_s3(FakeS3({}))
    with pytest.raises(SystemExit):
        rig.run()
    assert rig.store.put == {}


def _real_parquet(path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"series_key": ["k"], "obs_date": ["2024-01-01"], "value": [1.0]}), str(path))


def test_a_directory_is_checked_for_every_file_the_reader_reads(rig, capsys):
    """pyarrow.dataset reads a directory's non-.parquet files too - a crashed atomic write's .tmp among
    them - so those are compared (finding 4); R2 has no such object, so the run refuses."""
    d = rig.data / "zz" / "T"
    _real_parquet(d / "T1.parquet")
    (d / "T1.parquet.999.deadbeef.tmp").write_bytes((d / "T1.parquet").read_bytes())
    rig.resolved["zz:a"] = str(d)
    rig.set_s3(FakeS3({"clean_full/zz/T/T1.parquet": _single((d / "T1.parquet").read_bytes())}))
    with pytest.raises(SystemExit):
        rig.run()
    assert rig.store.put == {}
    assert "clean_full/zz/T/T1.parquet.999.deadbeef.tmp: NO SUCH OBJECT" in capsys.readouterr().out


@pytest.mark.parametrize("r2_map", ["equal", "absent", "other"])
def test_a_part_id_s_split_map_is_compared_too(rig, monkeypatch, r2_map, capsys):
    """'#part' ids choose their rows by <src>/_split_map.json (finding 5)."""
    smap = rig.data / "zz" / "_split_map.json"
    smap.write_text('{"a": {"dim": "sex"}}', encoding="utf-8")
    rig.resolved["zz:a#M"] = str(rig.local)
    (rig.tmp / "ids.txt").write_text("zz:a#M\n", encoding="utf-8")
    monkeypatch.setattr(dc, "_catalog_ids", lambda limit, source: [("zz:a#M", "zz")])
    objects = {KEY: _single(rig.local.read_bytes())}
    if r2_map == "equal":
        objects["clean_full/zz/_split_map.json"] = _single(smap.read_bytes())
    elif r2_map == "other":
        objects["clean_full/zz/_split_map.json"] = _single(b'{"a": {"dim": "age"}}')
    rig.set_s3(FakeS3(objects))
    if r2_map == "equal":
        rig.run()
        assert list(rig.store.put) == ["series/zz%3Aa%23M.csv"]
        assert "== R2 clean_full/zz/_split_map.json" in capsys.readouterr().out, "the map was proven"
    else:
        with pytest.raises(SystemExit):
            rig.run()
        assert rig.store.put == {}


@pytest.mark.parametrize("workers", ["1", "3"])
def test_a_local_file_changed_during_the_derive_is_not_put(rig, monkeypatch, workers, capsys):
    """The check runs once, the derive can run for hours (finding 2): re-checked just before the PUT."""
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    from econdl import _resolve

    def derive_while_replaced(sid):
        _resolve.resolve(sid)
        rig.local.write_bytes(b"REPLACED-DURING-THE-DERIVE" * 50)
        st = rig.local.stat()
        os.utime(rig.local, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
        return BODY
    monkeypatch.setattr(dc, "_series_csv_bytes", derive_while_replaced)
    with pytest.raises(SystemExit, match="could not be written"):
        rig.run("--workers", workers)
    assert rig.store.put == {}
    assert "CHANGED since the --only check" in capsys.readouterr().out


def test_an_unknown_multipart_layout_is_settled_by_reading_the_object(rig, capsys):
    """R2 answered in a layout this code cannot rebuild: the object's own md5 decides (round 2, finding 5),
    read pinned to the ETag just seen."""
    n = rig.local.stat().st_size
    s3 = rig.set_s3(FakeS3({KEY: (n, "0" * 32 + "-7")}, bodies={KEY: rig.local.read_bytes()}))
    rig.run()
    assert list(rig.store.put) == ["series/zz%3Aa.csv"]
    assert s3.got == [("econ-data", KEY, '"' + "0" * 32 + '-7"')]
    assert "multipart layout unknown, object md5" in capsys.readouterr().out


def test_a_file_that_joins_the_read_set_during_the_derive_is_not_put(rig, monkeypatch, capsys):
    """A `_live` sibling that appears mid-run is a file the check never compared (round 2, finding 3)."""
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    from econdl import _resolve
    live = rig.data / "zz" / "_live" / "A.parquet"

    def derive_then_grow(sid):
        _resolve.resolve(sid)
        live.parent.mkdir(parents=True, exist_ok=True)
        live.write_bytes(b"LIVE-HALF")
        rig.resolved["zz:a"] = [str(rig.local), str(live)]
        return BODY
    monkeypatch.setattr(dc, "_series_csv_bytes", derive_then_grow)
    with pytest.raises(SystemExit, match="could not be written"):
        rig.run()
    assert rig.store.put == {}
    assert "reads a different set of files" in capsys.readouterr().out


def test_a_file_changing_while_it_is_hashed_differs_even_with_the_pin(rig, monkeypatch, capsys):
    """A file being rewritten is not one an operator can vouch for (round 2, finding 4)."""
    rig.set_s3(FakeS3(error=ConnectionError("reset by peer")))
    real = dc._local_digests

    def digest_while_rewritten(path):
        out = real(path)
        rig.local.write_bytes(b"REWRITTEN-MEANWHILE" * 300)
        return out
    monkeypatch.setattr(dc, "_local_digests", digest_while_rewritten)
    with pytest.raises(SystemExit) as e:
        rig.run(dc.ONLY_PIN_FLAG)
    assert e.value.code == 2, "refused at the check, not left for the re-check to catch"
    assert rig.store.put == {}
    assert "CHANGED WHILE BEING READ" in capsys.readouterr().out


def test_a_same_size_same_time_replacement_during_the_derive_is_not_put(rig, monkeypatch, capsys):
    """copy2 / robocopy /COPY:DAT keep size and mtime; the re-check re-hashes (round 2, finding 7)."""
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    from econdl import _resolve

    def derive_while_swapped(sid):
        _resolve.resolve(sid)
        st = rig.local.stat()
        rig.local.write_bytes(b"X" * st.st_size)
        os.utime(rig.local, ns=(st.st_atime_ns, st.st_mtime_ns))
        return BODY
    monkeypatch.setattr(dc, "_series_csv_bytes", derive_while_swapped)
    with pytest.raises(SystemExit, match="could not be written"):
        rig.run()
    assert rig.store.put == {}
    assert "same size and time, other bytes" in capsys.readouterr().out


def test_a_shared_file_changed_after_its_first_put_stops_the_next_id(rig, monkeypatch, capsys):
    """The re-hash runs once per file; after it the stat is what guards the file for later ids."""
    (rig.tmp / "ids.txt").write_text("zz:a\nzz:b\n", encoding="utf-8")
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    from econdl import _resolve
    calls = []

    def derive(sid):
        _resolve.resolve(sid)
        calls.append(sid)
        if len(calls) == 2:                                          # after zz:a was PUT
            rig.local.write_bytes(b"NEWER" * 400)
            st = rig.local.stat()
            os.utime(rig.local, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
        return BODY
    monkeypatch.setattr(dc, "_series_csv_bytes", derive)
    with pytest.raises(SystemExit, match="could not be written"):
        rig.run()
    assert list(rig.store.put) == ["series/zz%3Aa.csv"]
    assert "zz:b: CHANGED since the --only check" in capsys.readouterr().out


def test_the_re_check_refuses_a_file_it_has_no_record_of(tmp_path, monkeypatch):
    """Never a KeyError (counted 'unresolvable', exit 0): a PUT FAILED (round 2, finding 6)."""
    from econdl import _resolve
    f = tmp_path / "zz" / "A.parquet"
    f.parent.mkdir()
    f.write_bytes(b"1")
    monkeypatch.setenv("ECONDL_DATA", str(tmp_path))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: types.SimpleNamespace(parquet_path=str(f)))
    proof = dc._OnlyProof("econ-data", None, {"zz:a": [str(f)]}, {})
    with pytest.raises(dc._PutFailed, match="never compared"):
        proof.recheck("zz:a")


def test_the_streaming_path_is_re_checked_too(rig, monkeypatch, capsys):
    """A flow over --max-rows streams to a temp file and PUTs that; the re-check guards it as well."""
    rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))

    def too_large(sid):
        raise dc.TooLarge("over the in-memory ceiling")

    def stream_while_replaced(sid, out_path):
        open(out_path, "wb").write(b"\x1f\x8b" + b"0" * 20)
        rig.local.write_bytes(b"REPLACED-DURING-THE-STREAM" * 50)
        st = rig.local.stat()
        os.utime(rig.local, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))
        return 1
    streamed = []
    monkeypatch.setattr(dc, "_series_csv_bytes", too_large)
    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", stream_while_replaced)
    monkeypatch.setattr(rig.store, "head_meta", lambda key: None, raising=False)
    monkeypatch.setattr(rig.store, "put_gzip_file", lambda key, path, metadata=None: streamed.append(key),
                        raising=False)
    with pytest.raises(SystemExit, match="could not be written"):
        rig.run("--max-rows", "1", "--allow-stream")
    assert streamed == [] and rig.store.put == {}
    assert "CHANGED since the --only check" in capsys.readouterr().out


def test_an_r2_object_replaced_during_the_derive_is_not_put(rig, capsys):
    """A cloud run that replaced the parquet may also have written a fresher CSV: do not regress it."""
    s3 = rig.set_s3(FakeS3({KEY: _single(rig.local.read_bytes())}))
    real_head = s3.head_object

    def head(Bucket, Key):                                           # noqa: N803
        h = real_head(Bucket=Bucket, Key=Key)
        if len(s3.asked) > 1:
            h = dict(h, ETag='"' + "f" * 32 + '"')
        return h
    s3.head_object = head
    with pytest.raises(SystemExit, match="could not be written"):
        rig.run()
    assert rig.store.put == {}
    assert "R2 CHANGED since the --only check" in capsys.readouterr().out


def test_a_64_mib_part_multipart_object_is_compared(rig, capsys):
    """Three upload tools cut 64 MiB parts (finding 3)."""
    data = os.urandom(64 * 1024 * 1024 + 4096)
    rig.local.write_bytes(data)
    rig.set_s3(FakeS3({KEY: _multipart(data, 64 * 1024 * 1024)}))
    rig.run()
    assert list(rig.store.put) == ["series/zz%3Aa.csv"]
    assert f"== R2 {KEY}" in capsys.readouterr().out, "the 64 MiB layout was proven, not skipped"


def test_the_key_keeps_the_resolver_s_case(tmp_path):
    """realpath would substitute the on-disk case on Windows (finding 6)."""
    root = tmp_path / "data" / "clean_full"
    (root / "eurostat").mkdir(parents=True)
    (root / "eurostat" / "TEC00115.parquet").write_bytes(b"1")
    assert dc._store_key_for(str(root / "eurostat" / "TEC00115.parquet"), str(root)) == \
        "clean_full/eurostat/TEC00115.parquet"
    assert dc._store_key_for(str(root / "eurostat" / "tec00115.parquet"), str(root)) == \
        "clean_full/eurostat/tec00115.parquet"


def test_store_keys_follow_the_resolver_roots(tmp_path):
    root = tmp_path / "data" / "clean_full"
    for rel in ("eurostat/X.parquet", "eurostat/_live/X.parquet"):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"1")
    grouped = tmp_path / "data" / "clean_grouped" / "sec_edgar" / "XOM.parquet"
    grouped.parent.mkdir(parents=True)
    grouped.write_bytes(b"1")
    other = tmp_path / "elsewhere.parquet"
    other.write_bytes(b"1")
    assert dc._store_key_for(str(root / "eurostat" / "X.parquet"), str(root)) == "clean_full/eurostat/X.parquet"
    assert dc._store_key_for(str(root / "eurostat" / "_live" / "X.parquet"), str(root)) == \
        "clean_full/eurostat/_live/X.parquet"
    assert dc._store_key_for(str(grouped), str(root)) == "clean_grouped/sec_edgar/XOM.parquet"
    assert dc._store_key_for(str(other), str(root)) is None


def test_the_health_remedy_names_the_safe_procedure(monkeypatch):
    from updater import health
    from updater.state import StateStore
    fake = {"sources": [{"source_id": "zzflow", "strategy": "giant_changed_units",
                         "cadence": "monthly", "live": True, "csv_grain": "flow"}]}
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: fake)
    st = StateStore(path=":memory:")
    st.note_csv_desktop_owed("zzflow", [("zzflow:a", 99, "too large")])
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == "zzflow")
    line = row["attention"][0]
    assert line.startswith("1 CSV(s) OWED to the desktop derive"), line
    assert "from R2 into a NEW folder" in line and "set ECONDL_DATA" in line, line
    assert line.index("ECONDL_DATA") < line.index("python -m core.derive_csv"), "pin first, then derive"
    assert "--only <ids>" in line and "clear_csv_desktop_owed.py --source zzflow --apply" in line
