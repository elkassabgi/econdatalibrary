"""tools/r2_object_fetch.py against stubbed clients - never the network.

Each guard is shown to FIRE on a planted bad input and to STAND DOWN on its control. The production client
path (open_client over the real r2_util.client) is tested too, with only the credentials faked (AR-399:
a test seam that bypasses the production path proves nothing about it).
"""
import datetime as dt
import gzip
import hashlib
import io
import ntpath
import os
import sys

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import r2_object_census as C  # noqa: E402
from tools import r2_object_fetch as F  # noqa: E402

PLANT = "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"
T1 = dt.datetime(2026, 10, 30, 12, 0, 0, tzinfo=dt.timezone.utc)
CSV = b"series_id,obs_date,value\nx,2026-01-01,1\n"
ID_PLAIN = "census:intltrade__exports__hs#010"
ID_GZ = "census:eits__m3#no"
KEY_PLAIN = "series/census%3Aintltrade__exports__hs%23010.csv"
KEY_GZ = "series/census%3Aeits__m3%23no.csv"


def _gz(b):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as g:
        g.write(b)
    return buf.getvalue()


def _client_error(status, code):
    return ClientError({"Error": {"Code": code, "Message": "m"},
                        "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject")


class FakeS3:
    def __init__(self, objects=None, errors=None):
        self.objects = dict(objects or {})        # key -> dict(body, etag, enc, meta, clen)
        self.errors = dict(errors or {})
        self.log = []

    def get_object(self, Bucket, Key):
        self.log.append(("get", Key))
        if Key in self.errors:
            raise self.errors[Key]
        if Key not in self.objects:
            raise _client_error(404, "NoSuchKey")
        o = self.objects[Key]
        r = {"Body": io.BytesIO(o["body"]), "ETag": f'"{o.get("etag", hashlib.md5(o["body"]).hexdigest())}"',
             "ContentLength": o.get("clen", len(o["body"])), "LastModified": T1, "Metadata": o.get("meta", {})}
        if o.get("enc"):
            r["ContentEncoding"] = o["enc"]
        return r

    def put_object(self, **kw):
        self.log.append(("put", kw.get("Key")))

    def head_object(self, **kw):
        self.log.append(("head", kw.get("Key")))


def _objects():
    return {KEY_PLAIN: {"body": CSV},
            KEY_GZ: {"body": _gz(CSV), "enc": "gzip", "meta": {"csvmd5": hashlib.md5(CSV).hexdigest()}}}


def _ids(tmp_path, ids):
    p = tmp_path / "ids.txt"
    p.write_text("# comment\n" + "\n".join(ids) + "\n")
    return str(p)


def _main(tmp_path, ids, fake, *extra, dest=None):
    calls = []

    def factory():
        calls.append(1)
        return F.GetOnlyClient(fake, allow_unhooked=True)
    dest = dest or str(tmp_path / "out")
    rc = F.main(["--bucket", "b", "--ids-file", _ids(tmp_path, ids), "--dest", dest, *extra],
                client_factory=factory, dest_guard=lambda p: p)
    return rc, calls, dest


# ---- 1. the GET-only locks -----------------------------------------------------------------------------
def test_the_census_tool_still_refuses_get():
    """This tool must not have widened r2_object_census's list/head lock (AR-409 P6)."""
    assert C.ALLOWED_OPERATIONS == {"ListObjectsV2", "HeadObject"}
    assert F.ALLOWED_OPERATIONS == {"GetObject"}


@pytest.mark.parametrize("name", ["put_object", "delete_object", "copy_object", "head_object", "list_objects_v2",
                                  "upload_file", "get_paginator", "delete_objects", "generate_presigned_url"])
def test_the_wrapper_refuses_every_other_call(name):
    ro = F.GetOnlyClient(FakeS3(), allow_unhooked=True)
    with pytest.raises(F.WriteRefused):
        getattr(ro, name)


def test_the_wrapper_cannot_be_swapped_and_needs_events():
    ro = F.GetOnlyClient(FakeS3(), allow_unhooked=True)
    with pytest.raises(F.WriteRefused):
        ro._client = object()
    with pytest.raises(F.WriteRefused):
        F.GetOnlyClient(FakeS3())


class _Sent(Exception):
    pass


def _wire_recorders(raw):
    signed = []
    raw.meta.events.register("before-sign.s3", lambda operation_name=None, **_kw: signed.append(operation_name))

    def before_send(request, **_kw):
        raise _Sent(request.method)
    raw.meta.events.register("before-send.s3", before_send)
    return signed


WRITES = [("put_object", {"Bucket": "b", "Key": "k", "Body": b"x"}),
          ("delete_object", {"Bucket": "b", "Key": "k"}),
          ("copy_object", {"Bucket": "b", "Key": "k", "CopySource": {"Bucket": "b", "Key": "j"}}),
          ("head_object", {"Bucket": "b", "Key": "k"}),
          ("list_objects_v2", {"Bucket": "b", "Prefix": "series/"})]


def test_open_client_is_the_read_client_behind_both_locks(tmp_path, monkeypatch):
    """THE PRODUCTION PATH: the real open_client() over the real r2_util.client(), credentials faked."""
    from core import cutover, r2_util
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER"))
    asked = []

    def fake_creds(write=False):
        asked.append(write)
        return {"endpoint": "https://example.invalid", "key": "test", "secret": "test",
                "mode": "write" if write else "read"}
    monkeypatch.setattr(r2_util, "creds", fake_creds)
    ro = F.open_client()
    assert isinstance(ro, F.GetOnlyClient) and asked == [False]
    raw = ro._client
    signed = _wire_recorders(raw)
    for op, kw in WRITES:
        with pytest.raises(F.WriteRefused):
            getattr(ro, op)(**kw)                       # lock 1
        with pytest.raises(F.WriteRefused):
            getattr(raw, op)(**kw)                      # lock 2, on the raw client
    assert signed == []
    with pytest.raises(_Sent):                          # control: a GET is signed and would be sent
        ro.get_object(Bucket="b", Key=KEY_PLAIN)
    assert signed == ["GetObject"]


def test_the_hook_runs_before_another_before_call_responder(tmp_path, monkeypatch):
    import boto3
    from botocore.awsrequest import AWSResponse
    from botocore.config import Config
    from core import cutover, r2_util
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER"))
    raw = r2_util.guard_client(boto3.client("s3", endpoint_url="https://example.invalid",
                                            aws_access_key_id="test", aws_secret_access_key="test",
                                            config=Config(region_name="auto", retries={"max_attempts": 0})))
    answered = []

    def responder(model=None, **_kw):
        answered.append(model.name)
        return AWSResponse("https://example.invalid", 200, {}, None), {}
    raw.meta.events.register("before-call.*.*", responder)
    F.GetOnlyClient(raw)
    with pytest.raises(F.WriteRefused):
        raw.put_object(Bucket="b", Key="k", Body=b"x")
    assert answered == []
    raw.get_object(Bucket="b", Key="k")                 # control: the responder is live for a GET
    assert answered == ["GetObject"]


def test_a_write_attempt_through_the_get_mode_is_refused(tmp_path, monkeypatch, capsys):
    """Plant a write inside run_get's loop: the client main hands it refuses, and nothing is sent."""
    fake = FakeS3(_objects())
    orig = F.run_get

    def writing_run_get(client, *a, **k):
        client.put_object(Bucket="b", Key=KEY_PLAIN, Body=b"x")
        return orig(client, *a, **k)
    monkeypatch.setattr(F, "run_get", writing_run_get)
    rc, calls, _ = _main(tmp_path, [ID_PLAIN], fake)
    assert rc == F.EXIT_REFUSED and calls == [1] and fake.log == []
    assert "GET-only" in capsys.readouterr().out


def test_after_t0_it_is_refused_before_credentials_load(tmp_path, monkeypatch, capsys):
    from core import cutover, r2_util
    flag = tmp_path / "CUTOVER"
    flag.write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    loaded = []
    monkeypatch.setattr(r2_util, "client", lambda **kw: loaded.append(kw) or FakeS3())
    with pytest.raises(cutover.CutoverRefused):
        F.open_client()
    rc = F.main(["--bucket", "b", "--ids-file", _ids(tmp_path, [ID_PLAIN]), "--dest", str(tmp_path / "o")],
                dest_guard=lambda p: p)
    assert rc == F.EXIT_REFUSED and loaded == [] and "CutoverRefused" in capsys.readouterr().out
    assert not (tmp_path / "o").exists()


# ---- 2. caps -------------------------------------------------------------------------------------------
def _many(n):
    return [f"census:intltrade__exports__hs#{i:03d}" for i in range(n)]


def test_over_the_hard_cap_is_refused_before_any_client(tmp_path, capsys):
    fake = FakeS3()
    rc, calls, _ = _main(tmp_path, _many(26), fake)
    assert rc == F.EXIT_CAP and calls == [] and fake.log == []


def test_the_cap_cannot_be_raised(tmp_path):
    rc, calls, _ = _main(tmp_path, _many(26), FakeS3(), "--max-objects", "30")
    assert rc == F.EXIT_CAP and calls == []


def test_control_25_objects_is_within_the_cap(tmp_path):
    objs = {C.id_to_key(i): {"body": CSV} for i in _many(25)}
    fake = FakeS3(objs)
    rc, calls, _ = _main(tmp_path, _many(25), fake)
    assert rc == F.EXIT_OK and calls == [1] and len(fake.log) == 25


def test_the_cap_can_be_lowered(tmp_path):
    rc, calls, _ = _main(tmp_path, _many(3), FakeS3(), "--max-objects", "2")
    assert rc == F.EXIT_CAP and calls == []


def test_a_body_over_the_byte_cap_is_a_failure_and_not_written(tmp_path):
    fake = FakeS3({KEY_PLAIN: {"body": CSV}})
    rc, _, dest = _main(tmp_path, [ID_PLAIN], fake, "--max-bytes", "10")
    assert rc == F.EXIT_FAILED_ROWS and not os.path.exists(os.path.join(dest, "csv", KEY_PLAIN.split("/")[1]))
    assert "\t200:too_large:" in open(os.path.join(dest, "manifest.tsv")).read()


def test_the_byte_cap_cannot_be_raised(tmp_path):
    rc, calls, _ = _main(tmp_path, [ID_PLAIN], FakeS3(), "--max-bytes", str(F.HARD_MAX_BYTES + 1))
    assert rc == F.EXIT_REFUSED and calls == []


# ---- 3. which keys -------------------------------------------------------------------------------------
@pytest.mark.parametrize("sid", ["ilostat:EMP_TEMP_Q#p1", "series/census%3Ax.csv", "_aqueduct/catalog.db.zst",
                                 "censusx:abc", ":abc", "census"])
def test_an_id_outside_census_is_refused_before_any_client(tmp_path, sid):
    fake = FakeS3()
    rc, calls, _ = _main(tmp_path, [ID_PLAIN, sid], fake)
    assert rc == F.EXIT_REFUSED and calls == [] and fake.log == []


def test_the_scope_is_census_and_no_flag_widens_it(tmp_path):
    """AR-409 r2: --allow-source widened the set to any source; the scope is now a fixed list in code."""
    assert F.SOURCES == {"census"}
    with pytest.raises(SystemExit):                    # argparse: unknown option
        F.build_parser().parse_args(["--bucket", "b", "--ids-file", "x", "--dest", "y", "--allow-source", "ilostat"])
    rc, calls, _ = _main(tmp_path, ["ilostat:EMP_TEMP_Q#p1"], FakeS3())
    assert rc == F.EXIT_REFUSED and calls == []


def test_keys_are_the_writers_exact_keys():
    plan = F.plan_keys([ID_PLAIN, ID_GZ], {"census"}, 25)
    assert [k for _i, k, _n in plan] == [KEY_PLAIN, KEY_GZ]
    assert [n for _i, _k, n in plan] == [KEY_PLAIN.split("/")[1], KEY_GZ.split("/")[1]]


@pytest.mark.parametrize("bad", ["series/census%3Ax.json", "clean_full/census/x.csv", "series/a/census%3Ax.csv",
                                 "series/ilostat%3Ax.csv"])
def test_a_key_that_is_not_an_exact_series_csv_key_is_refused(monkeypatch, bad):
    """Defence in depth behind csv_key: should the layout function ever change, a key of another shape is
    refused rather than fetched."""
    monkeypatch.setattr(F, "id_to_key", lambda sid: bad)
    with pytest.raises(F.Refused):
        F.plan_keys([ID_PLAIN], {"census"}, 25)


@pytest.mark.parametrize("bad", ["series/census%3Aa b.csv", "series/census%3A<x>.csv", "series/census%3Aa:b.csv",
                                 "series/census%3A" + "a" * 200 + ".csv"])
def test_an_unsafe_file_name_is_refused(monkeypatch, bad):
    monkeypatch.setattr(F, "id_to_key", lambda sid: bad)
    with pytest.raises(F.Refused, match="unsafe file name"):
        F.plan_keys([ID_PLAIN], {"census"}, 25)


def test_two_ids_with_one_file_name_are_refused():
    with pytest.raises(F.Refused):
        F.plan_keys(["census:Abc", "census:abc"], {"census"}, 25)      # one name on a case-blind disk


def test_an_empty_ids_file_is_refused(tmp_path):
    rc, calls, _ = _main(tmp_path, [], FakeS3())
    assert rc == F.EXIT_REFUSED and calls == []


# ---- 4. destination ------------------------------------------------------------------------------------
REPO_W = r"F:\claude_wt\econ_census_smap"


@pytest.mark.parametrize("path", [r"D:\temp\out", r"E:\research\out", r"d:\x", "E:/x",
                                  r"F:\claude_wt\econ_census_smap\out", r"F:\claude_wt\econ_census_smap\tools\o",
                                  r"F:\work\data\clean_full\census", r"F:\x\CLEAN_FULL\y",
                                  r"C:\Users\x\out", "out", r"F:out", ""])
def test_the_destination_guard_refuses(path):
    with pytest.raises(F.Refused):
        F.check_dest(path, pathmod=ntpath, temp_roots=[r"C:\Temp"], repo_root=REPO_W, check_fs=False)


def test_a_temp_root_on_d_or_e_is_still_refused():
    for root, path in ((r"D:\temp", r"D:\temp\x"), (r"E:\tmp", r"E:\tmp\x")):
        with pytest.raises(F.Refused):
            F.check_dest(path, pathmod=ntpath, temp_roots=[root], repo_root=REPO_W, check_fs=False)


@pytest.mark.parametrize("path", [r"F:\work\census_get9", r"C:\Temp\census_get9", r"F:\work\clean_fullish"])
def test_control_the_destination_guard_allows_f_and_temp(path):
    assert F.check_dest(path, pathmod=ntpath, temp_roots=[r"C:\Temp"], repo_root=REPO_W, check_fs=False)


def test_the_real_guard_wants_a_new_or_empty_folder(tmp_path):
    full = tmp_path / "full"
    full.mkdir()
    (full / "x").write_text("x")
    afile = tmp_path / "afile"
    afile.write_text("x")
    tr = [str(tmp_path)]
    for bad in (full, afile, tmp_path / "nope" / "deeper"):
        with pytest.raises(F.Refused):
            F.check_dest(str(bad), temp_roots=tr, repo_root=None)
    empty = tmp_path / "empty"
    empty.mkdir()
    assert F.check_dest(str(empty), temp_roots=tr, repo_root=None)              # control: empty
    assert F.check_dest(str(tmp_path / "new"), temp_roots=tr, repo_root=None)   # control: new


def _dir_link(link, target):
    """A directory symlink, or on Windows without the privilege a junction. False when neither can be made."""
    try:
        os.symlink(str(target), str(link), target_is_directory=True)
        return True
    except (OSError, NotImplementedError, AttributeError):
        pass
    if os.name == "nt":
        try:
            import _winapi
            _winapi.CreateJunction(str(target), str(link))
            return True
        except (OSError, ImportError, AttributeError):
            return False
    return False


def test_a_link_that_leads_into_the_repo_or_a_store_is_refused(tmp_path):
    """The guard judges where the path LEADS (realpath): an allowed-looking link into the repo or into a
    clean_full tree is refused."""
    repo = tmp_path / "repo"
    (repo / "sub").mkdir(parents=True)
    store = tmp_path / "data" / "clean_full" / "census"
    store.mkdir(parents=True)
    l1, l2 = tmp_path / "looks_ok_1", tmp_path / "looks_ok_2"
    if not (_dir_link(l1, repo / "sub") and _dir_link(l2, store)):
        pytest.skip("cannot create a directory symlink or junction on this machine")
    with pytest.raises(F.Refused, match="inside the repository"):
        F.check_dest(str(l1), temp_roots=[str(tmp_path)], repo_root=str(repo))
    with pytest.raises(F.Refused, match="clean_full"):
        F.check_dest(str(l2), temp_roots=[str(tmp_path)], repo_root=str(repo))
    assert F.check_dest(str(tmp_path / "plain"), temp_roots=[str(tmp_path)], repo_root=str(repo))   # control


def test_main_refuses_a_bad_destination_before_any_client(tmp_path, capsys):
    calls = []
    rc = F.main(["--bucket", "b", "--ids-file", _ids(tmp_path, [ID_PLAIN]), "--dest", r"D:\temp\x"],
                client_factory=lambda: calls.append(1))
    assert rc == F.EXIT_REFUSED and calls == []


# ---- 5. what is written, and the checks ---------------------------------------------------------------
def test_control_plain_and_gzip_objects_are_written_and_checked(tmp_path, capsys):
    fake = FakeS3(_objects())
    rc, _, dest = _main(tmp_path, [ID_PLAIN, ID_GZ], fake)
    assert rc == F.EXIT_OK, capsys.readouterr().out
    np, ng = KEY_PLAIN.split("/")[1], KEY_GZ.split("/")[1]
    assert open(os.path.join(dest, "raw", np), "rb").read() == CSV
    assert open(os.path.join(dest, "csv", np), "rb").read() == CSV
    assert open(os.path.join(dest, "raw", ng), "rb").read() == _gz(CSV)          # stored bytes, untouched
    assert open(os.path.join(dest, "csv", ng), "rb").read() == CSV
    man = open(os.path.join(dest, "manifest.tsv")).read()
    assert f"{ID_GZ}\t{KEY_GZ}\t200\t" in man and "\tgzip\t" in man
    assert man.count("\tok\t") >= 2 and "# total\tobjects=2\tok=2\tfailed=0" in man


def test_an_etag_mismatch_is_a_failure(tmp_path):
    fake = FakeS3({KEY_PLAIN: {"body": CSV, "etag": "0" * 32}})
    rc, _, dest = _main(tmp_path, [ID_PLAIN], fake)
    assert rc == F.EXIT_FAILED_ROWS and "200:check_failed" in open(os.path.join(dest, "manifest.tsv")).read()


def test_a_multipart_etag_is_unverifiable_and_a_failure(tmp_path):
    fake = FakeS3({KEY_PLAIN: {"body": CSV, "etag": "abc-2"}})
    rc, _, dest = _main(tmp_path, [ID_PLAIN], fake)
    assert rc == F.EXIT_FAILED_ROWS and "\tunverifiable\t" in open(os.path.join(dest, "manifest.tsv")).read()


def test_a_csvmd5_mismatch_is_a_failure(tmp_path):
    fake = FakeS3({KEY_GZ: {"body": _gz(CSV), "enc": "gzip", "meta": {"csvmd5": "f" * 32}}})
    rc, _, dest = _main(tmp_path, [ID_GZ], fake)
    assert rc == F.EXIT_FAILED_ROWS and "\tMISMATCH" in open(os.path.join(dest, "manifest.tsv")).read()


@pytest.mark.parametrize("obj,status", [
    ({"body": b"not gzip", "enc": "gzip"}, "200:gzip_unreadable"),
    ({"body": CSV, "enc": "br"}, "200:encoding:br"),
    ({"body": CSV, "clen": len(CSV) + 5}, "200:short_body:"),
])
def test_a_body_that_cannot_be_trusted_is_a_failure(tmp_path, obj, status):
    fake = FakeS3({KEY_PLAIN: obj})
    rc, _, dest = _main(tmp_path, [ID_PLAIN], fake)
    assert rc == F.EXIT_FAILED_ROWS and f"\t{status}" in open(os.path.join(dest, "manifest.tsv")).read()


def test_a_404_and_an_error_are_failure_rows_never_skips(tmp_path, capsys):
    fake = FakeS3({KEY_PLAIN: {"body": CSV}},
                  errors={KEY_GZ: EndpointConnectionError(endpoint_url=f"{PLANT}/econ-data")})
    rc, _, dest = _main(tmp_path, [ID_PLAIN, ID_GZ, "census:missing"], fake)
    printed = capsys.readouterr().out
    man = open(os.path.join(dest, "manifest.tsv")).read()
    assert rc == F.EXIT_FAILED_ROWS
    assert f"{KEY_GZ}\terr:EndpointConnectionError\t" in man and "\t404\t" in man
    assert "cloudflarestorage" not in printed and "cloudflarestorage" not in man
    assert "# total\tobjects=3\tok=1\tfailed=2" in man


def test_a_client_build_failure_is_reduced_to_a_code(tmp_path, capsys):
    def bad_factory():
        raise RuntimeError(f"cannot reach {PLANT}")
    rc = F.main(["--bucket", "b", "--ids-file", _ids(tmp_path, [ID_PLAIN]), "--dest", str(tmp_path / "o")],
                client_factory=bad_factory, dest_guard=lambda p: p)
    printed = capsys.readouterr().out
    assert rc == F.EXIT_REFUSED and "cloudflarestorage" not in printed and "FAILED: RuntimeError" in printed


def test_a_gzip_bomb_is_bounded(monkeypatch):
    monkeypatch.setattr(F, "MAX_DECODED_BYTES", 100)
    with pytest.raises(ValueError):
        F._gunzip_bounded(_gz(b"a" * 1000), F.MAX_DECODED_BYTES)
    assert F._gunzip_bounded(_gz(b"a" * 50), 100) == b"a" * 50                  # control


_OWN = {"Refused", "WriteRefused", "_OverCap", "_RowFail"}


def _message_leaks(src):
    """Handlers that put an exception's TEXT into a string while catching something other than the tool's own
    exceptions (the same scan as tests/test_r2_object_census.py, over this tool's own exception names)."""
    import ast
    bad, handlers = [], 0
    for h in ast.walk(ast.parse(src)):
        if not isinstance(h, ast.ExceptHandler) or not h.name:
            continue
        handlers += 1
        caught = {n.id for n in ast.walk(h.type) if isinstance(n, ast.Name)} if h.type else {"BaseException"}
        for node in ast.walk(ast.Module(body=h.body, type_ignores=[])):
            used = (isinstance(node, ast.FormattedValue) and isinstance(node.value, ast.Name)
                    and node.value.id == h.name) or \
                   (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id in ("str", "repr", "format")
                    and any(isinstance(a, ast.Name) and a.id == h.name for a in node.args))
            if used and not caught <= _OWN:
                bad.append(sorted(caught))
    return bad, handlers


def test_the_tool_source_never_prints_a_foreign_exception_message():
    with open(F.__file__, encoding="utf-8") as fh:
        bad, handlers = _message_leaks(fh.read())
    assert handlers >= 4 and bad == []


def test_control_the_message_scan_catches_a_planted_leak():
    bad, handlers = _message_leaks("try:\n    pass\nexcept Exception as e:\n    print(f'{e}')\n"
                                   "try:\n    pass\nexcept _RowFail as e:\n    print(f'{e}')\n")
    assert handlers == 2 and bad == [["Exception"]]
