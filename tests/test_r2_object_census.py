"""tools/r2_object_census.py against a stubbed client - never the network.

Each guard is shown to FIRE on a planted bad input and to STAND DOWN on its negative control, so a guard
that passes everything (or refuses everything) fails here.
"""
import datetime as dt
import ntpath
import os
import sys

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import r2_object_census as C  # noqa: E402

PLANT = "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"
UTC = dt.timezone.utc
T1 = dt.datetime(2026, 10, 28, 20, 3, 34, tzinfo=UTC)


def _client_error(status, code, message="m", op="HeadObject"):
    return ClientError({"Error": {"Code": code, "Message": message},
                        "ResponseMetadata": {"HTTPStatusCode": status}}, op)


class FakeS3:
    """list_objects_v2 / head_object over an in-memory dict; every call is logged. Write methods exist so a
    test can prove the WRAPPER, not a missing attribute, is what refuses them."""

    def __init__(self, objects=None, head_errors=None, list_error=None, truncate_without_token=False):
        self.objects = dict(objects or {})        # key -> dict(Size, LastModified, ETag, Metadata, ContentEncoding)
        self.head_errors = dict(head_errors or {})
        self.list_error = list_error
        self.truncate_without_token = truncate_without_token
        self.log = []

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        self.log.append(("list", ContinuationToken))
        if self.list_error is not None:
            raise self.list_error
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        start = int(ContinuationToken or 0)
        page = keys[start:start + MaxKeys]
        more = start + MaxKeys < len(keys)
        out = {"Contents": [{"Key": k, "Size": self.objects[k]["Size"],
                             "LastModified": self.objects[k]["LastModified"],
                             "ETag": self.objects[k]["ETag"]} for k in page],
               "IsTruncated": more or self.truncate_without_token, "KeyCount": len(page)}
        if more:
            out["NextContinuationToken"] = str(start + MaxKeys)
        return out

    def head_object(self, Bucket, Key):
        self.log.append(("head", Key))
        if Key in self.head_errors:
            raise self.head_errors[Key]
        if Key not in self.objects:
            raise _client_error(404, "404", "Not Found")
        o = self.objects[Key]
        r = {"ETag": o["ETag"], "ContentLength": o["Size"], "LastModified": o["LastModified"],
             "Metadata": o.get("Metadata", {})}
        if o.get("ContentEncoding"):
            r["ContentEncoding"] = o["ContentEncoding"]
        return r

    def put_object(self, **kw):
        self.log.append(("put", kw.get("Key")))

    def delete_object(self, **kw):
        self.log.append(("delete", kw.get("Key")))

    def copy_object(self, **kw):
        self.log.append(("copy", kw.get("Key")))

    def get_object(self, **kw):
        self.log.append(("get", kw.get("Key")))


def _obj(size=10, lm=T1, etag='"abc"', **kw):
    return {"Size": size, "LastModified": lm, "ETag": etag, **kw}


def _ro(fake):
    return C.ReadOnlyClient(fake, allow_unhooked=True)


def _run(argv, fake, capsys):
    calls = []

    def factory():
        calls.append(1)
        return _ro(fake)
    rc = C.main(argv, client_factory=factory)
    return rc, calls, capsys.readouterr().out


# ---- 1. write refusal ----------------------------------------------------------------------------------------
@pytest.mark.parametrize("name", ["put_object", "delete_object", "copy_object", "get_object", "delete_objects",
                                  "upload_file", "upload_fileobj", "create_multipart_upload", "get_paginator",
                                  "generate_presigned_url", "put_object_acl"])
def test_the_wrapper_refuses_every_write_and_the_body_get(name):
    fake = FakeS3({"series/a.csv": _obj()})
    with pytest.raises(C.WriteRefused):
        getattr(_ro(fake), name)(Bucket="b", Key="series/a.csv", Body=b"x")
    assert fake.log == []


def test_control_reads_pass_through_the_wrapper():
    fake = FakeS3({"series/a.csv": _obj()})
    ro = _ro(fake)
    assert ro.list_objects_v2(Bucket="b", Prefix="series/")["Contents"][0]["Key"] == "series/a.csv"
    assert ro.head_object(Bucket="b", Key="series/a.csv")["ContentLength"] == 10
    assert fake.log == [("list", None), ("head", "series/a.csv")]


def test_the_wrapped_client_cannot_be_swapped():
    ro = _ro(FakeS3())
    with pytest.raises(C.WriteRefused):
        ro._client = FakeS3()


def test_a_client_without_events_is_refused_unless_a_stub():
    with pytest.raises(C.WriteRefused):
        C.ReadOnlyClient(FakeS3())
    C.ReadOnlyClient(FakeS3(), allow_unhooked=True)        # control: the tests' stub path


def test_no_body_get_is_allowed():
    assert C.ALLOWED_OPERATIONS == {"ListObjectsV2", "HeadObject"}


class _Sent(Exception):
    pass


@pytest.fixture
def real_client(tmp_path, monkeypatch):
    """A real boto3 client with fake credentials. A before-sign hook records what would be signed; a before-send
    hook stands in for the wire and raises, so nothing leaves the machine."""
    import boto3
    from botocore.config import Config
    from core import cutover, r2_util
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER"))   # T0 must not be what refuses
    c = r2_util.guard_client(boto3.client("s3", endpoint_url="https://example.invalid",
                                          aws_access_key_id="test", aws_secret_access_key="test",
                                          config=Config(region_name="auto", retries={"max_attempts": 0})))
    signed = []

    def before_sign(operation_name=None, **_kw):
        signed.append(operation_name)

    def before_send(request, **_kw):
        raise _Sent(request.method)
    c.meta.events.register("before-sign.s3", before_sign)
    c.meta.events.register("before-send.s3", before_send)
    return C.ReadOnlyClient(c), signed


@pytest.mark.parametrize("op,kw", [
    ("put_object", {"Bucket": "b", "Key": "k", "Body": b"x"}),
    ("delete_object", {"Bucket": "b", "Key": "k"}),
    ("copy_object", {"Bucket": "b", "Key": "k", "CopySource": {"Bucket": "b", "Key": "j"}}),
    ("get_object", {"Bucket": "b", "Key": "k"}),
])
def test_a_real_client_refuses_writes_and_gets_before_signing(real_client, op, kw):
    ro, signed = real_client
    with pytest.raises(C.WriteRefused):
        getattr(ro, op)(**kw)                            # lock 1: the wrapper
    with pytest.raises(C.WriteRefused):
        getattr(ro._client, op)(**kw)                    # lock 2: the before-call hook on the raw client
    assert signed == []


def test_control_a_real_client_signs_and_sends_list_and_head(real_client):
    ro, signed = real_client
    with pytest.raises(_Sent):
        ro.list_objects_v2(Bucket="b", Prefix="series/")
    with pytest.raises(_Sent):
        ro.head_object(Bucket="b", Key="series/a.csv")
    assert signed == ["ListObjectsV2", "HeadObject"]


def test_after_t0_the_real_client_is_refused_before_credentials_load(tmp_path, monkeypatch, capsys):
    from core import cutover, r2_util
    flag = tmp_path / "CUTOVER"
    flag.write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    loaded = []
    monkeypatch.setattr(r2_util, "client", lambda **kw: loaded.append(kw) or FakeS3())
    with pytest.raises(cutover.CutoverRefused):
        C.open_client()
    assert loaded == []
    keys = tmp_path / "keys.txt"
    keys.write_text("series/a.csv\n")
    rc = C.main(["head", "--bucket", "b", "--keys-file", str(keys), "--out", str(tmp_path / "h.tsv")])
    assert rc == C.EXIT_REFUSED and "CutoverRefused" in capsys.readouterr().out and loaded == []
    assert not (tmp_path / "h.tsv").exists() and not (tmp_path / "h.tsv.partial").exists()


def test_control_before_t0_the_real_client_loads_read_credentials(tmp_path, monkeypatch):
    from core import cutover, r2_util
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER"))
    loaded = []

    class _Events:
        def register_first(self, *a, **k):
            loaded.append("hook")

    class _Meta:
        events = _Events()

    class _Client:
        meta = _Meta()
    monkeypatch.setattr(r2_util, "client", lambda **kw: loaded.append(kw) or _Client())
    assert isinstance(C.open_client(), C.ReadOnlyClient)
    assert loaded == [{"write": False}, "hook", "hook"]          # read creds, then the two before-call locks


def _wire_recorders(raw):
    """before-sign records what would be signed; before-send stands in for the wire and raises."""
    signed = []
    raw.meta.events.register("before-sign.s3", lambda operation_name=None, **_kw: signed.append(operation_name))

    def before_send(request, **_kw):
        raise _Sent(request.method)
    raw.meta.events.register("before-send.s3", before_send)
    return signed


def test_open_client_is_the_read_client_behind_both_locks(tmp_path, monkeypatch):
    """THE PRODUCTION PATH (AR-399 D1): the real open_client() over the real r2_util.client(), with only the
    credentials faked. r2_util.creds(write=False) falls back to the WRITE keys when the read keys are absent,
    so the client the tool holds can be write-capable and the two locks are the barrier - test them here."""
    from core import cutover, r2_util
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER"))
    asked = []

    def fake_creds(write=False):
        asked.append(write)
        return {"endpoint": "https://example.invalid", "key": "test", "secret": "test",
                "mode": "write" if write else "read"}
    monkeypatch.setattr(r2_util, "creds", fake_creds)
    ro = C.open_client()
    assert isinstance(ro, C.ReadOnlyClient)
    assert asked == [False]                                      # the READ credentials were asked for
    raw = ro._client
    signed = _wire_recorders(raw)
    for op, kw in [("put_object", {"Bucket": "b", "Key": "k", "Body": b"x"}),
                   ("get_object", {"Bucket": "b", "Key": "k"}),
                   ("delete_object", {"Bucket": "b", "Key": "k"})]:
        with pytest.raises(C.WriteRefused):
            getattr(ro, op)(**kw)
        with pytest.raises(C.WriteRefused):
            getattr(raw, op)(**kw)
    assert signed == []
    with pytest.raises(_Sent):                                   # control: a HEAD is signed and would be sent
        ro.head_object(Bucket="b", Key="k")
    assert signed[:1] == ["HeadObject"]


def test_the_hook_runs_before_any_other_before_call_responder(tmp_path, monkeypatch):
    """register_first on the wildcard, not register and not the s3 name alone: a handler that ANSWERS
    before-call on 'before-call.*.*' (botocore's Stubber is one) otherwise replies to a put before the lock is
    asked - measured: with only register_first('before-call.s3') this test failed."""
    import boto3
    from botocore.awsrequest import AWSResponse
    from botocore.config import Config
    from core import cutover, r2_util
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER"))
    raw = r2_util.guard_client(boto3.client("s3", endpoint_url="https://example.invalid",
                                            aws_access_key_id="test", aws_secret_access_key="test",
                                            config=Config(region_name="auto", retries={"max_attempts": 0})))
    answered = []

    def responder(model=None, **_kw):                            # answers any call, as Stubber's handler does
        answered.append(model.name)
        return AWSResponse("https://example.invalid", 200, {}, None), {"IsTruncated": False}
    raw.meta.events.register("before-call.*.*", responder)       # registered BEFORE the tool wraps the client
    ro = C.ReadOnlyClient(raw)
    for _ in range(2):
        with pytest.raises(C.WriteRefused):
            raw.put_object(Bucket="b", Key="k", Body=b"x")
    assert answered == []
    # control: the responder is live and answers an allowed call, so it would have answered the put
    assert ro.list_objects_v2(Bucket="b", Prefix="p")["IsTruncated"] is False
    assert answered == ["ListObjectsV2"]


# ---- 2. output-path guard ------------------------------------------------------------------------------------
REPO_W = r"F:\claude_wt\econ_r2census"


@pytest.mark.parametrize("path", [r"D:\temp\out.tsv", r"E:\research\out.tsv", r"d:\x.tsv", r"E:/x.tsv",
                                  r"F:\claude_wt\econ_r2census\out.tsv",
                                  r"F:\claude_wt\econ_r2census\tools\out.tsv",
                                  r"C:\Users\x\out.tsv", "out.tsv", r"F:out.tsv",
                                  r"C:\Temp\..\Windows\out.tsv", ""])
def test_the_output_guard_refuses(path):
    with pytest.raises(C.Refused):
        C.check_out_path(path, pathmod=ntpath, temp_roots=[r"C:\Temp"], repo_root=REPO_W, check_fs=False)


@pytest.mark.parametrize("path", [r"F:\work\gfs_csv_debt\census.tsv", r"F:\tmp\x.tsv", r"C:\Temp\sub\x.tsv",
                                  r"F:\claude_wt\econ_r2census_other\x.tsv"])
def test_control_the_output_guard_allows_f_and_temp(path):
    assert C.check_out_path(path, pathmod=ntpath, temp_roots=[r"C:\Temp"], repo_root=REPO_W, check_fs=False)


def test_a_temp_root_on_d_is_still_refused():
    with pytest.raises(C.Refused):
        C.check_out_path(r"D:\temp\claude\x.tsv", pathmod=ntpath, temp_roots=[r"D:\temp"], repo_root=REPO_W,
                         check_fs=False)


def _dir_link(link, target):
    """A directory link: a symlink, or on Windows without the symlink privilege a junction (no privilege
    needed). Returns False when neither can be made."""
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


def test_a_link_that_points_into_a_refused_tree_is_refused(tmp_path):
    """AR-399 D2: the guard judges where the path LEADS (realpath), so an allowed-looking link into the repo
    tree (or, on the desktop, an F: link to D:) is refused."""
    repo = tmp_path / "repo"
    repo.mkdir()
    link = tmp_path / "looks_allowed"
    if not _dir_link(link, repo):
        pytest.skip("cannot create a directory symlink or junction on this machine")
    with pytest.raises(C.Refused, match="inside the repository"):
        C.check_out_path(str(link / "out.tsv"), temp_roots=[str(tmp_path)], repo_root=str(repo))
    # control: the same tree reached by a plain path is allowed when nothing is a link into the repo
    assert C.check_out_path(str(tmp_path / "plain.tsv"), temp_roots=[str(tmp_path)], repo_root=str(repo))


def test_the_real_guard_refuses_an_existing_file_and_a_missing_parent(tmp_path):
    existing = tmp_path / "there.tsv"
    existing.write_text("x")
    with pytest.raises(C.Refused):
        C.check_out_path(str(existing), temp_roots=[str(tmp_path)], repo_root="")
    with pytest.raises(C.Refused):
        C.check_out_path(str(tmp_path / "nope" / "x.tsv"), temp_roots=[str(tmp_path)], repo_root="")
    assert C.check_out_path(str(tmp_path / "new.tsv"), temp_roots=[str(tmp_path)], repo_root="")   # control


def test_main_refuses_a_d_path_before_building_a_client(tmp_path, capsys):
    keys = tmp_path / "keys.txt"
    keys.write_text("series/a.csv\n")
    fake = FakeS3({"series/a.csv": _obj()})
    calls = []

    def factory():
        calls.append(1)
        return _ro(fake)

    def nt_guard(p):
        return C.check_out_path(p, pathmod=ntpath, temp_roots=[], repo_root=REPO_W, check_fs=False)
    rc = C.main(["head", "--bucket", "b", "--keys-file", str(keys), "--out", r"D:\x.tsv"],
                client_factory=factory, out_guard=nt_guard)
    assert rc == C.EXIT_REFUSED and calls == [] and fake.log == []
    # control: a good path goes on to build the client (the default, real guard on a temp path)
    out = tmp_path / "ok.tsv"
    rc = C.main(["head", "--bucket", "b", "--keys-file", str(keys), "--out", str(out)], client_factory=factory)
    assert rc == C.EXIT_OK and calls == [1] and out.exists()


# ---- 3. ops cap ----------------------------------------------------------------------------------------------
def test_head_over_the_cap_is_refused_before_any_request(tmp_path, capsys):
    keys = tmp_path / "keys.txt"
    keys.write_text("series/a.csv\nseries/b.csv\nseries/c.csv\nseries/a.csv\n")    # 3 distinct
    fake = FakeS3({"series/a.csv": _obj()})
    rc, calls, out = _run(["head", "--bucket", "b", "--keys-file", str(keys), "--out",
                           str(tmp_path / "h.tsv"), "--max-ops", "2"], fake, capsys)
    assert rc == C.EXIT_CAP and calls == [] and fake.log == [] and not (tmp_path / "h.tsv").exists()
    assert "planned: HEAD" in out and "3 Class B" in out
    rc, calls, _ = _run(["head", "--bucket", "b", "--keys-file", str(keys), "--out",
                         str(tmp_path / "h2.tsv"), "--max-ops", "3"], fake, capsys)              # control
    assert rc == C.EXIT_OK and calls == [1] and len([x for x in fake.log if x[0] == "head"]) == 3


def _many(n, prefix="series/s%3A"):
    return {f"{prefix}{i:05d}.csv": _obj() for i in range(n)}


def test_list_stops_void_at_the_cap(tmp_path, capsys):
    fake = FakeS3(_many(2500))                                                              # 3 pages
    rc, _c, out = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--out",
                        str(tmp_path / "l.tsv"), "--max-ops", "2"], fake, capsys)
    assert rc == C.EXIT_CAP and "VOID" in out and not (tmp_path / "l.tsv").exists()
    assert len(fake.log) == 2                                                               # page 3 never asked
    rc, _c, out = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--out",
                        str(tmp_path / "l2.tsv"), "--max-ops", "3"], fake, capsys)           # control
    assert rc == C.EXIT_OK and "pages=3 objects=2500" in out


def test_list_ids_floor_over_the_cap_is_refused_before_the_client(tmp_path, capsys):
    ids = tmp_path / "ids.txt"
    ids.write_text("".join(f"s:{i}\n" for i in range(2001)))                               # >= 3 pages
    fake = FakeS3()
    rc, calls, _ = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--ids-file", str(ids),
                         "--out", str(tmp_path / "l.tsv"), "--max-ops", "2"], fake, capsys)
    assert rc == C.EXIT_CAP and calls == []
    rc, calls, _ = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--ids-file", str(ids),
                         "--out", str(tmp_path / "l2.tsv"), "--max-ops", "3"], fake, capsys)  # control
    assert rc == C.EXIT_OK and calls == [1]


def test_help_names_class_a_and_class_b(capsys):
    with pytest.raises(SystemExit):
        C.main(["--help"])
    out = capsys.readouterr().out
    assert "LIST page" in out and "Class A" in out and "HEAD is a Class B" in out and "2,000" in out
    assert "not HTTP requests" in out                            # AR-399 D3: the bound is stated honestly


# ---- 4. redaction --------------------------------------------------------------------------------------------
def _planted_errors():
    return [EndpointConnectionError(endpoint_url=f"{PLANT}/econ-data/series/a.csv"),
            Exception(f"Could not connect to the endpoint URL: {PLANT}/econ-data"),
            _client_error(500, f"{PLANT}/boom", message=f"see {PLANT}"),
            _client_error(None, f"x {PLANT}", message=PLANT)]


@pytest.mark.parametrize("exc", _planted_errors())
def test_error_codes_never_carry_the_endpoint(exc):
    assert PLANT in str(exc) or PLANT in repr(getattr(exc, "response", ""))    # the plant is real
    code = C.error_code(exc)
    assert "cloudflarestorage" not in code and "://" not in code and code


def test_control_error_codes_keep_safe_tokens():
    assert C.error_code(_client_error(503, "SlowDown")) == "SlowDown"
    assert C.error_code(_client_error(403, "AccessDenied")) == "AccessDenied"
    assert C.error_code(EndpointConnectionError(endpoint_url=PLANT)) == "EndpointConnectionError"
    assert C.error_code(_client_error(500, f"{PLANT}")) == "500"


def test_a_planted_endpoint_never_reaches_output_or_file(tmp_path, capsys):
    keys = tmp_path / "keys.txt"
    keys.write_text("series/a.csv\nseries/bad.csv\n")
    fake = FakeS3({"series/a.csv": _obj()},
                  head_errors={"series/bad.csv": EndpointConnectionError(endpoint_url=f"{PLANT}/econ-data")})
    out = tmp_path / "h.tsv"
    rc, _c, printed = _run(["head", "--bucket", "b", "--keys-file", str(keys), "--out", str(out)], fake, capsys)
    body = out.read_text()
    assert rc == C.EXIT_ERROR_ROWS
    assert "cloudflarestorage" not in printed and "cloudflarestorage" not in body
    assert "series/bad.csv\terr:EndpointConnectionError\t" in body


def test_a_planted_endpoint_in_a_list_failure_or_client_build_is_reduced(tmp_path, capsys):
    fake = FakeS3(list_error=EndpointConnectionError(endpoint_url=PLANT))
    rc, _c, printed = _run(["list", "--bucket", "b", "--prefix", "series/x", "--out",
                            str(tmp_path / "l.tsv")], fake, capsys)
    assert rc == C.EXIT_VOID and "cloudflarestorage" not in printed and "EndpointConnectionError" in printed

    def bad_factory():
        raise RuntimeError(f"cannot reach {PLANT}")
    keys = tmp_path / "keys.txt"
    keys.write_text("series/a.csv\n")
    rc = C.main(["head", "--bucket", "b", "--keys-file", str(keys), "--out", str(tmp_path / "h.tsv")],
                client_factory=bad_factory)
    printed = capsys.readouterr().out
    assert rc == C.EXIT_REFUSED and "cloudflarestorage" not in printed and "FAILED: RuntimeError" in printed


_OWN = {"Refused", "VoidListing", "WriteRefused"}


def _message_leaks(src):
    """Handlers that put an exception's TEXT into a string (f"{e}", str(e), repr(e), format(e)) while catching
    something other than the tool's own exceptions, whose messages it writes itself."""
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
    with open(C.__file__, encoding="utf-8") as fh:
        bad, handlers = _message_leaks(fh.read())
    assert handlers >= 5, "found too few handlers - the scan cannot be a check"
    assert bad == []


def test_control_the_message_scan_catches_a_planted_leak():
    planted = ("try:\n    pass\nexcept Exception as e:\n    x = 1\n    print(f'failed {e}')\n"
               "try:\n    pass\nexcept (OSError, ValueError) as err:\n    msg = str(err)\n"
               "try:\n    pass\nexcept Refused as e:\n    print(f'{e}')\n")
    bad, handlers = _message_leaks(planted)
    assert handlers == 3 and bad == [["Exception"], ["OSError", "ValueError"]]


# ---- 5. TSV format -------------------------------------------------------------------------------------------
def test_list_tsv_format(tmp_path, capsys):
    plus2 = dt.timezone(dt.timedelta(hours=2))
    fake = FakeS3({"series/s%3Aa.csv": _obj(5, T1, '"e1"'),
                   "series/s%3Ab.csv": _obj(7, dt.datetime(2026, 8, 2, 3, 0, 0, tzinfo=plus2), '"e2-3"'),
                   "series/t%3Ac.csv": _obj(9)})
    out = tmp_path / "l.tsv"
    rc, _c, printed = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--out", str(out)], fake, capsys)
    assert rc == C.EXIT_OK
    assert out.read_text() == ("#\tkey\tsize\tlast_modified\tetag\n"
                               "series/s%3Aa.csv\t5\t2026-10-28T20:03:34Z\te1\n"
                               "series/s%3Ab.csv\t7\t2026-08-02T01:00:00Z\te2-3\n"
                               "# total\tpages=1\tobjects=2\tbytes=12\n")
    assert "pages=1 objects=2 bytes=12" in printed


def test_list_with_ids_marks_listed_and_names_absent(tmp_path, capsys):
    fake = FakeS3({"series/s%3AA.B.csv": _obj(), "series/s%3Aother.csv": _obj()})
    ids = tmp_path / "ids.txt"
    ids.write_text("# a comment\ns:A.B\ns:ZZZ.NOPE\n")
    out = tmp_path / "l.tsv"
    rc, _c, printed = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--ids-file", str(ids),
                            "--out", str(out)], fake, capsys)
    assert rc == C.EXIT_OK
    lines = out.read_text().splitlines()
    assert lines[0] == "#\tkey\tsize\tlast_modified\tetag\tin_ids"
    assert lines[1].endswith("\t1") and lines[1].startswith("series/s%3AA.B.csv\t")
    assert lines[2].endswith("\t0") and lines[2].startswith("series/s%3Aother.csv\t")
    assert "# ids\tgiven=2\tlisted=1\tabsent=1" in lines and "# absent\ts:ZZZ.NOPE" in lines
    assert "absent=1" in printed


def test_an_id_outside_the_prefix_is_refused(tmp_path, capsys):
    ids = tmp_path / "ids.txt"
    ids.write_text("other:A\n")
    rc, calls, _ = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--ids-file", str(ids),
                         "--out", str(tmp_path / "l.tsv")], FakeS3(), capsys)
    assert rc == C.EXIT_REFUSED and calls == [] and not (tmp_path / "l.tsv").exists()


def test_id_to_key_uses_the_writer_layout():
    from tools.derive_csv_bulk import csv_key
    assert C.id_to_key("imf_gfsbs_direct:GFS_BS:ZZZ.NOPE") == csv_key("series", "imf_gfsbs_direct", "GFS_BS:ZZZ.NOPE")
    assert C.id_to_key("imf_gfsbs_direct:GFS_BS:ZZZ.NOPE") == "series/imf_gfsbs_direct%3AGFS_BS%3AZZZ.NOPE.csv"


def test_head_tsv_format(tmp_path, capsys):
    fake = FakeS3({"series/gz.csv": _obj(11, T1, '"g1"', Metadata={"CsvMD5": "d41d8"}, ContentEncoding="gzip"),
                   "series/plain.csv": _obj(12, dt.datetime(2026, 8, 2, 0, 0, 0, tzinfo=UTC), '"p1"')})
    keys = tmp_path / "keys.txt"
    keys.write_text("series/gz.csv\nseries/plain.csv\nseries/missing.csv\n")
    out = tmp_path / "h.tsv"
    rc, _c, printed = _run(["head", "--bucket", "b", "--keys-file", str(keys), "--out", str(out)], fake, capsys)
    assert rc == C.EXIT_OK
    assert out.read_text() == (
        "#\tkey\tstatus\tetag\tcontent_length\tlast_modified\tcsvmd5\tcontent_encoding\n"
        "series/gz.csv\t200\tg1\t11\t2026-10-28T20:03:34Z\td41d8\tgzip\n"
        "series/plain.csv\t200\tp1\t12\t2026-08-02T00:00:00Z\t\t\n"
        "series/missing.csv\t404\t\t\t\t\t\n"
        "# total\tkeys=3\t200=2\t404=1\terror=0\n")
    assert "200=2 404=1 error=0" in printed


def test_a_field_with_a_tab_is_refused():
    with pytest.raises(ValueError):
        C.tsv_line(["a\tb"])
    assert C.tsv_line(["a", 1, None]) == "a\t1\t\n"                                       # control


# ---- 6. VOID listings and error rows -------------------------------------------------------------------------
def test_a_listing_that_read_zero_pages_is_void(tmp_path, capsys):
    fake = FakeS3(list_error=_client_error(503, "SlowDown", op="ListObjectsV2"))
    out = tmp_path / "l.tsv"
    rc, _c, printed = _run(["list", "--bucket", "b", "--prefix", "series/x", "--out", str(out)], fake, capsys)
    assert rc == C.EXIT_VOID and "VOID" in printed and "0 page(s) read" in printed and not out.exists()


def test_control_an_empty_listing_that_read_a_page_is_a_result(tmp_path, capsys):
    out = tmp_path / "l.tsv"
    rc, _c, printed = _run(["list", "--bucket", "b", "--prefix", "series/zz_no_such_source%3A", "--out", str(out)],
                           FakeS3({"series/a.csv": _obj()}), capsys)
    assert rc == C.EXIT_OK and "pages=1 objects=0" in printed
    assert out.read_text().splitlines()[-1] == "# total\tpages=1\tobjects=0\tbytes=0"


def test_a_page_failing_mid_listing_is_void(tmp_path, capsys):
    fake = FakeS3(_many(1500))
    orig = fake.list_objects_v2

    def flaky(**kw):
        if kw.get("ContinuationToken"):
            raise _client_error(500, "InternalError", op="ListObjectsV2")
        return orig(**kw)
    fake.list_objects_v2 = flaky
    out = tmp_path / "l.tsv"
    rc, _c, printed = _run(["list", "--bucket", "b", "--prefix", "series/s%3A", "--out", str(out)], fake, capsys)
    assert rc == C.EXIT_VOID and "1 page(s) read" in printed and not out.exists()


def test_a_truncated_page_without_a_token_is_void(tmp_path, capsys):
    out = tmp_path / "l.tsv"
    rc, _c, printed = _run(["list", "--bucket", "b", "--prefix", "series/", "--out", str(out)],
                           FakeS3({"series/a.csv": _obj()}, truncate_without_token=True), capsys)
    assert rc == C.EXIT_VOID and "without a continuation token" in printed and not out.exists()


@pytest.mark.parametrize("exc,code", [(_client_error(403, "AccessDenied"), "err:AccessDenied"),
                                      (_client_error(500, "InternalError"), "err:InternalError"),
                                      (_client_error(503, f"{PLANT}"), "err:503")])
def test_an_error_row_is_a_failure_never_a_skip(tmp_path, capsys, exc, code):
    keys = tmp_path / "keys.txt"
    keys.write_text("series/a.csv\nseries/e.csv\n")
    out = tmp_path / "h.tsv"
    rc, _c, printed = _run(["head", "--bucket", "b", "--keys-file", str(keys), "--out", str(out)],
                           FakeS3({"series/a.csv": _obj()}, head_errors={"series/e.csv": exc}), capsys)
    rows = out.read_text().splitlines()
    assert rc == C.EXIT_ERROR_ROWS and "FAILED" in printed and "error=1" in printed
    assert f"series/e.csv\t{code}\t\t\t\t\t" in rows                                      # the row is written


def test_control_200_and_404_only_exit_zero(tmp_path, capsys):
    keys = tmp_path / "keys.txt"
    keys.write_text("series/a.csv\nseries/zz.csv\n")
    rc, _c, printed = _run(["head", "--bucket", "b", "--keys-file", str(keys), "--out", str(tmp_path / "h.tsv")],
                           FakeS3({"series/a.csv": _obj()}), capsys)
    assert rc == C.EXIT_OK and "error=0" in printed and "FAILED" not in printed
