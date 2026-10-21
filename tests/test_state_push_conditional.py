"""The state push's compare-and-swap is a real one: the PUT itself is conditional (ledger R1197).

push_state() compares the remote ETag with the one recorded at --pull-state, then spends minutes on the
shrink guard, VACUUM INTO and zstd-9 before it uploads. Until R1197 that upload was a plain PUT, so a
writer that saved inside those minutes (the cloud updater-daily / updater-heavy, the workstation heavy
pass, a hand tool such as tools/clear_csv_desktop_owed.py) was overwritten without a word. The upload
is now If-Match <the compared ETag>, or If-None-Match '*' for the first seed, and R2 refuses it with
412 PreconditionFailed when the object moved.

The stand-in client below ENFORCES those headers the way R2 documents them, and can let "another
writer" save at the moment our PUT arrives - the window the old code left open. Every test here fails
against the old code: it sent no condition, so the stand-in let it overwrite the foreign save.
"""
from __future__ import annotations

import hashlib
import io
import os
import sqlite3
import sys
from types import SimpleNamespace

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from core import r2_util                       # noqa: E402
from updater import blob as blobmod            # noqa: E402
from updater import config                     # noqa: E402
from updater import run as runmod              # noqa: E402

BACKUPS = "_aqueduct/backups/"


def _md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def _client_error(code: str, status: int, op: str):
    from botocore.exceptions import ClientError
    return ClientError({"Error": {"Code": code, "Message": code},
                        "ResponseMetadata": {"HTTPStatusCode": status}}, op)


class ConditionalS3:
    """In-memory boto3-shaped client that evaluates If-Match / If-None-Match at the write, as R2 does
    (developers.cloudflare.com/r2/reference/consistency/ "Conditional writes"). ETag = md5 of the body,
    what R2 reports for a single-part PUT.

    before_put(key) runs as a PUT arrives and BEFORE its condition is evaluated: another writer that
    saves inside push_state's VACUUM + zstd window. after_put(key) runs after a PUT committed: another
    writer that saves between our PUT and any HEAD that follows it."""

    def __init__(self):
        self.objs: dict[str, bytes] = {}
        self.puts: list[tuple[str, dict]] = []      # (key, condition headers sent)
        self.before_put = None
        self.after_put = None
        self.land_then_412 = False      # next state PUT commits, then answers 412 (a retried attempt)
        self.no_etag = False            # PUT responses carry no ETag

    def _etag(self, key):
        return _md5(self.objs[key]) if key in self.objs else None

    def put_object(self, Bucket, Key, Body, **kw):  # noqa: N803 - boto3's names
        self.puts.append((Key, {k: v for k, v in kw.items() if k in ("IfMatch", "IfNoneMatch")}))
        if self.before_put:
            self.before_put(Key)
        cur = self._etag(Key)
        if "IfMatch" in kw and (cur is None or kw["IfMatch"].strip('"') != cur):
            raise _client_error("PreconditionFailed", 412, "PutObject")
        if kw.get("IfNoneMatch") == "*" and cur is not None:
            raise _client_error("PreconditionFailed", 412, "PutObject")
        self.objs[Key] = bytes(Body)
        etag = '"' + _md5(self.objs[Key]) + '"'
        if self.land_then_412 and Key == runmod.STATE_KEY:
            # botocore's re-send after a lost response: the first attempt committed, the retry's
            # condition now fails against our own object.
            self.land_then_412 = False
            raise _client_error("PreconditionFailed", 412, "PutObject")
        if self.after_put:
            self.after_put(Key)
        return {} if self.no_etag else {"ETag": etag}

    def get_object(self, Bucket, Key):              # noqa: N803
        if Key not in self.objs:
            raise _client_error("NoSuchKey", 404, "GetObject")
        return {"Body": io.BytesIO(self.objs[Key])}

    def head_object(self, Bucket, Key):             # noqa: N803
        if Key not in self.objs:
            raise _client_error("404", 404, "HeadObject")
        return {"ETag": '"' + self._etag(Key) + '"', "ContentLength": len(self.objs[Key])}

    def delete_object(self, Bucket, Key):           # noqa: N803
        self.objs.pop(Key, None)

    def get_paginator(self, name):
        objs = self.objs

        class _P:
            def paginate(self, Bucket, Prefix):     # noqa: N803
                yield {"Contents": [{"Key": k} for k in sorted(objs) if k.startswith(Prefix)]}
        return _P()

    def state_puts(self):
        return [cond for key, cond in self.puts if key == runmod.STATE_KEY]

    def backups(self):
        return sorted(k for k in self.objs if k.startswith(BACKUPS))


@pytest.fixture()
def cas(monkeypatch, tmp_path):
    fake = ConditionalS3()
    monkeypatch.setattr(r2_util, "client", lambda *a, **kw: fake)
    state_dir = str(tmp_path / "_aqueduct")
    state_db = os.path.join(state_dir, "state.db")
    monkeypatch.setattr(config, "STATE_DIR", state_dir)
    monkeypatch.setattr(config, "STATE_DB", state_db)
    monkeypatch.setattr(runmod, "ETAG_PATH", os.path.join(state_dir, ".state_etag"))
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    out = str(tmp_path / "step_output.txt")
    monkeypatch.setenv("GITHUB_OUTPUT", out)
    os.makedirs(state_dir, exist_ok=True)
    con = sqlite3.connect(state_db)
    con.execute("CREATE TABLE t(x)")
    con.execute("INSERT INTO t VALUES (42)")
    con.commit()
    con.close()
    return SimpleNamespace(s3=fake, state_db=state_db, out=out)


def _stored():
    if not os.path.exists(runmod.ETAG_PATH):
        return None
    with open(runmod.ETAG_PATH, encoding="utf-8") as f:
        return f.read().strip()


def _outputs(path):
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return dict(line.split("=", 1) for line in f.read().splitlines() if line)


def _touch_state(db):
    con = sqlite3.connect(db)
    con.execute("INSERT INTO t VALUES (43)")
    con.commit()
    con.close()


def _foreign_save_on_next_state_put(s3, body=b"another writer's state"):
    """Another writer saves the state at the moment our PUT arrives - after push_state's compare."""
    def hook(key):
        if key == runmod.STATE_KEY:
            s3.objs[key] = body
            s3.before_put = None
    s3.before_put = hook
    return body


# ---------------------------------------------------------------- the race the old code lost
def test_remote_changed_between_the_compare_and_the_put_is_refused(cas, capsys):
    assert runmod.push_state() == 0                        # seed: the ETag is recorded
    pulled = _stored()
    backups_before = cas.s3.backups()
    _touch_state(cas.state_db)
    foreign = _foreign_save_on_next_state_put(cas.s3)

    assert runmod.push_state() == 2, "a PUT refused by R2 is the CAS refusal: exit 2"
    assert cas.s3.objs[runmod.STATE_KEY] == foreign, "the other writer's save must survive"
    assert cas.s3.backups() == backups_before, "no backup is written when the push is refused"
    assert _stored() == pulled, "the recorded ETag must not move on a refused push"
    assert _outputs(cas.out) == {runmod.REFUSED_OUTPUT: runmod.REFUSED_ANOTHER_WRITER}
    err = capsys.readouterr().err
    assert "ABORT: compare-and-swap failed" in err and "NOT overwriting" in err
    assert cas.s3.state_puts()[-1] == {"IfMatch": f'"{pulled}"'}


def test_a_refused_push_is_refused_again_until_a_new_pull(cas):
    """After the refusal the stored ETag is still the old one, so the early compare now refuses too -
    the push cannot slip through on a second try without --pull-state."""
    assert runmod.push_state() == 0
    _foreign_save_on_next_state_put(cas.s3)
    assert runmod.push_state() == 2
    n_puts = len(cas.s3.puts)
    assert runmod.push_state() == 2
    assert len(cas.s3.puts) == n_puts, "the early compare refuses before any PUT"


# ---------------------------------------------------------------- the seed
def test_seed_uses_if_none_match(cas):
    assert runmod.push_state() == 0
    assert cas.s3.state_puts() == [{"IfNoneMatch": "*"}]
    assert _stored() == _md5(cas.s3.objs[runmod.STATE_KEY])


def test_seed_raced_by_another_creator_is_refused(cas):
    foreign = _foreign_save_on_next_state_put(cas.s3, b"someone else seeded first")
    assert runmod.push_state() == 2
    assert cas.s3.objs[runmod.STATE_KEY] == foreign
    assert cas.s3.backups() == []
    assert _stored() is None, "a refused seed records no ETag"
    assert _outputs(cas.out) == {}, "never pulled: refused the same way, not named for the mail"


# ---------------------------------------------------------------- the ordinary push
def test_normal_push_sends_if_match_of_the_stored_etag_and_records_the_new_one(cas):
    assert runmod.push_state() == 0
    pulled = _stored()
    _touch_state(cas.state_db)
    assert runmod.push_state() == 0
    new = _stored()
    assert cas.s3.state_puts()[-1] == {"IfMatch": f'"{pulled}"'}
    assert new == _md5(cas.s3.objs[runmod.STATE_KEY]) and new != pulled
    backup_puts = [cond for key, cond in cas.s3.puts if key.startswith(BACKUPS)]
    assert len(backup_puts) == 2 and all(c == {} for c in backup_puts), "the backup PUT stays unconditional"
    assert _outputs(cas.out) == {}


def test_the_recorded_etag_is_the_one_our_put_returned(cas):
    """A writer that saves right AFTER our PUT must not have its ETag recorded as ours - the next push
    would then pass the compare and overwrite it. The PUT response names our object exactly."""
    assert runmod.push_state() == 0
    _touch_state(cas.state_db)
    ours = {}

    def after(key):
        if key == runmod.STATE_KEY:
            ours["etag"] = _md5(cas.s3.objs[key])
            cas.s3.objs[key] = b"saved right after ours"
            cas.s3.after_put = None
    cas.s3.after_put = after
    assert runmod.push_state() == 0
    assert _stored() == ours["etag"]
    assert runmod.push_state() == 2, "the next push sees the foreign save and refuses"
    assert cas.s3.objs[runmod.STATE_KEY] == b"saved right after ours"


@pytest.mark.parametrize("seed", [True, False])
def test_a_412_over_our_own_landed_attempt_is_a_successful_push(cas, seed):
    """botocore re-sends a PUT whose response was lost; if the first attempt committed, the re-send's
    condition fails against OUR object. That push succeeded and must not read as "another writer won"
    (the daily's D1 syncs are gated on the push step succeeding)."""
    if not seed:
        assert runmod.push_state() == 0
        _touch_state(cas.state_db)
    n_backup_puts = sum(1 for key, _ in cas.s3.puts if key.startswith(BACKUPS))
    cas.s3.land_then_412 = True
    assert runmod.push_state() == 0
    assert _stored() == _md5(cas.s3.objs[runmod.STATE_KEY]), "our object's ETag is recorded"
    # pushes in the same SECOND share one backup key (the stamp is to the second, R1650), and these
    # run back to back, so count the PUTs and check what the newest backup holds
    assert sum(1 for key, _ in cas.s3.puts if key.startswith(BACKUPS)) == n_backup_puts + 1, \
        "the backup is written as for any push"
    assert cas.s3.objs[cas.s3.backups()[-1]] == cas.s3.objs[runmod.STATE_KEY]
    assert _outputs(cas.out) == {}
    _touch_state(cas.state_db)
    assert runmod.push_state() == 0, "and the next push passes its compare"


def test_no_etag_in_the_response_and_a_foreign_object_after_it_is_not_recorded(cas):
    assert runmod.push_state() == 0
    pulled = _stored()
    _touch_state(cas.state_db)
    cas.s3.no_etag = True

    def after(key):
        if key == runmod.STATE_KEY:
            cas.s3.objs[key] = b"saved right after ours"
            cas.s3.after_put = None
    cas.s3.after_put = after
    assert runmod.push_state() == 1
    assert _stored() == pulled, "the foreign ETag must not be recorded as ours"
    assert runmod.push_state() == 2, "so the next push refuses instead of overwriting"
    assert cas.s3.objs[runmod.STATE_KEY] == b"saved right after ours"


def test_no_etag_in_the_response_falls_back_to_the_head(cas):
    cas.s3.no_etag = True
    assert runmod.push_state() == 0
    assert _stored() == _md5(cas.s3.objs[runmod.STATE_KEY])


# ---------------------------------------------------------------- R2Blob.put_if on a real botocore client
def _real_client():
    import boto3
    return boto3.client("s3", region_name="auto", endpoint_url="https://stub.invalid",
                        aws_access_key_id="test-not-real", aws_secret_access_key="test-not-real")


def _blob_with(client):
    b = blobmod.R2Blob()
    b._client = client
    return b


def test_put_if_sends_headers_the_installed_botocore_accepts():
    """Stubber validates parameters against botocore's own PutObject model, so this fails if the
    botocore installed by requirements-updater.txt does not know IfMatch / IfNoneMatch."""
    from botocore.stub import Stubber
    c = _real_client()
    with Stubber(c) as st:
        st.add_response("put_object", {"ETag": '"new1"'},
                        {"Bucket": blobmod.R2_BUCKET, "Key": "_aqueduct/x.db.zst", "Body": b"a",
                         "IfMatch": '"old1"'})
        st.add_response("put_object", {"ETag": '"new2"'},
                        {"Bucket": blobmod.R2_BUCKET, "Key": "_aqueduct/x.db.zst", "Body": b"b",
                         "IfNoneMatch": "*"})
        b = _blob_with(c)
        assert b.put_if("_aqueduct/x.db.zst", b"a", if_match="old1") == "new1"
        assert b.put_if("_aqueduct/x.db.zst", b"b", if_none_match=True) == "new2"
        st.assert_no_pending_responses()


def test_put_if_returns_none_on_412_and_raises_on_anything_else():
    from botocore.exceptions import ClientError
    from botocore.stub import Stubber
    c = _real_client()
    with Stubber(c) as st:
        st.add_client_error("put_object", service_error_code="PreconditionFailed",
                            http_status_code=412)
        st.add_client_error("put_object", service_error_code="InternalError", http_status_code=500)
        b = _blob_with(c)
        assert b.put_if("_aqueduct/x.db.zst", b"a", if_match="old") is None
        with pytest.raises(ClientError):
            b.put_if("_aqueduct/x.db.zst", b"a", if_match="old")


def test_put_if_needs_exactly_one_condition():
    b = _blob_with(object())
    with pytest.raises(ValueError):
        b.put_if("k", b"a")
    with pytest.raises(ValueError):
        b.put_if("k", b"a", if_match="e", if_none_match=True)
    with pytest.raises(ValueError):
        b.put_if("k", b"a", if_match='""')
