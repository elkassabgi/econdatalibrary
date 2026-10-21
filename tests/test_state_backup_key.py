"""Every state push keeps a backup of its own (ledger R1650).

updater/run.py push_state used to name the backup `_aqueduct/backups/state-<YYYYMMDD>-<runid>.db.zst`.
Every desktop push has runid 'local', so the second desktop push of a UTC day wrote the SAME key and
replaced the first push's backup. It happened on 2026-10-21: a 14:13Z operator push rewrote
`state-20261021-local.db.zst`, which held the 01:55Z local heavy pass's backup.

The stamp now carries the UTC time of day for every writer. These tests pin:
  * two same-day desktop pushes leave two backups, each holding its own push's bytes;
  * the cloud key still ends in the GITHUB_RUN_ID, now with the time of day;
  * retention still keeps the 7 newest, in the order documented at run.BACKUP_STAMP_FMT, over a mix of old-form keys (already in
    the bucket) and new-form keys, and on old-form keys alone it prunes exactly what the old rule did;
  * a push never deletes the backup it has just written.

Each fails against the old code: a date-only stamp gives one key for both pushes, and the old retention
had no guard for the key just written.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import os
import random
import sqlite3
import sys
from types import SimpleNamespace

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from core import r2_util                       # noqa: E402
from updater import config                     # noqa: E402
from updater import run as runmod              # noqa: E402

BACKUPS = "_aqueduct/backups/"
UTC = dt.timezone.utc


def _client_error(code: str, status: int, op: str):
    from botocore.exceptions import ClientError
    return ClientError({"Error": {"Code": code, "Message": code},
                        "ResponseMetadata": {"HTTPStatusCode": status}}, op)


class FakeS3:
    """In-memory boto3-shaped client: PUT (honouring If-Match / If-None-Match), GET, HEAD, DELETE, LIST.
    ETag = md5 of the body, what R2 reports for a single-part PUT."""

    def __init__(self):
        self.objs: dict[str, bytes] = {}
        self.deleted: list[str] = []

    def _etag(self, key):
        return hashlib.md5(self.objs[key]).hexdigest() if key in self.objs else None

    def put_object(self, Bucket, Key, Body, **kw):  # noqa: N803 - boto3's names
        cur = self._etag(Key)
        if "IfMatch" in kw and (cur is None or kw["IfMatch"].strip('"') != cur):
            raise _client_error("PreconditionFailed", 412, "PutObject")
        if kw.get("IfNoneMatch") == "*" and cur is not None:
            raise _client_error("PreconditionFailed", 412, "PutObject")
        self.objs[Key] = bytes(Body)
        return {"ETag": '"' + self._etag(Key) + '"'}

    def get_object(self, Bucket, Key):              # noqa: N803
        if Key not in self.objs:
            raise _client_error("NoSuchKey", 404, "GetObject")
        return {"Body": io.BytesIO(self.objs[Key])}

    def head_object(self, Bucket, Key):             # noqa: N803
        if Key not in self.objs:
            raise _client_error("404", 404, "HeadObject")
        return {"ETag": '"' + self._etag(Key) + '"', "ContentLength": len(self.objs[Key])}

    def delete_object(self, Bucket, Key):           # noqa: N803
        self.deleted.append(Key)
        self.objs.pop(Key, None)

    def get_paginator(self, name):
        objs = self.objs

        class _P:
            def paginate(self, Bucket, Prefix):     # noqa: N803
                yield {"Contents": [{"Key": k} for k in list(objs) if k.startswith(Prefix)]}
        return _P()

    def backups(self):
        return sorted(k for k in self.objs if k.startswith(BACKUPS))


@pytest.fixture()
def r2(monkeypatch, tmp_path):
    fake = FakeS3()
    monkeypatch.setattr(r2_util, "client", lambda *a, **kw: fake)
    state_dir = str(tmp_path / "_aqueduct")
    state_db = os.path.join(state_dir, "state.db")
    monkeypatch.setattr(config, "STATE_DIR", state_dir)
    monkeypatch.setattr(config, "STATE_DB", state_db)
    monkeypatch.setattr(runmod, "ETAG_PATH", os.path.join(state_dir, ".state_etag"))
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    os.makedirs(state_dir, exist_ok=True)
    con = sqlite3.connect(state_db)
    con.execute("CREATE TABLE t(x)")
    con.execute("INSERT INTO t VALUES (42)")
    con.commit()
    con.close()
    clock = {"now": dt.datetime(2026, 10, 21, 1, 55, 35, tzinfo=UTC)}
    monkeypatch.setattr(runmod, "_utcnow", lambda: clock["now"])
    return SimpleNamespace(s3=fake, state_db=state_db, clock=clock)


def _touch_state(db, v):
    con = sqlite3.connect(db)
    con.execute("INSERT INTO t VALUES (?)", (v,))
    con.commit()
    con.close()


# ---------------------------------------------------------------- the incident: two desktop pushes, one day
def test_same_day_double_desktop_push_keeps_both_backups(r2):
    assert runmod.push_state() == 0                                   # the 01:55Z local heavy pass
    first_state = r2.s3.objs[runmod.STATE_KEY]
    _touch_state(r2.state_db, 43)
    r2.clock["now"] = dt.datetime(2026, 10, 21, 14, 13, 0, tzinfo=UTC)   # the 14:13Z operator push
    assert runmod.push_state() == 0
    second_state = r2.s3.objs[runmod.STATE_KEY]
    assert first_state != second_state

    early = BACKUPS + "state-20261021T015535Z-local.db.zst"
    late = BACKUPS + "state-20261021T141300Z-local.db.zst"
    assert r2.s3.backups() == [early, late], "each push keeps a backup of its own"
    assert r2.s3.objs[early] == first_state, "the first push's restore point is NOT overwritten"
    assert r2.s3.objs[late] == second_state
    assert r2.s3.deleted == []


def test_cloud_key_carries_the_time_and_the_run_id(r2, monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "39591300199")
    r2.clock["now"] = dt.datetime(2026, 10, 21, 6, 5, 12, tzinfo=UTC)
    assert runmod.push_state() == 0
    assert r2.s3.backups() == [BACKUPS + "state-20261021T060512Z-39591300199.db.zst"]


def test_a_re_run_attempt_of_one_cloud_run_keeps_both_backups(r2, monkeypatch):
    """A GitHub re-run attempt keeps its GITHUB_RUN_ID; with a date-only stamp it replaced the first
    attempt's backup the same way."""
    monkeypatch.setenv("GITHUB_RUN_ID", "39591300199")
    assert runmod.push_state() == 0
    _touch_state(r2.state_db, 44)
    r2.clock["now"] = dt.datetime(2026, 10, 21, 9, 0, 0, tzinfo=UTC)
    assert runmod.push_state() == 0
    assert len(r2.s3.backups()) == 2


def test_the_stamp_is_utc_whatever_the_clock_zone():
    plus2 = dt.timezone(dt.timedelta(hours=2))
    key = runmod.backup_key(dt.datetime(2026, 10, 22, 1, 30, 0, tzinfo=plus2), "local")
    assert key == BACKUPS + "state-20261021T233000Z-local.db.zst"


# ---------------------------------------------------------------- retention over old and new key forms
# The order retention treats as oldest-first. Across days and among new-form keys it is the real time
# order. Old-form keys of ONE day carry no time, so the old rule broke their tie on the runid text
# (digits before 'local'): the old-form same-day pairs below are listed in THAT order, which is not their
# real time order (on 2026-10-18 the -local key was written at 01:20Z, after neither cloud key).
CHRONO = [
    BACKUPS + "state-20261017-39100000001.db.zst",
    BACKUPS + "state-20261018-39200000002.db.zst",
    BACKUPS + "state-20261018-local.db.zst",
    BACKUPS + "state-20261019-39300000003.db.zst",
    BACKUPS + "state-20261020-local.db.zst",
    BACKUPS + "state-20261021-39591300199.db.zst",      # old code, cloud, 2026-10-21 morning
    BACKUPS + "state-20261021-local.db.zst",            # old code, desktop, same day
    BACKUPS + "state-20261021T141300Z-local.db.zst",    # new code from here on
    BACKUPS + "state-20261021T153000Z-39600000001.db.zst",
    BACKUPS + "state-20261021T231500Z-local.db.zst",
    BACKUPS + "state-20261022T015500Z-local.db.zst",
    BACKUPS + "state-20261022T060500Z-39700000002.db.zst",
]


def test_keys_from_the_real_key_function_sort_in_time_order():
    """Keys built by backup_key() itself, at shuffled times and for both writers, are pruned oldest
    first by backups_to_prune() - across days, within a day, across writers."""
    rng = random.Random(1650)
    start = dt.datetime(2026, 10, 19, 0, 0, 0, tzinfo=UTC)
    times = sorted({start + dt.timedelta(seconds=rng.randint(0, 4 * 86400)) for _ in range(40)})
    keys = [runmod.backup_key(t, rng.choice(["local", "39591300199", "39600000001"])) for t in times]
    shuffled = keys[:]
    rng.shuffle(shuffled)
    assert runmod.backups_to_prune(shuffled, just_written=keys[-1]) == keys[:-7]


def test_transition_day_old_code_key_written_late_counts_as_older():
    """Documented limit: a writer still on old code (the desktop before its checkout is pulled) writes
    `state-<day>-local` AFTER new-form keys of that day exist. It sorts before them whatever its real
    time, so retention treats it as the older one."""
    new_early = BACKUPS + "state-20261022T150500Z-39700000002.db.zst"     # 15:05Z, new code
    old_late = BACKUPS + "state-20261022-local.db.zst"                    # 20:00Z, old code
    later_day = BACKUPS + "state-20261023T010000Z-local.db.zst"
    assert runmod.backups_to_prune([later_day, new_early, old_late], just_written=later_day,
                                   keep=2) == [old_late]
    assert runmod.backups_to_prune([new_early, old_late], just_written=new_early, keep=1) == [old_late]


def test_retention_keeps_the_seven_newest_across_mixed_forms():
    shuffled = CHRONO[:]
    random.Random(7).shuffle(shuffled)
    prune = runmod.backups_to_prune(shuffled, just_written=CHRONO[-1])
    assert prune == CHRONO[:-7]
    kept = [k for k in CHRONO if k not in prune]
    assert kept == CHRONO[-7:] and len(kept) == runmod.BACKUP_KEEP


def test_old_form_keys_alone_prune_exactly_as_the_old_rule_did():
    rng = random.Random(42)
    keys = []
    for day in range(1, 21):
        keys.append(f"{BACKUPS}state-202610{day:02d}-local.db.zst")
        for _ in range(rng.randint(0, 3)):
            keys.append(f"{BACKUPS}state-202610{day:02d}-{rng.randint(10**10, 10**11 - 1)}.db.zst")
    rng.shuffle(keys)
    old_rule = sorted(keys)[:-7]                       # the retention before R1650, verbatim
    assert runmod.backups_to_prune(keys, just_written=max(keys)) == old_rule


def test_a_push_never_prunes_the_backup_it_just_wrote():
    """A clock behind another writer's can put the new key anywhere in the order; it is still kept."""
    behind = BACKUPS + "state-20261016T120000Z-local.db.zst"   # sorts before every key in CHRONO
    keys = CHRONO + [behind]
    prune = runmod.backups_to_prune(keys, just_written=behind)
    assert behind not in prune
    assert prune == CHRONO[:-7]


def test_push_prunes_old_form_keys_and_keeps_its_own(r2):
    """End to end: a bucket holding nine old-form backups, then one new-form push."""
    old = [BACKUPS + f"state-202610{d:02d}-{'local' if d % 2 else '39000000%03d' % d}.db.zst"
           for d in range(10, 19)]
    for k in old:
        r2.s3.objs[k] = b"old backup " + k.encode()
    r2.clock["now"] = dt.datetime(2026, 10, 21, 14, 13, 0, tzinfo=UTC)
    assert runmod.push_state() == 0
    ours = BACKUPS + "state-20261021T141300Z-local.db.zst"
    assert r2.s3.backups() == sorted(old)[-6:] + [ours]
    assert r2.s3.objs[ours] == r2.s3.objs[runmod.STATE_KEY]
    assert sorted(r2.s3.deleted) == sorted(old)[:3]


def test_retention_log_reports_what_was_really_kept_when_the_guard_fires(r2, capsys):
    """A clock behind the bucket's keys puts our key outside the newest 7; the guard keeps it, so 8
    stay, and the log line must say 8, not 7."""
    future = [BACKUPS + f"state-202610{d:02d}T060000Z-3900000{d:04d}.db.zst" for d in range(22, 31)]
    for k in future:
        r2.s3.objs[k] = b"x"
    r2.clock["now"] = dt.datetime(2026, 10, 21, 14, 13, 0, tzinfo=UTC)
    assert runmod.push_state() == 0
    ours = BACKUPS + "state-20261021T141300Z-local.db.zst"
    assert ours in r2.s3.backups() and len(r2.s3.backups()) == 8
    assert "pruned 2 old backup(s), kept 8" in capsys.readouterr().out
