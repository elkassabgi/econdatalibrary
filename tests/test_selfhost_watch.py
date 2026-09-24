"""tools/selfhost/watch_edge.py - the off-machine check (plan code change 6). No network: the HTTP and
GraphQL calls are replaced. Each check must FAIL when it cannot measure, and must see a planted write
(review R1174)."""
import datetime as dt
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))

import watch_edge as W  # noqa: E402

NOW = dt.datetime(2026, 10, 20, 12, 0, tzinfo=dt.timezone.utc)
ECON, USERS = ["e1", "e2"], "u1"
WANT = {"forward": False, "edge_state": "econ", "forward_raw": "", "edge_state_raw": "",
        "econ_d1_ids": ECON, "users_d1_id": USERS}


def test_the_committed_config_is_read_from_the_real_wrangler_toml():
    c = W.committed_config()
    assert c["forward"] is False and c["edge_state"] in ("econ", "users")
    assert len(c["econ_d1_ids"]) == 2 and all(len(i) == 36 for i in c["econ_d1_ids"])
    assert len(c["users_d1_id"]) == 36 and c["users_d1_id"] not in c["econ_d1_ids"]
    assert len(c["account_id"]) == 32, "the account id must be found (today it sits inside [limits])"


D1 = ('[[d1_databases]]\nbinding = "CATALOG"\ndatabase_id = "a"\n'
      '[[d1_databases]]\nbinding = "CATALOG_CLIMATE"\ndatabase_id = "b"\n'
      '[[d1_databases]]\nbinding = "USERS"\ndatabase_id = "u"\n')


def _config(tmp_path, body):
    p = tmp_path / "w.toml"
    p.write_text(body)
    return W.committed_config(str(p))


def test_forward_and_edge_state_follow_the_vars(tmp_path):
    assert _config(tmp_path, D1)["edge_state"] == "econ"
    c = _config(tmp_path, D1 + '[vars]\nEDGE_STATE = "users"\n')
    assert c["edge_state"] == "users" and c["edge_state_raw"] == "users" and c["forward"] is False
    c = _config(tmp_path, D1 + '[vars]\nFORWARD = "on"\n')
    assert c["forward"] is True and c["edge_state"] == "users" and c["forward_raw"] == "on"
    with pytest.raises(SystemExit):
        _config(tmp_path, '[[d1_databases]]\nbinding = "USERS"\ndatabase_id = "u"\n')


def _status(monkeypatch, code, body):
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (code, body))


def test_a_missing_route_passes_only_while_the_committed_state_is_the_default(monkeypatch):
    _status(monkeypatch, 404, None)
    assert W.check_status("https://e", WANT) == []
    # R1174 blocker: committed EDGE_STATE=users, the deployed (old) worker has no route -> FAIL
    assert W.check_status("https://e", {**WANT, "edge_state": "users", "edge_state_raw": "users"}) != []
    assert W.check_status("https://e", {**WANT, "forward": True, "edge_state": "users", "forward_raw": "on"}) != []


def test_every_status_field_is_compared_and_a_commit_is_required(monkeypatch):
    want = {**WANT, "edge_state": "users", "edge_state_raw": "users"}
    good = {"forward": False, "edge_state": "users", "forward_raw": "", "edge_state_raw": "users", "commit": "abc"}
    _status(monkeypatch, 200, good)
    assert W.check_status("https://e", want) == []
    for k, v in [("forward", True), ("edge_state", "econ"), ("forward_raw", "on"), ("edge_state_raw", "")]:
        _status(monkeypatch, 200, {**good, k: v})
        bad = W.check_status("https://e", want)
        assert len(bad) == 1 and k in bad[0] and "abc" in bad[0], k
    _status(monkeypatch, 200, {**good, "commit": None})
    assert any("commit" in m for m in W.check_status("https://e", want))


def _updates(stamps):
    return {"datasets": [{"source": "s", "last_updated": s} for s in stamps] + [{"source": "t", "last_updated": None}]}


def test_up_and_fresh(monkeypatch):
    def fake(url, timeout=60):
        return (200, _updates(["2026-10-19T20:00:00+00:00", "2026-09-01T00:00:00+00:00"])) \
            if url.endswith("last-updates") else (200, {"sources": []})
    monkeypatch.setattr(W, "get_json", fake)
    assert W.check_up("https://e", NOW) == []
    assert any("STALE" in m for m in W.check_up("https://e", NOW + dt.timedelta(days=3)))


def test_unreachable_or_empty_is_a_failure(monkeypatch):
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (0, "URLError: down"))
    assert len(W.check_up("https://e", NOW)) == 2
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (200, {"datasets": [{"last_updated": None}]}))
    assert any("no dataset" in m for m in W.check_up("https://e", NOW))


def test_t0_must_be_a_moment_and_the_window_is_clipped():
    with pytest.raises(ValueError):
        W.parse_t0("2026-10-05")
    t0 = W.parse_t0("2026-10-05T14:00:00Z")
    assert W.window(t0, NOW) == ("2026-10-05T14:00:00Z", "2026-10-20T12:00:00Z")
    old = W.parse_t0("2026-06-01T00:00:00Z")
    assert W.window(old, NOW)[0] == "2026-09-20T12:00:00Z", "never wider than WINDOW_DAYS (the 32-day cap)"


def r2row(bucket, action, n, date="2026-10-19"):
    return {"dimensions": {"date": date, "actionType": action, "bucketName": bucket}, "sum": {"requests": n}}


def d1row(db, written=0, wq=0, rq=0, date="2026-10-19"):
    return {"dimensions": {"date": date, "databaseId": db},
            "sum": {"rowsWritten": written, "writeQueries": wq, "readQueries": rq}}


def _fake(r2_rows, d1_rows, seen):
    def g(token, query, variables):
        seen.append(variables)
        assert variables["start"] == "2026-10-05T14:00:00Z" and variables["end"] == "2026-10-20T12:00:00Z"
        if "r2OperationsAdaptiveGroups" in query:
            assert "datetime_geq" in query and variables["buckets"] == ["econ-data", *W.CONTROL_BUCKETS]
            return {"r2OperationsAdaptiveGroups": r2_rows}
        assert variables["ids"] == [*ECON, USERS]
        return {"d1AnalyticsAdaptiveGroups": d1_rows}
    return g


T0 = W.parse_t0("2026-10-05T14:00:00Z")
CONTROL_R2 = [r2row("hfdatalibrary-data", "PutObject", 50)]
CONTROL_D1 = [d1row(USERS, written=9, wq=9, rq=100)]


def test_a_planted_write_is_seen_and_reads_are_not(monkeypatch):
    seen = []
    monkeypatch.setattr(W, "graphql", _fake(
        CONTROL_R2 + [r2row("econ-data", "GetObject", 900), r2row("econ-data", "PutObject", 1)],
        CONTROL_D1 + [d1row("e1", rq=40), d1row("e2", written=3, wq=1)], seen))
    bad = W.check_writes("t", "acct", T0, NOW, WANT)
    assert len(bad) == 2 and "PutObject" in bad[0] and "3 rows" in bad[1]
    assert len(seen) == 2


@pytest.mark.parametrize("action", ["DeleteObject", "DeleteObjects", "CopyObject", "CreateMultipartUpload",
                                    "UploadPart", "CompleteMultipartUpload", "PutBucketLifecycleConfiguration",
                                    "SomeFutureWrite"])
def test_every_non_read_action_is_a_write(monkeypatch, action):
    """AR-151 finding 2: a check that counted only PutObject survived. Anything not on the read list is a
    write - including an action name that does not exist yet."""
    monkeypatch.setattr(W, "graphql", _fake(CONTROL_R2 + [r2row("econ-data", action, 1)], CONTROL_D1, []))
    bad = W.check_writes("t", "acct", T0, NOW, WANT)
    assert len(bad) == 1 and action in bad[0]


def test_a_write_query_that_wrote_no_rows_still_counts(monkeypatch):
    monkeypatch.setattr(W, "graphql", _fake(CONTROL_R2, CONTROL_D1 + [d1row("e1", written=0, wq=1)], []))
    assert len(W.check_writes("t", "acct", T0, NOW, WANT)) == 1, "a DROP TABLE can write zero rows"


def test_control_writes_are_not_reported_and_quiet_econ_is_ok(monkeypatch):
    monkeypatch.setattr(W, "graphql", _fake(CONTROL_R2 + [r2row("econ-data", "GetObject", 5)],
                                            CONTROL_D1 + [d1row("e1", rq=3)], []))
    assert W.check_writes("t", "acct", T0, NOW, WANT) == []


def test_an_empty_answer_is_blind_not_zero_writes(monkeypatch):
    monkeypatch.setattr(W, "graphql", _fake([], CONTROL_D1, []))
    with pytest.raises(RuntimeError, match="control buckets"):
        W.check_writes("t", "acct", T0, NOW, WANT)
    monkeypatch.setattr(W, "graphql", _fake(CONTROL_R2, [], []))
    with pytest.raises(RuntimeError, match="users database"):
        W.check_writes("t", "acct", T0, NOW, WANT)


def test_an_answer_outside_the_filter_voids_the_check(monkeypatch):
    monkeypatch.setattr(W, "graphql", _fake(CONTROL_R2 + [r2row("some-other-bucket", "GetObject", 1)], CONTROL_D1, []))
    with pytest.raises(RuntimeError, match="outside the filter"):
        W.check_writes("t", "acct", T0, NOW, WANT)
    monkeypatch.setattr(W, "graphql", _fake(CONTROL_R2, CONTROL_D1 + [d1row("zz")], []))
    with pytest.raises(RuntimeError, match="outside the filter"):
        W.check_writes("t", "acct", T0, NOW, WANT)


def test_a_truncated_answer_is_refused():
    with pytest.raises(RuntimeError, match="truncated"):
        W.rows_of({"x": [{}] * 5}, "x", 5)


def _quiet(monkeypatch):
    monkeypatch.setattr(W, "check_up", lambda edge, now: [])
    monkeypatch.setattr(W, "check_status", lambda edge, want: [])
    monkeypatch.setattr(W, "undeployed_worker_changes", lambda edge: None)
    monkeypatch.delenv("RESEND_API_KEY", raising=False)


@pytest.mark.parametrize("rc,expect", [(0, None), (1, "not live"), (128, "not in this checkout")])
def test_undeployed_worker_changes_is_a_warning(monkeypatch, rc, expect):
    import subprocess
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (200, {"commit": "abc123def456789"}))
    seen = {}

    def run(cmd, **kw):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, rc)
    monkeypatch.setattr(subprocess, "run", run)
    got = W.undeployed_worker_changes("https://e")
    assert (got is None) if expect is None else (expect in got)
    assert seen["cmd"] == ["git", "diff", "--quiet", "abc123def456789", "HEAD", "--", "api/worker"]
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (404, None))
    assert W.undeployed_worker_changes("https://e") is None, "no route, no commit: nothing to compare"


def test_writes_check_without_a_token_is_blind_and_fails(monkeypatch, capsys):
    _quiet(monkeypatch)
    monkeypatch.delenv("CF_ANALYTICS_TOKEN", raising=False)
    assert W.main(["--no-writes-since", "2026-10-05T14:00:00Z"], now=NOW) == 1
    assert "BLIND" in capsys.readouterr().out
    assert W.main([], now=NOW) == 0, "before T0 the writes check is off"
    assert W.main(["--no-writes-since", "2026-10-05"], now=NOW) == 1, "a bare date is refused"


def test_a_graphql_failure_is_blind_not_ok(monkeypatch, capsys):
    _quiet(monkeypatch)
    monkeypatch.setenv("CF_ANALYTICS_TOKEN", "t")

    def boom(*_a):
        raise RuntimeError("GraphQL errors: denied")
    monkeypatch.setattr(W, "graphql", boom)
    assert W.main(["--no-writes-since", "2026-10-05T14:00:00Z"], now=NOW) == 1
    assert "BLIND" in capsys.readouterr().out
