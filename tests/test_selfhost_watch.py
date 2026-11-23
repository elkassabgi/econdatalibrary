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
SHA = "8025b45ed7d736b7de98238cde9d6f15d1cc0502"          # a real 40-hex commit (econ main, 2026-11-22)
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
    good = {"forward": False, "edge_state": "users", "forward_raw": "", "edge_state_raw": "users", "commit": SHA}
    _status(monkeypatch, 200, good)
    assert W.check_status("https://e", want) == []
    for k, v in [("forward", True), ("edge_state", "econ"), ("forward_raw", "on"), ("edge_state_raw", "")]:
        _status(monkeypatch, 200, {**good, k: v})
        bad = W.check_status("https://e", want)
        assert len(bad) == 1 and k in bad[0] and SHA in bad[0], k
    _status(monkeypatch, 200, {**good, "commit": None})
    assert any("commit" in m for m in W.check_status("https://e", want))


# R2038: the 2026-11-21 deploy bypassed deploy_edge.sh; /v1/edge-status answered 200 with commit null for two
# days, the committed state was the DEFAULT, and every run printed OK. These are the bodies of that window
# and its relatives: each must FAIL the status check in the default state as well.
NO_OR_BAD_COMMIT = [None, "", 0, SHA[:39], SHA.upper(), SHA + "\n", "-c" + "a" * 38, 12345, ["x"], {"a": 1},
                    "x\n::error::planted"]


@pytest.mark.parametrize("commit", NO_OR_BAD_COMMIT + ["<absent>"], ids=repr)
def test_a_200_answer_without_a_40_hex_commit_fails_in_the_default_state(monkeypatch, commit):
    body = {"forward": False, "edge_state": "econ", "forward_raw": "", "edge_state_raw": ""}
    if commit != "<absent>":
        body["commit"] = commit
    _status(monkeypatch, 200, body)
    bad = W.check_status("https://e", WANT)
    assert len(bad) == 1 and bad[0].startswith(("NO-COMMIT", "BAD-COMMIT")), bad
    assert "deploy_edge.sh" in bad[0] and "\n" not in bad[0]
    # control: the same body with a real commit passes
    _status(monkeypatch, 200, {**body, "commit": SHA})
    assert W.check_status("https://e", WANT) == []


def test_a_deployed_edge_that_calls_itself_a_soak_worker_is_a_failure(monkeypatch, tmp_path):
    """SOAK = "1" belongs to the soak worker only; on the edge the page-view routes would answer 404 with no
    error anywhere. An edge deployed before the field existed has no `soak` key: not a failure."""
    good = {"forward": False, "edge_state": "econ", "forward_raw": "", "edge_state_raw": "", "commit": SHA}
    for body in (good, {**good, "soak": False}):
        _status(monkeypatch, 200, body)
        assert W.check_status("https://e", WANT) == [], body
    for value in (True, "true", 1, "1"):                      # anything but false / absent is a failure
        _status(monkeypatch, 200, {**good, "soak": value})
        bad = W.check_status("https://e", WANT)
        assert len(bad) == 1 and f"soak={value!r}" in bad[0] and SHA in bad[0]
    # a checkout that commits SOAK fails against a plain edge ...
    _status(monkeypatch, 200, good)
    assert len(W.check_status("https://e", {**WANT, "soak": True})) == 1
    # ... AND against a soak edge: "the edge is what main commits" must never read as fine here
    _status(monkeypatch, 200, {**good, "soak": True})
    assert len(W.check_status("https://e", {**WANT, "soak": True})) == 2
    # the committed config: the real wrangler.toml has no SOAK; ANY value of the key counts
    assert W.committed_config()["soak"] is False
    assert _config(tmp_path, D1 + '[vars]\nSOAK = "1"\n')["soak"] is True
    assert _config(tmp_path, D1 + '[vars]\nSOAK = "true"\n')["soak"] is True


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


GOOD_STATUS = (200, {"forward": False, "edge_state": "econ", "forward_raw": "", "edge_state_raw": "", "commit": SHA})


def _quiet(monkeypatch):
    monkeypatch.setattr(W, "check_up", lambda edge, now: [])
    monkeypatch.setattr(W, "read_status", lambda edge: GOOD_STATUS)
    monkeypatch.setattr(W, "check_status", lambda edge, want, status=None: [])
    monkeypatch.setattr(W, "undeployed_worker_changes", lambda edge, status=None: None)
    monkeypatch.delenv("RESEND_API_KEY", raising=False)


@pytest.mark.parametrize("rc,out,expect", [(0, b"", None),
                                           (1, b"api/worker/test/x.test.ts\n", "not live"),
                                           (128, b"", "not in this checkout")])
def test_undeployed_worker_changes_is_a_warning(monkeypatch, rc, out, expect):
    import subprocess
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (200, {"commit": SHA}))
    seen = {}

    def run(cmd, **kw):
        seen["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, rc, stdout=out, stderr=b"")
    monkeypatch.setattr(subprocess, "run", run)
    got = W.undeployed_worker_changes("https://e")
    assert (got is None) if expect is None else (expect in got)
    assert seen["cmd"] == ["git", "diff", "--name-only", "--exit-code", SHA, "HEAD", "--", "api/worker"]


def test_the_lag_warning_names_the_changed_paths(monkeypatch):
    """A diff of test files only must read as one (review condition 10: on 2026-11-19 the live watch said
    'not live' about api/worker/test/guardHeartbeat.test.ts alone)."""
    import subprocess
    paths = [f"api/worker/test/t{i}.test.ts" for i in range(7)]
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(
        cmd, 1, stdout=("\n".join(paths) + "\n").encode(), stderr=b""))
    got = W.undeployed_worker_changes("https://e", (200, {"commit": SHA}))
    assert got.startswith("LAG: ") and "7 path(s)" in got and SHA[:12] in got
    assert all(p in got for p in paths[:5]) and paths[5] not in got and "and 2 more" in got


# Every case where the compare cannot run is a NAMED warning, never None (R2038: None for "no commit" read the
# same as "no difference"). None is kept for exactly one case: the compare ran and found nothing.
@pytest.mark.parametrize("status,prefix", [((200, {"commit": None}), "NO-COMMIT"),
                                           ((200, {}), "NO-COMMIT"),
                                           ((200, {"commit": ""}), "NO-COMMIT"),
                                           ((200, {"commit": SHA + "\n"}), "BAD-COMMIT"),
                                           ((200, {"commit": SHA[:12]}), "BAD-COMMIT"),
                                           ((200, {"commit": "--output=x" + "a" * 30}), "BAD-COMMIT"),
                                           ((200, {"commit": 12345}), "BAD-COMMIT"),
                                           ((200, {"commit": ["x"]}), "BAD-COMMIT"),
                                           ((404, None), "UNREAD"),
                                           ((503, None), "UNREAD"),
                                           ((0, "URLError: down"), "UNREAD"),
                                           ((200, ["not", "a", "dict"]), "UNREAD")], ids=repr)
def test_a_compare_that_cannot_run_is_a_named_warning_not_none(monkeypatch, status, prefix):
    import subprocess

    def run(cmd, **kw):
        raise AssertionError(f"git must not run on a commit that is not a 40-hex sha: {cmd}")
    monkeypatch.setattr(subprocess, "run", run)
    got = W.undeployed_worker_changes("https://e", status)
    assert isinstance(got, str) and got.startswith(prefix + ": "), got
    assert "\n" not in got
    # the compare itself failing is UNREAD too, not None
    def boom(cmd, **kw):
        raise OSError("git missing")
    monkeypatch.setattr(subprocess, "run", boom)
    assert W.undeployed_worker_changes("https://e", (200, {"commit": SHA})).startswith("UNREAD: ")


def _main_with(monkeypatch, capsys, status, warn_rc=0, actions=None):
    """main() with the network replaced: /v1/edge-status answers `status`, the git compare answers `warn_rc`.
    Counts the reads of /v1/edge-status and captures the alert mail."""
    import subprocess
    reads, mails = [], []

    def fake(url, timeout=60):
        if url.endswith("/v1/edge-status"):
            reads.append(url)
            return status
        return (200, _updates([NOW.isoformat()])) if url.endswith("last-updates") else (200, {"sources": []})
    monkeypatch.setattr(W, "get_json", fake)
    monkeypatch.setattr(W, "send_alert", lambda subject, body: mails.append((subject, body)))
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(
        cmd, warn_rc, stdout=b"api/worker/src/edge.ts\n" if warn_rc == 1 else b"", stderr=b""))
    if actions is None:
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    else:
        monkeypatch.setenv("GITHUB_ACTIONS", actions)
    rc = W.main([], now=NOW)
    return rc, capsys.readouterr().out, reads, mails


def test_the_r2038_window_is_a_red_run_with_a_mail(monkeypatch, capsys):
    """The live state of 2026-11-21..23: 200, default state, commit null. It printed OK; now it fails, the
    failure mail carries the named warning too, and /v1/edge-status is read ONCE."""
    body = {"forward": False, "edge_state": "econ", "forward_raw": "", "edge_state_raw": "", "commit": None}
    rc, out, reads, mails = _main_with(monkeypatch, capsys, (200, body))
    assert rc == 1 and "STATUS: NO-COMMIT" in out and "WARN NO-COMMIT" in out and "\nOK" not in out
    assert len(reads) == 1, reads
    assert len(mails) == 1 and mails[0][0] == "[econ self-host] 1 check(s) failed"
    assert "NO-COMMIT" in mails[0][1] and "WARN NO-COMMIT" in mails[0][1]
    # control: the same run with the deployed commit passes, reads once, mails nothing
    rc, out, reads, mails = _main_with(monkeypatch, capsys, (200, {**body, "commit": SHA}))
    assert rc == 0 and out.rstrip().endswith("OK") and "WARN" not in out and len(reads) == 1 and mails == []


@pytest.mark.parametrize("actions", [None, "false", "true"])
def test_a_lag_warning_is_printed_and_annotated_but_not_mailed(monkeypatch, capsys, actions):
    body = {"forward": False, "edge_state": "econ", "forward_raw": "", "edge_state_raw": "", "commit": SHA}
    rc, out, reads, mails = _main_with(monkeypatch, capsys, (200, body), warn_rc=1, actions=actions)
    assert rc == 0 and "WARN LAG: " in out and "api/worker/src/edge.ts" in out and out.rstrip().endswith("OK")
    assert mails == [], "a lag warning alone is not mailed (R1988: the owner must not be the first reader)"
    notes = [ln for ln in out.splitlines() if ln.startswith("::")]
    if actions == "true":
        assert len(notes) == 1 and notes[0].startswith("::warning title=selfhost-watch::LAG: ")
    else:
        assert notes == [], "an annotation only inside GitHub Actions"


def test_a_served_value_cannot_become_a_workflow_command(monkeypatch, capsys):
    """A commit value carrying a newline and '::error::' gives exactly ONE output line that starts with '::'
    (the escaped warning annotation), never a second one."""
    body = {"forward": False, "edge_state": "econ", "forward_raw": "", "edge_state_raw": "",
            "commit": "x\n::error::planted"}
    rc, out, reads, mails = _main_with(monkeypatch, capsys, (200, body), actions="true")
    starts = [ln for ln in out.splitlines() if ln.startswith("::")]
    assert rc == 1 and len(starts) == 1 and starts[0].startswith("::warning title=selfhost-watch::BAD-COMMIT"), starts
    assert W._annotation("50% a\r\nb") == "::warning title=selfhost-watch::50%25 a%0D%0Ab"


def test_a_long_served_value_is_cut(monkeypatch):
    body = {"forward": "z" * 500, "edge_state": "econ", "forward_raw": "", "edge_state_raw": "", "commit": SHA}
    _status(monkeypatch, 200, body)
    bad = W.check_status("https://e", WANT)
    assert len(bad) == 1 and "...(cut)" in bad[0] and len(bad[0]) < 300


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
