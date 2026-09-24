"""tools/selfhost/watch_edge.py - the off-machine check (plan code change 6). No network: the HTTP and
GraphQL calls are replaced. Each check must FAIL when it cannot measure, and must see a planted write."""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))

import watch_edge as W  # noqa: E402


def test_the_committed_config_is_read_from_the_real_wrangler_toml():
    c = W.committed_config()
    assert c["forward"] is False and c["edge_state"] in ("econ", "users")
    assert len(c["econ_d1_ids"]) == 2 and all(len(i) == 36 for i in c["econ_d1_ids"])
    assert len(c["account_id"]) == 32, "the account id must be found (today it sits inside [limits])"


def _config(tmp_path, body):
    p = tmp_path / "w.toml"
    p.write_text(body)
    return W.committed_config(str(p))


D1 = ('[[d1_databases]]\nbinding = "CATALOG"\ndatabase_id = "a"\n'
      '[[d1_databases]]\nbinding = "CATALOG_CLIMATE"\ndatabase_id = "b"\n')


def test_forward_and_edge_state_follow_the_vars(tmp_path):
    assert _config(tmp_path, D1)["edge_state"] == "econ"
    assert _config(tmp_path, D1 + '[vars]\nEDGE_STATE = "users"\n')["edge_state"] == "users"
    c = _config(tmp_path, D1 + '[vars]\nFORWARD = "on"\n')
    assert c["forward"] is True and c["edge_state"] == "users"
    with pytest.raises(SystemExit):
        _config(tmp_path, '[[d1_databases]]\nbinding = "USERS"\ndatabase_id = "u"\n')


def test_status_mismatch_is_a_failure_and_404_only_when_required(monkeypatch):
    want = {"forward": True, "edge_state": "users"}
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (404, None))
    assert W.check_status("https://e", want, require=False) == []
    assert W.check_status("https://e", want, require=True) != []
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (200, {"forward": False, "edge_state": "users",
                                                                       "commit": "abc"}))
    bad = W.check_status("https://e", want, require=True)
    assert len(bad) == 1 and "forward" in bad[0] and "abc" in bad[0]
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (200, {"forward": True, "edge_state": "users"}))
    assert W.check_status("https://e", want, require=True) == []


def test_an_unreachable_edge_is_a_failure(monkeypatch):
    monkeypatch.setattr(W, "get_json", lambda url, timeout=60: (0, "URLError: down"))
    assert len(W.check_up("https://e")) == 2


def _fake_graphql(r2_rows, d1_rows):
    def g(token, query, variables):
        if "r2OperationsAdaptiveGroups" in query:
            return {"r2OperationsAdaptiveGroups": r2_rows}
        return {"d1AnalyticsAdaptiveGroups": d1_rows}
    return g


def test_a_planted_write_is_seen_and_reads_are_not(monkeypatch):
    r2 = [{"dimensions": {"date": "2026-10-01", "actionType": "GetObject", "bucketName": "econ-data"},
           "sum": {"requests": 900}},
          {"dimensions": {"date": "2026-10-01", "actionType": "PutObject", "bucketName": "econ-data"},
           "sum": {"requests": 1}}]
    d1 = [{"dimensions": {"date": "2026-10-01", "databaseId": "a"}, "sum": {"rowsWritten": 0}},
          {"dimensions": {"date": "2026-10-02", "databaseId": "b"}, "sum": {"rowsWritten": 3}}]
    monkeypatch.setattr(W, "graphql", _fake_graphql(r2, d1))
    bad = W.check_writes("t", "acct", "2026-10-01", "2026-10-02", ["a", "b"])
    assert len(bad) == 2 and "PutObject" in bad[0] and "3 rows" in bad[1]
    monkeypatch.setattr(W, "graphql", _fake_graphql(r2[:1], d1[:1]))
    assert W.check_writes("t", "acct", "2026-10-01", "2026-10-02", ["a", "b"]) == [], "reads are not writes"


def test_an_answer_outside_the_filter_voids_the_check(monkeypatch):
    other = [{"dimensions": {"date": "2026-10-01", "actionType": "GetObject", "bucketName": "hfdatalibrary-data"},
              "sum": {"requests": 1}}]
    monkeypatch.setattr(W, "graphql", _fake_graphql(other, []))
    with pytest.raises(RuntimeError, match="under a econ-data filter"):
        W.check_writes("t", "acct", "2026-10-01", "2026-10-02", ["a", "b"])


def test_a_truncated_answer_is_refused():
    with pytest.raises(RuntimeError, match="truncated"):
        W.rows_of({"x": [{}] * 5}, "x", 5)


def test_writes_check_without_a_token_is_blind_and_fails(monkeypatch, capsys):
    monkeypatch.setattr(W, "check_up", lambda edge: [])
    monkeypatch.setattr(W, "check_status", lambda edge, want, require: [])
    monkeypatch.delenv("CF_ANALYTICS_TOKEN", raising=False)
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    assert W.main(["--no-writes-since", "2026-10-01"]) == 1
    assert "BLIND" in capsys.readouterr().out
    assert W.main([]) == 0, "before T0 the writes check is off"
    assert W.main(["--no-writes-since", "Oct 1"]) == 1


def test_a_graphql_failure_is_blind_not_ok(monkeypatch, capsys):
    monkeypatch.setattr(W, "check_up", lambda edge: [])
    monkeypatch.setattr(W, "check_status", lambda edge, want, require: [])
    monkeypatch.setenv("CF_ANALYTICS_TOKEN", "t")
    monkeypatch.delenv("RESEND_API_KEY", raising=False)

    def boom(*_a):
        raise RuntimeError("GraphQL errors: denied")
    monkeypatch.setattr(W, "graphql", boom)
    assert W.main(["--no-writes-since", "2026-10-01"]) == 1
    assert "BLIND" in capsys.readouterr().out
