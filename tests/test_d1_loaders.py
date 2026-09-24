"""core/load_d1_rest.py and core/load_d1_chunked.py on core.d1_remote (plan step 1). No network, no wrangler."""
import pytest

from core import cutover, d1_remote, load_d1_chunked, load_d1_rest


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(load_d1_rest.time, "sleep", lambda s: None)
    monkeypatch.setattr(load_d1_chunked.time, "sleep", lambda s: None)


def test_rest_loader_retries_then_succeeds(monkeypatch):
    calls = []

    def query(db, sql, **kw):
        calls.append((db, sql, kw.get("timeout")))
        if len(calls) < 3:
            raise RuntimeError("D1 econ-catalog: HTTP 503")
        return {"results": []}
    monkeypatch.setattr(d1_remote, "query", query)
    load_d1_rest.execute("tok", "INSERT OR REPLACE INTO t VALUES (1);")
    assert len(calls) == 3 and calls[0] == ("econ-catalog", "INSERT OR REPLACE INTO t VALUES (1);", 90)


def test_rest_loader_gives_up_after_its_retries(monkeypatch):
    monkeypatch.setattr(d1_remote, "query", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("HTTP 500")))
    with pytest.raises(RuntimeError, match="HTTP 500"):
        load_d1_rest.execute("tok", "x")


def test_rest_loader_never_retries_a_refusal(monkeypatch):
    calls = []

    def query(*a, **k):
        calls.append(1)
        raise cutover.CutoverRefused("refused: after T0")
    monkeypatch.setattr(d1_remote, "query", query)
    with pytest.raises(cutover.CutoverRefused):
        load_d1_rest.execute("tok", "INSERT INTO t VALUES (1)")
    assert calls == [1]


def test_chunk_loader_counts_error_in_the_output_as_a_failure(monkeypatch):
    outs = iter(["Executed... ERROR: SQLITE_CONSTRAINT", "Executed 40 commands"])
    seen = []
    monkeypatch.setattr(d1_remote, "execute_file", lambda db, p, **kw: seen.append((db, p, kw)) or next(outs))
    assert load_d1_chunked.run_chunk("c1.sql") is True
    assert len(seen) == 2 and seen[0][0] == "econ-catalog" and seen[0][2] == {"timeout": 1800}


def test_chunk_loader_gives_up_after_three(monkeypatch):
    def fail(*a, **k):
        raise d1_remote.D1Unreachable("wrangler timed out after 1800 s")
    monkeypatch.setattr(d1_remote, "execute_file", fail)
    assert load_d1_chunked.run_chunk("c1.sql") is False


def test_chunk_loader_never_retries_a_refusal(monkeypatch):
    calls = []

    def refused(*a, **k):
        calls.append(1)
        raise cutover.CutoverRefused("refused after T0")
    monkeypatch.setattr(d1_remote, "execute_file", refused)
    with pytest.raises(cutover.CutoverRefused):
        load_d1_chunked.run_chunk("c1.sql")
    assert calls == [1]


def test_remote_count_goes_through_the_chokepoint(monkeypatch):
    monkeypatch.setattr(d1_remote, "query", lambda db, sql, **kw: {"results": [{"n": 7}]} if
                        (db, sql) == ("econ-catalog", "SELECT COUNT(*) AS n FROM series") else None)
    assert load_d1_chunked.remote_count("tok", "series") == 7
