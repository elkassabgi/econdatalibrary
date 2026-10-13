"""The UNCTAD title tool reads dimension labels through the updater's own request, and a refused
request is a FAILURE, not an empty label table.

Until 2026-10-13 tools/title_unctad_from_api.py asked the keyless dimension address itself and
returned {} on any status that was not 200. When the publisher blocked that address (403), every
source "titled 0" and the tool exited 0 with no reason printed. Hermetic: no network, the store and
the catalogue are not read.
"""
import importlib.util
import os
import sys

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

TOOL = os.path.join(ROOT, "tools", "title_unctad_from_api.py")


@pytest.fixture
def tool():
    spec = importlib.util.spec_from_file_location("title_unctad_under_test", TOOL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ROWS = [{"Code": "0000", "Label": "World"}, {"Code": 76, "Label": "Brazil"},
        {"Code": 0, "Label": "Zero is a code"}, {"Code": "X1", "Label": ""},
        {"Code": "X2"}, {"Code": None, "Label": "no code"}, {"Label": "no code at all"}]


def test_labels_are_built_from_the_rows_of_the_shared_request(tool, monkeypatch):
    asked = []
    monkeypatch.setattr(tool.iu, "dim_rows", lambda ds, v, t: asked.append((ds, v, t)) or list(ROWS))
    got = tool._dim_labels("US.X", 10005, "Economies")
    assert asked == [("US.X", 10005, "Economies")]
    # a code is a string in a series key; a row with no label, or no code, is left out
    assert got == {"0000": "World", "76": "Brazil", "0": "Zero is a code"}


def test_a_refused_request_is_an_error_not_an_empty_table(tool, monkeypatch):
    def refuse(ds, v, t):
        raise requests.HTTPError("403 for the dimension table")
    monkeypatch.setattr(tool.iu, "dim_rows", refuse)
    with pytest.raises(requests.HTTPError):
        tool._dim_labels("US.X", 1, "Economies")


class _Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} for the dimension table", response=self)

    def json(self):
        return self._body


def test_through_the_real_request_a_403_is_one_credentialed_request_and_an_error(tool, monkeypatch):
    """Not a stand-in for dim_rows this time: the tool's own module copy of the ingest, with only
    requests.get and the credentials replaced."""
    seen = []

    def get(url, headers=None, timeout=None, **kw):
        assert not kw, sorted(kw)
        seen.append((url, dict(headers or {})))
        return _Resp(403, "Blocked")
    monkeypatch.setattr(tool.iu, "creds", lambda: ("the-id", "the-secret"))
    monkeypatch.setattr(tool.iu.requests, "get", get)
    with pytest.raises(requests.HTTPError):
        tool._dim_labels("US.X", 7, "Economies")
    (url, headers), = seen
    assert url.startswith(tool.iu.FACTS + "/US.X/7/Economies?")
    assert headers["ClientId"] == "the-id" and headers["ClientSecret"] == "the-secret"
    assert "the-secret" not in url and "datamart-api" not in url


def test_the_tool_makes_no_dimension_request_of_its_own(tool):
    src = open(TOOL, encoding="utf-8").read()
    assert "datamart-api" not in src, "the keyless dimension address is blocked; read through iu.dim_rows"
    assert "iu.dim_rows(" in src


def _registry(tmp_path):
    os.makedirs(tmp_path / "updater")
    (tmp_path / "updater" / "registry.yaml").write_text(
        "sources:\n- source_id: unctad_a\n- source_id: unctad_b\n", encoding="utf-8")
    os.makedirs(tmp_path / "dist" / "titles")


def test_a_source_that_fails_is_named_counted_and_turns_the_exit_status(tool, monkeypatch, tmp_path, capsys):
    _registry(tmp_path)
    monkeypatch.setattr(tool, "ROOT", str(tmp_path))
    monkeypatch.setattr(tool, "_ds_for", lambda sid, reg: "US." + sid[-1].upper())

    def titles_for(sid, ds):
        if sid == "unctad_a":
            raise requests.HTTPError("403 for the dimension table")
        return {f"{sid}:076.M1": "Report - Brazil (Measure)"}, {"skipped": 0}
    monkeypatch.setattr(tool, "titles_for", titles_for)
    monkeypatch.setattr(sys, "argv", ["title_unctad_from_api.py", "unctad_a", "unctad_b", "--write"])
    rc = tool.main()
    out = capsys.readouterr().out
    assert rc == 1, "a failed source must not leave exit status 0"
    assert "unctad_a" in out and "FAILED HTTPError: 403" in out
    assert "sources that FAILED: 1" in out
    # the source that failed writes nothing; the one that worked is written
    assert sorted(os.listdir(tmp_path / "dist" / "titles")) == ["unctad_b.json"]


def test_every_source_read_gives_exit_status_0(tool, monkeypatch, tmp_path, capsys):
    _registry(tmp_path)
    monkeypatch.setattr(tool, "ROOT", str(tmp_path))
    monkeypatch.setattr(tool, "_ds_for", lambda sid, reg: "US.A")
    monkeypatch.setattr(tool, "titles_for", lambda sid, ds: ({f"{sid}:1.M1": "t"}, {"skipped": 0}))
    monkeypatch.setattr(sys, "argv", ["title_unctad_from_api.py", "unctad_a"])
    assert tool.main() == 0
    assert "sources that FAILED: 0" in capsys.readouterr().out
    assert os.listdir(tmp_path / "dist" / "titles") == [], "no --write, no file"


def test_a_refused_datacenter_lookup_is_an_error_not_an_empty_list(tool, monkeypatch):
    # It was swallowed: the lookup returned [] and the source read as "no UNCTAD dataset - skipped".
    monkeypatch.setattr(tool, "_REPORTS", [])
    monkeypatch.setattr(tool.requests, "get", lambda url, headers=None, timeout=None: _Resp(403, "Blocked"))
    with pytest.raises(requests.HTTPError):
        tool._advertised_reports()
    assert tool._REPORTS == [], "a failed lookup must not be cached as an answer"


def test_the_datacenter_names_are_read_when_it_answers(tool, monkeypatch):
    monkeypatch.setattr(tool, "_REPORTS", [])
    body = {"a": [{"reportName": "US.One"}, {"x": {"reportName": "US.Two"}}]}
    monkeypatch.setattr(tool.requests, "get", lambda url, headers=None, timeout=None: _Resp(200, body))
    assert tool._advertised_reports() == ["US.One", "US.Two"]


def test_a_source_whose_dataset_lookup_is_refused_is_a_failed_source(tool, monkeypatch, tmp_path, capsys):
    """The registry names no dataset for it, so the tool asks the datacenter; the datacenter refuses."""
    _registry(tmp_path)
    monkeypatch.setattr(tool, "ROOT", str(tmp_path))
    monkeypatch.setattr(tool, "_REPORTS", [])
    monkeypatch.setattr(tool.requests, "get", lambda url, headers=None, timeout=None: _Resp(403, "Blocked"))
    monkeypatch.setattr(tool, "titles_for", lambda sid, ds: pytest.fail("titles_for must not run"))
    monkeypatch.setattr(sys, "argv", ["title_unctad_from_api.py", "unctad_a"])
    assert tool.main() == 1
    out = capsys.readouterr().out
    assert "FAILED HTTPError: 403" in out and "no UNCTAD dataset" not in out
    assert "sources that FAILED: 1" in out


def test_a_source_with_no_store_file_says_so_and_is_a_failed_source(tool, monkeypatch, tmp_path, capsys):
    _registry(tmp_path)
    monkeypatch.setattr(tool, "ROOT", str(tmp_path))
    monkeypatch.setattr(tool, "_ds_for", lambda sid, reg: "US.A")
    monkeypatch.setattr(tool, "titles_for", lambda sid, ds: ({}, {"reason": "no store file"}))
    monkeypatch.setattr(sys, "argv", ["title_unctad_from_api.py", "unctad_a", "--write"])
    assert tool.main() == 1
    out = capsys.readouterr().out
    assert "FAILED no store file" in out and "titled=" not in out
    assert os.listdir(tmp_path / "dist" / "titles") == []


def test_a_source_the_registry_and_the_datacenter_do_not_name_is_skipped_with_its_reason(tool, monkeypatch, tmp_path, capsys):
    # Not a failure: the lookup answered, and it names no dataset for this id.
    _registry(tmp_path)
    monkeypatch.setattr(tool, "ROOT", str(tmp_path))
    monkeypatch.setattr(tool, "_ds_for", lambda sid, reg: None)
    monkeypatch.setattr(sys, "argv", ["title_unctad_from_api.py", "unctad_a"])
    assert tool.main() == 0
    assert "no UNCTAD dataset in its registry vintage_signal - skipped" in capsys.readouterr().out


def test_no_test_can_read_the_real_unctad_credentials(tool):
    # conftest puts test values in the environment for every test; creds() reads the environment
    # first, in the tool's own copy of the ingest as well as in jobs.ingest_unctad_ds.
    assert tool.iu.creds() == ("test-client-id-not-real", "test-api-key-not-real")
    import jobs.ingest_unctad_ds as j
    assert j.creds() == ("test-client-id-not-real", "test-api-key-not-real")
