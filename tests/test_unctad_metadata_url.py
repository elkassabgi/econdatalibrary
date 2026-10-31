"""unctad: the keyless metadata and catalogue addresses carry NO language segment.

From the 2026-10-29 22:45Z updater-daily run on, every unctad probe failed with
"404 Client Error ... /api/reportMetadata/<DS>/en" (run 41054519780: 68 such lines), so no unctad unit
could probe or fetch. Measured 2026-10-31T00:45Z: `.../reportMetadata/US.TradeMerchTotal/en` answers
404 with 0 B; `.../reportMetadata/US.TradeMerchTotal` answers 200 with 4,399 B; `.../datacenter/en`
answers 404 and `.../datacenter` 200.

What these tests pin:
- the updater and the title tool never ask an `/en` address: a planted server answers 404 for any
  `/en` address and 200 without it, exactly as UNCTAD does today;
- the answer without `/en` is the answer the code was built on. The fixture is the 2026-10-31 no-/en
  body, and its sha256 equals the sha256 recorded for the `/en` body on 2026-10-20 in
  docs/evidence/unctad_trademerchgr_withdrawal_20261020.json - the same bytes - so the release token
  (version|lastUpdated, the format every stored vintage has) and the layout are unchanged;
- the catalogue answer without `/en` changed SHAPE (`reportName` became `name`); the title tool reads
  both, and a 200 with no report name is an error, not an empty list;
- no code under jobs/, updater/ or tools/ builds an UNCTAD `/en` address (AST scan of the strings the
  code uses; docstrings and comments are history and may name the old address).

Fixture tests/fixtures/unctad/datacenter_noen_20261031.json is the 2026-10-31T00:53:14Z answer of
`/api/datacenter` (600,770 B, sha256 698e7e48...c545c7) with each report's long `notes` and
`keywords` text dropped; nothing else was changed. Hermetic: no network.
"""
import ast
import hashlib
import importlib.util
import json
import os
import re
import sys

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.ingest_unctad_ds as j  # noqa: E402
from updater.strategies.fetchers import _unctad as U  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "unctad")
META_FILE = os.path.join(FIX, "reportMetadata_US.TradeMerchTotal.json")
DC_FILE = os.path.join(FIX, "datacenter_noen_20261031.json")
RECEIPT = os.path.join(ROOT, "docs", "evidence", "unctad_trademerchgr_withdrawal_20261020.json")
DC_RECEIPT = os.path.join(ROOT, "docs", "evidence", "unctad_datacenter_catalogue_20261020.json")
EN_URL = "https://unctadstat-api.unctad.org/api/reportMetadata/US.TradeMerchTotal/en"
HOST = "https://unctadstat-api.unctad.org/api"

_EN_SEGMENT = re.compile(r"/en(?:$|[/?#])")


class _Resp:
    def __init__(self, url, status, body=b""):
        self.url, self.status_code, self.content = url, status, body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error: Not Found for url: {self.url}",
                                     response=self)

    def json(self):
        return json.loads(self.content)


def _unctad_today(asked, bodies):
    """UNCTAD as measured 2026-10-31: an /en address is 404 with an empty body; without it, 200."""
    def get(url, headers=None, timeout=None, **kw):
        asked.append(url)
        if _EN_SEGMENT.search(url):
            return _Resp(url, 404)
        if url not in bodies:
            return _Resp(url, 404)
        return _Resp(url, 200, bodies[url])
    return get


def _meta_bytes():
    return open(META_FILE, "rb").read()


def _receipt_en_probe():
    rec = json.load(open(RECEIPT, encoding="utf-8"))
    return {p["url"]: p for p in rec["probes"]}[EN_URL]


# ---------------------------------------------------------------- the updater's metadata request

def test_report_metadata_asks_the_address_without_en(monkeypatch):
    asked = []
    monkeypatch.setattr(j.requests, "get", _unctad_today(
        asked, {f"{HOST}/reportMetadata/US.TradeMerchTotal": _meta_bytes()}))
    meta = j.report_metadata("US.TradeMerchTotal")
    assert asked == [f"{HOST}/reportMetadata/US.TradeMerchTotal"]
    assert meta["version"] == 10011 and meta["lastUpdated"] == "2026-09-18T09:41:30"


def test_the_planted_server_refuses_the_en_address(monkeypatch):
    """Control: the fake really behaves like UNCTAD today, so a builder that adds /en fails above."""
    asked = []
    get = _unctad_today(asked, {f"{HOST}/reportMetadata/US.TradeMerchTotal": _meta_bytes()})
    with pytest.raises(requests.HTTPError, match="404"):
        get(EN_URL).raise_for_status()
    assert get(f"{HOST}/reportMetadata/US.TradeMerchTotal").status_code == 200


def test_the_metadata_address_has_no_language_segment_for_any_registered_dataset():
    import yaml
    reg = yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))["sources"]
    names = set()
    for e in reg:
        m = re.search(r"reportMetadata/([A-Za-z0-9_]+\.[A-Za-z0-9_]+)",
                      str((e.get("adapter") or {}).get("vintage_signal", "")))
        if m:
            names.add(m.group(1))
    assert len(names) >= 90, f"positive control: the registry names {len(names)} UNCTAD datasets"
    for ds in sorted(names):
        url = j.report_metadata_url(ds)
        assert url == f"{HOST}/reportMetadata/{ds}", url
        assert not _EN_SEGMENT.search(url), url


def test_the_fetcher_probe_reads_the_release_token_through_the_no_en_address(monkeypatch):
    asked = []
    monkeypatch.setattr(j.requests, "get", _unctad_today(
        asked, {f"{HOST}/reportMetadata/US.TradeMerchTotal": _meta_bytes()}))
    current_vintage, _update = U.make("US.TradeMerchTotal", "unctad_trademerchtotal")
    assert current_vintage(None) == "10011|2026-09-18T09:41:30"
    assert asked and not any(_EN_SEGMENT.search(u) for u in asked), asked


# ---------------------------------------------------------------- the answer is the one the code reads

def test_the_no_en_answer_is_the_recorded_en_answer_byte_for_byte():
    body = _meta_bytes()
    probe = _receipt_en_probe()
    assert probe["http_status"] == 200
    assert len(body) == probe["bytes"] == 4399
    assert hashlib.sha256(body).hexdigest() == probe["sha256"], (
        "the fixture is no longer the /en answer recorded on 2026-10-20 - re-measure before trusting it")


def test_the_no_en_answer_gives_the_same_token_and_layout_as_the_en_answer():
    meta = json.loads(_meta_bytes())
    fields = _receipt_en_probe()["fields"]
    # the token every stored vintage carries: version|lastUpdated, e.g. '10003|2026-03-11T08:15:49'
    tok = U._token(meta)
    assert tok == f"{fields['version']}|{fields['lastUpdated']}" == "10011|2026-09-18T09:41:30"
    assert re.fullmatch(r"\d+\|\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d", tok)
    assert meta["name"] == fields["name"] and meta["title"] == fields["title"]
    # the layout the store's keys were built with: <Economy>.<Flow>.M<measure>, annual Year axis
    assert j.dataset_layout(meta) == (["Economy", "Flow"], "Year", True, False, ["5011", "0100", "4023"])
    tdim = j.dataset_time_dim(meta)
    assert (tdim["name"], tdim["field"], tdim["codetype"]) == ("Years", "Year", "number")


# ---------------------------------------------------------------- the title tool's catalogue request

@pytest.fixture
def tool():
    spec = importlib.util.spec_from_file_location("title_unctad_url_under_test",
                                                  os.path.join(ROOT, "tools", "title_unctad_from_api.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _receipt_names():
    out = []

    def walk(o):
        if isinstance(o, dict):
            if o.get("reportName"):
                out.append(o["reportName"])
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(json.load(open(DC_RECEIPT, encoding="utf-8")))
    return out


def test_the_title_tool_reads_the_catalogue_without_en_in_its_new_shape(tool, monkeypatch):
    asked = []
    monkeypatch.setattr(tool, "_REPORTS", [])
    monkeypatch.setattr(tool.requests, "get", _unctad_today(
        asked, {f"{HOST}/datacenter": open(DC_FILE, "rb").read()}))
    got = tool._advertised_reports()
    assert asked == [f"{HOST}/datacenter"]
    old = _receipt_names()
    assert len(old) == 99 and len(got) == 99
    assert sorted(got) == sorted(old), "the same 99 reports the 2026-10-20 /en catalogue named"


def test_the_title_tool_still_reads_the_old_shape(tool, monkeypatch):
    monkeypatch.setattr(tool, "_REPORTS", [])
    monkeypatch.setattr(tool.requests, "get", _unctad_today(
        [], {f"{HOST}/datacenter": open(DC_RECEIPT, "rb").read()}))
    assert sorted(tool._advertised_reports()) == sorted(_receipt_names())


def test_a_catalogue_answer_with_no_report_name_is_an_error(tool, monkeypatch):
    monkeypatch.setattr(tool, "_REPORTS", [])
    body = json.dumps([{"id": 1, "title": "folder", "reports": [{"id": 2, "title": "no name"}],
                        "subFolders": []}]).encode()
    monkeypatch.setattr(tool.requests, "get", _unctad_today([], {f"{HOST}/datacenter": body}))
    with pytest.raises(RuntimeError, match="advertised no report name"):
        tool._advertised_reports()
    assert tool._REPORTS == [], "an empty answer must not be cached"


def test_a_folder_title_is_not_read_as_a_report_name(tool, monkeypatch):
    monkeypatch.setattr(tool, "_REPORTS", [])
    body = json.dumps([{"id": 1, "title": "Trade", "name": "US.NotAReport",
                        "reports": [{"id": 2, "name": "US.Real", "title": "t"}], "subFolders": []}]).encode()
    monkeypatch.setattr(tool.requests, "get", _unctad_today([], {f"{HOST}/datacenter": body}))
    assert tool._advertised_reports() == ["US.Real"]


@pytest.mark.parametrize("vs, ds", [
    ("GET /api/reportMetadata/US.TradeFoodProcByCat -> version|lastUpdated", "US.TradeFoodProcByCat"),
    ("GET https://unctadstat-api.unctad.org/api/reportMetadata/US.TradeMerchTotal\n(keyless) -> x",
     "US.TradeMerchTotal"),
    ("GET /api/reportMetadata/US.CriticalMinerals_TradeByPart -> v", "US.CriticalMinerals_TradeByPart"),
    ("GET /api/reportMetadata/US.Cpi_A/en -> version|lastUpdated", "US.Cpi_A"),       # the old spelling
])
def test_the_dataset_is_read_from_a_vintage_signal_with_or_without_en(tool, vs, ds):
    assert tool._ds_for("unctad_x", {"unctad_x": {"adapter": {"vintage_signal": vs}}}) == ds


def test_every_registry_entry_that_names_reportmetadata_yields_its_dataset(tool):
    """R1553 class: a regex that silently finds nothing would make the title tool skip the source."""
    import yaml
    reg = {e["source_id"]: e for e in yaml.safe_load(
        open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))["sources"]}
    naming = [s for s, e in reg.items()
              if "reportMetadata/" in str((e.get("adapter") or {}).get("vintage_signal", ""))]
    assert len(naming) >= 90, f"positive control: {len(naming)} entries name reportMetadata"
    missed = [s for s in naming if not tool._ds_for(s, reg)]
    assert not missed, missed


# ---------------------------------------------------------------- no code builds an /en address

_BUILT_EN = re.compile(r"(?:reportMetadata|datacenter)(?:/[^/\s\"']*)?/en(?![A-Za-z0-9_])")


def _code_strings(src: str):
    """Every string the code USES: constants and f-strings (placeholders as '{}'), minus docstrings."""
    tree = ast.parse(src)
    docs = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                docs.add(id(body[0].value))
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            yield node.lineno, "".join(v.value if isinstance(v, ast.Constant) else "{}" for v in node.values)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs:
            yield node.lineno, node.value


def _offenders(src: str):
    try:
        return [(ln, s) for ln, s in _code_strings(src) if _BUILT_EN.search(s)]
    except SyntaxError:
        # A file this interpreter cannot parse is not skipped: every non-comment line is read instead,
        # which can only over-report (a docstring naming the old address would then count).
        return [(ln, line) for ln, line in enumerate(src.splitlines(), 1)
                if _BUILT_EN.search(line) and not line.lstrip().startswith("#")]


def test_the_scanner_finds_a_planted_en_builder_and_ignores_history():
    planted = 'META = "x"\ndef f(ds):\n    """GET /api/reportMetadata/{DS}/en is history."""\n' \
              '    return f"{META}/reportMetadata/{ds}/en"\nDC = "https://h/api/datacenter/en"\n'
    assert sorted(ln for ln, _ in _offenders(planted)) == [4, 5]
    clean = 'def f(ds):\n    # GET .../reportMetadata/{DS}/en was the old address\n' \
            '    return f"{META}/reportMetadata/{ds}"\nDC = "https://h/api/datacenter"\n'
    assert _offenders(clean) == []


def test_no_code_under_jobs_updater_or_tools_builds_an_unctad_en_address():
    scanned, hits = 0, []
    for top in ("jobs", "updater", "tools"):
        for dirpath, _dirs, files in os.walk(os.path.join(ROOT, top)):
            for fn in files:
                p = os.path.join(dirpath, fn)
                if fn.endswith(".py"):
                    src = open(p, encoding="utf-8-sig").read()          # some files carry a BOM
                    scanned += 1
                    hits += [(os.path.relpath(p, ROOT), ln, s[:120]) for ln, s in _offenders(src)]
                elif fn.endswith((".ps1", ".sh", ".cmd")):
                    for ln, line in enumerate(open(p, encoding="utf-8", errors="replace"), 1):
                        if "unctadstat" in line and _BUILT_EN.search(line) and not line.lstrip().startswith("#"):
                            hits.append((os.path.relpath(p, ROOT), ln, line.strip()[:120]))
    assert scanned > 300, f"positive control: only {scanned} python files scanned - wrong root?"
    assert "jobs/ingest_unctad_ds.py" in {os.path.relpath(os.path.join(dp, f), ROOT).replace(os.sep, "/")
                                          for dp, _d, fs in os.walk(os.path.join(ROOT, "jobs")) for f in fs}
    assert not hits, hits
