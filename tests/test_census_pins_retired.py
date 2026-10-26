"""census: the request's own geography/time is never a pin; retired-upstream flows are skipped by ONE
dated list; a non-200 refusal is reported by its HTTP code.

THE PIN DEFECT (345fa2dec, 2026-09-21). _dims_from_store derives request pins from the store: any
column holding ONE value everywhere becomes `<col>=<value>`. `us` (EITS) and `world` (the 8 BETA
intltrade flows) hold one value everywhere precisely because they are the geography the request
asked for, so every EITS and BETA request sent us=1 / world=1 and Census answered HTTP 400
"unknown predicate variable". Run 2701 (2026-10-10) refused 29 of 45 flows. The pin sets below are
the 45 `pinning` lines of that run, verbatim (logs/local_heavy_updater_20261009-223942.log 55-128):
the fix must remove us/world (and qtax's NATION, a geography column with no predicateType in the
publisher's variables.json) from the 29, and leave every other pin - and the 16 flows that merged
that night - exactly as they were.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import sys
import types

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import _census_retired as R  # noqa: E402
from updater.strategies.fetchers import census as cs  # noqa: E402

# ---- the 45 pin sets of run 2701, verbatim ------------------------------------------------------
_E = {"us": "1", "time_slot_id": "0"}
RUN_2701 = {
    "eits/advm3": {"program_code": "M3ADV", "geo_level_code": "US", "error_data": "no", **_E},
    "eits/bfs": {"program_code": "BFS", "geo_level_code": "US", "error_data": "no", **_E},
    "eits/ftd": {"program_code": "FTD", "geo_level_code": "US", "error_data": "no", **_E},
    "eits/ftdadv": {"category_code": "CBG", "program_code": "FTDADV", "geo_level_code": "US",
                    "error_data": "no", **_E},
    "eits/hv": {"program_code": "HV", "geo_level_code": "US", **_E},
    "eits/m3": {"program_code": "M3", "geo_level_code": "US", "error_data": "no", **_E},
    "eits/marts": {"program_code": "MARTS", "geo_level_code": "US", **_E},
    "eits/mhs": {"program_code": "MHS", "geo_level_code": "US", "us": "1"},
    "eits/mhs2": {"program_code": "MHS2", "geo_level_code": "US", "error_data": "no", **_E},
    "eits/mrts": {"program_code": "MRTS", "geo_level_code": "US", **_E},
    "eits/mrtsadv": {"program_code": "MRTSADV", "geo_level_code": "US", **_E},
    "eits/mtis": {"program_code": "MTIS", "geo_level_code": "US", **_E},
    "eits/mwts": {"program_code": "MWTS", "geo_level_code": "US", **_E},
    "eits/mwtsadv": {"program_code": "MWTSADV", "geo_level_code": "US", **_E},
    "eits/qfr": {"seasonally_adj": "no", "program_code": "QFR", "geo_level_code": "US", **_E},
    "eits/qpr": {"seasonally_adj": "no", "program_code": "QPR", "geo_level_code": "US",
                 "error_data": "no", **_E},
    "eits/qss": {"program_code": "QSS", "geo_level_code": "US", **_E},
    "eits/qtax": {"ERROR_DATA": "no", "GEOCOMP": "00", "PROGRAM_CODE": "QTAX", "NATION": "1",
                  "TIME_SLOT_ID": "0", "us": "1"},
    "eits/resconst": {"program_code": "RESCONST", "geo_level_code": "US", **_E},
    "eits/ressales": {"program_code": "RESSALES", "geo_level_code": "US", **_E},
    "eits/vip": {"program_code": "VIP", "geo_level_code": "US", **_E},
    "intltrade/exports/enduseexport": {"SUMMARY_LVL": "DET", "DISTRICT": "-", "world": "1"},
    "intltrade/exports/hitechexport": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "DISTRICT": "-",
                                       "world": "1"},
    "intltrade/exports/sitcexport": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "DISTRICT": "-",
                                     "world": "1"},
    "intltrade/exports/usdaexport": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "DISTRICT": "-",
                                     "world": "1"},
    "intltrade/imports/enduseimport": {"SUMMARY_LVL": "DET", "DISTRICT": "-", "world": "1"},
    "intltrade/imports/hitechimport": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "DISTRICT": "-",
                                       "world": "1"},
    "intltrade/imports/sitcimport": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "DISTRICT": "-",
                                     "world": "1"},
    "intltrade/imports/usdaimport": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "DISTRICT": "-",
                                     "world": "1"},
    # the 16 flows that MERGED on 2026-10-10 - their pins are the control: unchanged
    "intltrade/exports/enduse": {"SUMMARY_LVL": "DET", "CTY_CODE": "-"},
    "intltrade/exports/hitech": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "CTY_CODE": "-"},
    "intltrade/exports/hs": {"SUMMARY_LVL": "DET", "CTY_CODE": "-", "DISTRICT": "-"},
    "intltrade/exports/naics": {"SUMMARY_LVL": "DET", "CTY_CODE": "-"},
    "intltrade/exports/sitc": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "CTY_CODE": "-"},
    "intltrade/exports/statehs": {"SUMMARY_LVL": "DET", "CTY_CODE": "-"},
    "intltrade/exports/statenaics": {"SUMMARY_LVL": "DET", "CTY_CODE": "-"},
    "intltrade/exports/usda": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "CTY_CODE": "-"},
    "intltrade/imports/enduse": {"SUMMARY_LVL": "DET", "CTY_CODE": "-"},
    "intltrade/imports/hitech": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "CTY_CODE": "-"},
    "intltrade/imports/hs": {"SUMMARY_LVL": "DET", "CTY_CODE": "-", "DISTRICT": "-"},
    "intltrade/imports/naics": {"SUMMARY_LVL": "DET", "CTY_CODE": "-"},
    "intltrade/imports/sitc": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "CTY_CODE": "-"},
    "intltrade/imports/statehs": {"SUMMARY_LVL": "DET", "SUMMARY_LVL2": "HSST", "COMM_LVL": "HS2",
                                  "CTY_CODE": "-"},
    "intltrade/imports/statenaics": {"SUMMARY_LVL": "DET", "CTY_CODE": "-"},
    "intltrade/imports/usda": {"SUMMARY_LVL": "DET", "COMM_LVL": "-", "CTY_CODE": "-",
                               "USDA": "1"},
}
REFUSED_2701 = sorted(f for f in RUN_2701 if f.startswith("eits/") or f.endswith(("export", "import")))
GEO = {"us", "world", "NATION"}


def test_the_fixture_is_the_whole_run():
    assert len(RUN_2701) == 45 and len(REFUSED_2701) == 29


@pytest.mark.parametrize("flow", REFUSED_2701)
def test_each_refused_flow_loses_only_its_geography(flow):
    """All 29, one by one (R1695): us/world/NATION go, every other pin stays with its value."""
    pins = RUN_2701[flow]
    sent, dropped = cs._request_pins(pins, cs._predicates_for(flow, {"US"}))
    assert not (set(sent) & GEO), sent
    assert sent == {k: v for k, v in pins.items() if k not in GEO}, sent
    assert set(dropped) == set(pins) & GEO and dropped, dropped
    assert list(sent) == [k for k in pins if k not in GEO], "order must be kept"


@pytest.mark.parametrize("flow", sorted(set(RUN_2701) - set(REFUSED_2701)))
def test_the_16_flows_that_merged_keep_every_pin(flow):
    """Control. CTY_CODE above all: dropping it would double count under published ids (R1066)."""
    pins = RUN_2701[flow]
    sent, dropped = cs._request_pins(pins, cs._predicates_for(flow, set()))
    assert sent == pins and dropped == []


@pytest.mark.parametrize("name", ["us", "world", "time", "US", "World"])
def test_a_geography_or_time_column_is_never_a_pin_even_with_no_predicate(name):
    sent, dropped = cs._request_pins({name: "1", "CTY_CODE": "-"}, ())
    assert sent == {"CTY_CODE": "-"} and dropped == [name]


def test_nation_goes_only_with_for_us():
    """NATION is qtax's nation-level geography column: dropped where for=us:* is sent, kept as an
    ordinary variable anywhere else (AR-388: scope it, do not ban the name)."""
    pins = {"NATION": "1", "GEOCOMP": "00"}
    assert cs._request_pins(pins, ())[0] == pins
    assert cs._request_pins(pins, [None])[0] == pins
    sent, dropped = cs._request_pins(pins, ["us:*"])
    assert sent == {"GEOCOMP": "00"} and dropped == ["NATION"]


def test_state_is_an_ordinary_variable_unless_the_flow_sends_for_state():
    """intltrade statehs/statenaics key on STATE as a plain variable (AR-388): kept there. EITS
    sends for=state:* for a flow with state rows: dropped there."""
    pins = {"STATE": "06", "SUMMARY_LVL": "DET"}
    assert cs._request_pins(pins, cs._predicates_for("intltrade/imports/statehs", set()))[0] == pins
    sent, dropped = cs._request_pins(pins, cs._predicates_for("eits/qtax", {"US", "CA"}))
    assert sent == {"SUMMARY_LVL": "DET"} and dropped == ["STATE"]


@pytest.mark.parametrize("name", ["get", "for", "in", "key", "ucgid"])
def test_a_query_parameter_name_is_never_a_pin(name):
    """A pin named like a parameter would overwrite it in _fetch's params dict."""
    sent, _d = cs._request_pins({name: "x", "A": "1"}, ())
    assert sent == {"A": "1"}


def test_the_hierarchy_of_every_sent_predicate_is_dropped():
    """Not only the listed names: whatever `for=` this flow sends, its hierarchy is not a pin."""
    sent, dropped = cs._request_pins({"COUNTY": "001", "A": "1"}, ["county:*", None])
    assert sent == {"A": "1"} and dropped == ["COUNTY"]


# ---- end to end through update(): a fake API that enforces the Census contract -------------------

def _ingest_stub():
    """The two jobs/ingest_census helpers update() uses (same rules, without that module's
    import-time makedirs into the checkout)."""
    def series_key_of(row, hidx, dim_cols, path):
        parts = [path.replace("timeseries/", "")]
        for c in dim_cols:
            i = hidx.get(c)
            if i is not None and i < len(row) and row[i] not in (None, ""):
                parts.append(f"{c}={row[i]}")
        return "|".join(parts)

    def parse_obs_date(d):
        y, m = str(d).split("-")[:2]
        return dt.date(int(y), int(m), 1)
    return types.SimpleNamespace(series_key_of=series_key_of, parse_obs_date=parse_obs_date)


class _Resp:
    def __init__(self, status, body=None, text=""):
        self.status_code = status
        self._body = body
        self.text = text or ("" if body is None else str(body))
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return self._body


class _CensusAPI:
    """Answers like api.census.gov: a parameter that is not a declared predicate is HTTP 400
    "unknown predicate variable"; otherwise the rows of the requested month (204 if none)."""

    def __init__(self, flows):
        self.flows = flows                  # flow -> dict(predicates=set, header=list, rows=list)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        flow = url.split("/timeseries/", 1)[1]
        spec = self.flows[flow]
        self.calls.append((flow, dict(params)))
        for k in params:
            if k in ("get", "time", "for", "key"):
                continue
            if k not in spec["predicates"]:
                return _Resp(400, text=f"error: unknown predicate variable: '{k}'")
        tv = params["time"]
        want = tv.replace("from ", "")
        rows = [r for r in spec["rows"] if r[spec["header"].index("time")].startswith(want)]
        if not rows:
            return _Resp(204)
        return _Resp(200, [spec["header"]] + rows)


def _store(path, table):
    pq.write_table(table, path)


def _setup(tmp_path, monkeypatch, *, beta_value="999"):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    monkeypatch.delenv(R.ENV_ACTIVATE, raising=False)
    monkeypatch.setattr(cs.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(cs, "_ingest", _ingest_stub)
    monkeypatch.setattr(cs, "_api_key", lambda: "test-key-not-real")
    monkeypatch.setattr(cs.time, "sleep", lambda s: None)
    # EITS marts: two series, two stored months; `us` is in the key AND a column, like the store.
    keys = ["eits/marts|category_code=44X72|program_code=MARTS|geo_level_code=US|us=1",
            "eits/marts|category_code=4400A|program_code=MARTS|geo_level_code=US|us=1"]
    _store(os.path.join(tmp_path, "eits__marts.parquet"), pa.table({
        "series_key": [keys[0], keys[1], keys[0], keys[1]],
        "obs_date": pa.array([dt.date(2026, 1, 1)] * 2 + [dt.date(2026, 2, 1)] * 2, pa.date32()),
        "cell_value": ["10", "20", "11", "21"],
        "category_code": ["44X72", "4400A", "44X72", "4400A"],
        "program_code": ["MARTS"] * 4, "geo_level_code": ["US"] * 4,
        "time_slot_id": ["0"] * 4, "time": ["2026-01", "2026-01", "2026-02", "2026-02"],
        "us": ["1"] * 4}))
    # The two ORIGINS of a geography pin (AR-388), one each: marts carries `us=1` IN its
    # series_key (the key-derived pins, like 20 of the 21 EITS flows); sitcexport carries `world`
    # only as a COLUMN, not in its key (the _single_valued_columns pins, like the 8 BETA flows).
    bkey = "intltrade/exports/sitcexport|SITC=00"
    _store(os.path.join(tmp_path, "intltrade__exports__sitcexport.parquet"), pa.table({
        "series_key": [bkey], "obs_date": pa.array([dt.date(2025, 12, 1)], pa.date32()),
        "SITC": ["00"], "SUMMARY_LVL": ["DET"], "COMM_LVL": ["-"], "DISTRICT": ["-"],
        "ALL_VAL_MO": ["100"], "time": ["2025-12"], "world": ["1"]}))
    eits_h = ["cell_value", "category_code", "program_code", "geo_level_code", "time_slot_id",
              "time", "us"]
    api = _CensusAPI({
        "eits/marts": {"predicates": {"cell_value", "category_code", "program_code",
                                      "geo_level_code", "time_slot_id", "error_data"},
                       "header": eits_h,
                       "rows": [["11", "44X72", "MARTS", "US", "0", "2026-02", "1"],
                                ["21", "4400A", "MARTS", "US", "0", "2026-02", "1"],
                                ["12", "44X72", "MARTS", "US", "0", "2026-03", "1"],
                                ["22", "4400A", "MARTS", "US", "0", "2026-03", "1"]]},
        "intltrade/exports/sitcexport": {
            "predicates": {"SITC", "SUMMARY_LVL", "COMM_LVL", "DISTRICT", "ALL_VAL_MO"},
            "header": ["SITC", "SUMMARY_LVL", "COMM_LVL", "DISTRICT", "ALL_VAL_MO", "time", "world"],
            "rows": [["00", "DET", "-", "-", beta_value, "2025-12", "1"]]},
    })
    monkeypatch.setattr(cs.requests, "Session", lambda: _Sess(api))
    return api


class _Sess:
    def __init__(self, api):
        self.api = api
        self.headers = {}

    def get(self, url, params=None, timeout=None):
        return self.api.get(url, params=params, timeout=timeout)


def _rows(tmp_path, stem):
    return pq.read_table(os.path.join(tmp_path, stem + ".parquet")).to_pylist()


def test_update_sends_no_geography_pin_and_merges_rows_returned(tmp_path, monkeypatch, capsys):
    api = _setup(tmp_path, monkeypatch)
    res = cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    assert res.status in ("ok", "no_change"), res
    for flow, params in api.calls:
        assert "us" not in params and "world" not in params, (flow, params)
    marts = [p for f, p in api.calls if f == "eits/marts"]
    assert marts and all(p["program_code"] == "MARTS" and p["time_slot_id"] == "0"
                         and p["geo_level_code"] == "US" and p["for"] == "us:*" for p in marts)
    beta = [p for f, p in api.calls if f == "intltrade/exports/sitcexport"]
    assert beta and all(p["SUMMARY_LVL"] == "DET" and p["COMM_LVL"] == "-"
                        and p["DISTRICT"] == "-" and p["for"] == "world:*" for p in beta)
    # rows returned == rows stored for the window (the post-merge proof's receipt, in miniature)
    mar = [r for r in _rows(tmp_path, "eits__marts") if r["obs_date"] == dt.date(2026, 3, 1)]
    assert sorted(r["cell_value"] for r in mar) == ["12", "22"]
    out = capsys.readouterr().out
    assert "eits/marts: not pinning ['us']" in out
    assert "intltrade/exports/sitcexport: not pinning ['world']" in out


def test_the_fixture_reproduces_both_origins_of_the_bad_pin(tmp_path, monkeypatch):
    """Guard on the fixture itself: the STORE still yields us (from the key) and world (from a
    column) as single-valued, so the update() tests above exercise the final filter on both."""
    _setup(tmp_path, monkeypatch)
    dims, _c, _s, pins = cs._dims_from_store(os.path.join(tmp_path, "eits__marts.parquet"))
    assert "us" in dims and pins.get("us") == "1"
    dims, _c, _s, pins = cs._dims_from_store(
        os.path.join(tmp_path, "intltrade__exports__sitcexport.parquet"))
    assert "world" not in dims and pins.get("world") == "1"
    assert cs._single_valued_columns(
        os.path.join(tmp_path, "intltrade__exports__sitcexport.parquet"), ["world"]) == {"world": "1"}


def test_the_beta_refresh_pass_stores_the_revised_boundary_value(tmp_path, monkeypatch):
    """Before the switch bites, the BETA flow is tailed and its 2025-12 value is replaced."""
    _setup(tmp_path, monkeypatch, beta_value="123")
    cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    rows = _rows(tmp_path, "intltrade__exports__sitcexport")
    assert [r["ALL_VAL_MO"] for r in rows] == ["123"], rows


def test_after_activation_the_beta_flow_is_not_requested_and_is_named(tmp_path, monkeypatch, capsys):
    api = _setup(tmp_path, monkeypatch)
    monkeypatch.setenv(R.ENV_ACTIVATE, "2026-01-01")
    res = cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    assert all(f != "intltrade/exports/sitcexport" for f, _p in api.calls), api.calls
    assert res.status in ("ok", "no_change"), "an explained exclusion must not demote the run"
    assert "RETIRED UPSTREAM" in (res.error or "") and "sitcexport" in res.error, res.error
    assert "intltrade/exports/sitcexport: RETIRED UPSTREAM - not fetched" in capsys.readouterr().out
    rows = _rows(tmp_path, "intltrade__exports__sitcexport")
    assert [r["ALL_VAL_MO"] for r in rows] == ["100"], "stored rows stay untouched"


def test_a_listed_flow_gone_early_is_skipped_loudly_not_a_whole_source_failure(tmp_path, monkeypatch,
                                                                               capsys):
    """Before its listed date, a listed flow answering 404 means the removal came early: named
    skip, the rest of the pass keeps its Result (AR-388 item 3)."""
    api = _setup(tmp_path, monkeypatch)
    real_get = api.get
    api.get = lambda url, params=None, timeout=None: (
        _Resp(404, text="error: unknown dataset") if url.endswith("sitcexport")
        else real_get(url, params=params, timeout=timeout))
    res = cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    assert res.status in ("ok", "no_change"), res
    assert "sitcexport (HTTP 404 before its listed date)" in (res.error or ""), res.error
    assert "RETIRED UPSTREAM EARLY - HTTP 404" in capsys.readouterr().out


def test_a_404_on_an_unlisted_flow_still_fails_the_source(tmp_path, monkeypatch):
    """The early-retirement path is for LISTED flows only; an unlisted 404 stays a refusal."""
    api = _setup(tmp_path, monkeypatch)
    real_get = api.get
    api.get = lambda url, params=None, timeout=None: (
        _Resp(404, text="error: unknown dataset") if url.endswith("marts")
        else real_get(url, params=params, timeout=timeout))
    with pytest.raises(DefinitiveError) as ei:
        cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    assert "REFUSED by the API" in str(ei.value) and "HTTP 404 x1" in str(ei.value)


def test_a_refusal_is_reported_by_its_http_code(tmp_path, monkeypatch):
    """With the pin fix undone, the API refuses both flows: the note must say HTTP 400 and the
    API's reason, not "returned 200 but parsed 0 rows"."""
    _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(cs, "_request_pins", lambda pins, preds=(): (dict(pins), []))
    with pytest.raises(DefinitiveError) as ei:
        cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    msg = str(ei.value)
    assert "2/2 sub-unit(s) REFUSED by the API with a non-200 status (HTTP 400 x2)" in msg, msg
    assert "unknown predicate variable" in msg
    assert "returned 200 but parsed 0 rows" not in msg


def test_a_mixed_failure_names_both_classes(tmp_path, monkeypatch):
    """One refused flow and one genuine boundary-empty flow: each reported as what it is."""
    api = _setup(tmp_path, monkeypatch)
    api.flows["intltrade/exports/sitcexport"]["rows"] = []        # boundary re-fetch: nothing
    real = cs._request_pins
    monkeypatch.setattr(cs, "_request_pins",
                        lambda pins, preds=(): (dict(pins), []) if "us" in pins else real(pins, preds))
    with pytest.raises(DefinitiveError) as ei:
        cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    msg = str(ei.value)
    assert "1/2 sub-unit(s) REFUSED by the API" in msg and "eits/marts" in msg, msg
    assert "1/2 sub-unit(s) returned 200 but parsed 0 rows" in msg and "sitcexport" in msg, msg


def test_a_non_refusal_definitive_error_keeps_the_shared_text(tmp_path, monkeypatch):
    """Only refusals are reworded; a plain structural break still reads as finalize says."""
    api = _setup(tmp_path, monkeypatch)
    api.flows["eits/marts"]["rows"] = []
    api.flows["intltrade/exports/sitcexport"]["rows"] = []
    with pytest.raises(DefinitiveError) as ei:
        cs.update(types.SimpleNamespace(source_id="census", unit_id="_all"), None)
    assert "returned 200 but parsed 0 rows" in str(ei.value)
    assert "REFUSED" not in str(ei.value)


# ---- the shared list and its switch -------------------------------------------------------------

STORED_8 = {"intltrade/exports/enduseexport", "intltrade/exports/hitechexport",
            "intltrade/exports/sitcexport", "intltrade/exports/usdaexport",
            "intltrade/imports/enduseimport", "intltrade/imports/hitechimport",
            "intltrade/imports/sitcimport", "intltrade/imports/usdaimport"}


def test_the_list_holds_all_18_beta_datasets_dated_and_with_a_notice():
    assert len(R.RETIRED_UPSTREAM) == 18
    assert STORED_8 <= set(R.RETIRED_UPSTREAM)
    assert all(f.endswith(("export", "import")) for f in R.RETIRED_UPSTREAM)
    assert {d for d, _n in R.RETIRED_UPSTREAM.values()} == {dt.date(2026, 11, 19)}
    assert all("2026-11-19" in n for _d, n in R.RETIRED_UPSTREAM.values())
    assert R.OPERATOR_ACTIVATED_ON is None, "activation is a separate, reviewed one-line commit"


def test_the_safety_net_bites_on_the_removal_day_not_before():
    paths = sorted(STORED_8) + ["eits/marts", "intltrade/exports/sitc"]
    kept, gone = R.split_retired(paths, today=dt.date(2026, 11, 18), environ={})
    assert kept == paths and gone == []
    kept, gone = R.split_retired(paths, today=dt.date(2026, 11, 19), environ={})
    assert kept == ["eits/marts", "intltrade/exports/sitc"]
    assert sorted(p for p, _e, _n in gone) == sorted(STORED_8)


def test_the_operator_date_brings_it_forward_never_later():
    env = {R.ENV_ACTIVATE: "2026-11-02"}
    assert R.effective_from("intltrade/exports/sitcexport", env) == dt.date(2026, 11, 2)
    late = {R.ENV_ACTIVATE: "2027-01-01"}
    assert R.effective_from("intltrade/exports/sitcexport", late) == dt.date(2026, 11, 19)
    kept, gone = R.split_retired(["intltrade/exports/sitcexport"], today=dt.date(2026, 11, 2),
                                 environ=env)
    assert kept == [] and gone[0][1] == dt.date(2026, 11, 2)


def test_the_committed_operator_date_works_without_the_environment(monkeypatch):
    monkeypatch.setattr(R, "OPERATOR_ACTIVATED_ON", dt.date(2026, 11, 5))
    assert R.effective_from("intltrade/imports/usdaimport", {}) == dt.date(2026, 11, 5)
    assert R.effective_from("intltrade/imports/usdaimport",
                            {R.ENV_ACTIVATE: "2026-11-03"}) == dt.date(2026, 11, 3)


def test_a_malformed_operator_date_is_loud_and_ignored(capsys):
    env = {R.ENV_ACTIVATE: "next tuesday"}
    assert R.effective_from("intltrade/exports/sitcexport", env) == dt.date(2026, 11, 19)
    assert "IGNORED" in capsys.readouterr().out


def test_an_unlisted_flow_is_never_excluded():
    kept, gone = R.split_retired(["intltrade/exports/sitc", "eits/qtax"],
                                 today=dt.date(2030, 1, 1), environ={R.ENV_ACTIVATE: "2026-01-01"})
    assert kept == ["intltrade/exports/sitc", "eits/qtax"] and gone == []


def test_both_spellings_of_a_path_are_recognised():
    for p in ("timeseries/intltrade/exports/sitcexport", "intltrade/exports/sitcexport",
              "/timeseries/intltrade/exports/sitcexport/"):
        assert R.effective_from(p, {}) == dt.date(2026, 11, 19), p


# ---- the full re-ingest job reads the same list --------------------------------------------------

def _load_job(monkeypatch):
    """Import jobs/_census_intltrade_full.py without its import-time side effects (it creates
    directories in the checkout and reads data/raw/census/ts_meta.json)."""
    import builtins
    import io
    real_open = builtins.open
    monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)
    monkeypatch.setattr(builtins, "open", lambda f, *a, **k: io.StringIO("[]")
                        if str(f).endswith("ts_meta.json") else real_open(f, *a, **k))
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "jobs", "_census_intltrade_full.py")
    spec = importlib.util.spec_from_file_location("_census_intltrade_full_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(builtins, "open", real_open)
    base = ["enduse", "hitech", "hs", "naics", "porths", "sitc", "statehs", "statenaics", "usda"]
    meta = {}
    for side, suf in (("exports", "export"), ("imports", "import")):
        for b in base:
            meta[f"timeseries/intltrade/{side}/{b}"] = {"group": "intltrade", "geos": [],
                                                        "allvars": ["CTY_CODE"]}
            meta[f"timeseries/intltrade/{side}/{b}{suf}"] = {"group": "intltrade",
                                                             "geos": ["world"], "allvars": []}
    mod.META = meta
    called = []
    mod.ingest = lambda p, budget_s=None: (called.append(p) or (0, 0, 0, []))
    return mod, called


@pytest.mark.parametrize("argv,expect", [
    ([], 18),                                                        # default: all 36 -> 18 kept
    (["--beta"], 0),
    (["--only", "timeseries/intltrade/exports/sitcexport"], 0),
    (["--match", "sitc"], 2),                                        # sitc + sitc, not the 2 BETA
])
def test_the_job_skips_retired_paths_on_every_route_after_activation(monkeypatch, capsys, argv, expect):
    mod, called = _load_job(monkeypatch)
    monkeypatch.setenv(R.ENV_ACTIVATE, "2026-01-01")
    monkeypatch.setattr(sys, "argv", ["job"] + argv)
    mod.main()
    assert len(called) == expect, called
    assert not any(R.flow_id(p) in R.RETIRED_UPSTREAM for p in called), called
    if expect < (36 if not argv else 99):
        assert "RETIRED UPSTREAM - not fetched" in capsys.readouterr().out


def test_the_job_still_pulls_the_8_before_activation(monkeypatch):
    """The refresh pass needs this: before the date --beta reaches exactly BETA_OK."""
    mod, called = _load_job(monkeypatch)
    monkeypatch.delenv(R.ENV_ACTIVATE, raising=False)
    monkeypatch.setattr(R, "today_utc", lambda: dt.date(2026, 10, 26))
    monkeypatch.setattr(sys, "argv", ["job", "--beta"])
    mod.main()
    assert {R.flow_id(p) for p in called} == STORED_8


# ---- the first-pass ingester reads the same list (AR-388 item 2) ---------------------------------

def _load_ingest_census(monkeypatch, tmp_path):
    monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "jobs", "ingest_census.py")
    spec = importlib.util.spec_from_file_location("_ingest_census_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.OUT = str(tmp_path)                    # the manifest it writes lands in the test's dir
    meta = {f"timeseries/{f}": {"group": "intltrade", "allvars": [], "path": f}
            for f in R.RETIRED_UPSTREAM}
    meta["timeseries/intltrade/exports/sitc"] = {"group": "intltrade", "allvars": ["CTY_CODE"]}
    meta["timeseries/eits/marts"] = {"group": "eits", "allvars": []}
    mod.load_meta = lambda: meta
    called = []
    mod.ingest_dataset = lambda m, max_child_states=None, budget_s=None: (
        called.append(m) or (0, 0, 0, False))
    return mod, meta, called


@pytest.mark.parametrize("argv,expect", [([], 2), (["--group", "intltrade"], 1),
                                         (["--match", "sitc"], 1)])
def test_ingest_census_skips_retired_paths_after_activation(monkeypatch, tmp_path, argv, expect):
    mod, meta, called = _load_ingest_census(monkeypatch, tmp_path)
    monkeypatch.setenv(R.ENV_ACTIVATE, "2026-01-01")
    monkeypatch.setattr(sys, "argv", ["ingest"] + argv)
    mod.main()
    assert len(called) == expect, called
    assert not any(m is meta[f"timeseries/{f}"] for m in called for f in R.RETIRED_UPSTREAM)


def test_ingest_census_still_reaches_them_before_activation(monkeypatch, tmp_path):
    mod, meta, called = _load_ingest_census(monkeypatch, tmp_path)
    monkeypatch.delenv(R.ENV_ACTIVATE, raising=False)
    monkeypatch.setattr(R, "today_utc", lambda: dt.date(2026, 10, 26))
    monkeypatch.setattr(sys, "argv", ["ingest", "--group", "intltrade"])
    mod.main()
    assert len(called) == 19, len(called)
