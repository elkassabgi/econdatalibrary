"""bea v1: the seven sound datasets' group files are refreshed in place (2026-09-23; review R1104).

The real update() runs over a real store directory (local backend); the BEA calls are faked at the
ingester's fetch_* seam, so the merge, the profile, the cycle and the state are real.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("BEA_API_KEY", "test-key-not-real")

import jobs.ingest_bea_full as ig  # noqa: E402
from updater.errors import DefinitiveError, TransientError  # noqa: E402
from updater.strategies.fetchers import bea  # noqa: E402

THIS_YEAR = dt.date.today().year
_REAL_FETCH_TABLE_FREQ = ig.fetch_table_freq      # captured before any fixture fakes it


def _write(path, rows, tsid=False):
    cols = {"series_key": [r[0] for r in rows],
            "obs_date": pa.array([r[1] for r in rows], pa.date32()),
            "value": [r[2] for r in rows]}
    if tsid:
        cols["time_series_id"] = [f"ts-{r[0]}" for r in rows]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table(cols), path)


def _rows(path):
    t = pq.read_table(path).sort_by([("series_key", "ascending"), ("obs_date", "ascending")])
    return [(r["series_key"], r["obs_date"], r["value"]) for r in t.to_pylist()]


D = lambda y, m=12, d=31: dt.date(y, m, d)  # noqa: E731


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(bea.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(bea, "api_key", lambda name: "test-key-not-real")
    monkeypatch.setattr(ig, "load_manifest", lambda: {"param_values": {}})
    y = THIS_YEAR - 1
    # a NIPA table with an EXACT duplicate row (the review measured 54 such files)
    _write(str(tmp_path / "NIPA" / "T10101.parquet"),
           [("A191RC:A", D(y - 1), 1.0), ("A191RC:A", D(y), 2.0), ("A191RC:A", D(y), 2.0)])
    # a discontinued FixedAssets table (newest 1990)
    _write(str(tmp_path / "FixedAssets" / "FAAt999.parquet"), [("K1", D(1990), 5.0)])
    # IIP carries an extra column
    _write(str(tmp_path / "IIP" / "all.parquet"), [("Assets:X:A", D(y), 7.0)], tsid=True)
    # Regional collides across tables and must NOT be touched
    _write(str(tmp_path / "Regional" / "CAGDP2.parquet"), [("13:48317", D(y), 245.0)])
    calls = []

    def _tf(dataset, table, freqs=("A", "Q", "M"), year="ALL", strict=False):
        calls.append((dataset, table, year, strict))
        return ["A191RC:A", "A191RC:A"], [D(y), D(y + 1)], [2.5, 3.0]

    def _fa(table, year="ALL", strict=False):
        calls.append(("FixedAssets", table, year, strict))
        return ["K1"], [D(1990)], [5.0]

    def _iip(M, year="ALL", strict=False):
        calls.append(("IIP", "all", year, strict))
        return ["Assets:X:A"], [D(y + 1)], [8.0], ["ts-Assets:X:A"]
    monkeypatch.setattr(ig, "fetch_table_freq", _tf)
    monkeypatch.setattr(ig, "fetch_fixedassets", _fa)
    monkeypatch.setattr(ig, "fetch_iip", _iip)
    return tmp_path, calls, y


def test_a_sound_group_is_refreshed_in_place_with_its_window(store):
    d, calls, y = store
    before_regional = (d / "Regional" / "CAGDP2.parquet").read_bytes()
    res = bea.update(None, None)
    nipa = [c for c in calls if c[0] == "NIPA"]
    assert nipa and nipa[0][3] is True, "strict calls only"
    assert nipa[0][2] == "ALL", "the FIRST cycle is a full pull (review R1106)"
    assert _rows(str(d / "NIPA" / "T10101.parquet")) == [
        ("A191RC:A", D(y - 1), 1.0), ("A191RC:A", D(y), 2.5), ("A191RC:A", D(y + 1), 3.0)], \
        "revised, extended, and the exact duplicate collapsed without the guard refusing"
    assert (d / "Regional" / "CAGDP2.parquet").read_bytes() == before_regional
    assert not any(c[0] == "Regional" for c in calls)
    assert all(u.split("/")[0] in bea.SOUND for u in bea._group_units(str(d))), "Regional is not a unit"
    assert not (d / "bea.parquet").exists(), "the shadowed copy is no longer written"
    assert res.status in ("ok", "no_change"), (res.status, res.error)


def test_the_extra_column_is_carried_through_the_merge(store):
    d, calls, y = store
    bea.update(None, None)
    t = pq.read_table(str(d / "IIP" / "all.parquet"))
    assert "time_series_id" in t.schema.names and t.num_rows == 2


def test_after_the_first_full_cycle_a_group_gets_its_year_window(store):
    d, calls, y = store
    bea.update(None, None)                                     # cycle 1: Year=ALL, closes
    calls.clear()
    bea.update(None, None)                                     # cycle 2: windowed
    nipa = [c for c in calls if c[0] == "NIPA"]
    assert nipa[0][2] == ",".join(str(v) for v in range(y + 1 - bea.LOOKBACK_YEARS, THIS_YEAR + 2)), nipa


def test_a_discontinued_group_is_skipped_until_its_yearly_full_repull(store, monkeypatch):
    d, calls, y = store
    bea.update(None, None)                                     # first cycle: everything, Year=ALL
    assert any(c[0] == "FixedAssets" and c[2] == "ALL" for c in calls)
    calls.clear()
    bea.update(None, None)
    assert not any(c[0] == "FixedAssets" for c in calls), "newest 1990: skipped until its yearly pull"
    st = json.loads((d / bea.GROUP_STATE).read_text())
    st["FixedAssets/FAAt999.parquet"]["last_full"] = (dt.date.today()
                                                     - dt.timedelta(days=bea.FULL_REPULL_DAYS)).isoformat()
    (d / bea.GROUP_STATE).write_text(json.dumps(st))
    calls.clear()
    bea.update(None, None)
    fa = [c for c in calls if c[0] == "FixedAssets"]
    assert fa and fa[0][2] == "ALL", "a year after its last full pull it is re-pulled with Year=ALL"
    assert json.loads((d / bea.GROUP_STATE).read_text())["FixedAssets/FAAt999.parquet"]["last_full"] \
        == dt.date.today().isoformat()


def test_conflicting_pairs_refuse_the_merge_and_keep_the_group_owed(store):
    d, calls, y = store
    _write(str(d / "NIPA" / "T10101.parquet"), [("A191RC:A", D(y), 2.0), ("A191RC:A", D(y), 9.0)])
    before = (d / "NIPA" / "T10101.parquet").read_bytes()
    with pytest.raises(DefinitiveError):
        bea.update(None, None)
    assert (d / "NIPA" / "T10101.parquet").read_bytes() == before
    assert "NIPA/T10101.parquet" in bea.RotationCycle(str(d), ["NIPA/T10101.parquet"]).unvisited()


def test_an_empty_answer_for_a_window_inside_the_data_is_a_failure(store, monkeypatch):
    d, calls, y = store
    monkeypatch.setattr(ig, "fetch_table_freq", lambda *a, **k: ([], [], []))
    res = bea.update(None, None)
    assert res.status == "partial" and "0 rows" in (res.error or "")
    assert "NIPA/T10101.parquet" in bea.RotationCycle(str(d), ["NIPA/T10101.parquet"]).unvisited()


def test_a_strict_call_failure_keeps_the_group_owed(store, monkeypatch):
    d, calls, y = store

    def _boom(*a, **k):
        raise ig.CallFailed("retries exhausted")
    monkeypatch.setattr(ig, "fetch_table_freq", _boom)
    res = bea.update(None, None)
    assert res.status == "partial" and "fetch failed" in (res.error or "")
    assert "NIPA/T10101.parquet" in bea.RotationCycle(str(d), ["NIPA/T10101.parquet"]).unvisited()


def test_a_budget_stop_defers_the_unvisited_groups(store, monkeypatch):
    class _Dl:
        def __init__(self, minutes=None):
            self.n = 0

        def spent(self):
            self.n += 1
            return self.n > 1
    monkeypatch.setattr(bea, "Deadline", _Dl)
    res = bea.update(None, None)
    assert res.status == "partial" and "budget" in (res.error or "")


def test_no_sound_group_visible_is_an_unreachable_store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(bea.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(bea, "api_key", lambda name: "k")
    monkeypatch.setattr(ig, "load_manifest", lambda: {"param_values": {}})
    with pytest.raises(TransientError, match="unreachable"):
        bea.update(None, None)


# ---- the ingester's strict call ------------------------------------------------------------
def test_a_strict_call_raises_when_its_retries_are_exhausted(monkeypatch):
    class _S:
        def get(self, *a, **k):
            raise ConnectionError("down")
    monkeypatch.setattr(ig, "_session", lambda: _S())
    monkeypatch.setattr(ig, "_rate_limit_acquire", lambda: None)
    monkeypatch.setattr(ig.time, "sleep", lambda s: None)
    with pytest.raises(ig.CallFailed):
        ig.call(datasetname="NIPA", TableName="T1", strict=True)
    assert ig.call(datasetname="NIPA", TableName="T1") == [], "the bulk ingester's default is unchanged"


def test_fetch_table_freq_parses_exactly_as_the_ingest_did(monkeypatch):
    rows = {"A": [{"SeriesCode": "X", "TimePeriod": "2025", "DataValue": "1,234.5"},
                  {"SeriesCode": "X", "TimePeriod": "2026", "DataValue": "(NA)"}],
            "Q": [{"SeriesCode": "X", "TimePeriod": "2026Q1", "DataValue": "7"}], "M": []}
    monkeypatch.setattr(ig, "call", lambda **p: rows[p["Frequency"]])
    assert ig.fetch_table_freq("NIPA", "T1") == (["X:A", "X:Q"], [D(2025), D(2026, 1, 1)], [1234.5, 7.0])


def test_exact_duplicates_past_three_percent_merge_under_the_exact_ratio(store, monkeypatch):
    """Review R1104: 54 sound-dataset files carry exact duplicates (worst 16.6%). A merge collapses
    them; at the default 0.97 guard it refuses. The ratio is set to exactly (rows - dups) / rows."""
    d, calls, y = store
    rows = [(f"S{i}:A", D(y), float(i)) for i in range(5)]
    _write(str(d / "NIPA" / "T10101.parquet"), rows + rows)          # 10 rows, 5 exact duplicates
    monkeypatch.setattr(ig, "fetch_table_freq",
                        lambda *a, **k: ([r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows]))
    res = bea.update(None, None)
    assert pq.read_metadata(str(d / "NIPA" / "T10101.parquet")).num_rows == 5, (res.status, res.error)
    assert "merge refused" not in (res.error or "")


def test_a_strict_call_raises_when_every_attempt_gets_a_5xx(monkeypatch):
    class _R:
        status_code, content = 500, b""

    class _S:
        def get(self, *a, **k):
            return _R()
    monkeypatch.setattr(ig, "_session", lambda: _S())
    monkeypatch.setattr(ig, "_rate_limit_acquire", lambda: None)
    monkeypatch.setattr(ig, "_rate_limit_record_bytes", lambda n: None)
    monkeypatch.setattr(ig.time, "sleep", lambda s: None)
    with pytest.raises(ig.CallFailed, match="retries exhausted"):
        ig.call(datasetname="NIPA", TableName="T1", strict=True)


def test_a_revision_only_pass_is_a_change_and_names_the_revised_key(store, monkeypatch):
    """Review R1106 P4: values revised, no new period -> it read no_change and the served CSVs were
    never re-derived."""
    d, calls, y = store
    bea.update(None, None)                                     # first cycle
    monkeypatch.setattr(ig, "fetch_table_freq",
                        lambda *a, **k: (["A191RC:A", "A191RC:A"], [D(y), D(y + 1)], [2.6, 3.0]))
    res = bea.update(None, None)
    assert res.status == "ok" and res.changed_keys == {"A191RC:A": D(y).isoformat()}, \
        (res.status, res.changed_keys)


def test_an_identical_refetch_is_a_quiet_pass_not_a_structural_break(store):
    """Review R1106 P3: identical data plus discontinued skips tripped the all-empty guard."""
    d, calls, y = store
    bea.update(None, None)
    res = bea.update(None, None)                               # same answers again
    assert res.status in ("ok", "no_change") and res.changed_keys == {}, (res.status, res.error)


def test_many_quiet_groups_are_not_a_wholesale_outage(store, monkeypatch):
    """Review R1106 P3 at scale: finalize's all-empty guard fires past 10 attempted-and-empty units,
    so a clean pass over the 449 sound groups read as a structural break."""
    d, calls, y = store
    for i in range(12):
        _write(str(d / "NIPA" / f"T9{i:04d}.parquet"), [("A191RC:A", D(y), 2.5), ("A191RC:A", D(y + 1), 3.0)])
    bea.update(None, None)                                     # first cycle
    res = bea.update(None, None)                               # every group re-fetches identical data
    assert res.status in ("ok", "no_change"), (res.status, res.error)


def test_a_stored_key_whose_call_came_back_empty_keeps_the_group_owed(store, monkeypatch):
    """Review R1106 P1: one call that errored silently must not close the cycle. The Q call
    answered nothing although the store holds Q data in the window."""
    d, calls, y = store
    _write(str(d / "NIPA" / "T10101.parquet"), [("A191RC:A", D(y), 2.0), ("B230RC:Q", D(y), 5.0)])
    res = bea.update(None, None)                               # the fetch returns only A191RC:A
    assert res.status == "partial" and "coverage" in (res.error or "")
    assert "NIPA/T10101.parquet" in bea.RotationCycle(str(d), ["NIPA/T10101.parquet"]).unvisited()


def test_a_series_the_publisher_dropped_from_an_answered_call_is_not_owed(store, monkeypatch, capsys):
    """Dry run 2026-09-23: IIP GoldReserveAssets:ChgPosXRate:A (one stored row, 2025 = 0.0) was
    absent from a call that returned the rest of its type. Owed, it kept IIP out of every cycle."""
    d, calls, y = store
    _write(str(d / "IIP" / "all.parquet"), [("Assets:X:A", D(y), 7.0), ("Assets:Gone:A", D(y), 0.0)],
           tsid=True)
    res = bea.update(None, None)                               # IIP returns only Assets:X:A
    assert res.status != "partial", res.error
    assert ("Assets:Gone:A", D(y), 0.0) in _rows(str(d / "IIP" / "all.parquet")), "stored rows kept"
    assert "IIP/all.parquet: BEA sent no value for 1 stored series" in capsys.readouterr().out
    assert "BEA sent no value for 1 stored series" in (res.error or ""), \
        f"the note the digest shows must carry it, not only stdout (AR-130): {res.error!r}"


def test_the_call_unit_is_read_off_each_dataset_key():
    assert bea._call_unit("NIPA", "A191RC:Q") == "Q"
    assert bea._call_unit("UnderlyingGDPbyIndustry", "T210:11:A") == "A"
    assert bea._call_unit("IIP", "GoldReserveAssets:ChgPosXRate:A") == "GoldReserveAssets"
    assert bea._call_unit("IntlServTrade", "Travel:Exp:AllAffiliations:Canada") == "Canada"
    assert bea._call_unit("IntlServSTA", "Ch:De:Ind:Mexico") == "Mexico"
    assert bea._call_unit("FixedAssets", "K100001") == ""


def test_a_strict_call_refuses_an_error_it_does_not_recognise(monkeypatch):
    class _R:
        status_code, content = 200, b"{}"

        def json(self):
            return {"BEAAPI": {"Error": {"APIErrorCode": "999", "APIErrorDescription": "odd"}}}

    asked = []

    class _S:
        def get(self, *a, **k):
            asked.append(1)
            return _R()
    monkeypatch.setattr(ig, "_session", lambda: _S())
    monkeypatch.setattr(ig, "_rate_limit_acquire", lambda: None)
    monkeypatch.setattr(ig, "_rate_limit_record_bytes", lambda n: None)
    with pytest.raises(ig.CallFailed, match="not recognised"):
        ig.call(datasetname="IntlServSTA", AreaOrCountry="X", strict=True)
    assert len(asked) == 1, f"a verdict is not retried: {len(asked)} requests (and ~4.4 min of sleeps)"
    assert ig.call(datasetname="IntlServSTA", AreaOrCountry="X") == [], "the ingester default is unchanged"


# BEA's VERBATIM answer for a table/frequency that does not exist (live 2026-09-23 10:13Z, NIPA
# T10101 M; the same envelope for T10111 A and NIUnderlyingDetail U001A A). Review R1107.
_NO_SUCH_FREQUENCY = {"BEAAPI": {"Results": {"Error": {
    "APIErrorDescription": "Error retrieving NIPA data.", "APIErrorCode": "201",
    "ErrorDetail": {"Description": "Data for this table and frequency are not currently available. "
                                   "Please check BEAs release schedule for more information."}}}}}


def _real_calls(monkeypatch, answer_for):
    """Undo the fixture's fetch fake: the real fetch_table_freq and call() run, BEA's HTTP is faked."""
    real = ig
    monkeypatch.setattr(real, "fetch_table_freq", _REAL_FETCH_TABLE_FREQ)
    monkeypatch.setattr(real, "_rate_limit_acquire", lambda: None)
    monkeypatch.setattr(real, "_rate_limit_record_bytes", lambda n: None)
    monkeypatch.setattr(real.time, "sleep", lambda s: None)

    class _R:
        status_code, content = 200, b"{}"

        def __init__(self, body):
            self.body = body

        def json(self):
            return self.body

    class _S:
        def get(self, url, params=None, timeout=None):
            return _R(answer_for(params))
    monkeypatch.setattr(real, "_session", lambda: _S())
    return real


def test_a_frequency_the_table_does_not_have_is_no_data_not_a_failure(store, monkeypatch):
    """Review R1107: 241 of 252 NIPA tables have no M. Read as a failure, nearly every NIPA group
    stayed owed for ever and the value that WAS fetched was thrown away."""
    d, calls, y = store
    _real_calls(monkeypatch, lambda p: _NO_SUCH_FREQUENCY if p.get("Frequency") != "A" else
                {"BEAAPI": {"Results": {"Data": [
                    {"SeriesCode": "A191RC", "TimePeriod": str(y), "DataValue": "2"},
                    {"SeriesCode": "A191RC", "TimePeriod": str(y + 1), "DataValue": "3"}]}}})
    res = bea.update(None, None)
    assert res.status != "partial", res.error
    assert ("A191RC:A", D(y + 1), 3.0) in _rows(str(d / "NIPA" / "T10101.parquet"))
    cyc = json.loads((d / bea.RotationCycle.FILE).read_text())
    assert cyc.get("completed_utc"), f"the one pass reached every group, so the cycle closes: {cyc}"


def test_no_such_frequency_on_a_frequency_the_store_holds_keeps_the_group_owed(store, monkeypatch):
    """Negative control: the same envelope for a frequency the table DOES have must not close the
    group - the coverage check is what guards it."""
    d, calls, y = store
    _write(str(d / "NIPA" / "T10101.parquet"), [("A191RC:A", D(y), 2.0), ("A191RC:Q", D(y), 9.0)])
    _real_calls(monkeypatch, lambda p: _NO_SUCH_FREQUENCY if p.get("Frequency") != "A" else
                {"BEAAPI": {"Results": {"Data": [
                    {"SeriesCode": "A191RC", "TimePeriod": str(y), "DataValue": "2"}]}}})
    res = bea.update(None, None)
    assert res.status == "partial" and "coverage" in (res.error or ""), res.error
    assert "NIPA/T10101.parquet" in bea.RotationCycle(str(d), ["NIPA/T10101.parquet"]).unvisited()


def test_the_classifier_reads_the_error_detail(monkeypatch):
    err = _NO_SUCH_FREQUENCY["BEAAPI"]["Results"]["Error"]
    assert ig._classify_error(err, strict=True) == "nodata"
    odd = dict(err, ErrorDetail={"Description": "Something else went wrong."})
    assert ig._classify_error(odd, strict=True) == "fatal", "only the no-such-frequency wording is empty"


# ---- review round 3 (R1109): its probes P6, P7, P9, P10, pinned. Real update(), call() and parse. ----
_REAL_IIP = ig.fetch_iip                            # captured before any fixture fakes it
_REAL_INTLSERVTRADE = ig.fetch_intlservtrade

# An outage worded with the no-data phrase: the coverage check, not the classifier, must catch it.
_OUTAGE = {"BEAAPI": {"Results": {"Error": {
    "APIErrorDescription": "Error retrieving data.", "APIErrorCode": "201",
    "ErrorDetail": {"Description": "The service is not currently available. Try later."}}}}}


def _data(rows):
    return {"BEAAPI": {"Results": {"Data": rows}}}


def _nipa_answer(p, y):
    if p.get("datasetname") == "NIPA":
        if p.get("Frequency") != "A":
            return _NO_SUCH_FREQUENCY
        return _data([{"SeriesCode": "A191RC", "TimePeriod": str(y), "DataValue": "2"}])
    return None


def test_an_outage_worded_as_no_data_on_one_country_keeps_the_group_owed(store, monkeypatch):
    d, calls, y = store
    _write(str(d / "IntlServTrade" / "all.parquet"),
           [("S:Exp:Aff:C1", D(y), 1.0), ("S:Exp:Aff:C2", D(y), 2.0)], tsid=True)
    monkeypatch.setattr(ig, "load_manifest", lambda: {"param_values": {
        "IntlServTrade": {"AreaOrCountry": [{"Key": "C1"}, {"Key": "C2"}]}}})

    def answer(p):
        a = _nipa_answer(p, y)
        if a is not None:
            return a
        if p.get("datasetname") == "IntlServTrade":
            if p.get("AreaOrCountry") == "C2":
                return _OUTAGE
            return _data([{"TypeOfService": "S", "TradeDirection": "Exp", "Affiliation": "Aff",
                           "TimePeriod": str(y), "DataValue": "1.5"}])
        return _data([])
    _real_calls(monkeypatch, answer)
    monkeypatch.setattr(ig, "fetch_intlservtrade", _REAL_INTLSERVTRADE)
    res = bea.update(None, None)
    assert res.status == "partial" and "IntlServTrade/all.parquet: coverage" in (res.error or ""), res.error
    assert "IntlServTrade/all.parquet" in bea.RotationCycle(str(d), ["IntlServTrade/all.parquet"]).unvisited()


def test_a_frequency_outage_at_the_edge_of_the_window_is_caught(store, monkeypatch):
    """R1109 P7: the mutant 'coverage window = newest year only' survived every earlier test."""
    d, calls, y = store
    _write(str(d / "NIPA" / "T10101.parquet"),
           [("A191RC:A", D(y), 2.0), ("A191RC:Q", D(y - bea.LOOKBACK_YEARS, 10, 1), 9.0)])
    _real_calls(monkeypatch, lambda p: _nipa_answer(p, y) or _data([]))
    res = bea.update(None, None)
    assert res.status == "partial" and "coverage" in (res.error or ""), res.error


def test_a_budget_stopped_pass_reports_the_store_total(store, monkeypatch):
    """R1109 P9 (the ssb defect, AR-124 P7): groups after the stop dropped out of obs."""
    d, calls, y = store

    class _DL:
        n = 0

        def __init__(self, minutes=None):
            pass

        def spent(self):
            _DL.n += 1
            return _DL.n > 1

    monkeypatch.setattr(bea, "Deadline", _DL)
    units = bea._group_units(str(d))
    res = bea.update(None, None)
    after = sum(pq.read_metadata(str(d / u)).num_rows for u in units)
    assert res.status == "partial"
    # EXACTLY the store as it stands after the pass - an overcount (a group counted twice at the
    # stop, review AR-130 A7) fails as surely as an omission
    assert res.obs == after, (res.obs, after)


def test_a_series_bea_now_sends_blank_does_not_hold_the_cycle_open(store, monkeypatch):
    """R1109 P10, inverted. Dry run 2026-09-23: IIP GoldReserveAssets:ChgPosXRate:A and
    StDebtSecAssets:ChgPosPrice:A are stored (2025 = 0.0) and BEA now sends DataValue '' for every
    year (reviewer, live, 10:48Z). The parse drops blanks; owing the key held IIP - and so the
    whole cycle - open for ever, and NIPA was never asked again."""
    d, calls, y = store
    _write(str(d / "IIP" / "all.parquet"),
           [("Gold:Pos:A", D(y), 7.0), ("Gold:ChgPosXRate:A", D(y), 0.0)], tsid=True)
    monkeypatch.setattr(ig, "load_manifest", lambda: {"param_values": {
        "IIP": {"TypeOfInvestment": [{"Key": "Gold"}]}}})
    nipa_asks = []

    def answer(p):
        if p.get("datasetname") == "NIPA":
            nipa_asks.append(p.get("Frequency"))
        a = _nipa_answer(p, y)
        if a is not None:
            return a
        if p.get("datasetname") == "IIP":
            return _data([
                {"Component": "Pos", "Frequency": "A", "TimePeriod": str(y), "DataValue": "7.5"},
                {"Component": "ChgPosXRate", "Frequency": "A", "TimePeriod": str(y), "DataValue": ""}])
        return _data([])
    _real_calls(monkeypatch, answer)
    monkeypatch.setattr(ig, "fetch_iip", _REAL_IIP)
    first = bea.update(None, None)
    assert first.status != "partial", first.error
    assert json.loads((d / bea.RotationCycle.FILE).read_text()).get("completed_utc"), "the cycle closes"
    assert ("Gold:ChgPosXRate:A", D(y), 0.0) in _rows(str(d / "IIP" / "all.parquet")), \
        "never-shrink keeps the value BEA no longer publishes"
    nipa_asks.clear()
    bea.update(None, None)
    assert nipa_asks, "the next cycle reaches NIPA again"


def test_a_group_killed_mid_work_counts_as_a_failed_attempt(store, monkeypatch):
    """AR-127 P5: a raise or kill inside a group never reached visit(); begin() marks it in flight."""
    d, calls, y = store

    class _Kill(BaseException):
        pass

    def _boom(*a, **k):
        raise _Kill()
    monkeypatch.setattr(ig, "fetch_table_freq", _boom)
    with pytest.raises(_Kill):
        bea.update(None, None)
    units = bea._group_units(str(d))
    assert bea.RotationCycle(str(d), units).failing == {"NIPA/T10101.parquet": 1}
