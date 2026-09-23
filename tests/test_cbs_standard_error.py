"""CBS's published STANDARD ERROR is its own series - never an observation inside a value series.

Four CBS tables (37471, 7042mc, 7068gi, 7069LS) name their period dimension
`Perioden(Incl|Inclusief)Standaardfout` and carry, beside the years, one code '0000X000' titled
'Standaardfout'. CBS's own description (fetched 2026-09-23, identical in 7042mc, 7068gi and
7069LS): the standard error is nearly the same in every year, so ONE average standard error is
shown for the table; multiplied by 1.65 or 1.96 it gives the 90% and 95% margins.

Two earlier parsers each got it wrong in a different direction:
  - dated it year 0000 + 2, 31 July, and served it as the FIRST OBSERVATION of every value
    series - so three catalogue rows advertised a start date of 0002-07-31 (R1079);
  - then refused year 0000 and DISCARDED it: 37471's re-pull on 2026-09-05 dropped all 22 of
    its standard errors from the store ("replacing 352 served rows with 330 (93.8%;
    discards={'unparsed:X0': 1})"), let through by the 50% replace floor.

The owner's decision (2026-09-22) is to keep the figure and move the marker out of the time axis
into the series key. Every fixture here is CBS's real code list for the table it names - not an
invented shape (R1077, R801).
"""
import datetime as dt
import json
import os
import urllib.parse

import pyarrow.parquet as pq
import pytest

from jobs import ingest_cbs_nl as mod

SE = mod.SE_TAG

# CBS's real period code lists, read 2026-09-23 from ODataFeed /<tid>/<TimeDimension>.
T7042MC = {"0000X000": "Standaardfout", **{f"{y}JJ00": str(y) for y in range(1981, 2010)}}
T37471 = {**{f"{y}JJ00": str(y) for y in (1991, 1995, *range(1997, 2010))}, "0000X000": "Standaardfout"}   # no 1996
T37450 = {f"{y}X000": f"{y} tot {y + 5}" for y in range(1861, 2007, 5)}   # 5-year spans, NO SE
# The ENGLISH twins, read the same day: key 'Stf ' (trailing space - get_period_titles strips
# map keys, so the map holds 'Stf'), title 'Standard error', and BARE four-digit years.
T7042ENG = {"Stf": "Standard error", **{str(y): str(y) for y in range(1981, 2010)}}
T7068ENG = {"Stf": "Standard error", **{str(y): str(y) for y in range(1981, 2001)}}
T7069ENG = {"Stf": "Standard error", **{str(y): str(y) for y in range(1989, 2001)}}


def test_the_real_code_lists_are_the_ones_CBS_publishes():
    """Guard the fixtures themselves: a hand-edit that made them easier would make every test
    below measure an invented table."""
    assert len(T7042MC) == 30 and list(T7042MC)[0] == "0000X000"
    assert len(T37471) == 16 and list(T37471)[-1] == "0000X000"
    assert len(T37450) == 30 and "2006X000" in T37450


@pytest.mark.parametrize("titles,first_year", [(T7042MC, 1981), (T37471, 1991)])
def test_the_standard_error_is_keyed_and_dated_to_the_start_of_the_span(titles, first_year):
    d, tag = mod.resolve_period("0000X000", titles)
    assert tag == SE
    assert d == dt.date(first_year, 1, 1), d


@pytest.mark.parametrize("titles,first_year", [(T7042ENG, 1981), (T7068ENG, 1981), (T7069ENG, 1989)])
def test_the_english_twins_standard_error_is_keyed_too(titles, first_year):
    """The adversarial review found these: the same average standard error, under 'Stf ' /
    'Standard error', which the first version of this rule could not see - and which the parser
    had always dropped, so these tables never carried it at all."""
    for raw in ("Stf ", "Stf"):                      # the data rows carry the trailing space
        d, tag = mod.resolve_period(raw, titles)
        assert tag == SE and d == dt.date(first_year, 1, 1), (raw, d, tag)


def test_ordinary_years_are_untouched():
    assert mod.resolve_period("1981JJ00", T7042MC) == (dt.date(1981, 12, 31), None)
    assert mod.resolve_period("2009JJ00", T37471) == (dt.date(2009, 12, 31), None)


def test_CONTROL_an_X000_span_table_is_untouched():
    """37450's X000 codes are five-year spans ('1861 tot 1866'), one of the fifteen meanings
    X000 carries (R588). None of them is a standard error, and none may change here."""
    before = {k: mod._PARSE_EX_RAW(k) for k in T37450}
    after = {k: mod.resolve_period(k, T37450) for k in T37450}
    assert after == before
    assert all(tag != SE for _, tag in after.values())


def test_CONTROL_a_0000X000_titled_something_else_is_NOT_a_standard_error():
    """The shape only nominates a candidate; CBS's Title decides."""
    titles = dict(T7042MC, **{"0000X000": "Overig"})
    assert mod.resolve_period("0000X000", titles) == (None, None)   # the old refusal, unchanged


class _NoTitlesAllowed:
    """A title map that fails the test if it is consulted. `fetched` is read by the annual
    point-title counter and must stay readable."""
    fetched = False

    def get(self, *a, **k):
        pytest.fail("an ordinary period code cost a CBS title request")

    def codes(self):
        pytest.fail("an ordinary period code cost a CBS title request")


def test_an_ordinary_code_never_costs_a_title_request():
    """The shape check exists so the standard-error rule does not fetch titles for the ~5,000
    tables that have no standard error. Only '0000X000' may consult them."""
    for code in ("1981JJ00", "2022MM01", "2022KW03", "2000SJ00", "19990924", "2022"):
        mod.resolve_period(code, _NoTitlesAllowed())


def test_without_a_title_the_candidate_is_dropped_and_counted_never_guessed():
    before = dict(mod.PERIOD_DISCARDS)
    assert mod.resolve_period("0000X000", {"1981JJ00": "1981"}) == (None, None)
    assert mod.discards_since(before).get("no-title-for-code:standard-error-candidate") == 1


def test_a_table_whose_other_codes_are_not_plain_years_is_refused_not_guessed():
    titles = {"0000X000": "Standaardfout", "1981JJ00": "1981", "1981MM01": "1981 januari"}
    before = dict(mod.PERIOD_DISCARDS)
    assert mod.resolve_period("0000X000", titles) == (None, None)
    assert mod.discards_since(before).get("standard-error-span-not-annual") == 1


# =========================================================================================
# End to end: the REAL ingest_table, only the HTTP boundary replaced
# =========================================================================================

TOPICS = ["Anticonceptiepil16Tot50Jaar_382", "IZAIZR_105"]     # 7042mc's real column names
DIM = "PeriodenInclStandaardfout"


def _install(monkeypatch, codes, fail_columns=False):
    """A 7042mc-shaped table: no dimension columns, two topics, CBS's real period codes."""
    rows = []
    for i, code in enumerate(codes):
        v = 1.5 if code == "0000X000" else 10.0 + i
        rows.append({"ID": i, DIM: code, TOPICS[0]: v, TOPICS[1]: v + 100})

    def get_json(url):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        if "/DataProperties" in url:
            return {"value": [{"Key": DIM, "Type": "TimeDimension"}]
                    + [{"Key": t, "Type": "Topic", "Datatype": "Double"} for t in TOPICS]}
        if f"/{DIM}" in url and "TypedDataSet" not in url:
            return {"value": [{"Key": c, "Title": T7042MC[c]} for c in codes]}
        skip = int(q.get("$skip", ["0"])[0])
        top = int(q.get("$top", ["50"])[0])
        return {"value": rows[skip:skip + top]}

    monkeypatch.setattr(mod, "PAGE", 50, raising=False)
    monkeypatch.setattr(mod, "get_json", get_json)
    monkeypatch.setattr(mod, "get_table_columns",
                        (lambda t: None) if fail_columns else (lambda t: ["ID", DIM, *TOPICS]))
    monkeypatch.setattr(mod, "table_row_count", lambda t: len(rows))
    return rows


OLD = 1_000_000_000                    # 2001: a time Windows accepts, older than any real write
CODES = ["0000X000", "1981JJ00", "1982JJ00", "1983JJ00"]
STAMP = "2011-11-09T02:00:00"


def _read(d, tid="T"):
    t = pq.read_table(os.path.join(d, f"{tid}.parquet"))
    out = {}
    for k, od, v in zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                        t.column("value").to_pylist()):
        out.setdefault(k, []).append((od, v))
    return out


def test_a_crawl_keeps_the_standard_error_as_its_own_series(tmp_path, monkeypatch):
    d = str(tmp_path)
    _install(monkeypatch, CODES)
    n = mod.ingest_table("T", "t", d, STAMP)
    got = _read(d)
    assert n == 8, got                               # 3 years x 2 topics + 1 SE x 2 topics
    for topic in TOPICS:
        parent = got[f"T:{topic}"]
        assert [od for od, _ in parent] == [dt.date(y, 12, 31) for y in (1981, 1982, 1983)]
        se = got[f"T:{SE}:{topic}"]
        assert se == [(dt.date(1981, 1, 1), 1.5 if topic == TOPICS[0] else 101.5)], se
    # the parent series carry NO row in year 0002, and no key carries a date before 1981
    assert all(od.year >= 1981 for rows in got.values() for od, _ in rows)


def _held(tmp_path, monkeypatch):
    """A table already held at CBS's current stamp - the state of 7042mc today."""
    d = str(tmp_path)
    _install(monkeypatch, CODES)
    mod.ingest_table("T", "t", d, STAMP)
    assert mod.load_modified(d).get("T") == STAMP
    return d


def test_CONTROL_without_a_request_an_unrevised_table_is_skipped(tmp_path, monkeypatch):
    d = _held(tmp_path, monkeypatch)
    mtime = os.path.getmtime(os.path.join(d, "T.parquet"))
    mod.ingest_table("T", "t", d, STAMP)
    assert os.path.getmtime(os.path.join(d, "T.parquet")) == mtime


def test_a_request_re_pulls_an_unrevised_table_once_and_is_closed(tmp_path, monkeypatch):
    d = _held(tmp_path, monkeypatch)
    mod.record_repull_requests(d, ["T"], "test")
    assert "T" in mod.load_repull_requests(d)
    os.utime(os.path.join(d, "T.parquet"), (OLD, OLD))    # so a rewrite is visible
    mod.ingest_table("T", "t", d, STAMP)
    assert os.path.getmtime(os.path.join(d, "T.parquet")) > OLD, "the request did not re-pull"
    assert "T" not in mod.load_repull_requests(d), "a successful re-pull must close the request"
    assert mod.load_modified(d).get("T") == STAMP, "the manifest keeps CBS's stamp - nothing invented"
    assert not os.path.exists(os.path.join(d, "T.repull.json")), "no marker left open"


def test_a_request_survives_a_transient_failure_and_is_honoured_next_pass(tmp_path, monkeypatch):
    d = _held(tmp_path, monkeypatch)
    mod.record_repull_requests(d, ["T"], "test")
    _install(monkeypatch, CODES, fail_columns=True)       # the column probe fails this pass
    mod.ingest_table("T", "t", d, STAMP)
    assert "T" in mod.load_repull_requests(d), "a transient failure must not consume the request"
    _install(monkeypatch, CODES)
    os.utime(os.path.join(d, "T.parquet"), (OLD, OLD))
    mod.ingest_table("T", "t", d, STAMP)
    assert os.path.getmtime(os.path.join(d, "T.parquet")) > OLD
    assert "T" not in mod.load_repull_requests(d)


def test_a_request_for_a_vintage_already_re_pulled_to_ZERO_is_closed_not_looped(tmp_path, monkeypatch):
    """R589: the ZERO registry stops a deterministic failure. The request must stop with it."""
    d = _held(tmp_path, monkeypatch)
    mod._note_vintage(d, mod.ZERO_FILE, "T", STAMP, {})
    mod.record_repull_requests(d, ["T"], "test")
    mod.ingest_table("T", "t", d, STAMP)
    assert "T" not in mod.load_repull_requests(d)


def test_the_repull_flag_needs_a_reason_and_never_starts_a_crawler(tmp_path, monkeypatch):
    d = str(tmp_path)
    for t in ("7042mc", "37471"):
        mod.record_modified(d, t, STAMP)             # held here
    monkeypatch.setattr(mod, "OUT", d)
    monkeypatch.setattr(mod, "get_catalog",
                        lambda: pytest.fail("--repull must record and exit, not crawl (R600)"))
    monkeypatch.setattr("sys.argv", ["ingest_cbs_nl.py", "--repull=7042mc,37471:standard errors",
                                     "--only", "7042mc"])       # --only must not start one either
    mod.main()
    assert set(mod.load_repull_requests(d)) == {"7042mc", "37471"}
    monkeypatch.setattr("sys.argv", ["ingest_cbs_nl.py", "--repull=7042mc"])
    with pytest.raises(SystemExit):
        mod.main()


def test_the_repull_flag_refuses_a_table_that_is_not_held(tmp_path, monkeypatch):
    """A mistyped id - the match is case-sensitive - would otherwise wait for ever, doing nothing."""
    d = str(tmp_path)
    mod.record_modified(d, "7042mc", STAMP)
    with pytest.raises(SystemExit, match="not held"):
        mod.record_repull_requests(d, ["7042MC"], "typo")
    assert mod.load_repull_requests(d) == {}, "nothing may be recorded when any id is refused"


def test_the_repull_flag_does_not_swallow_a_later_flag(tmp_path, monkeypatch):
    d = str(tmp_path)
    mod.record_modified(d, "7042mc", STAMP)
    monkeypatch.setattr(mod, "OUT", d)
    seen = []
    monkeypatch.setattr(mod, "record_accepts", lambda out, ids: seen.append(ids))
    monkeypatch.setattr(mod, "get_catalog", lambda: pytest.fail("no crawler"))
    monkeypatch.setattr("sys.argv", ["ingest_cbs_nl.py", "--repull=7042mc:r", "--accept-shrink=X1"])
    mod.main()
    assert seen == [["X1"]], "an --accept-shrink after --repull was dropped"


def test_pending_requests_are_listed_where_the_crawler_reports_its_registries(tmp_path, monkeypatch):
    d = str(tmp_path)
    mod.record_modified(d, "7042mc", STAMP)
    mod.record_repull_requests(d, ["7042mc"], "r")
    lines = []
    monkeypatch.setattr(mod, "log", lambda m: lines.append(m))
    mod.registry_summary(d)
    assert any("re-pull requests pending" in m and "7042mc" in m for m in lines), lines


def test_a_re_pull_that_would_DROP_the_standard_error_is_refused(tmp_path, monkeypatch):
    """R1079's shape: a dropped standard error is 1 row in 4 here and 22 in 352 in 37471 - far
    above REPLACE_FLOOR - so only an explicit refusal stops it deleting a published figure."""
    d = _held(tmp_path, monkeypatch)                  # held WITH its standard error
    before = _read(d)
    mod.record_repull_requests(d, ["T"], "test")
    rows = _install(monkeypatch, CODES)

    real = mod.get_json

    def no_se_title(url):                             # CBS now lists no title for the SE code
        out = real(url)
        if f"/{DIM}" in url and "TypedDataSet" not in url:
            out = {"value": [v for v in out["value"] if v["Key"] != "0000X000"]}
        return out
    monkeypatch.setattr(mod, "get_json", no_se_title)
    assert rows                                        # the data still carries the SE row
    mod.ingest_table("T", "t", d, STAMP)
    assert _read(d) == before, "the served copy must be kept"
    reg = json.load(open(os.path.join(d, mod.REFUSED_FILE), encoding="utf-8"))
    assert "standard error dropped" in reg["T"]["reason"]
