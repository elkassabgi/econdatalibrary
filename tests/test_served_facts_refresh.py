"""catalog/served_facts.json: tools/refresh_served_facts.py writes it, catalog/gen_site.py refuses to build without
a current one (2026-11-18).

HERMETIC: no network and no server. The refresh tool's HTTP layer is a fake passed in as `get`; every facts file
is written under tmp_path; "now" is passed explicitly wherever an age is judged. No test here reads the
COMMITTED served_facts.json through the age check, so nothing turns red on a timer.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from tools import refresh_served_facts as R  # noqa: E402

NOW = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_served_facts_refresh", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _stamp(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _facts_file(tmp_path, measured, n_series=None):
    p = tmp_path / "served_facts.json"
    p.write_text(json.dumps({"_about": "x", "measured_utc": measured, "instrument": "x",
                             "n_series": n_series or {"a": 8, "b": 9}, "frequencies": {}}), encoding="utf-8")
    return str(p)


# ---- gen_site: missing / stale / fresh / absent source / zero -------------------------------------------
def test_missing_file_refuses(G, tmp_path):
    with pytest.raises(SystemExit, match="no .*served_facts.json.*refresh_served_facts.py"):
        G.load_served_facts(str(tmp_path / "served_facts.json"))


def test_missing_file_refuses_even_with_the_stale_override(G, tmp_path, monkeypatch):
    monkeypatch.setenv(G.SERVED_FACTS_STALE_ENV, "1")
    with pytest.raises(SystemExit, match="no "):
        G.load_served_facts(str(tmp_path / "served_facts.json"))


def test_stale_file_refuses(G, monkeypatch):
    monkeypatch.delenv(G.SERVED_FACTS_STALE_ENV, raising=False)
    old = _stamp(NOW - timedelta(hours=G.SERVED_FACTS_MAX_AGE_HOURS + 1))
    with pytest.raises(SystemExit, match="h ago .*refresh_served_facts.py"):
        G.check_served_facts_age(old, now=NOW)


def test_the_limit_is_24_hours_and_inside_it_passes(G, monkeypatch):
    monkeypatch.delenv(G.SERVED_FACTS_STALE_ENV, raising=False)
    assert G.SERVED_FACTS_MAX_AGE_HOURS == 24
    assert G.check_served_facts_age(_stamp(NOW - timedelta(hours=23, minutes=59)), now=NOW) < 24
    with pytest.raises(SystemExit):
        G.check_served_facts_age(_stamp(NOW - timedelta(hours=24, minutes=1)), now=NOW)


def test_stale_override_is_loud(G, monkeypatch, capsys):
    monkeypatch.setenv(G.SERVED_FACTS_STALE_ENV, "1")
    G.check_served_facts_age(_stamp(NOW - timedelta(days=3)), now=NOW)
    out = capsys.readouterr().out
    assert "STALE served_facts.json" in out and "!!!!!" in out


def test_future_or_malformed_stamp_refuses(G):
    with pytest.raises(SystemExit, match="future"):
        G.check_served_facts_age(_stamp(NOW + timedelta(hours=2)), now=NOW)
    for bad in (None, "", "2026-11-18", "yesterday"):
        with pytest.raises(SystemExit, match="not a UTC timestamp"):
            G.check_served_facts_age(bad, now=NOW)


def test_fresh_file_caps_the_pages(G, tmp_path, monkeypatch):
    monkeypatch.delenv(G.SERVED_FACTS_STALE_ENV, raising=False)
    p = _facts_file(tmp_path, _stamp(datetime.now(timezone.utc) - timedelta(minutes=5)))
    facts = G.load_served_facts(p)
    roll = {"a": {"n_series": 10}, "b": {"n_series": 5}}
    assert G.apply_served_facts(roll, facts, required={"a", "b"}) == {"a": (10, 8)}
    assert roll["a"]["n_series"] == 8 and roll["b"]["n_series"] == 5


def test_a_page_source_absent_from_the_file_refuses(G):
    facts = {"n_series": {"a": 8}, "frequencies": {}, "measured_utc": "x"}
    roll = {"a": {"n_series": 10}, "c": {"n_series": 7}}
    with pytest.raises(SystemExit, match=r"have no served count.*\['c'\]"):
        G.apply_served_facts(roll, facts, required={"a", "c"})
    assert roll["c"]["n_series"] == 7          # nothing half-applied before the refusal


def test_served_zero_renders_not_yet_served_never_the_local_count(G):
    facts = {"n_series": {"z": 0}, "frequencies": {}, "measured_utc": "x"}
    roll = {"z": {"n_series": 1234}}
    G.apply_served_facts(roll, facts, required={"z"})
    assert roll["z"]["n_series"] == 0 and roll["z"]["not_yet_served"] is True
    rec = {"name": "Producer: A dataset", "subtitle": "Monthly prices", "n_series": 0, "not_yet_served": True,
           "cov_start": None, "cov_end": None, "reservable": True, "license_label": "CC BY 4.0"}
    desc = G.build_meta_desc(rec)
    assert "not yet served" in desc and "1,234" not in desc and "Free download" not in desc


# One fixture catalogue for every predicate test. Every clause has a source that only it excludes:
#   b  licence not reservable          c  no series          dd  denylisted (planted into G.DENYLISTED)
#   ab shares a prefix with a ('ab:1' must not count as a series of 'a')
FIXTURE_SQL = """
    CREATE TABLE license (license_id TEXT PRIMARY KEY, reservable INTEGER);
    CREATE TABLE source (source_id TEXT PRIMARY KEY, license_id TEXT);
    CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT);
    INSERT INTO license VALUES ('open', 1), ('closed', 0);
    INSERT INTO source VALUES ('a', 'open'), ('b', 'closed'), ('c', 'open'), ('dd', 'open'), ('ab', 'open'),
                              ('e', 'open');
    INSERT INTO series VALUES ('a:1', 'a'), ('b:1', 'b'), ('dd:1', 'dd'), ('ab:1', 'ab'), ('e:1', 'e'), ('e:2', 'e');
"""
EXPECTED_PAGES = ["a", "ab", "e"]


@pytest.fixture
def fixture_catalogue(G, monkeypatch):
    monkeypatch.setattr(G, "DENYLISTED", set(G.DENYLISTED) | {"dd"})
    monkeypatch.setattr(G, "RESOLVABLE", set(G.RESOLVABLE or ()) | {"a", "ab", "e"})   # the download link renders
    con = sqlite3.connect(":memory:")
    con.executescript(FIXTURE_SQL)
    sources = {r[0]: {"source_id": r[0], "license_id": r[1]} for r in con.execute("SELECT * FROM source")}
    licenses = {r[0]: {"license_id": r[0], "reservable": r[1]} for r in con.execute("SELECT * FROM license")}
    roll = {}                                   # gen_site's rollup shape: only sources WITH series appear
    for sid, n in con.execute("SELECT source_id, COUNT(*) FROM series GROUP BY source_id"):
        roll[sid] = {"source_id": sid, "n_series": n, "min_start": None, "max_end": None, "n_geo": 0,
                     "last_updated": None, "frequencies": [], "categories": [], "geographies": []}
    return con, sources, licenses, roll


def test_page_predicate_equals_mains_page_filter(G, fixture_catalogue):
    """page_source_ids must pick exactly the sources main() writes a page for (build_record's reservable AND a
    rollup entry), on the same fixture - every exclusion clause is exercised, the denylisted one included."""
    _, sources, licenses, roll = fixture_catalogue
    mains = {sid for sid in sources
             if G.build_record(sid, sources[sid], licenses, roll.get(sid), None, None)["reservable"]
             and bool(roll.get(sid))}
    assert sorted(G.page_source_ids(sources, licenses, roll)) == sorted(mains) == EXPECTED_PAGES


def test_tool_measures_exactly_gen_sites_page_sources(G, fixture_catalogue):
    con, sources, licenses, roll = fixture_catalogue
    sids, skipped = R.page_sources(con, G)
    assert sids == sorted(G.page_source_ids(sources, licenses, roll)) == EXPECTED_PAGES
    assert skipped == 1                                          # dd: would be a page but for the denylist


def test_a_served_zero_page_offers_no_download_anywhere(G, fixture_catalogue):
    """Render the page, its Dataset JSON-LD and its Croissant for a source the API serves 0 of. Control: the
    same source served 2 shows every claim, so the absence assertions can see them."""
    _, sources, licenses, roll = fixture_catalogue
    side = {"measured_obs": 99, "description": "Prices. Compiled and redistributed by the Elkassabgi Data Library."}
    claims = ["Open &middot; redistributed", "Redistributable.", "Select &amp; download", "Measured observations",
              "Distinct geographies", "download.html?source=", "DataDownload", "cr:FileObject",
              "Compiled and redistributed", "Permitted (served here)"]

    def render(served):
        r = {"e": dict(roll["e"], n_series=2, n_geo=3, min_start="2001-01-01", max_end="2020-12-31")}
        G.apply_served_facts(r, {"n_series": {"e": served}, "frequencies": {}, "measured_utc": "x"}, required={"e"})
        # description_key is outside render_dataset_page's early field rewrite, so only its page-level sweep
        # can catch the sentence there
        s5 = {"description_key": ["Key facts. Compiled and redistributed by the Elkassabgi Data Library."]}
        rec = G.build_record("e", sources["e"], licenses, r["e"], dict(side), s5)
        return G.render_dataset_page(rec), G.dataset_jsonld(rec), G.croissant_jsonld(rec)

    html, ds, cr = render(2)                                         # control: served
    for c in claims:
        assert c in html, f"control lost {c!r} - the absence checks below would be blind"
    assert ds["distribution"] and ds["isAccessibleForFree"] is True and cr["distribution"]

    html, ds, cr = render(0)
    present = [c for c in claims if c in html]
    assert not present, f"a served-0 page still claims: {present}"
    assert "Not yet served" in html and "not yet served" in G.build_meta_desc(
        G.build_record("e", sources["e"], licenses, dict(roll["e"], n_series=0, not_yet_served=True), None, None))
    for ld in (ds, cr):
        assert "distribution" not in ld and ld["isAccessibleForFree"] is False
        assert ld["usageInfo"] == G.NOT_YET_SERVED_USAGE


# ---- the refresh tool, through a fake HTTP layer --------------------------------------------------------
class FakeAPI:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def __call__(self, url):
        sid = url.split("source=")[1].split("&")[0]
        self.calls.append(sid)
        a = self.answers[sid]
        a = a.pop(0) if isinstance(a, list) else a
        if isinstance(a, Exception):
            raise a
        status, total, hdr = a
        return status, hdr, json.dumps({"total": total, "results": []}).encode()


def _clock():
    t = [NOW]

    def now():
        t[0] += timedelta(seconds=1)
        return t[0]
    return now


def _run(tmp_path, answers, sources, argv=()):
    api = FakeAPI(answers)
    out = str(tmp_path / "served_facts.json")
    rc = R.run(list(argv), get=api, now=_clock(), sleep=lambda s: None, sources=sources, out_path=out)
    return rc, api, out


def test_refresh_writes_counts_zero_and_edge_age(tmp_path):
    prev = tmp_path / "served_facts.json"
    prev.write_text(json.dumps({"n_series": {"a": 1}, "measured_utc": "2026-11-17T00:00:00Z",
                                "frequencies": {"a": {"codes": ["M"], "why": "hand-measured"}}}), encoding="utf-8")
    rc, api, out = _run(tmp_path, {"a": (200, 8, {"cf-cache-status": "HIT", "age": "3600"}),
                                   "z": (200, 0, {"cf-cache-status": "MISS"})}, ["a", "z"])
    assert rc == 0
    doc = json.load(open(out, encoding="utf-8"))
    assert doc["n_series"] == {"a": 8, "z": 0}                      # 0 is a measurement, not a failure
    assert doc["edge_cache"]["a"] == {"cf_cache_status": "HIT", "age_s": 3600}
    assert doc["edge_cache"]["z"] == {"cf_cache_status": "MISS", "age_s": None}
    # a cache status WITHOUT Age is charged the cache's full lifetime (z, fetched 1 s after a)
    assert doc["measured_utc"] == _stamp(NOW + timedelta(seconds=2) - timedelta(seconds=R.EDGE_MAX_AGE_S))
    # otherwise the earliest fetch - Age; no cache header at all = computed at fetch time
    rc, _, out = _run(tmp_path, {"a": (200, 8, {"age": "3600"}), "b": (200, 1, {}), "c": (200, 2, {"age": "10"})},
                      ["a", "b", "c"])
    assert json.load(open(out, encoding="utf-8"))["measured_utc"] == _stamp(NOW + timedelta(seconds=1) -
                                                                             timedelta(seconds=3600))
    rc, _, out = _run(tmp_path, {"b": (200, 1, {})}, ["b"])
    assert json.load(open(out, encoding="utf-8"))["measured_utc"] == _stamp(NOW + timedelta(seconds=1))
    assert doc["frequencies"] == {"a": {"codes": ["M"], "why": "hand-measured"}}                # carried unchanged
    assert doc["instrument"] == R.INSTRUMENT


def test_refresh_refuses_to_write_when_one_source_fails(tmp_path):
    prev = tmp_path / "served_facts.json"
    prev.write_text('{"sentinel": true}', encoding="utf-8")
    rc, api, out = _run(tmp_path, {"a": (200, 8, {}), "b": [OSError("reset"), OSError("reset")],
                                   "c": (200, 3, {})}, ["a", "b", "c"])
    assert rc == 1
    assert open(out, encoding="utf-8").read() == '{"sentinel": true}'
    assert not os.path.exists(out + ".tmp")


def test_refresh_retries_a_5xx_once_and_a_4xx_never(tmp_path):
    rc, api, _ = _run(tmp_path, {"a": [(503, None, {}), (200, 5, {})]}, ["a"], ["--dry-run"])
    assert rc == 0 and api.calls == ["a", "a"]
    rc, api, _ = _run(tmp_path, {"a": [(503, None, {}), (503, None, {}), (200, 5, {})]}, ["a"], ["--dry-run"])
    assert rc == 1 and api.calls == ["a", "a"]                      # at most TWO tries
    rc, api, _ = _run(tmp_path, {"a": (451, None, {})}, ["a"], ["--dry-run"])
    assert rc == 1 and api.calls == ["a"]


def test_refresh_rejects_a_body_without_a_count(tmp_path):
    for bad in (None, -1, "12", True):
        rc, _, out = _run(tmp_path, {"a": (200, bad, {})}, ["a"])
        assert rc == 1 and not os.path.exists(out)


def test_dry_run_writes_nothing_and_prints_the_diff(tmp_path, capsys):
    prev = tmp_path / "served_facts.json"
    prev.write_text(json.dumps({"n_series": {"a": 8, "gone": 2}, "measured_utc": "2026-11-17T00:00:00Z"}),
                    encoding="utf-8")
    before = prev.read_text(encoding="utf-8")
    rc, _, _ = _run(tmp_path, {"a": (200, 9, {}), "new": (200, 4, {})}, ["a", "new"], ["--dry-run"])
    assert rc == 0 and prev.read_text(encoding="utf-8") == before
    out = capsys.readouterr().out
    assert "changed  a: 8 -> 9" in out and "added    new: 4" in out and "removed  gone: 2" in out


def test_single_source_mode_never_writes(tmp_path):
    rc, _, out = _run(tmp_path, {"a": (200, 9, {})}, None, ["--source", "a"])
    assert rc == 0 and not os.path.exists(out)


def test_the_written_file_satisfies_gen_site(G, tmp_path, monkeypatch):
    """Round trip: what the tool writes is what gen_site accepts, at the real clock (written seconds ago)."""
    monkeypatch.delenv(G.SERVED_FACTS_STALE_ENV, raising=False)
    api = FakeAPI({"a": (200, 8, {"age": "60"}), "z": (200, 0, {})})
    out = str(tmp_path / "served_facts.json")
    assert R.run([], get=api, sleep=lambda s: None, sources=["a", "z"], out_path=out) == 0
    facts = G.load_served_facts(out)
    roll = {"a": {"n_series": 10}, "z": {"n_series": 4}}
    G.apply_served_facts(roll, facts, required={"a", "z"})
    assert roll["a"]["n_series"] == 8 and roll["z"].get("not_yet_served") is True
