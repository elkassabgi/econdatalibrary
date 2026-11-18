"""No page may claim more series than the public API serves (pre-deploy review C1, 2026-11-17).

The local catalogue can run ahead of the served one. On 2026-11-17 statcan's lane had catalogued 1,270 ids
that the frozen serving catalogue does not hold (/v1/series answers "unknown series id"), and a full build
put "467,611 series" on statcan.html while GET /v1/catalog?source=statcan answered total 466,341.

catalog/served_facts.json holds the measured served counts; gen_site.apply_served_facts holds each page at or
below them. These tests check the committed pages against that file, and the cap itself, with controls that
plant the defect.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = os.path.join(ROOT, "catalog", "site")
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
FACTS = os.path.join(ROOT, "catalog", "served_facts.json")
METRIC_RE = re.compile(r'<div class="mlabel">Series catalogued</div><div class="mval">([\d,]+)</div>')


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_served_counts", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _facts():
    with open(FACTS, encoding="utf-8") as fh:
        return json.load(fh)


def _idx():
    with open(os.path.join(SITE, "catalog.html"), encoding="utf-8") as fh:
        t = fh.read()
    return {r["id"]: r for r in json.loads(re.search(r"const IDX=(\[.*?\]);\n", t, re.S).group(1))}


def over_served(page_counts: dict, served: dict) -> dict:
    """{source: (page, served)} for every page that claims more than is served, or names a source not served."""
    return {s: (n, served.get(s)) for s, n in page_counts.items() if s not in served or n > served[s]}


def page_metric(source: str) -> int:
    with open(os.path.join(SITE, f"{source}.html"), encoding="utf-8") as fh:
        m = METRIC_RE.findall(fh.read())
    assert len(m) == 1, f"{source}.html: expected one 'Series catalogued' metric, found {len(m)}"
    return int(m[0].replace(",", ""))


def test_facts_file_is_a_measurement():
    f = _facts()
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", f["measured_utc"])
    assert "/v1/catalog?source=" in f["instrument"]
    assert len(f["n_series"]) >= 300 and all(isinstance(v, int) and v >= 0 for v in f["n_series"].values())


def test_no_catalogue_entry_claims_more_than_is_served():
    idx = _idx()
    bad = over_served({s: r["n_series"] for s, r in idx.items()}, _facts()["n_series"])
    assert not bad, f"pages claim more series than the API serves: {bad}"


def test_no_dataset_page_metric_claims_more_than_is_served():
    served = _facts()["n_series"]
    pages = {s: page_metric(s) for s in _idx()}
    bad = over_served(pages, served)
    assert not bad, f"dataset pages claim more series than the API serves: {bad}"


def test_statcan_page_shows_the_served_count_and_frequencies():
    served = _facts()["n_series"]["statcan"]
    assert page_metric("statcan") == served
    with open(os.path.join(SITE, "statcan.html"), encoding="utf-8") as fh:
        t = fh.read()
    assert f"{served:,} series" in t
    assert "Irregular" not in t


def test_control_the_check_catches_an_inflated_page():
    served = _facts()["n_series"]
    assert over_served({"statcan": served["statcan"] + 1270}, served)
    assert over_served({"not_a_served_source": 1}, served)
    assert not over_served({"statcan": served["statcan"]}, served)


def test_cap_lowers_never_raises_and_overrides_frequencies(G):
    roll = {"a": {"n_series": 10, "frequencies": ["irregular", "M"]}, "b": {"n_series": 5, "frequencies": ["A"]},
            "c": {"n_series": 7, "frequencies": ["Q"]}}
    facts = {"n_series": {"a": 8, "b": 9}, "frequencies": {"a": ["M", "A"]}, "measured_utc": "x"}
    lowered = G.apply_served_facts(roll, facts)
    assert lowered == {"a": (10, 8)}
    assert roll["a"]["n_series"] == 8 and roll["b"]["n_series"] == 5 and roll["c"]["n_series"] == 7
    assert roll["a"]["frequencies"] == ["A", "M"] and roll["b"]["frequencies"] == ["A"]


def test_missing_or_unreadable_facts_file_refuses(G, tmp_path):
    """A missing file used to print a warning and leave every page uncapped; since 2026-11-18 it refuses
    (tests/test_served_facts_refresh.py has the age and coverage refusals)."""
    with pytest.raises(SystemExit, match="refresh_served_facts"):
        G.load_served_facts(str(tmp_path / "absent.json"))
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    with pytest.raises(SystemExit):
        G.load_served_facts(str(bad))


def test_full_build_applies_the_cap(G):
    import inspect
    src = inspect.getsource(G.load_registry)
    assert "apply_served_facts(series_roll, required=page_source_ids(sources, licenses, series_roll))" in src
