"""sec_edgar: the catalogue's coverage end is the last period that had been FILED, not max(obs_date).

WHY. `tools/refresh_sec_edgar.py` used to write `max(obs_date)` into series.end_date. XBRL
company facts carry two kinds of period ends that are not coverage: filer typos (a fact dated
6016-06-30 on VICR, 3015-03-31 on PAMT, 2201..2215 on nine more companies) and forward-looking
contexts (lease-maturity and remaining-performance-obligation schedules ending 2027..2050). Both
reached /v1/series/{id}.metadata.json as "end_date" — the 2026-09-05 census found 11 sec_edgar
rows beyond 2200 and 141 beyond today+400 days. A date threshold cannot separate the two
classes (2053 is a real lease schedule; 2104 is a typo), but the store already carries the
discriminator: a reported period cannot end after the fact was filed. `coverage_span` keeps the
facts exactly as filed and defines end = max(end where end <= filed).

The negative control matters (R346): the OLD rule must be shown to return the typo, so a
regression back to max(obs_date) fails this file.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _load():
    p = os.path.join(ROOT, "tools", "refresh_sec_edgar.py")
    spec = importlib.util.spec_from_file_location("_refresh_sec_edgar", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


D = dt.date
FACTS = [
    # (period end, filed)
    (D(2019, 12, 31), D(2020, 2, 20)),
    (D(2020, 12, 31), D(2021, 2, 18)),
    (D(2021, 12, 31), D(2022, 2, 17)),   # last REPORTED period
    (D(2027, 12, 31), D(2022, 2, 17)),   # forward-looking context (lease schedule) — not coverage
    (D(2201, 8, 31), D(2017, 4, 19)),    # filer typo — not coverage
]


def test_end_is_last_filed_period_not_max_obs_date():
    mod = _load()
    odate = [e for e, _ in FACTS]
    vint = [f for _, f in FACTS]
    lo, hi = mod.coverage_span(odate, vint)
    assert lo == D(2019, 12, 31)
    assert hi == D(2021, 12, 31)


def test_negative_control_old_rule_returns_the_typo():
    """The behaviour this file exists to prevent: max(obs_date) is the typo."""
    odate = [e for e, _ in FACTS]
    assert max(odate) == D(2201, 8, 31)


def test_fallback_when_no_filed_dates():
    mod = _load()
    odate = [e for e, _ in FACTS[:3]]
    lo, hi = mod.coverage_span(odate, [None, None, None])
    assert (lo, hi) == (D(2019, 12, 31), D(2021, 12, 31))


def test_a_company_with_no_forward_rows_is_unchanged():
    mod = _load()
    odate = [e for e, _ in FACTS[:3]]
    vint = [f for _, f in FACTS[:3]]
    lo, hi = mod.coverage_span(odate, vint)
    assert (lo, hi) == (min(odate), max(odate))


def test_mixed_none_vintages_are_excluded_from_the_end():
    """A row without a filed date cannot prove its period had ended: it does not set the end."""
    mod = _load()
    odate = [D(2020, 12, 31), D(2021, 12, 31), D(2035, 12, 31)]
    vint = [D(2021, 2, 1), D(2022, 2, 1), None]
    assert mod.coverage_span(odate, vint) == (D(2020, 12, 31), D(2021, 12, 31))


def test_all_forward_falls_back_to_the_latest_ended_period_not_the_typo():
    mod = _load()
    odate = [D(2020, 12, 31), D(2201, 8, 31)]          # both filed "before" their period end
    vint = [D(2019, 1, 1), D(2017, 4, 19)]
    lo, hi = mod.coverage_span(odate, vint)
    assert hi == D(2020, 12, 31)                        # ended, even if not provably filed after
    assert hi != D(2201, 8, 31)


def test_empty_input():
    mod = _load()
    assert mod.coverage_span([], []) == (None, None)
