"""Regression gate: PxWeb ingesters must read the time axis from LABELS when the codes
are positional.

THE BUG THIS LOCKS DOWN. Some PxWeb tables index the time dimension positionally --
`category.index` is {"0":0,"1":1,...} -- and carry the real period only in
`category.label`. The parsers looked the date up in the CODES alone, so parse_date("0")
returned None, every observation was skipped, and a perfectly good HTTP 200 produced ZERO
rows. Downstream that surfaced as a "structural break" (hagstofa reported 26 of them) or a
"returned 200 but parsed 0 rows" line -- never as a parse bug, which is why it survived.

WHY A NEGATIVE CONTROL. "2 rows came out" does not prove the LABEL path produced them; the
parser might have found the date some other way. So each module is run twice on the same
cube: with labels (must yield both periods) and with the labels stripped (must yield
nothing). The stripped case reproduces the exact pre-patch behaviour, so a regression that
removes the fallback fails here rather than going quiet in production.

WHY ONLY THE YEAR IS ASSERTED. Annual periods are stored per each source's own convention
(ssb and statfin store 2023 as 2023-12-31; hagstofa's monthly data is period-start).
Pinning a specific day would assert MY convention over the publisher's and would fail for
reasons that have nothing to do with this bug.

TWO JSON-STAT SHAPES. dst parses JSON-stat v1, where `id`/`size`/`role` nest UNDER
`dimension`; the others parse JSON-stat2, where they sit at the root. Feeding one shape to
both makes v1 return early on an empty dim list -- indistinguishable from the fix failing.
"""
from __future__ import annotations
import importlib
import inspect
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Every ingester whose parser resolves a period from the time dimension's codes.
# Keep in step with:
#   grep -l "obs_date = parse_date(t_codes\[t_pos\])" jobs/ingest_*.py
MODULES = ["cso_ireland", "dst", "stat_slovenia", "stat_latvia",
           "stat_estonia", "statfin", "ssb", "hagstofa"]
V1 = {"dst"}          # JSON-stat v1: id/size/role nested under `dimension`


def _payload(with_labels: bool, v1: bool):
    time_dim = {"label": "time", "role": "time",
                "category": {"index": {"0": 0, "1": 1}}}     # POSITIONAL codes
    if with_labels:
        time_dim["category"]["label"] = {"0": "2023", "1": "2024"}
    dims = {"Tid": time_dim,
            "Maal": {"label": "measure",
                     "category": {"index": {"X": 0}, "label": {"X": "Value"}}}}
    if v1:
        return {"dataset": {"dimension": dict(dims, id=["Tid", "Maal"], size=[2, 1],
                                              role={"time": ["Tid"]}),
                            "value": [11.0, 22.0]}}
    return {"id": ["Tid", "Maal"], "size": [2, 1], "dimension": dims,
            "role": {"time": ["Tid"]}, "value": [11.0, 22.0]}


def _parser(name: str):
    mod = importlib.import_module(f"jobs.ingest_{name}")
    for attr in ("parse_jsonstat2", "parse_jsonstat"):
        if hasattr(mod, attr):
            return getattr(mod, attr)
    pytest.fail(f"jobs/ingest_{name}.py exposes no JSON-stat parser")


@pytest.mark.parametrize("name", MODULES)
def test_positional_time_codes_fall_back_to_labels(name):
    fn = _parser(name)
    nargs = len(inspect.signature(fn).parameters)
    v1 = name in V1

    def run(with_labels):
        p = _payload(with_labels, v1)
        return fn(p, "TBL1") if nargs >= 2 else fn(p)

    rows = run(True)
    assert sorted(r[1].year for r in rows) == [2023, 2024], (
        f"{name}: positional time codes with labelled periods parsed {len(rows)} row(s); "
        f"expected both 2023 and 2024 via the label fallback")

    # Negative control: no labels -> nothing parses. Proves the rows above came from the
    # LABEL path and reproduces the pre-patch failure.
    assert run(False) == [], (
        f"{name}: rows appeared with the time labels stripped, so the positive case does "
        f"not prove the label fallback ran")


# --- hagstofa: sentinel periods ON the time axis -------------------------------------------
# Statistics Iceland puts AGGREGATE periods on the time variable itself, coded as bare 4-digit
# numbers in the 3000s. UMH11130.px is the proven case: `Ar` is flagged time=True AND carries
# role.time, and its values are ['3002','3003','3004','3001','1949','1950', ...]. So the right
# axis is chosen and the codes on it simply are not all periods — a DIFFERENT defect from the
# selection bug, and the reason a synthetic cube passed while live data failed (120 of 168 rows
# past the year 2100 on a live re-parse).

def _hagstofa():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_hags_pd", os.path.join(ROOT, "jobs", "ingest_hagstofa.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.mark.parametrize("code", ["3001", "3001M03", "3001Q2", "3001H1", "3001W05",
                                  "3001-06-15", "1499", "2101"])
def test_hagstofa_rejects_out_of_range_years_in_every_branch(code):
    r"""EVERY branch must be bounded, not just the bare-year one. Bounding only `^\d{4}$` left
    '3001M03' parsing to 3001-03-01 — the same bug with a smaller footprint, waiting for a
    table with a monthly sentinel."""
    assert _hagstofa().parse_date(code) is None, f"{code} produced a date"


@pytest.mark.parametrize("code,year", [("1949", 1949), ("1500", 1500), ("2100", 2100),
                                       ("2020M03", 2020), ("2020Q2", 2020), ("2020H1", 2020),
                                       ("2020W05", 2020), ("2020-06-15", 2020)])
def test_hagstofa_still_parses_real_periods(code, year):
    d = _hagstofa().parse_date(code)
    assert d is not None and d.year == year, f"{code} -> {d}"
