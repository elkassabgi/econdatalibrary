"""idb series keys must carry the breakdown, not collapse it (R669).

`rows_to_long` built each key from [pkg_slug, indicator, country] and dropped every other
descriptive column, so a cell's sex / area / age / quintile breakdowns all landed on the same
(series_key, obs_date) with different values and the served CSV handed the user each in turn.

Measured on the store 2026-09-06 with DuckDB over data/clean_full/idb (554 files, 123.3 MB):
15,066,444 rows -> 331,386 distinct (key, date) pairs (2.20% unique), 131,606 pairs carrying
CONTRADICTORY values, 11,339 of 18,854 series (60.1%) affected. Worst single cell:
prangoedad_16_30_PHC at 1990-12-31 with 1,380 rows and 1,353 distinct values.

The last test in this file is the DISCRIMINATION CONTROL: it re-implements the OLD key rule and
asserts the same fixtures collapse under it. Without that, every assertion here could be passing
for reasons unrelated to the fix, which is the failure R793/R798 kept finding in my own harnesses.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs import ingest_idb as _mod  # noqa: E402

r2l = _mod.rows_to_long


def _f(*specs):
    """fields list in CKAN's shape: _f(('year','int'), ('sex','text'))."""
    return [{"id": n, "type": t} for n, t in specs]


NARROW = _f(("year", "int"), ("iso3", "text"), ("indicator", "text"),
            ("sex", "text"), ("area", "text"), ("value", "numeric"))


def _rows(*dicts):
    return list(dicts)


def test_a_breakdown_column_makes_the_keys_distinct():
    """Two rows differing ONLY in sex must not share a key - the whole defect in one case."""
    rows = _rows(
        {"year": 2020, "iso3": "BRA", "indicator": "unemp", "sex": "M", "area": "urban",
         "value": 1.0},
        {"year": 2020, "iso3": "BRA", "indicator": "unemp", "sex": "F", "area": "urban",
         "value": 2.0},
    )
    keys, dates, vals = r2l(rows, "pkg", "res", NARROW)
    assert len(keys) == 2 and vals == [1.0, 2.0]
    assert keys[0] != keys[1], keys
    assert len(set(zip(keys, dates))) == 2


def test_the_breakdown_is_NAMED_in_the_key_not_a_bare_code():
    rows = _rows({"year": 2020, "iso3": "BRA", "indicator": "unemp", "sex": "M",
                  "area": "urban", "value": 1.0})
    keys, _d, _v = r2l(rows, "pkg", "res", NARROW)
    assert keys[0] == "IDB:pkg:unemp:BRA:area=urban:sex=M", keys[0]


def test_the_dimension_order_does_not_depend_on_the_column_order_CKAN_returns():
    """CKAN is free to reorder columns between calls; the key must not move with it."""
    a, _d, _v = r2l([{"year": 2020, "iso3": "BRA", "indicator": "u", "sex": "M",
                      "area": "urban", "value": 1.0}], "pkg", "res", NARROW)
    shuffled = _f(("value", "numeric"), ("sex", "text"), ("indicator", "text"),
                  ("area", "text"), ("iso3", "text"), ("year", "int"))
    b, _d2, _v2 = r2l([{"year": 2020, "iso3": "BRA", "indicator": "u", "sex": "M",
                        "area": "urban", "value": 1.0}], "pkg", "res", shuffled)
    assert a == b, (a, b)


def test_a_NUMERIC_breakdown_SURVIVES_the_narrow_shape():
    """The subtle one. In the narrow shape the ONLY measure is the value column, so a numeric
    extra column - income quintile 1-5, an age code - is a DIMENSION. Excluding `numeric_fields`
    here (the natural-looking shortcut) would silently re-drop exactly the column R669 named."""
    fields = _f(("year", "int"), ("iso3", "text"), ("indicator", "text"),
                ("quintile", "int"), ("value", "numeric"))
    rows = _rows(
        {"year": 2020, "iso3": "BRA", "indicator": "inc", "quintile": 1, "value": 10.0},
        {"year": 2020, "iso3": "BRA", "indicator": "inc", "quintile": 5, "value": 90.0},
    )
    keys, dates, _v = r2l(rows, "pkg", "res", fields)
    assert len(set(keys)) == 2, keys
    assert "quintile=1" in keys[0] and "quintile=5" in keys[1]
    assert len(set(zip(keys, dates))) == 2


def test_the_WIDE_shape_treats_numeric_columns_as_MEASURES_not_dimensions():
    """With no value column, each numeric column IS a series. It must not also appear as a
    dimension on its sibling's key - that would put pop= into the gdp series' id."""
    fields = _f(("year", "int"), ("iso3", "text"), ("area", "text"),
                ("gdp", "numeric"), ("pop", "numeric"))
    rows = _rows({"year": 2020, "iso3": "BRA", "area": "urban", "gdp": 5.0, "pop": 7.0})
    keys, _d, vals = r2l(rows, "pkg", "res", fields)
    assert sorted(keys) == ["IDB:pkg:gdp:BRA:area=urban", "IDB:pkg:pop:BRA:area=urban"], keys
    assert sorted(vals) == [5.0, 7.0]
    for k in keys:
        assert "gdp=" not in k and "pop=" not in k, k


def test_a_resource_with_NO_breakdown_columns_keeps_EXACTLY_its_old_key():
    """Only the collapsed series may be re-keyed. A clean resource must be byte-identical, or the
    change re-keys series it had no reason to touch."""
    fields = _f(("year", "int"), ("iso3", "text"), ("indicator", "text"), ("value", "numeric"))
    rows = _rows({"year": 2020, "iso3": "BRA", "indicator": "unemp", "value": 1.0})
    keys, _d, _v = r2l(rows, "pkg", "res", fields)
    assert keys == ["IDB:pkg:unemp:BRA"], keys


def test_a_breakdown_column_that_is_BLANK_on_this_row_adds_nothing():
    """A column present in the schema but empty in the record must not append 'sex=' - that would
    re-key every row of every resource that merely DECLARES a column it does not populate."""
    rows = _rows({"year": 2020, "iso3": "BRA", "indicator": "unemp", "sex": "",
                  "area": None, "value": 1.0})
    keys, _d, _v = r2l(rows, "pkg", "res", NARROW)
    assert keys == ["IDB:pkg:unemp:BRA"], keys


def test_the_date_column_is_not_ALSO_a_dimension():
    """year is the date here. If it leaked into the key, every observation of a series would get
    its own id and the series would cease to be a series."""
    keys, dates, _v = r2l(
        _rows({"year": 2020, "iso3": "BRA", "indicator": "u", "sex": "M", "area": "x",
               "value": 1.0},
              {"year": 2021, "iso3": "BRA", "indicator": "u", "sex": "M", "area": "x",
               "value": 2.0}),
        "pkg", "res", NARROW)
    assert len(set(keys)) == 1, keys
    assert "year=" not in keys[0]
    assert len(set(dates)) == 2


def test_the_country_and_indicator_columns_are_not_ALSO_dimensions():
    keys, _d, _v = r2l(_rows({"year": 2020, "iso3": "BRA", "indicator": "u", "value": 1.0}),
                       "pkg", "res", _f(("year", "int"), ("iso3", "text"),
                                        ("indicator", "text"), ("value", "numeric")))
    assert "iso3=" not in keys[0] and "indicator=" not in keys[0], keys[0]


def test_a_colon_in_a_value_cannot_forge_a_different_breakdown():
    """':' separates key parts. Left alone, sex='M:area=rural' would produce the same string as a
    genuine two-column breakdown, so two different cells would share an id."""
    forged, _d, _v = r2l(_rows({"year": 2020, "iso3": "BRA", "indicator": "u",
                                "sex": "M:area=rural", "value": 1.0}),
                         "pkg", "res", NARROW)
    genuine, _d2, _v2 = r2l(_rows({"year": 2020, "iso3": "BRA", "indicator": "u",
                                   "sex": "M", "area": "rural", "value": 1.0}),
                            "pkg", "res", NARROW)
    assert forged[0] != genuine[0], (forged[0], genuine[0])


def test_the_COLLAPSE_IS_GONE_on_the_shape_that_produced_1380_rows_on_one_date():
    """The real offender's shape: one indicator, one country, one date, many breakdowns. Before
    the fix these were 24 rows under ONE (key, date). Now every pair must be unique."""
    rows = [{"year": 1990, "iso3": "BRA", "indicator": "prangoedad_16_30_PHC",
             "sex": s, "area": a, "quintile": q, "value": float(i)}
            for i, (s, a, q) in enumerate(
                (s, a, q) for s in ("M", "F", "T") for a in ("urban", "rural")
                for q in (1, 2, 3, 4))]
    fields = _f(("year", "int"), ("iso3", "text"), ("indicator", "text"), ("sex", "text"),
                ("area", "text"), ("quintile", "int"), ("value", "numeric"))
    keys, dates, vals = r2l(rows, "pkg", "res", fields)
    assert len(rows) == 24 and len(keys) == 24
    assert len(set(zip(keys, dates))) == 24, (
        f"{24 - len(set(zip(keys, dates)))} of 24 rows still share a (key, date) pair")
    assert len(set(vals)) == 24


# --------------------------------------------------------------- DISCRIMINATION CONTROL
# Every assertion above is worthless if it would also pass against the code being replaced. This
# re-implements the OLD rule verbatim - key = [pkg_slug, indicator, country], breakdown dropped -
# and asserts the same fixtures collapse under it. If this test ever fails, the tests above have
# stopped measuring the defect and must be repaired, not deleted.
def _old_key(rec, pkg_slug):
    parts = [x for x in [pkg_slug, str(rec.get("indicator", "") or ""),
                         str(rec.get("iso3", "") or "")] if x]
    return "IDB:" + ":".join(parts)


def test_the_OLD_rule_collapses_these_very_fixtures():
    rows = [{"year": 1990, "iso3": "BRA", "indicator": "prangoedad_16_30_PHC",
             "sex": s, "area": a, "quintile": q, "value": float(i)}
            for i, (s, a, q) in enumerate(
                (s, a, q) for s in ("M", "F", "T") for a in ("urban", "rural")
                for q in (1, 2, 3, 4))]
    old = {_old_key(r, "pkg") for r in rows}
    assert len(old) == 1, "the control is broken: the old rule already separated these rows"
    assert len({(_old_key(r, "pkg"), r["year"]) for r in rows}) == 1
    # ... and the same 24 rows carry 24 different values under that single id.
    assert len({r["value"] for r in rows}) == 24
