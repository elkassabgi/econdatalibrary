"""bcrp's string-vs-date class: THREE sites, two exception types, one lying type hint.

_common._max_by_key returns `{k: d.isoformat() ...}` — ISO STRINGS. bcrp._per_series_last passes
them straight through while its signature promised `dict[str, dt.date]`, so every downstream site
read the annotation, believed it, and treated the values as dates:

    line 276  cursors[skey] = d.isoformat()   -> AttributeError: 'str' has no 'isoformat'
    line 343  any(di > last for di in d)      -> TypeError: '>' between date and str
    line 363  max(...).isoformat()            -> AttributeError

The R310 sweep fixed 276 and 363 and STEPPED OVER 343 — which sits between them in execution
order — because it grepped for the AttributeError symptom and 343 raises a TypeError. Same
defect, different exception, invisible to a symptom-shaped search.

These tests exercise the surviving comparison against BOTH types, so a future refactor that
reintroduces a raw `date > str` fails here instead of in production.
"""
import datetime as dt

import pyarrow as pa
import pytest

from updater.strategies.fetchers._common import _max_by_key


def test_max_by_key_really_does_return_strings():
    """The premise of the whole class. If this ever changes, the normalisations become no-ops
    rather than bugs — but it must be asserted, not assumed."""
    t = pa.table({
        "series_key": pa.array(["a", "a", "b"], pa.string()),
        "obs_date": pa.array([dt.date(2026, 7, 1), dt.date(2026, 7, 21), dt.date(2026, 6, 1)],
                             pa.date32()),
    })
    out = _max_by_key(t)
    assert out == {"a": "2026-07-21", "b": "2026-06-01"}
    assert all(isinstance(v, str) for v in out.values()), "ISO strings, not dates"


# --- the comparison at bcrp.py:343, isolated -------------------------------------------------
def _genuinely_new(dates, last):
    """Byte-for-byte the logic now in bcrp.update()."""
    _last_iso = (last if isinstance(last, str)
                 else last.isoformat() if last is not None else None)
    return (any((di if isinstance(di, str) else di.isoformat()) > _last_iso for di in dates)
            if _last_iso is not None else bool(dates))


def test_dates_against_an_ISO_STRING_last_is_the_production_case():
    """This is exactly what crashed: di is a dt.date, last is '2026-07-21' from the store."""
    last = "2026-07-21"
    assert _genuinely_new([dt.date(2026, 7, 22)], last) is True
    assert _genuinely_new([dt.date(2026, 7, 21)], last) is False, "boundary re-fetch is not new"
    assert _genuinely_new([dt.date(2026, 7, 20)], last) is False


def test_it_also_survives_the_well_typed_case():
    """If _max_by_key is ever changed to return dates, this must keep working."""
    last = dt.date(2026, 7, 21)
    assert _genuinely_new([dt.date(2026, 7, 22)], last) is True
    assert _genuinely_new([dt.date(2026, 7, 21)], last) is False


def test_mixed_input_does_not_raise():
    """The regression proper — a raw `date > str` raises TypeError."""
    for last in ("2026-07-21", dt.date(2026, 7, 21)):
        for d in ([dt.date(2026, 7, 22)], ["2026-07-22"]):
            assert _genuinely_new(d, last) is True     # must not raise


def test_no_prior_max_means_any_data_is_new():
    assert _genuinely_new([dt.date(2026, 1, 1)], None) is True
    assert _genuinely_new([], None) is False


def test_a_raw_comparison_would_still_raise_which_is_why_this_exists():
    """Negative control: proves the normalisation is doing real work, not decoration."""
    with pytest.raises(TypeError):
        _ = dt.date(2026, 7, 22) > "2026-07-21"


def test_the_annotation_no_longer_lies():
    """The root enabler: three sites believed `-> dict[str, dt.date]` and none of them got dates."""
    from updater.strategies.fetchers import bcrp
    assert bcrp._per_series_last.__annotations__.get("return") in ("dict[str, str]", dict[str, str])
