"""WHO Road Safety — from WHO's OWN Global Health Observatory API (ghoapi.azureedge.net).

Migrated off the relay mirror 2026-08-02: the relay aggregator is banned (CLAUDE.md §0, ledger R251)
and every source now comes from its publisher. Behaviour lives in _who_base.py, including the
key grammar and the proof that it reconstructs our published ids exactly.

TEXT-VALUE INDICATORS. For RS_208, RS_212 and RS_217 every WHO row has NumericValue null and
carries the number only in the text field Value (re-fetched 2026-11-20: RS_208 175 rows, 89 with
a strictly numeric Value; RS_212 700 / 249; RS_217 350 / 309; all TimeDim 2017). Our 647
catalogued ids for these indicators are exactly the keys whose Value is strictly numeric, and
until this switch they were served from relay-era rows only. _who_base.run reads Value for
these three codes ONLY when NumericValue is null and the text is a plain decimal number; this
set is passed by who_rs alone, so who_hwf and who_sdg are unaffected.
"""
from . import _who_base as _base

SOURCE, PREFIX = "who_rs", "WHO_RS"
TEXT_VALUE_INDICATORS = frozenset({"RS_208", "RS_212", "RS_217"})


def current_vintage(unit):
    return _base.current_vintage(unit, SOURCE)


def update(unit, since):
    return _base.run(SOURCE, PREFIX, text_value_indicators=TEXT_VALUE_INDICATORS)
