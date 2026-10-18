"""tools/catalog_statcan_tables.FREQ is pinned to StatCan's OWN frequency code list (R1615).

The map it replaced was written by hand and disagreed with getCodeSets on 15 of 17 codes (9 Quarterly -> A,
18 Occasional -> M, 1 Daily -> A ...), labelling 415,015 of 467,611 statcan catalogue rows wrongly. A comment
saying "a wrong frequency is worse than an absent one" sat on top of it and checked nothing.

The expected letter is DERIVED from StatCan's text in tests/fixtures/statcan_getCodeSets_frequency.json
(getCodeSets `object.frequency`, copied verbatim), never restated here by code number, so the test pins the
rule, not a copy of the map:
  * the text names exactly one of Daily/Weekly/Monthly/Quarterly/Annual -> that letter;
  * "Occasional <one of those>" -> that letter (the observations are at it; only the release is occasional);
  * plain "Occasional" -> "irregular" (the contract's word, connectors/base.py);
  * anything else (Biweekly, Bimonthly, Semi-annual, Every N years) has no equivalent -> absent (NULL).
"""
import importlib.util
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "statcan_getCodeSets_frequency.json")
LETTER = {"daily": "D", "weekly": "W", "monthly": "M", "quarterly": "Q", "annual": "A"}
VOCAB = {"D", "W", "M", "Q", "A", "irregular"}          # connectors/base.py SeriesMeta.frequency


def _freq():
    spec = importlib.util.spec_from_file_location("_cat_statcan_freq_test",
                                                  os.path.join(ROOT, "tools", "catalog_statcan_tables.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.FREQ


def _codes():
    with open(FIXTURE, encoding="utf-8") as fh:
        doc = json.load(fh)
    assert doc["status"] == "SUCCESS" and doc["source"].endswith("/getCodeSets")
    return {r["frequencyCode"]: r["frequencyDescEn"] for r in doc["frequency"]}


def expected_letter(text: str):
    words = text.strip().lower().split()
    if words == ["occasional"]:
        return "irregular"
    if len(words) == 2 and words[0] == "occasional":
        return LETTER.get(words[1])
    if len(words) == 1:
        return LETTER.get(words[0])
    return None


def mismatches(freq: dict, codes: dict) -> list:
    out = [f"{c} {codes[c]!r}: map {freq.get(c)!r}, StatCan's text says {expected_letter(codes[c])!r}"
           for c in sorted(codes) if freq.get(c) != expected_letter(codes[c])]
    out += [f"{c}: not a StatCan frequency code" for c in sorted(set(freq) - set(codes))]
    out += [f"{c}: {v!r} is outside the contract vocabulary" for c, v in sorted(freq.items()) if v not in VOCAB]
    out += [f"{c}: maps to None explicitly - leave it out" for c, v in sorted(freq.items()) if v is None]
    return out


def test_the_fixture_is_the_full_code_list():
    codes = _codes()
    assert len(codes) == 17 and set(codes) == {1, 2, 4, 6, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}
    # the derivation reaches every letter and the absent case - a rule that mapped everything to None
    # would make a wrong map look right only if the map were empty
    assert {expected_letter(t) for t in codes.values()} == VOCAB | {None}


def test_freq_matches_statcans_code_list():
    bad = mismatches(_freq(), _codes())
    assert not bad, "tools/catalog_statcan_tables.FREQ disagrees with StatCan's getCodeSets:\n  " + "\n  ".join(bad)


def test_the_check_fails_on_the_map_it_replaced():
    """Negative control: the pre-R1615 map must FAIL, on the very codes AR-343 named."""
    old = {1: "A", 2: "A", 4: "Q", 6: "M", 7: "M", 9: "A", 11: "A", 12: "A", 13: "A",
           14: "D", 15: "W", 16: "D", 17: "Q", 18: "M", 19: "M", 20: "Q", 21: "A"}
    bad = mismatches(old, _codes())
    assert len(bad) == 15
    for code in (1, 9, 16, 18):
        assert any(b.startswith(f"{code} ") for b in bad)


def test_each_single_error_is_caught():
    freq, codes = _freq(), _codes()
    for c in codes:                                   # every code changed to every other value, one at a time
        for v in sorted(VOCAB) + [None]:
            if v == freq.get(c):
                continue
            m = dict(freq)
            if v is None:
                m.pop(c, None)
            else:
                m[c] = v
            assert mismatches(m, codes), f"code {c} -> {v!r} was not caught"
    assert mismatches({**freq, 99: "A"}, codes)       # a code StatCan does not have
    assert mismatches({**freq, 11: "S"}, codes)       # a letter outside the contract
