"""who_rs text-value switch (RS_208 / RS_212 / RS_217): NumericValue is null on every WHO row and
the number sits in the text field Value. _who_base.run reads Value ONLY for the indicator codes the
SOURCE passes as text_value_indicators, ONLY when NumericValue is null, and ONLY when
str(Value).strip() is a plain decimal number. who_hwf and who_sdg pass nothing and are unchanged.

No network, no server: _get, the catalogue read, the bookmark and the merge are all replaced.
The failure strings below are real WHO Values seen in the 2026-11-20 re-fetch.
"""
from __future__ import annotations

import pytest

from updater import config, merge
from updater.strategies.fetchers import _who_base, who_hwf, who_rs, who_sdg

TEXT_CODES = {"RS_208", "RS_212", "RS_217"}

# Real non-numeric shapes from WHO's Value field, plus edge cases a loose parser would accept.
REJECTED = ["11 - 24", "13 (GB), 23 (NI)", "No", "?", "", "18.4h", "95.9 - 97.7j", "94.9/89.5",
            "< 1", "No ", "�", "None", "1,5", "1e3", "nan", "inf", "+5", ".5", "5.",
            "٣", "--5", "5 5"]
ACCEPTED = {"5.2": 5.2, "12": 12.0, "-3.5": -3.5, "0": 0.0, " 7 ": 7.0, "\t64.25\n": 64.25}


def _row(code, spatial, value, numeric=None, year=2017, **dims):
    r = {"IndicatorCode": code, "SpatialDim": spatial, "TimeDim": year,
         "NumericValue": numeric, "Value": value}
    r.update(dims)
    return r


def _fixture():
    rows = []
    n = 0
    for code in sorted(TEXT_CODES):
        for txt in ACCEPTED:
            n += 1
            rows.append(_row(code, f"A{n:03d}", txt))
        for txt in REJECTED:
            n += 1
            rows.append(_row(code, f"R{n:03d}", txt))
        n += 1
        rows.append(_row(code, f"N{n:03d}", None))                     # Value absent entirely
        n += 1
        rows.append(_row(code, f"V{n:03d}", "9.9", numeric=3.0))       # NumericValue wins
        n += 1
        rows.append(_row(code, f"T{n:03d}", "5.2", year=None))         # no TimeDim -> skipped
    # Indicators NOT in the switch: the RS_246-style footnoted row, a clean text number, and a
    # NumericValue row that must survive with its numeric value.
    rows += [_row("RS_246", "G001", "18.4h"),
             _row("RS_246", "G002", "5.2"),
             _row("RS_246", "G003", "abc", numeric=4.0),
             _row("RS_198", "H001", "5.2"),
             _row("RS_198", "H002", "2.5", numeric=2.5, Dim1="SEX_BTSX")]
    return rows


FIXTURE = _fixture()
CODES = sorted({r["IndicatorCode"] for r in FIXTURE})


def _old_rule(v):
    """The pre-change keep rule, verbatim in behaviour: NumericValue and TimeDim both non-null."""
    val, yr = v.get("NumericValue"), v.get("TimeDim")
    if val is None or yr is None:
        return None
    try:
        int(yr)
        return float(val)
    except (TypeError, ValueError):
        return None


@pytest.fixture
def harness(monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setattr(config, "source_dir", lambda sid: str(tmp_path / sid))
    monkeypatch.setattr(_who_base.blob, "exists", lambda p: False)
    monkeypatch.setattr(_who_base, "_published_indicators", lambda sid, prefix: list(CODES))
    monkeypatch.setattr(_who_base, "load_rotation", lambda d: "")
    monkeypatch.setattr(_who_base, "save_rotation", lambda d, k: None)
    monkeypatch.setattr(_who_base.time, "sleep", lambda s: None)

    def fake_get(url):
        code = url.rsplit("/", 1)[-1]
        return {"value": [r for r in FIXTURE if r["IndicatorCode"] == code]}
    monkeypatch.setattr(_who_base, "_get", fake_get)

    def fake_merge(path, tbl, mode, dedup_keys):
        captured["tbl"] = tbl
        return tbl.num_rows, None
    monkeypatch.setattr(merge, "merge_and_write", fake_merge)

    def go(fn):
        captured.clear()
        res = fn()
        tbl = captured.get("tbl")
        out = {} if tbl is None else dict(zip(tbl.column("series_key").to_pylist(),
                                              tbl.column("value").to_pylist()))
        return res, out
    return go


def _expected(prefix, text_codes):
    out = {}
    for v in FIXTURE:
        if v.get("TimeDim") is None:
            continue
        val = _old_rule(v)
        if val is None and v["IndicatorCode"] in text_codes and v.get("Value") is not None:
            s = str(v["Value"]).strip()
            if s in {k.strip() for k in ACCEPTED}:
                val = float(s)
        if val is not None:
            out[_who_base._key(v, prefix)] = val
    return out


# ---------------------------------------------------------------- the row-level rule

@pytest.mark.parametrize("txt,val", sorted(ACCEPTED.items()))
def test_plain_decimal_text_is_read_for_a_switched_code(txt, val):
    assert _who_base._row_value(_row("RS_208", "X", txt), TEXT_CODES) == val


@pytest.mark.parametrize("txt", REJECTED)
def test_non_numeric_text_is_skipped(txt):
    assert _who_base._row_value(_row("RS_208", "X", txt), TEXT_CODES) is None


def test_absent_value_is_skipped():
    assert _who_base._row_value(_row("RS_212", "X", None), TEXT_CODES) is None


def test_numeric_value_wins_over_text():
    assert _who_base._row_value(_row("RS_217", "X", "9.9", numeric=3.0), TEXT_CODES) == 3.0


def test_code_outside_the_switch_never_reads_text():
    assert _who_base._row_value(_row("RS_246", "X", "5.2"), TEXT_CODES) is None
    assert _who_base._row_value(_row("RS_246", "X", "18.4h"), TEXT_CODES) is None


def test_default_switch_is_empty():
    assert _who_base._row_value(_row("RS_208", "X", "5.2")) is None


# ---------------------------------------------------------------- end to end through run()

def test_who_rs_reads_text_values_for_its_three_codes_only(harness):
    res, out = harness(lambda: who_rs.update(None, None))
    assert out == _expected("WHO_RS", TEXT_CODES)
    # spot checks that pin each property independently of _expected
    assert out["WHO_RS:RS_208.A001.A"] == 5.2
    assert out["WHO_RS:RS_208.A005.A"] == 7.0                    # ' 7 ' kept after strip()
    assert sum(1 for k in out if ".A0" in k) == 3 * len(ACCEPTED)
    assert not any(k.split(".")[1][0] in "RNT" for k in out)      # no rejected/absent/dateless row
    assert all(out[f"WHO_RS:{c}.V{n}.A"] == 3.0 for c, n in _v_ids())
    assert "WHO_RS:RS_246.G001.A" not in out          # '18.4h' on a code outside the switch
    assert "WHO_RS:RS_246.G002.A" not in out          # clean text, but code outside the switch
    assert out["WHO_RS:RS_246.G003.A"] == 4.0
    assert "WHO_RS:RS_198.H001.A" not in out
    assert out["WHO_RS:RS_198.H002.SEX_BTSX.A"] == 2.5
    assert res.status == "ok"


def _v_ids():
    return [(r["IndicatorCode"], r["SpatialDim"][1:]) for r in FIXTURE
            if r["SpatialDim"].startswith("V")]


def test_who_rs_every_accepted_string_is_kept_and_every_rejected_one_is_not(harness):
    _, out = harness(lambda: who_rs.update(None, None))
    for v in FIXTURE:
        if v["IndicatorCode"] not in TEXT_CODES or v.get("NumericValue") is not None:
            continue
        k = _who_base._key(v, "WHO_RS")
        if v.get("TimeDim") is not None and v["Value"] in ACCEPTED:
            assert out.get(k) == ACCEPTED[v["Value"]], (v, out.get(k))
        else:
            assert k not in out, (v, out.get(k))


@pytest.mark.parametrize("mod,prefix", [(who_hwf, "WHO_HWF"), (who_sdg, "WHO_SDG")])
def test_hwf_and_sdg_are_unchanged_on_the_same_fixture(harness, mod, prefix):
    _, out = harness(lambda: mod.update(None, None))
    assert out == _expected(prefix, frozenset())
    old = {}
    for v in FIXTURE:
        val = _old_rule(v)
        if val is not None:
            old[_who_base._key(v, prefix)] = val
    assert out == old
    assert not any(f"{prefix}:{c}." in k and ".A0" in k for c in TEXT_CODES for k in out)


def test_hwf_and_sdg_produce_identical_output_modulo_prefix(harness):
    _, hwf = harness(lambda: who_hwf.update(None, None))
    _, sdg = harness(lambda: who_sdg.update(None, None))
    assert {k.split(":", 1)[1]: v for k, v in hwf.items()} == \
           {k.split(":", 1)[1]: v for k, v in sdg.items()}


def test_run_without_the_switch_reads_no_text(harness):
    _, out = harness(lambda: _who_base.run("who_rs", "WHO_RS"))
    assert out == _expected("WHO_RS", frozenset())


def test_only_who_rs_declares_text_value_indicators():
    assert who_rs.TEXT_VALUE_INDICATORS == frozenset(TEXT_CODES)
    assert not hasattr(who_hwf, "TEXT_VALUE_INDICATORS")
    assert not hasattr(who_sdg, "TEXT_VALUE_INDICATORS")
