"""The shared SDMX parsers must return the right VALUES, not merely import.

Why this exists: when the parsers were moved into `jobs/_sdmx_common.py`, the move left behind the
`datetime as dt` and `time` imports and the helpers `log` and `_build_series_key`. CI caught it only
because `dt` happened to sit in a return annotation, which Python 3.11 evaluates at import. On 3.14,
which defers annotations, the module imported cleanly and every period parsed to None, because the
parser's own `except Exception` swallowed the NameError. A lost `log` or `_build_series_key` on its own
would have passed every test on every Python: no test called these parsers (review AR-047).

So each test below calls a parser on real input and checks what comes back, and together they reach
the key-column path, the key-builder path, the bad-value skips, the error-logging paths and both XML
formats. Each would fail if a helper the parsers call were missing.
"""
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from jobs import _sdmx_common as sx                              # noqa: E402

D = dt.date


# --------------------------------------------------------------------------- periods
def test_period_codes():
    cases = {
        "2020": D(2020, 12, 31),
        " 2020 ": D(2020, 12, 31),
        "2020-Q1": D(2020, 1, 1),
        "2020-Q4": D(2020, 10, 1),
        "2020-S1": D(2020, 1, 1),
        "2020-S2": D(2020, 7, 1),
        "2020-03": D(2020, 3, 1),
        "2020-03-15": D(2020, 3, 15),
        "2022-A1": D(2022, 12, 31),
    }
    for code, want in cases.items():
        assert sx.parse_sdmx_period(code) == want, code


def test_period_rejects():
    for code in (None, "", "abcd", "2020-13", "2020-Q", "2020-02-30"):
        assert sx.parse_sdmx_period(code) is None, code


def test_period_iso_week_is_not_parsed_yet():
    # INHERITED, pinned on purpose. The weekly branch tests s[6] == "W", so an ISO week such as
    # "2020-W05" (W at index 5) falls through and returns None, and its observations are dropped.
    # Fixing it changes series content, which is a decision of its own - when it is made, change this
    # test in the same commit so the change is deliberate rather than silent.
    assert sx.parse_sdmx_period("2020-W05") is None


# --------------------------------------------------------------------------- SDMX-CSV
def test_csv_key_column_and_bom():
    content = "﻿KEY,TIME_PERIOD,OBS_VALUE\nA.IT,2020,1.5\nA.IT,2020-Q3,2\n".encode("utf-8")
    keys, dates, vals = sx.parse_sdmx_csv(content)
    assert keys == ["A.IT", "A.IT"]
    assert dates == [D(2020, 12, 31), D(2020, 7, 1)]
    assert vals == [1.5, 2.0]


def test_csv_builds_keys_and_skips_bad_rows():
    content = (
        "DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
        "IT1:X,A,IT,2019,3.5,A\n"        # kept: key built from FREQ + REF_AREA
        "IT1:X,A,,2019-S2,4,A\n"         # kept: an empty dimension is left out of the key
        "IT1:X,A,IT,2019,NaN,A\n"        # skipped: missing-value marker
        "IT1:X,A,IT,2019,abc,A\n"        # skipped: not a number
        "IT1:X,A,IT,2019-13x,1,A\n"      # skipped: unparseable period
    ).encode("utf-8")
    keys, dates, vals = sx.parse_sdmx_csv(content)
    assert keys == ["FREQ=A:REF_AREA=IT", "FREQ=A"]
    assert dates == [D(2019, 12, 31), D(2019, 7, 1)]
    assert vals == [3.5, 4.0]


def test_csv_without_time_or_value_column_is_empty():
    assert sx.parse_sdmx_csv(b"KEY,OBS_VALUE\nA,1\n") == ([], [], [])
    assert sx.parse_sdmx_csv(b"") == ([], [], [])


def test_csv_error_is_logged_not_raised(capsys):
    assert sx.parse_sdmx_csv(None) == ([], [], [])
    assert "CSV parse error" in capsys.readouterr().out


def test_build_series_key():
    row = {"FREQ": "M", "REF_AREA": "", "ADJ": "N", "TIME_PERIOD": "2020-01"}
    assert sx._build_series_key(row, {"TIME_PERIOD"}) == "FREQ=M:ADJ=N"


# --------------------------------------------------------------------------- SDMX-ML
GEN = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
MES = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"


def test_xml_generic_format():
    content = f"""<mes:GenericData xmlns:mes="{MES}" xmlns:gen="{GEN}"><mes:DataSet>
      <gen:Series>
        <gen:SeriesKey><gen:Value id="FREQ" value="A"/><gen:Value id="REF_AREA" value="IT"/></gen:SeriesKey>
        <gen:Obs><gen:ObsDimension value="2020"/><gen:ObsValue value="1.5"/></gen:Obs>
        <gen:Obs><gen:ObsDimension value="2021-Q2"/><gen:ObsValue value="2.5"/></gen:Obs>
        <gen:Obs><gen:ObsDimension value="bad"/><gen:ObsValue value="3"/></gen:Obs>
        <gen:Obs><gen:ObsDimension value="2022"/><gen:ObsValue value="x"/></gen:Obs>
      </gen:Series>
    </mes:DataSet></mes:GenericData>""".encode("utf-8")
    keys, dates, vals = sx.parse_sdmx_xml(content)
    assert keys == ["FREQ=A:REF_AREA=IT", "FREQ=A:REF_AREA=IT"]
    assert dates == [D(2020, 12, 31), D(2021, 4, 1)]
    assert vals == [1.5, 2.5]


def test_xml_structure_specific_format():
    content = f"""<message:StructureSpecificData xmlns:message="{MES}"><message:DataSet>
      <Series>
        <Obs FREQ="M" TIME_PERIOD="2020-03" OBS_VALUE="4.0" OBS_STATUS="A"/>
        <Obs FREQ="M" TIME_PERIOD="2020-04" OBS_VALUE="n/a"/>
      </Series>
    </message:DataSet></message:StructureSpecificData>""".encode("utf-8")
    keys, dates, vals = sx.parse_sdmx_xml(content)
    assert keys == ["FREQ=M"]
    assert dates == [D(2020, 3, 1)]
    assert vals == [4.0]


def test_xml_parse_error_is_logged_not_raised(capsys):
    assert sx.parse_sdmx_xml(b"<not xml") == ([], [], [])
    assert "XML parse error" in capsys.readouterr().out
