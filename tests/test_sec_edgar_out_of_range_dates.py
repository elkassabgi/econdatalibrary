"""sec_edgar: a date SEC can publish but timestamp[ns] cannot hold must not kill the source.

THE FAILURE THIS RELATES TO. sec_edgar has never once reported a success; its state row reads

    UNEXPECTED:ArrowInvalid('Casting from timestamp[us] to timestamp[ns] would result in out of
     bounds timestamp: -61950355200000000')

That value, read as MICROSECONDS, is 0006-11-15.

CAUSE, CONFIRMED — AND IT IS A VERSION SPLIT BETWEEN DEV AND CI.

    local  pandas 2.3.3  ->  to_datetime("15-NOV-0006", errors="coerce") = NaT, dtype ns, cast OK
    CI     pandas 3.0.5  ->  parses to datetime64[us]; year 6 is representable, so it SURVIVES

requirements-updater.txt pins `pandas>=2.2` with no upper cap, and pip on the runner resolves that
to pandas-3.0.5-cp311. pandas 3.0 parses to non-nanosecond resolution by default, so the coerce
that nulls this date on a 2.x laptop keeps it on the runner. The stored insider parquets are
timestamp[ns] — measured, all 972 timestamp columns across 648 files — so aligning a us column
carrying year 6 to them raises, and takes every table in the run with it.

REPRODUCED EXACTLY, by constructing the input pandas 3.0 produces rather than relying on the local
parser (which is why test_reproduces_the_recorded_arrow_error builds a datetime64[us] array
directly). That yields the recorded message verbatim, including the value:

    ArrowInvalid: Casting from timestamp[us] to timestamp[ns] would result in out of bounds
    timestamp: -61950355200000000

Building the us input directly is what makes these tests VERSION-INDEPENDENT: they fail on the
un-guarded code on any pandas, instead of silently passing on 2.x because the parser already
returned NaT. An earlier version of this file did exactly that and proved nothing.

WHY merge._report_impossible_dates DOES NOT COVER THIS: it reports, deliberately does not drop,
and runs AFTER the merge — it cannot prevent a cast that fails during schema alignment.

WHY merge._report_impossible_dates DOES NOT COVER THIS EITHER: it reports, deliberately does not
drop, and runs AFTER the merge — it cannot prevent a cast that fails during schema alignment.

The check is on the ARROW conversion, not just the dtype, because Arrow is where the original
exception came from. A test asserting only `dtype == datetime64[ns]` would pass on a fix that
still blew up in pa.Table.from_pandas.
"""
from __future__ import annotations
import os
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers.sec_edgar import _coerce_insider   # noqa: E402

CFG = {"insider_dtypes": {"nonderiv_trans": {"datetime": ["TRANS_DATE"], "float": ["TRANS_SHARES"]}}}


def _coerce(values):
    df = pd.DataFrame({"TRANS_DATE": values, "TRANS_SHARES": ["1"] * len(values)})
    return _coerce_insider(df, "nonderiv_trans", CFG)


def test_reproduces_the_recorded_arrow_error():
    """The failure itself, built from the dtype pandas 3.0 produces — not from the local parser.

    This is the version-independent half. On a 2.x laptop to_datetime already returns NaT, so a
    test that went through the parser would pass on unguarded code and prove nothing; on the
    runner's pandas 3.0.5 the same call yields datetime64[us] and year 6 survives. Constructing
    the us array directly reproduces the runner's state anywhere, and asserts the exact recorded
    message and value."""
    us = np.array(["0006-11-15", "2024-01-02"], dtype="datetime64[us]")
    tbl = pa.Table.from_pandas(pd.DataFrame({"d": pd.Series(us)}), preserve_index=False)
    assert tbl.schema.field("d").type.unit == "us"
    with pytest.raises(pa.ArrowInvalid) as ei:
        tbl.cast(pa.schema([pa.field("d", pa.timestamp("ns"))]))   # what merge does
    assert "-61950355200000000" in str(ei.value), "must be the value sec_edgar actually recorded"


def test_the_guard_neutralises_that_exact_input():
    """Same us input, through the guard's logic, then the cast that used to raise."""
    us = np.array(["0006-11-15", "2024-01-02"], dtype="datetime64[us]")
    s = pd.Series(us)
    oor = s.notna() & ((s < pd.Timestamp.min) | (s > pd.Timestamp.max))
    assert int(oor.sum()) == 1
    fixed = s.mask(oor).astype("datetime64[ns]")
    tbl = pa.Table.from_pandas(pd.DataFrame({"d": fixed}), preserve_index=False)
    tbl.cast(pa.schema([pa.field("d", pa.timestamp("ns"))]))        # must not raise
    assert pd.isna(fixed.iloc[0]) and fixed.iloc[1] == pd.Timestamp("2024-01-02")


def test_the_recorded_value_converts_to_arrow_without_raising():
    """0006-11-15 through the real _coerce_insider, in SEC's own DD-MON-YYYY form."""
    out = _coerce(["15-NOV-0006", "02-JAN-2024"])
    tbl = pa.Table.from_pandas(out, preserve_index=False)      # this is what used to raise
    assert pa.types.is_timestamp(tbl.schema.field("TRANS_DATE").type)
    assert tbl.schema.field("TRANS_DATE").type.unit == "ns"


def test_out_of_range_becomes_null_and_real_dates_survive():
    out = _coerce(["15-NOV-0006", "02-JAN-2024"])
    assert pd.isna(out["TRANS_DATE"].iloc[0]), "year-6 date must be nulled, not kept"
    assert out["TRANS_DATE"].iloc[1] == pd.Timestamp("2024-01-02"), "a real date must survive"


def test_nulled_not_clamped():
    """A clamp to pd.Timestamp.min would be indistinguishable from a genuine 1677 date."""
    out = _coerce(["15-NOV-0006"])
    assert pd.isna(out["TRANS_DATE"].iloc[0])
    assert out["TRANS_DATE"].iloc[0] is not pd.Timestamp.min


def test_far_future_is_handled_too():
    """The window has two ends; 2262 is as real a publisher typo as year 6."""
    out = _coerce(["01-JAN-9999", "02-JAN-2024"])
    assert pd.isna(out["TRANS_DATE"].iloc[0])
    tbl = pa.Table.from_pandas(out, preserve_index=False)
    assert tbl.schema.field("TRANS_DATE").type.unit == "ns"


def test_ordinary_input_is_untouched():
    """The guard must not perturb the normal path — every value in range, none nulled."""
    out = _coerce(["02-JAN-2024", "15-JUN-1999", "31-DEC-2020"])
    assert out["TRANS_DATE"].notna().all()
    assert list(out["TRANS_DATE"]) == [pd.Timestamp("2024-01-02"),
                                       pd.Timestamp("1999-06-15"),
                                       pd.Timestamp("2020-12-31")]


def test_unparseable_still_coerces_to_null():
    """The original errors='coerce' behaviour must survive the change."""
    out = _coerce(["not-a-date", "02-JAN-2024"])
    assert pd.isna(out["TRANS_DATE"].iloc[0])
    assert out["TRANS_DATE"].iloc[1] == pd.Timestamp("2024-01-02")


def test_empty_frame_is_returned_unchanged():
    df = pd.DataFrame({"TRANS_DATE": [], "TRANS_SHARES": []})
    assert _coerce_insider(df, "nonderiv_trans", CFG).empty
