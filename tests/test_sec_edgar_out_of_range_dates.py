"""sec_edgar: a date SEC can publish but timestamp[ns] cannot hold must not kill the source.

THE FAILURE THIS RELATES TO. sec_edgar has never once reported a success; its state row reads

    UNEXPECTED:ArrowInvalid('Casting from timestamp[us] to timestamp[ns] would result in out of
     bounds timestamp: -61950355200000000')

That value, read as MICROSECONDS, is 0006-11-15.

READ THIS BEFORE TRUSTING THE TESTS BELOW. The production path that produces a timestamp[us]
column is NOT identified, and these tests do NOT reproduce the recorded failure. Measured on
pandas 2.3.3 / pyarrow 23.0.0:

    pd.to_datetime(["15-NOV-0006"], format="%d-%b-%Y", errors="coerce")  ->  dtype datetime64[ns],
    value NaT, arrow type timestamp[ns], and the cast to timestamp[ns] SUCCEEDS.

So on this stack the bug does not occur here, and the plausible story — "pandas 2.x returns a
non-nanosecond dtype and year 6 survives coercion" — is FALSE for this version. CI installs
pandas>=2.2 uncapped, i.e. the same 2.3.x, so a version split is not a satisfying explanation
either. Nor is the stored data: all 1,019 parquets under clean_full/edgar_insider and
clean_full/edgar_13f were scanned and NONE holds a timestamp outside the ns window.

These tests therefore pin a GUARD, not a repro. They assert that _coerce_insider yields ns-typed,
Arrow-convertible output with out-of-range dates nulled — which is correct behaviour worth keeping
regardless of origin — and they will keep passing whether or not the real cause is ever found.
Do not read a green run here as evidence that sec_edgar is fixed. The open question is where a
timestamp[us] column comes from; the next lead is the 13f path (_coerce_13f does not parse dates
at all, so a date-typed column there would arrive as Python objects, which Arrow infers as us).

WHY merge._report_impossible_dates DOES NOT COVER THIS EITHER: it reports, deliberately does not
drop, and runs AFTER the merge — it cannot prevent a cast that fails during schema alignment.

The check is on the ARROW conversion, not just the dtype, because Arrow is where the original
exception came from. A test asserting only `dtype == datetime64[ns]` would pass on a fix that
still blew up in pa.Table.from_pandas.
"""
from __future__ import annotations
import os
import sys

import pandas as pd
import pyarrow as pa
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers.sec_edgar import _coerce_insider   # noqa: E402

CFG = {"insider_dtypes": {"nonderiv_trans": {"datetime": ["TRANS_DATE"], "float": ["TRANS_SHARES"]}}}


def _coerce(values):
    df = pd.DataFrame({"TRANS_DATE": values, "TRANS_SHARES": ["1"] * len(values)})
    return _coerce_insider(df, "nonderiv_trans", CFG)


def test_the_recorded_value_converts_to_arrow_without_raising():
    """0006-11-15 is the value from the recorded failure, in SEC's own DD-MON-YYYY form.

    NOTE: on pandas 2.3.3 this passes with OR without the guard, because to_datetime already
    yields NaT here. It is kept as the shape of the failure, not as proof of a fix."""
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
