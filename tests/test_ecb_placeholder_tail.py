"""ecb: an ALL-NULL pre-listed period is a quiet tail, not a structural break.

MEASURED 2026-08-27: ECB__RESV.parquet (max obs 2026-01-01) tail-requested
startPeriod=2026-01 and got HTTP 200 with 108 body rows for 2026-Q1 — every one
with an EMPTY OBS_VALUE (ECB pre-lists the next quarter before publishing values,
the stat_slovenia 2221405S class). `_parse_csv` rightly parsed 0 rows; the
classifier then called it `structural_unit` because 108 >= STRUCT_MIN_BODY, and
the source sat ATTENTION on a healthy upstream (probed live: RESV serves real
data at v1.0 for any earlier window).

Discriminating pair pinned here (R414): the all-null body must be QUIET at any
size; a same-size body whose rows CARRY values that fail to parse must stay
STRUCTURAL — that is the schema-break signal the check exists for.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers.ecb import _parse_csv, STRUCT_MIN_BODY  # noqa: E402

HDR = ("KEY,FREQ,REF_AREA,REGION,ADJUSTMENT,PROPERTY_TYPE,INDICATOR,DATA_PROVIDER,"
       "PRICE_TYPE,TRANSFORMATION,UNIT_MEASURE,TIME_PERIOD,OBS_VALUE")


def _body(n: int, value: str, period: str = "2026-Q1") -> bytes:
    rows = [HDR] + [
        f"RESV.Q.AT._T.N._TR.RVAV.4F0._Z._Z.PT,Q,AT,_T,N,_TR,RVAV,4F0,_Z,_Z,PT,{period},{value}"
        for _ in range(n)]
    return ("\r\n".join(rows) + "\r\n").encode()


def test_all_null_body_counts_nulls_and_parses_zero():
    # The verbatim RESV shape: substantial body, every OBS_VALUE empty.
    n = max(STRUCT_MIN_BODY, 108)
    keys, dates, vals, freqs, n_body, n_null = _parse_csv(_body(n, ""))
    assert (len(dates), n_body, n_null) == (0, n, n)
    # The classifier's condition `n_body >= STRUCT_MIN_BODY and n_null_vals < n_body`
    # must NOT fire here:
    assert not (n_body >= STRUCT_MIN_BODY and n_null < n_body)


def test_valued_but_unparseable_body_stays_structural():
    # Same size, but rows CARRY a value while the period is garbage — a real break.
    n = max(STRUCT_MIN_BODY, 108)
    keys, dates, vals, freqs, n_body, n_null = _parse_csv(
        _body(n, "42.5", period="NOT-A-PERIOD"))
    assert (len(dates), n_body, n_null) == (0, n, 0)
    assert n_body >= STRUCT_MIN_BODY and n_null < n_body  # classifier fires


def test_mixed_null_and_valued_garbage_stays_structural():
    # Even one valued-but-unparseable row among nulls keeps the alarm armed.
    n = STRUCT_MIN_BODY
    raw = _body(n - 1, "") + _body(1, "7.7", period="NOT-A-PERIOD")[len(HDR) + 2:]
    keys, dates, vals, freqs, n_body, n_null = _parse_csv(raw)
    assert len(dates) == 0 and n_body == n and n_null == n - 1
    assert n_body >= STRUCT_MIN_BODY and n_null < n_body


def test_real_rows_still_parse():
    keys, dates, vals, freqs, n_body, n_null = _parse_csv(_body(3, "101.25", period="2025-Q4"))
    assert len(dates) == 3 and n_null == 0 and vals == [101.25] * 3
    assert keys[0].startswith("RESV.Q.AT")
