"""An age in the digest is only meaningful next to the cadence it is measured against.

MEASURED 2026-09-03 across all 229 live registry sources: 6 are late against their own declared
cadence, while the four OLDEST `tried=` values in that morning's render — pwt, oxcgrt, barro_lee
and gppd, all at 38.2 days — are cadence `static` and entirely fine. The column read as alarming
exactly where nothing was wrong, and read as unremarkable for `eia` at 11.6 days, which is a
DAILY source and four cycles late.

The cases below are those real sources and their real ages, so a change that breaks the rule
breaks against the fleet it was built from rather than against invented numbers.

WHY `irregular` AND `static` ARE NEVER LATE: they declare no expectation, so no age can be judged
against them. Marking them would recreate the false alarm this rule exists to remove — the same
mistake in the opposite direction.

WHY A MISSING OR UNPARSEABLE TIMESTAMP IS NEVER LATE: not knowing when something last ran is a
different condition from knowing it ran too long ago. Reporting the first as the second is a
guess wearing a measurement's clothes, and this file's whole subject is the difference.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.send_digest import CADENCE_LIMIT_DAYS, is_late  # noqa: E402

NOW = dt.datetime(2026, 9, 3, 12, 0, tzinfo=dt.timezone.utc)


def ago(days: float) -> str:
    return (NOW - dt.timedelta(days=days)).isoformat().replace("+00:00", "Z")


# (cadence, days ago, expected, the real source this came from)
REAL_CASES = [
    ("daily", 11.6, True, "eia — four cycles late, and it did NOT stand out in the render"),
    ("weekly", 17.1, True, "unctad_tradefoodcatbyproc"),
    ("weekly", 15.6, True, "unctad_tradefoodprocbycat"),
    ("weekly", 14.1, True, "unctad_creativegoodsvalue"),
    ("static", 38.2, False, "pwt / oxcgrt / barro_lee / gppd — the oldest ages, all fine"),
    ("static", 32.7, False, "cepii_gravity"),
    ("irregular", 12.4, False, "ons_uk / wid"),
    ("irregular", 10.5, False, "whr"),
]


@pytest.mark.parametrize("cadence,days,expected,who", REAL_CASES,
                         ids=[c[3].split(" ")[0] for c in REAL_CASES])
def test_the_real_fleet(cadence, days, expected, who):
    assert is_late(cadence, ago(days), NOW) is expected, who


@pytest.mark.parametrize("cadence,limit", sorted(CADENCE_LIMIT_DAYS.items()))
def test_each_cadence_brackets_its_own_limit(cadence, limit):
    """Just inside the limit is fine; just outside is late. No cadence is vacuous."""
    assert is_late(cadence, ago(limit - 0.5), NOW) is False, f"{cadence} fired early"
    assert is_late(cadence, ago(limit + 0.5), NOW) is True, f"{cadence} did not fire"


@pytest.mark.parametrize("ts", [None, "", "not-a-date", "2026-13-45T99:99:99Z", 12345])
def test_an_unusable_timestamp_is_never_late(ts):
    """Absent or unreadable is UNKNOWN, and unknown must not be reported as late."""
    assert is_late("daily", ts, NOW) is False


@pytest.mark.parametrize("cadence", ["irregular", "static", "", None, "fortnightly"])
def test_a_cadence_with_no_expectation_is_never_late(cadence):
    assert is_late(cadence, ago(10_000), NOW) is False


def test_a_naive_timestamp_is_read_as_utc():
    """State rows are not guaranteed to carry a zone; treating them as local would shift the
    answer by hours and, at a boundary, flip it."""
    naive = (NOW - dt.timedelta(days=10)).replace(tzinfo=None).isoformat()
    assert is_late("daily", naive, NOW) is True
    assert is_late("monthly", naive, NOW) is False
