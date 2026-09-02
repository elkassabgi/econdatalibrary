"""One definition of "is this last_obs_date a real, orderable date".

TWO TOOLS ASKED THE SAME QUESTION DIFFERENTLY, which is the R483/R484 shape. The progress probe
tested the ISO SHAPE, to decide whether two values could be ordered; gen_runbook tested the
HORIZON (`c10 > "2200-01-01" or c10 < "1500-01-01"`), to decide whether a value was believable
in a manual. Each was right for its own purpose and neither knew about the other, so:

  * sec_edgar's '01mar2026-31may2026' is caught by both, by different tests;
  * cso's '5630-12-31' is shape-valid, so the probe orders it happily while the runbook flags it
    as not a real date.

A value that is not believable is not orderable either - a sentinel that sorts after every real
observation is exactly the thing a "did it advance?" test must not read as progress. So the two
tests belong together, here, and both callers use them.
"""
import re

_ISO_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")

# The horizon gen_runbook has used since 2026-08: outside it, a value is a sentinel or a
# counter rather than an observation date. Kept as strings so the comparison needs no parsing.
HORIZON_LO = "1500-01-01"
HORIZON_HI = "2200-01-01"


def is_orderable_obs_date(value) -> bool:
    """Is this value an ISO day inside a believable horizon, so `>` means something?"""
    if not value:
        return False
    s = str(value)
    if not _ISO_DAY.match(s):
        return False
    day = s[:10]
    return HORIZON_LO <= day <= HORIZON_HI


def advanced(new, old) -> bool:
    """Did a date-ish field move FORWARD?

    Three cases, because two values can be incomparable in two different ways:
      * both orderable      -> order them, which is what "advanced" means;
      * neither orderable   -> the same KIND of value, so any change IS the movement.
                               sec_edgar's '01mar2026-31may2026' becoming '01jun2026-31aug2026'
                               is a real advance and the only signal that unit has, being
                               `partial` with a NULL last_success_utc;
      * one of each         -> the value CROSSED the boundary between the two kinds. That is a
                               reformat (or a sentinel becoming a date), and it says nothing
                               about the data, so it is NOT progress.

    The third case is a departure from the review's suggested "fall back to inequality", and the
    reviewer withdrew that half on the cost argument: inequality stamps the 20-hour clock on the
    reformat itself, where excluding it costs ONE re-run, ONCE - the pass in which the format
    flips is mixed and uncounted, and every pass after it is both-orderable and ordered again
    (R638/R639).

    The residual, stated rather than hidden: if two code paths ever produced both shapes for the
    same unit and alternated, every pass would be mixed and that unit's progress would be
    permanently invisible rather than invisible once."""
    if not new:
        return False
    if not old:
        return True
    n_ok = is_orderable_obs_date(new)
    o_ok = is_orderable_obs_date(old)
    if n_ok and o_ok:
        return str(new) > str(old)
    if n_ok != o_ok:
        return False
    return str(new) != str(old)
