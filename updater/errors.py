"""Failure contract shared by every strategy adapter.

Distinguishing transient from definitive failure is the single most important
correctness rule in the whole system: a timeout / 5xx / dropped connection must
NEVER be recorded as 'no data' or 'done'. That conflation is the exact bug that
silently froze sources before (and that the GUS/DBnomics TransientError fix cured).
"""


class UpdaterError(Exception):
    pass


class TransientError(UpdaterError):
    """Timeout / 5xx / 429 / network drop / connection reset.

    Discard any partial in-memory rows, leave the unit's existing published data
    UNTOUCHED, mark the unit `transient_fail`, and retry on the next run with
    backoff. Never treat as no-data or done — coverage stays complete because the
    unit is re-queued.
    """


class DefinitiveError(UpdaterError):
    """Hard 4xx (not 404/429), hard caps (offset ceilings, series caps), BadZip,
    structural no-table, or a write that would shrink/empty good data.

    Keep the max obtainable slice, mark the unit `partial` with a reason, surface
    it to monitoring, and do NOT blind-retry. Distinct from `ok` so partial
    coverage is visible and re-attemptable, never silently frozen.
    """
