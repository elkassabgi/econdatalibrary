"""The csv fence's UnitTimeout must PROPAGATE out of _derive_changed_csvs. (R353's class.)

The fence at the call site (`with _unit_deadline(unit.key + " (csv phase)", ...)`) carries a
DESIGNED handler: abandon the phase as a non-demoting coverage note, cursors recorded, the
next run re-derives. But `_derive_changed_csvs` ends in a broad `except Exception` that used
to catch the SIGALRM-raised UnitTimeout FIRST — converting the designed abandon into
"enqueue every mapped id as csv_derive crashed + demote". That is precisely how `abs` parked
100,000 retry-queue rows and `ilostat` 50,000 (each cohort one CURSOR_CAP batch, every row
attempts=1 with the identical UnitTimeout error string, 2026-08-18..24) — rows that can never
drain under the r2 backend.

Both directions (R414): the control signal must escape, and an ORDINARY exception must still
take the queue-mapped-ids path — the protection this except exists for.
"""
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402


class _Unit:
    source_id = "testsrc"
    unit_id = "_all"
    key = "testsrc/_all"


class _Res:
    obs = 5
    # Catalog-shaped ids (source-prefixed, colon-delimited) so the crash path's
    # queue filter keeps them.
    series_cursors = {"testsrc:AAA": "2026-01-01", "testsrc:BBB": "2026-01-01"}


def _run(monkeypatch, exc):
    """Drive _derive_changed_csvs to the point inside its try where derive raises `exc`."""
    # The mapping step is `ids, unmapped = _catalog_ids_for(unit.source_id, changed)` —
    # make every changed id map to itself (first stub used the wrong shape and produced
    # ids=[], so the crash path queued nothing and the test failed for the wrong reason).
    monkeypatch.setattr(orchestrate, "_catalog_ids_for",
                        lambda source_id, changed: (list(changed), []))

    def boom(*a, **kw):
        raise exc

    fake_derive = types.SimpleNamespace(derive_and_put=boom)
    monkeypatch.setitem(sys.modules, "updater.derive", fake_derive)
    monkeypatch.setattr(orchestrate, "derive", fake_derive, raising=False)

    # THE PACKAGE ATTRIBUTE IS THE ONE THAT ACTUALLY WINS, and without this line these two
    # tests pass or fail depending on COLLECTION ORDER. `_derive_changed_csvs` reaches derive
    # through a lazy `from . import derive` (orchestrate.py:648). For `from package import
    # name`, Python returns `getattr(package, name)` when the package already carries that
    # attribute - and importing `updater.derive` ANYWHERE sets it, permanently, on the real
    # module. Patching `sys.modules` then reaches nothing, `boom` is never called, the real
    # deriver runs, and the assertions fail against a note they were never meant to see.
    #
    # pytest imports every test module during COLLECTION, before any test runs, so a single
    # module-level `from updater import derive` in an unrelated test file breaks these two no
    # matter which order the files are given. Measured: adding
    # tests/test_derive_reports_skipped.py made both fail in BOTH orders, while this file
    # alone still passed. `raising=False` above hid it - it silently created an attribute
    # nothing reads instead of failing loudly.
    import updater as _updater_pkg
    monkeypatch.setattr(_updater_pkg, "derive", fake_derive, raising=False)
    return orchestrate._derive_changed_csvs(_Unit(), _Res(), blob=None)


def test_unittimeout_propagates_to_the_fence(monkeypatch):
    """The fence's control signal must escape — its designed handler lives OUTSIDE."""
    with pytest.raises(orchestrate.UnitTimeout):
        _run(monkeypatch, orchestrate.UnitTimeout("testsrc/_all (csv phase) exceeded"))


def test_ordinary_exception_still_queues_mapped_ids(monkeypatch):
    """The accept direction: a real crash must keep the never-sink-the-publish behaviour."""
    failed, note, deferred, reasons = _run(monkeypatch, ValueError("boom"))
    assert failed, "a real crash must still queue the mapped catalog ids"
    assert all(s.startswith("testsrc:") for s in failed)
    assert "csv_derive crashed" in (note or "")
    assert all("csv_derive crashed" in r for r in reasons.values())
