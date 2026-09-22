"""The flow-grain cataloguer must not undo the repairs another tool applied, and must not
report success over a search index it failed to rebuild.

Both defects were live until 2026-09-17 and both are silent, which is what makes them worth a test:

  * `tools/refresh_flowgrain_dates.py` has excluded a list of known store defects from every write
    since 2026-09-05 (ledger R722/R724) — ids whose STORE range is itself a wrong-axis parse. The
    cataloguer never read that list, so a re-catalogue wrote the defective range straight over the
    hand-applied correction. The date tool then could not put it back, because it only builds a
    plan entry where catalogue and store DISAGREE: once the store value is in the catalogue they
    agree, and the exclusion never fires again.
  * The FTS rebuild caught OperationalError, printed "[fts] skipped" and exited 0 — leaving every
    row just catalogued present in `series` and absent from search. Catalogued and unsearchable,
    reported as success.

A third case is new-row hygiene: a start_date before 1500 is a parse defect, not data, and
cataloguing one re-lists the "timeless table" class delisted on 2026-09-05.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "..", "tools", "catalog_pxweb_flowgrain.py")


def _load():
    spec = importlib.util.spec_from_file_location("catalog_pxweb_flowgrain", TOOL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def cat():
    return _load()


def test_the_module_imports_without_the_parquet_store(cat):
    """The store guard belongs to a RUN. At import time it only made the tool untestable —
    CI ignores data/ — and an untestable guard is how both defects above survived."""
    assert cat is not None
    assert hasattr(cat, "_require_store")


def test_the_store_guard_still_refuses_at_run_time(cat, monkeypatch):
    monkeypatch.setattr(cat, "DATA", os.path.join(HERE, "no-such-store"))
    with pytest.raises(SystemExit) as e:
        cat._require_store()
    assert "parquet store not found" in str(e.value)


def test_known_defects_are_read_and_scoped_to_the_source(cat):
    """The file is shared with refresh_flowgrain_dates.py; each source must see only its own."""
    hag = cat.known_store_defects("hagstofa")
    scb = cat.known_store_defects("scb")
    assert hag, "hagstofa has a known store defect recorded; reading none means the file moved"
    assert scb, "scb has known store defects recorded"
    assert all(s.startswith("hagstofa:") for s in hag), hag
    assert all(s.startswith("scb:") for s in scb), scb
    assert not (hag & scb)
    # a source with no entries must get an empty set, not everything
    assert cat.known_store_defects("ssb") == set()


def test_a_missing_defects_file_refuses_rather_than_cataloguing_unguarded(cat, monkeypatch):
    """Fail closed: if the exclusions cannot be read, do not write the defects back in."""
    monkeypatch.setattr(cat, "KNOWN_DEFECTS", os.path.join(HERE, "no-such-file.txt"))
    with pytest.raises(SystemExit) as e:
        cat.known_store_defects("hagstofa")
    assert "R722" in str(e.value) or "missing" in str(e.value)


@pytest.mark.parametrize("value,expected", [
    ("2026-12-31", 2026), ("1000-12-31", 1000), (None, None), ("", None), ("not-a-date", None),
])
def test_year_parsing_is_total(cat, value, expected):
    assert cat._year(value) == expected


def test_the_implausible_threshold_is_set_and_sane(cat):
    assert cat.IMPLAUSIBLE_BEFORE == 1500
    assert cat._year("1000-12-31") < cat.IMPLAUSIBLE_BEFORE       # the delisted class
    assert cat._year("1911-01-01") >= cat.IMPLAUSIBLE_BEFORE      # a real old series stays


def test_the_write_path_applies_both_gates_and_a_busy_timeout(cat):
    """Source-level pins. The three behaviours live inside main()'s loop, which needs a parquet
    store and a catalogue to drive end to end; these assert the code is wired, and the tests above
    assert each helper behaves. Named precisely enough that a rewrite cannot pass by accident."""
    src = inspect.getsource(cat)
    assert "PRAGMA busy_timeout" in src, "the crawlers write to this file; a lock must wait"
    assert "known_store_defects(src)" in src, "the R722 exclusions must be consulted per source"
    assert "preserved.append(sid)" in src, "a known defect must keep its corrected catalogue range"
    assert "refused.append((sid, mn, mx))" in src, "an implausible start_date must not be written"
    i = src.index("[fts] rebuilt")
    tail = src[i:i + 1200]
    assert "raise SystemExit" in tail, (
        "a failed or short FTS rebuild must fail the run: catalogued-and-unsearchable reported as "
        "success is the defect this replaces")
    # Strip comments and docstring lines first: the code deliberately QUOTES the removed message
    # when explaining why it was removed, and a test that cannot tell an explanation from the
    # thing it explains is noise.
    code = "\n".join(ln.split("#")[0] for ln in src.splitlines())
    assert "[fts] skipped" not in code, "the silent skip must be gone from the code itself"
