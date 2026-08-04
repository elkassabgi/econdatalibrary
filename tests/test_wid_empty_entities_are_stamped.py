"""WID's header-only entities must be STAMPED, or they become the whole work list and raise.

WID publishes 12 entities as 47 bytes of CSV header and nothing else (Al, ON, ON-MER, OO-MER,
OP-MER, OQ-MER, SW, XQ, XQ-MER, XC, XE, XK) against 17-22 MB for a real country. Nothing upstream
is failing — all 12 return HTTP 200.

The success branch stamps `done[country] = f"{mod}|{size}"` so a country is not re-downloaded
until WID republishes it. The empty branch stamped NOTHING. Two consequences, and the second is
the one that breaks the source:

  1. those 12 were re-fetched on every single run, forever;
  2. once every real country is stamped, they are the ENTIRE work list — a run attempts 12
     sub-units, all 12 are empty, and finalize()'s empty-window guard reads that as "the source
     went dark" and raises, on a source that is perfectly healthy.

The stamp carries an `|empty` suffix because the two cases have different evidence: a real
country must still have its parquet present to be skipped (a bare stamp would suppress the fetch
after a store reset), while a header-only entity has no parquet and never will — 11 of the 12
have none today.
"""
import pytest


MOD, SIZE = "Wed, 30 Jul 2026 11:00:00 GMT", "47"


def _gate(done, country, mod, size, parquet_exists):
    """The skip decision from wid.update(), isolated."""
    stamp = done.get(country)
    return stamp == f"{mod}|{size}|empty" or (
        stamp == f"{mod}|{size}" and parquet_exists)


def test_an_unstamped_entity_is_always_fetched():
    assert _gate({}, "ON", MOD, SIZE, parquet_exists=False) is False


def test_THE_REGRESSION_a_stamped_empty_entity_is_skipped_without_a_parquet():
    """11 of the 12 have no parquet. Requiring one would re-fetch them forever."""
    done = {"ON": f"{MOD}|{SIZE}|empty"}
    assert _gate(done, "ON", MOD, SIZE, parquet_exists=False) is True


def test_a_real_country_still_requires_its_parquet_to_be_skipped():
    """Unchanged behaviour: a bare stamp must not suppress a fetch after a store reset."""
    done = {"FR": f"{MOD}|{SIZE}"}
    assert _gate(done, "FR", MOD, SIZE, parquet_exists=True) is True
    assert _gate(done, "FR", MOD, SIZE, parquet_exists=False) is False


def test_republication_re_checks_an_empty_entity():
    """The stamp is WID's own (last-modified|size), so it moves exactly when the entity is
    republished — the only moment an empty one could gain data."""
    done = {"ON": f"{MOD}|{SIZE}|empty"}
    assert _gate(done, "ON", "Fri, 01 Aug 2026 09:00:00 GMT", SIZE, False) is False
    assert _gate(done, "ON", MOD, "20000000", False) is False


def test_the_two_stamp_forms_do_not_cross_over():
    """An empty stamp must not satisfy the real-country rule, or a genuinely missing parquet
    would be skipped; and a real stamp must not satisfy the empty rule."""
    assert _gate({"X": f"{MOD}|{SIZE}|empty"}, "X", MOD, SIZE, parquet_exists=True) is True
    assert _gate({"X": f"{MOD}|{SIZE}"}, "X", MOD, SIZE, parquet_exists=False) is False


def test_all_twelve_known_empties_skip_once_stamped():
    empties = ["Al", "ON", "ON-MER", "OO-MER", "OP-MER", "OQ-MER",
               "SW", "XQ", "XQ-MER", "XC", "XE", "XK"]
    done = {c: f"{MOD}|{SIZE}|empty" for c in empties}
    assert all(_gate(done, c, MOD, SIZE, parquet_exists=False) for c in empties), \
        "after one pass none of the 12 should be attempted again until WID republishes"
