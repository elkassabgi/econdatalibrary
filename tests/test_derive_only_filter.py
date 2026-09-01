"""`--only` scopes a derive to a named list of series ids.

Why it exists: on 2026-09-01 the local eurostat parquet mirror was found to be behind R2 for
68 of 7,654 flows, and the SERVED CSVs had been derived from that stale mirror — tec00108
served 5,328 rows where R2's store held 5,415. Rebuilding exactly those 68 was awkward with
the existing flags: --skip-existing skips every key (they all exist, which is the point), and
--skip-newer-than would page the whole source's keys to reach a handful.

The two tests that matter are the negative ones. A targeted rebuild is run by an operator who
believes a specific list is being rebuilt, so an id that quietly fails to match, or a
selection that comes out empty, must be LOUD rather than a fast clean exit.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.derive_csv import _apply_only  # noqa: E402

ROWS = [("eurostat:tec00108", "eurostat"),
        ("eurostat:ei_isind_q", "eurostat"),
        ("eurostat:aact_ali01", "eurostat"),
        ("abs:ANA_AGG:M1.GPM.20.AUS.Q", "abs")]


def _write(tmp_path, text):
    p = tmp_path / "only.txt"
    p.write_text(text, encoding="utf-8")
    return str(p)


def test_selects_only_the_named_ids(tmp_path):
    got = _apply_only(ROWS, _write(tmp_path, "eurostat:tec00108\neurostat:ei_isind_q\n"))
    assert [r[0] for r in got] == ["eurostat:tec00108", "eurostat:ei_isind_q"]


def test_comments_and_blank_lines_are_ignored(tmp_path):
    body = "# the 68 stale flows\n\neurostat:tec00108   \n  # trailing note\n"
    got = _apply_only(ROWS, _write(tmp_path, body))
    assert [r[0] for r in got] == ["eurostat:tec00108"]


def test_an_id_not_in_the_catalogue_is_NAMED_not_silently_dropped(tmp_path, capsys):
    """The silent-drop shape: the run must not report a tidy success over a smaller set
    than the operator asked for."""
    path = _write(tmp_path, "eurostat:tec00108\neurostat:NOT_A_REAL_FLOW\n")
    got = _apply_only(ROWS, path)
    out = capsys.readouterr().out
    assert len(got) == 1
    assert "NOT IN THE CATALOGUE" in out, f"the unmatched id was not reported: {out!r}"
    assert "eurostat:NOT_A_REAL_FLOW" in out, "the unmatched id was not named"


def test_an_empty_selection_EXITS_NONZERO(tmp_path):
    """A mistyped path or a stale id list must not look like a successful no-op run."""
    path = _write(tmp_path, "eurostat:nothing_matches_this\n")
    with pytest.raises(SystemExit) as e:
        _apply_only(ROWS, path)
    assert e.value.code != 0, "an empty selection exited zero"


def test_case_is_significant(tmp_path):
    """Catalogue ids are lowercase while store filenames are uppercase (TEC00108.parquet vs
    eurostat:tec00108). Feeding the FILENAME stem must not silently match nothing and pass —
    it must trip the empty-selection guard."""
    with pytest.raises(SystemExit):
        _apply_only(ROWS, _write(tmp_path, "eurostat:TEC00108\n"))


def test_ids_from_other_sources_are_excluded(tmp_path):
    got = _apply_only(ROWS, _write(tmp_path, "abs:ANA_AGG:M1.GPM.20.AUS.Q\n"))
    assert [r[0] for r in got] == ["abs:ANA_AGG:M1.GPM.20.AUS.Q"]


def test_duplicate_requests_do_not_duplicate_work(tmp_path):
    got = _apply_only(ROWS, _write(tmp_path, "eurostat:tec00108\neurostat:tec00108\n"))
    assert len(got) == 1, "a repeated id derived the same series twice"
