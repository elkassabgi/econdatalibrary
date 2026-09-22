"""A statcan cube whose keys the vector parser cannot read must not look like a cube with nothing new.

WHAT IS ACTUALLY WRONG, after an adversarial review corrected an earlier version of this file.

`_fold_vectors` drops any series_key that is not "v"/"V" + digits. When a whole cube is keyed otherwise,
`_disk_vector_map` returns `{}`, `update()` books `empty_unit`, and the run reports "no new rows" - the same
thing it reports for a cube the publisher genuinely did not change. That is failure class H in
.claude/skills/econ-completion/references/failure-classes.md: "could not look" and "nothing there" become
indistinguishable. It is demonstrated: on 2026-09-11 the run spent ~50 minutes on six such cubes
(12100147..12100152, 756,053,043 rows) to produce an empty map, and said nothing.

WHAT IS NOT WRONG, and what an earlier version of this file asserted:

- 525 of the 531 coordinate-keyed cubes are the PUBLISHER'S Census Program layout, which has no VECTOR column.
  jobs/ingest_statcan.py:369-376 keys those rows by Coordinate deliberately. Re-pulling reproduces the same
  keys, so "the remedy is a re-pull (R22)" was wrong and is no longer claimed anywhere.
- The six that ARE a defect come from a different mechanism: a blank VECTOR cell falling back to COORDINATE at
  ingest_statcan.py:282-288.
- "89.2% of the source is frozen by this skip" was wrong by ~67x. The run blamed for it is recorded
  killed_external at 13,567 s with its last write at pid 12100175, and 525 of the 531 are 98xxxxxx, which sort
  last under `for pid in sorted(changed)` - never reached. See R1043/R1046.
"""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import statcan  # noqa: E402

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

COORD_KEYS = ["10.1.1.1.1.1.1.1", "1000.1.1.1.1.1", "104.1.3.4.1.10.1"]
VECTOR_KEYS = ["v7719", "v742070", "v735045"]


def _batch(keys):
    n = len(keys)
    return {"series_key": keys, "geo": ["Canada"] * n, "uom": ["Dollars"] * n,
            "coordinate": ["1.1.1"] * n}


def _write(tmp_path, name, keys):
    p = os.path.join(str(tmp_path), name)
    pq.write_table(pa.table({"series_key": pa.array(keys, pa.string()),
                             "geo": pa.array(["Canada"] * len(keys), pa.string()),
                             "uom": pa.array(["Dollars"] * len(keys), pa.string()),
                             "coordinate": pa.array(["1.1.1"] * len(keys), pa.string())}), p)
    return p


# --- the counter -------------------------------------------------------------------------------------------

def test_coordinate_keys_are_counted_as_skipped_not_silently_dropped():
    out, seen = {}, [0, 0]
    statcan._fold_vectors(_batch(COORD_KEYS), out, seen)
    assert out == {}, "coordinate keys must not be parsed as vectors"
    assert seen == [len(COORD_KEYS), len(COORD_KEYS)], seen


def test_vector_keys_are_parsed_and_not_counted_as_skipped():
    out, seen = {}, [0, 0]
    statcan._fold_vectors(_batch(VECTOR_KEYS), out, seen)
    assert sorted(out) == [7719, 735045, 742070], out
    assert seen == [len(VECTOR_KEYS), 0], seen


def test_a_mixed_cube_reports_the_skipped_share():
    keys = VECTOR_KEYS + COORD_KEYS
    out, seen = {}, [0, 0]
    statcan._fold_vectors(_batch(keys), out, seen)
    assert len(out) == len(VECTOR_KEYS)
    assert seen == [len(keys), len(COORD_KEYS)], seen


def test_the_counter_is_optional_so_existing_callers_still_work():
    out = {}
    statcan._fold_vectors(_batch(VECTOR_KEYS), out)
    assert sorted(out) == [7719, 735045, 742070]


# --- the footer short-circuit ------------------------------------------------------------------------------

def test_the_footer_proves_a_coordinate_keyed_cube_holds_no_vectors(tmp_path):
    p = _write(tmp_path, "98100001.parquet", COORD_KEYS)
    assert statcan._footer_proves_no_vectors(p) is True


def test_the_footer_does_not_claim_proof_for_a_vector_keyed_cube(tmp_path):
    p = _write(tmp_path, "24100055.parquet", VECTOR_KEYS)
    assert statcan._footer_proves_no_vectors(p) is False


def test_uppercase_V_keys_are_not_skipped_by_the_footer_bound(tmp_path):
    """The bound is "V" (0x56), not "v" (0x76), because _fold_vectors accepts both cases.

    A cube keyed 'V123' sorts below 'v' but IS readable, so the looser bound would skip a cube that has
    vectors - the exact failure this short-circuit exists to avoid, inverted.
    """
    p = _write(tmp_path, "mixedcase.parquet", ["V7719", "V742070"])
    assert statcan._footer_proves_no_vectors(p) is False
    out, seen = {}, [0, 0]
    statcan._fold_vectors(_batch(["V7719", "V742070"]), out, seen)
    assert sorted(out) == [7719, 742070], "uppercase V keys are parseable and must not be dropped"


def test_an_unreadable_footer_falls_through_instead_of_claiming_emptiness(tmp_path):
    """Cannot look must never read as nothing there."""
    missing = os.path.join(str(tmp_path), "does_not_exist.parquet")
    assert statcan._footer_proves_no_vectors(missing) is False


def test_the_short_circuit_avoids_the_stream_entirely(tmp_path, monkeypatch, capsys):
    """The point of the footer path: a coordinate-keyed cube must not be decoded at all."""
    p = _write(tmp_path, "98100002.parquet", COORD_KEYS)

    def _boom(*_a, **_k):
        raise AssertionError("iter_batches was called; the footer should have answered first")

    monkeypatch.setattr(statcan.blob, "iter_batches", _boom, raising=False)
    assert statcan._disk_vector_map(p) == {}
    said = capsys.readouterr().out
    assert "no vector ids at all" in said, said
    assert "re-pull" not in said.lower(), "the re-pull remedy was refuted; it must not reappear"


def test_a_cube_the_footer_cannot_settle_is_still_read_and_still_speaks(monkeypatch, capsys):
    """When the footer cannot prove it, the stream runs and an all-unreadable cube still says so."""
    class _Blob:
        def iter_batches(self, path, columns=None):
            class _B:
                def to_pydict(self_inner):
                    return _batch(COORD_KEYS)
            yield _B()

    monkeypatch.setattr(statcan, "blob", _Blob(), raising=False)
    monkeypatch.setattr(statcan, "_footer_proves_no_vectors", lambda _p: False)
    out = statcan._disk_vector_map("12100147.parquet")
    said = capsys.readouterr().out
    assert out == {}
    assert "NONE parse as a vector id" in said, said

    # NEGATIVE CONTROL: a readable cube must not warn, or the check fires on everything.
    class _Good(_Blob):
        def iter_batches(self, path, columns=None):
            class _B:
                def to_pydict(self_inner):
                    return _batch(VECTOR_KEYS)
            yield _B()

    monkeypatch.setattr(statcan, "blob", _Good(), raising=False)
    out2 = statcan._disk_vector_map("24100055.parquet")
    said2 = capsys.readouterr().out
    assert sorted(out2) == [7719, 735045, 742070]
    assert "NONE parse as a vector id" not in said2, said2


def test_a_footer_without_statistics_never_claims_emptiness(tmp_path):
    """Mutation found this hole in my own fix: no statistics must mean "cannot tell", not "no vectors".

    A parquet written without column statistics has nothing to prove anything with. Returning True there is
    the very failure this short-circuit exists to avoid - "could not look" reported as "nothing there" - and
    it would silently skip a cube full of readable vector ids.
    """
    p = os.path.join(str(tmp_path), "nostats.parquet")
    pq.write_table(pa.table({"series_key": pa.array(VECTOR_KEYS, pa.string()),
                             "geo": pa.array(["Canada"] * 3, pa.string()),
                             "uom": pa.array(["Dollars"] * 3, pa.string()),
                             "coordinate": pa.array(["1.1.1"] * 3, pa.string())}),
                   p, write_statistics=False)
    assert statcan._footer_proves_no_vectors(p) is False, (
        "a footer with no statistics proves nothing and must fall through to the stream")
