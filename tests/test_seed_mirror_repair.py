"""The seeder must repair a ONE-SIDED store mirror in both directions.

Both halves are real, not hypothetical:

  * local present / R2 absent — the crash window between os.replace and publish_file. Nothing
    downstream detects it: a re-run short-circuits on refused-exists, the re-key re-stamp
    counts what R2 actually holds, and verify_source_served never lists store parquets.

  * R2 present / local absent — this bit namq_10_gdp on 2026-09-01. It was excluded from
    seeding because R2 already held a fresh API pull, so the seeder returned "refused-exists"
    and left the local mirror one-sided. core/derive_csv resolves a eurostat flow to a LOCAL
    path, so the derive reported "unresolvable eurostat:namq_10_gdp: expected eurostat file
    'E:\\...'" and eurostat's QUARTERLY GDP had no served CSV at all despite being catalogued.

Refusing to overwrite an existing target is correct. Leaving the mirror one-sided is not.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import seed_eurostat_440 as seeder  # noqa: E402


@pytest.fixture
def store(tmp_path, monkeypatch):
    d = tmp_path / "eurostat"
    d.mkdir()
    monkeypatch.setattr(seeder, "STORE", str(d))
    monkeypatch.setattr(seeder.config, "BACKEND", "r2")
    return d


def test_r2_present_local_absent_is_mirrored_DOWN(store, monkeypatch):
    """The namq_10_gdp case. The derive resolves locally, so a missing local copy makes a
    catalogued flow undeliverable — the seeder must pull it down rather than shrug."""
    payload = b"PAR1-not-really-but-opaque-to-the-seeder"
    monkeypatch.setattr(seeder.blob, "exists", lambda p: True)
    monkeypatch.setattr(seeder.blob, "read_bytes", lambda p: payload)
    status, rows = seeder.seed_one("ZZTEST_DOWN")
    assert status == "mirrored-down", f"expected a downward mirror, got {status!r}"
    target = os.path.join(str(store), "ZZTEST_DOWN.parquet")
    assert os.path.exists(target), "the local copy was not written"
    assert open(target, "rb").read() == payload, "the mirrored bytes differ from R2's"
    assert rows == 0, "a mirror mints no new rows and must not claim any"


def test_local_present_r2_absent_is_republished_UP(store, monkeypatch):
    """The crash-window case: the bytes on disk are already correct, they simply never
    reached R2."""
    target = store / "ZZTEST_UP.parquet"
    target.write_bytes(b"already-correct-bytes")
    monkeypatch.setattr(seeder.blob, "exists", lambda p: False)
    published = {}
    monkeypatch.setattr(seeder.blob, "publish_file",
                        lambda p: published.setdefault("p", p) and 0 or len(target.read_bytes()))
    status, rows = seeder.seed_one("ZZTEST_UP")
    assert status == "republished", f"expected an upward republish, got {status!r}"
    assert published["p"].endswith("ZZTEST_UP.parquet")
    assert rows == 0


def test_both_sides_present_is_still_REFUSED(store, monkeypatch):
    """Never-shrink: an existing target on both sides is refused, never overwritten."""
    (store / "ZZTEST_BOTH.parquet").write_bytes(b"x")
    monkeypatch.setattr(seeder.blob, "exists", lambda p: True)
    status, rows = seeder.seed_one("ZZTEST_BOTH")
    assert status == "refused-exists", f"an existing flow was not refused: {status!r}"


def test_a_downward_mirror_is_not_attempted_under_the_local_backend(store, monkeypatch):
    """Under AQUEDUCT_BACKEND=local, blob reads the same local path — there is no second
    side to mirror from, and pretending otherwise would loop on itself."""
    monkeypatch.setattr(seeder.config, "BACKEND", "local")
    monkeypatch.setattr(seeder.blob, "exists", lambda p: True)
    status, rows = seeder.seed_one("ZZTEST_LOCALONLY")
    assert status == "refused-exists", f"expected a plain refusal under local, got {status!r}"
