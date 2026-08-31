"""The §5.7 no-cursors debt must PERSIST until a completed campaign clears it.

Before 2026-08-31 the debt evaporated: the no-cursors branch demoted the run and returned a
note, nothing was queued (ids unknown), the fetcher's vintage sidecar was already written, so
the next run skipped as unchanged and reported clean — which is how noaa served 3,138,159
CSVs one restatement behind with every instrument green.

Both directions per R414: the marker must be written by the branch's caller-side hook and
survive; and ONLY the campaign stamp clears it — an unrelated source's stamp must not.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402


def test_marker_persists_and_only_the_right_stamp_clears(tmp_path):
    st = StateStore(path=str(tmp_path / "state.db"))

    st.note_full_rederive_owed("noaa", vintage="noaa:abc123", note="csv coherence unmet…")
    owed = st.full_rederives_owed()
    assert [r["source_id"] for r in owed] == ["noaa"]
    assert owed[0]["vintage"] == "noaa:abc123"

    # Re-noting updates in place (one row per source, never a growing pile).
    st.note_full_rederive_owed("noaa", vintage="noaa:def456", note="again")
    owed = st.full_rederives_owed()
    assert len(owed) == 1 and owed[0]["vintage"] == "noaa:def456"

    # An unrelated clear must not touch it.
    st.clear_full_rederive_owed("norgesbank")
    assert len(st.full_rederives_owed()) == 1

    st.clear_full_rederive_owed("noaa")
    assert st.full_rederives_owed() == []


def test_the_note_constant_is_shared_not_retyped():
    """The branch returns the note built from _NO_CURSORS_NOTE and the caller matches on the
    SAME constant — R142's marker rule. If either side re-types the string, this fails."""
    src = open(os.path.join(os.path.dirname(orchestrate.__file__), "orchestrate.py"),
               encoding="utf-8").read()
    assert src.count("_NO_CURSORS_NOTE") >= 3, (
        "expected the shared constant at: definition, the branch's return, and the "
        "caller's startswith match"
    )
    # And the literal must appear exactly once — in the constant's definition.
    assert src.count('"csv coherence unmet: fetcher reported no series_cursors"') == 1


def test_health_holds_an_owed_source_at_attention_until_cleared():
    """The debt must be VISIBLE where verdicts are read: an otherwise-green source
    with an owed row reads ATTENTION with the owed note, and ONLY the clear (the
    campaign stamp's call) releases it back to OK. This is the signal that was
    missing while noaa served a stale corpus with every instrument green."""
    from datetime import datetime, timezone
    from updater import health

    st = StateStore(path=":memory:")
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    # Fabricate a healthy 'noaa' (a real registry id, so assess() emits its row):
    # succeeded just now, newest observation today -> OK on every existing signal.
    st.upsert_source("noaa", status="ok", last_success_utc=now.isoformat(),
                     last_attempt_utc=now.isoformat())
    st.upsert_unit("noaa", "_all", status="ok", last_obs_date=today,
                   last_attempt_utc=now.isoformat())

    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == "noaa")
    assert row["health"] == "OK", f"precondition: fabricated-healthy noaa reads {row['health']}"

    st.note_full_rederive_owed("noaa", vintage="noaa:abc", note="csv coherence unmet…")
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == "noaa")
    assert row["health"] == "ATTENTION"
    assert any("full re-derive OWED" in a for a in row["attention"]), row["attention"]

    st.clear_full_rederive_owed("noaa")
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == "noaa")
    assert row["health"] == "OK"


def test_owed_note_survives_the_attention_truncation():
    """R529 latent #3: attention is displayed as [:10], and a note that CHANGES THE
    VERDICT must survive that cut. Appended, a >=10-unit rotator flipped ATTENTION
    with its reason invisible; the note is prepended now — prove it on a source
    with 12 attention-status units."""
    from datetime import datetime, timezone
    from updater import health

    st = StateStore(path=":memory:")
    now = datetime.now(timezone.utc).isoformat()
    for i in range(12):
        st.upsert_unit("noaa", f"u{i:02d}", status="partial",
                       last_error=f"boom {i}", last_attempt_utc=now)
    st.note_full_rederive_owed("noaa", vintage="v", note="n")

    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == "noaa")
    assert row["health"] == "ATTENTION"
    assert len(row["attention"]) == 10          # the truncation is real in this shape
    assert row["attention"][0].startswith("full re-derive OWED"), row["attention"][0]


def test_orphan_owed_row_survives_deregistration():
    """R529 latent #4: assess() iterates registry entries, so an owed row whose
    source leaves registry.yaml surfaced NOWHERE. It must appear as its own
    ATTENTION row (live=False keeps the CI gate out of it)."""
    from updater import health

    st = StateStore(path=":memory:")
    st.note_full_rederive_owed("zz_not_a_registered_source", vintage="v", note="n")
    rows = [r for r in health.assess(store=st)["sources"]
            if r["source"] == "zz_not_a_registered_source"]
    assert len(rows) == 1
    assert rows[0]["health"] == "ATTENTION" and rows[0]["live"] is False
    assert any("NOT in registry.yaml" in a for a in rows[0]["attention"])


def test_durable_clear_refuses_lock_and_failed_pull(tmp_path, monkeypatch):
    """R529's core: the clear must move through pull->push, and must REFUSE both
    when a heavy pass holds the lock (a pull would clobber its in-progress state)
    and when the pull itself fails (clearing a stale copy dies at the next pull).
    Neither branch may reach the store."""
    import subprocess
    import types
    from tools import derive_csv_bulk as dcb

    monkeypatch.setattr(dcb, "ROOT", str(tmp_path))
    calls = []
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **k: calls.append(a) or
                        types.SimpleNamespace(stdout="", returncode=1))

    # Live lock -> refuse WITHOUT even pulling.
    lockdir = tmp_path / "logs"
    lockdir.mkdir()
    (lockdir / "local_heavy.lock").write_text("1234,000")
    assert dcb._durable_clear("noaa") is False
    assert calls == [], "a pull was attempted under a live heavy-pass lock"

    # Lock gone, pull fails (rc=1) -> refuse after exactly the pull attempt.
    (lockdir / "local_heavy.lock").unlink()
    assert dcb._durable_clear("noaa") is False
    assert len(calls) == 1, "expected one pull-state attempt and no push"


def test_no_cursors_branch_returns_the_shared_note():
    """Drive the actual branch: obs merged, no cursors, catalogued > 0."""
    class _Unit:
        source_id = "testsrc"
        unit_id = "_all"
        key = "testsrc/_all"

    class _Res:
        obs = 42
        series_cursors = None
        new_vintage = "v1"

    import unittest.mock as mock
    with mock.patch.object(orchestrate, "_catalog_series_count", return_value=10,
                           create=True):
        failed, note, deferred, reasons = orchestrate._derive_changed_csvs(
            _Unit(), _Res(), blob=None)
    assert failed == [] and deferred == [] and reasons == {}
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_bulk_derive_refuses_a_behind_mirror(tmp_path, monkeypatch):
    """R530: the campaign tool must refuse when the local mirror is behind the
    authoritative store — the guard core/derive_csv.py gained after R383 and this
    tool lacked while it rewrote 35,135 norgesbank CSVs to a stale vintage."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import core.derive_csv as cdc
    from tools import derive_csv_bulk as dcb

    root = tmp_path / "fixture_root"
    d = root / "data" / "clean_full" / "zz_mirror_test"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": pa.array(["a"], pa.string()),
                             "obs_date": pa.array(["2024-01-01"], pa.string()),
                             "value": pa.array([1.0], pa.float64())}),
                   str(d / "zz.parquet"))
    monkeypatch.setattr(dcb, "ROOT", str(root))
    monkeypatch.setattr(cdc, "_mirror_behind_store",
                        lambda sources, sample=0: [("zz_mirror_test",
                                                    "zz.parquet: local 1 vs R2 2")])
    monkeypatch.setattr(sys, "argv",
                        ["derive_csv_bulk.py", "--source", "zz_mirror_test",
                         "--bucket", "econ-data"])
    assert dcb.main() == 2, "a behind mirror must refuse the campaign"
    # ...and --allow-stale-mirror plus a clean mirror both proceed past the guard
    # (they will then fail later on licence import or R2 -- not asserted here).
