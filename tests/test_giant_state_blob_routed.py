"""The _giant rotation sidecar must be BLOB-ROUTED (R533's fleet finding).

save_state/load_state used plain open(), so `_giant_state.json` never survived an
ephemeral CI runner (R2 HEAD: absent for eurostat AND oecd) — every CI sweep started
state={} and re-walked the same first ~selection-cap TOC entries forever (R190's class
for the whole _giant family on CI). blob.read_bytes/write_bytes_atomic exist precisely
for store-adjacent sidecars.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import _giant  # noqa: E402
from updater import blob  # noqa: E402


def test_round_trip_and_corrupt_tolerance(tmp_path, monkeypatch):
    # HERMETIC (review change 3): blob reads AQUEDUCT_BACKEND per call, so on a
    # machine exporting r2 this test would write the REAL bucket. Pin local.
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    d = str(tmp_path / "src")
    assert _giant.load_state(d) == {}
    _giant.save_state(d, {"FLOW": {"status": "ok"}})
    assert _giant.load_state(d) == {"FLOW": {"status": "ok"}}
    # corrupt sidecar -> {} (never crash the run)
    open(_giant.state_path(d), "w").write("{not json")
    assert _giant.load_state(d) == {}


def test_reads_and_writes_go_through_blob(tmp_path, monkeypatch):
    """The binding assertion: under the r2 backend blob is what makes the sidecar
    durable — if either side stops routing through it, CI state dies again."""
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    d = str(tmp_path / "src")
    calls = {"r": 0, "w": 0}
    real_r, real_w = blob.read_bytes, blob.write_bytes_atomic
    monkeypatch.setattr(blob, "read_bytes",
                        lambda p: calls.__setitem__("r", calls["r"] + 1) or real_r(p))
    monkeypatch.setattr(blob, "write_bytes_atomic",
                        lambda p, b: calls.__setitem__("w", calls["w"] + 1) or real_w(p, b))
    _giant.save_state(d, {"x": 1})
    assert _giant.load_state(d) == {"x": 1}
    assert calls == {"r": 1, "w": 1}, calls


def test_all_giant_sidecar_consumers_route_through_the_fix():
    """R272: both ends, every consumer — asserted at the TRUE grain (review change 2;
    the first draft matched the substring 'load_state' against sec_edgar/statcan's
    PRIVATE _load_state and passed on files this fix does not touch).

    What this fix actually covers: `_giant_state.json`, whose only writers are
    _giant.load_state/save_state. eurostat and oecd reach them via run_giant();
    sdmx_nso calls them directly. Pin those call sites, and pin that nobody opens
    the sidecar around blob.

    THAT HOLE IS NOW CLOSED (2026-08-31, same session): sec_edgar's
    `_sec_edgar_incr_state.json` and statcan's STATE watermark were plain-open()
    per-runner state — sec_edgar runs in the CLOUD, so its per-window done-markers
    never survived and every CI run re-did completed windows. Both local copies
    were pushed to R2 first (124 B and 35 B, read-back identical) and both now
    route through blob. Pinned below so they cannot regress."""
    base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "updater", "strategies", "fetchers")

    def src(mod):
        return open(os.path.join(base, mod), encoding="utf-8").read()

    for mod in ("eurostat.py", "oecd.py"):
        assert "_giant.run_giant(" in src(mod), (
            f"{mod} no longer drives through run_giant — sidecar routing unverified")
    s = src("sdmx_nso.py")
    assert "_giant.load_state(" in s and "_giant.save_state(" in s, (
        "sdmx_nso stopped using the shared sidecar API")
    for mod in sorted(os.listdir(base)):
        if not mod.endswith(".py"):
            continue
        for i, line in enumerate(src(mod).splitlines(), 1):
            if "_giant_state.json" in line and "open(" in line:
                raise AssertionError(
                    f"{mod}:{i} opens the sidecar directly — bypasses the blob routing")

    # The two formerly-private states, now blob-routed. A plain open() here means the
    # per-runner disease is back: sec_edgar would re-do completed windows on every CI run.
    for mod, marker in (("sec_edgar.py", "_state_path()"), ("statcan.py", "STATE")):
        m = src(mod)
        assert f"blob.read_bytes({marker})" in m, (
            f"{mod} no longer READS its incremental state through blob")
        assert f"blob.write_bytes_atomic({marker}" in m, (
            f"{mod} no longer WRITES its incremental state through blob")
        assert "json.load(open(" not in m, f"{mod} reopened a raw json.load(open(...))"
