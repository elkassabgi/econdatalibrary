"""The six imf_gfs*_direct sources serve ANNUAL data (every served row dated 31 December, measured 2026-08-02 for
two of them and 2026-09-29 for the other four), so each declares data_cadence: annual with its measurement. The
four without it read RED-DATA at 272 days on their quarterly POLL clock (daily gate run 36495274373)."""
import os

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REG = os.path.join(ROOT, "updater", "registry.yaml")
GFS = {"imf_gfsbs_direct", "imf_gfscofog_direct", "imf_gfssfcp_direct", "imf_gfssoo_direct",
       "imf_gfsssuc_direct", "imf_gfssoef_direct"}


def test_every_gfs_source_has_the_measured_annual_data_clock():
    reg = yaml.safe_load(open(REG, encoding="utf-8"))
    by_id = {e["source_id"]: e for e in reg["sources"]}
    assert GFS <= set(by_id), sorted(GFS - set(by_id))
    assert {s: by_id[s].get("data_cadence") for s in GFS} == {s: "annual" for s in GFS}
    lines = open(REG, encoding="utf-8").read().splitlines()
    for s in GFS:
        start = lines.index(f"- source_id: {s}")
        end = next(i for i in range(start + 1, len(lines)) if lines[i].startswith("- source_id: "))
        block = lines[start:end]
        at = next(i for i, ln in enumerate(block) if ln.startswith("  data_cadence:"))
        above = []
        for ln in reversed(block[:at]):
            if not ln.strip().startswith("#"):
                break
            above.append(ln)
        assert any("MEASURED" in ln for ln in above), f"{s}: data_cadence without a MEASURED comment above it"


def test_every_imf_gfs_entry_is_covered():
    """A seventh GFS flow added later must be measured too, not silently left on the poll clock."""
    reg = yaml.safe_load(open(REG, encoding="utf-8"))
    found = {e["source_id"] for e in reg["sources"] if e["source_id"].startswith("imf_gfs")}
    assert found == GFS, sorted(found ^ GFS)
