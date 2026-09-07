"""The registry's `csv_grain` field (R882): validated, pinned to the resolver, and the thing
the health gate's owed-re-derive remedy keys on.

Both directions per R414: a typo is REFUSED (a silent default to series is how the
series-grain remedy got printed for a flow-grain source), the two legal values pass; the
flow-grain remedy names the flow tool and NOT the bulk tool, the series-grain remedy is the
old text unchanged; and every source that declares `flow` must resolve WHOLE-FILE through
the real resolver, with an example id recorded here — so a new declaration cannot land
without its proof (the R875 drift class: three grain registries already exist).
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
sys.path.insert(0, os.path.join(ROOT, "tools"))

from updater import registry  # noqa: E402
from updater.state import StateStore  # noqa: E402


def _reg(**entry):
    base = {"source_id": "zzgrain", "strategy": "giant_changed_units", "cadence": "monthly",
            "live": True}
    base.update(entry)
    return {"sources": [base]}


def test_validate_refuses_a_typo_and_accepts_the_two_grains():
    assert any("csv_grain" in p for p in registry.validate(_reg(csv_grain="flwo")))
    assert any("csv_grain" in p for p in registry.validate(_reg(csv_grain="")))
    assert not [p for p in registry.validate(_reg(csv_grain="flow")) if "csv_grain" in p]
    assert not [p for p in registry.validate(_reg(csv_grain="series")) if "csv_grain" in p]
    assert not [p for p in registry.validate(_reg()) if "csv_grain" in p]   # absent = series


def test_real_registry_validates_and_eurostat_declares_flow():
    reg = registry.load()
    assert not [p for p in registry.validate(reg) if "csv_grain" in p]
    by_id = {e["source_id"]: e for e in reg["sources"]}
    assert by_id["eurostat"].get("csv_grain") == "flow"
    declared = sorted(e["source_id"] for e in reg["sources"] if "csv_grain" in e)
    assert declared, "the field exists to be used"
    assert all(by_id[s]["csv_grain"] in registry.CSV_GRAINS for s in declared)


# RATCHET: every `csv_grain: flow` source needs an example catalogue id AND the store path
# its resolver builds for it. Adding a flow-grain declaration without a row here fails.
FLOW_EXAMPLES = {
    "eurostat": ("eurostat:aact_ali01", os.path.join("eurostat", "AACT_ALI01.parquet")),
}


def test_ratchet_every_flow_grain_source_resolves_whole_file(tmp_path):
    from econdl import _resolve
    from audit_store_vs_catalog import _predicate_shape       # the audit's own classifier
    reg = registry.load()
    flow_sources = sorted(e["source_id"] for e in reg["sources"] if e.get("csv_grain") == "flow")
    missing = [s for s in flow_sources if s not in FLOW_EXAMPLES]
    assert not missing, f"csv_grain: flow declared without a whole-file proof here: {missing}"
    for src in flow_sources:
        sid, rel = FLOW_EXAMPLES[src]
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"series_key": ["k"], "obs_date": ["2024-01-01"],
                                 "value": [1.0]}), str(p))
        res = _resolve.resolve(sid, str(tmp_path))
        shape = _predicate_shape(str(getattr(res, "predicate", "")),
                                 str(getattr(res, "key_col", "")))
        assert shape == "WHOLE-FILE", (src, sid, shape, getattr(res, "predicate", None))
        assert os.path.normcase(str(res.parquet_path)) == os.path.normcase(str(p))


def test_health_remedy_names_the_flow_tool_only_for_flow_grain(monkeypatch):
    from updater import health
    fake = {"sources": [
        {"source_id": "zzflow", "strategy": "giant_changed_units", "cadence": "monthly",
         "live": True, "csv_grain": "flow"},
        {"source_id": "zzseries", "strategy": "bulk_snapshot_if_changed", "cadence": "monthly",
         "live": True},
    ]}
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: fake)
    st = StateStore(path=":memory:")
    st.note_full_rederive_owed("zzflow", vintage="v1", note="unmet")
    st.note_full_rederive_owed("zzseries", vintage="v1", note="unmet")
    rows = {r["source"]: r for r in health.assess(store=st)["sources"]}
    flow_note = rows["zzflow"]["attention"][0]
    series_note = rows["zzseries"]["attention"][0]
    # the pinned prefix survives on both (tests/test_full_rederive_owed.py keys on it)
    assert flow_note.startswith("full re-derive OWED since ")
    assert series_note.startswith("full re-derive OWED since ")
    # flow grain: the flow tool, the read-back, the clear — and NOT a bare bulk-tool command
    assert "core.derive_csv" in flow_note and "--only" in flow_note
    assert "--clear-owed-only" in flow_note and "FLOW-grain" in flow_note
    assert "run tools/derive_csv_bulk.py --source zzflow;" not in flow_note
    # series grain: the old text, unchanged
    assert "run tools/derive_csv_bulk.py --source zzseries; its zero-error" in series_note
    assert "core.derive_csv" not in series_note
    for r in (rows["zzflow"], rows["zzseries"]):
        assert r["health"] not in ("OK", "ROTATING")
