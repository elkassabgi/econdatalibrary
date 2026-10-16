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
    "insee_melodi": ("insee_melodi:DD_CNA_AGREGATS", os.path.join("insee_melodi", "DD_CNA_AGREGATS.parquet")),
}
# Sources whose store files carry a `flow` column that the resolver ASSERTS (`flow == <native id>`,
# _resolve_file_grain). The fixture must carry the column too: without it the resolver falls back to
# is_valid(key) and this ratchet would prove a shape the real store does not have.
FLOW_COLUMN_SOURCES = {"insee_melodi"}


def test_ratchet_every_flow_grain_source_resolves_whole_file(tmp_path):
    from econdl import _resolve
    from audit_store_vs_catalog import _predicate_shape       # the audit's own classifier
    reg = registry.load()
    flow_sources = sorted(e["source_id"] for e in reg["sources"] if e.get("csv_grain") == "flow")
    missing = [s for s in flow_sources if s not in FLOW_EXAMPLES]
    assert not missing, f"csv_grain: flow declared without a whole-file proof here: {missing}"
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    for src in flow_sources:
        sid, rel = FLOW_EXAMPLES[src]
        native = sid.split(":", 1)[1]
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        cols = {"series_key": ["k"], "obs_date": ["2024-01-01"], "value": [1.0]}
        if src in FLOW_COLUMN_SOURCES:
            cols = {"flow": [native], **cols}
        pq.write_table(pa.table(cols), str(p))
        res = _resolve.resolve(sid, str(tmp_path))
        if src in FLOW_COLUMN_SOURCES:
            # the whole file is ONE flow; the predicate asserts it and the stream carries it
            # (tests/test_stream_flow_predicate.py proves the bytes)
            assert res.predicate.equals(pc.equal(ds.field("flow"), native)), (src, sid, res.predicate)
        else:
            shape = _predicate_shape(str(getattr(res, "predicate", "")),
                                     str(getattr(res, "key_col", "")))
            assert shape == "WHOLE-FILE", (src, sid, shape, getattr(res, "predicate", None))
        assert os.path.normcase(str(res.parquet_path)) == os.path.normcase(str(p))


def test_flow_column_sources_is_pinned_to_what_writes_the_store():
    """FLOW_COLUMN_SOURCES is self-declared, so pin it to the code that WRITES the store: insee_melodi's
    fetcher dedups on a `flow` column, so its files carry one and the resolver asserts it. Emptying the
    set (review mutant MF) must fail here, without needing the store on this machine."""
    from updater.strategies.fetchers import insee_melodi
    assert "flow" in insee_melodi.DEDUP
    assert "insee_melodi" in FLOW_COLUMN_SOURCES


def test_flow_column_sources_matches_the_real_store_when_present():
    """Where the store is on this machine (the desktop), every declared flow-grain source's files carry a
    `flow` column exactly when the source is in FLOW_COLUMN_SOURCES. Skips, and says so, elsewhere."""
    import pytest
    from econdl import _resolve
    reg = registry.load()
    root = _resolve.default_data_root()
    seen = 0
    for src in sorted(e["source_id"] for e in reg["sources"] if e.get("csv_grain") == "flow"):
        d = os.path.join(root, src)
        files = sorted(f for f in (os.listdir(d) if os.path.isdir(d) else []) if f.endswith(".parquet"))[:20]
        if not files:
            continue
        has = {("flow" in pq.read_schema(os.path.join(d, f)).names) for f in files}
        assert has == {src in FLOW_COLUMN_SOURCES}, (src, has)
        seen += 1
    if not seen:
        pytest.skip(f"no flow-grain store under {root} - run with ECONDL_DATA at the real clean_full root")


def test_ratchet_flow_grain_stores_hold_no_null_values():
    """Final-diff review condition 6: the streaming path writes a null `value` as '' and the
    in-memory path as 'nan', so a flow-grain store with nulls would serve two different bytes
    for the same data depending on the path. Measured 2026-09-07: eurostat holds 0 nulls in
    7,654 files. This ratchet reads only parquet FOOTERS (null_count statistics) of every declared
    flow-grain store; on a checkout without the store it SKIPS and says so (R779)."""
    import pytest
    from econdl import _resolve
    reg = registry.load()
    flow_sources = sorted(e["source_id"] for e in reg["sources"] if e.get("csv_grain") == "flow")
    checked = 0
    root = _resolve.default_data_root()          # honours ECONDL_DATA; else <checkout>/data/clean_full
    for src in flow_sources:
        d = os.path.join(root, src)
        files = sorted(f for f in (os.listdir(d) if os.path.isdir(d) else []) if f.endswith(".parquet"))
        if not files:
            pytest.skip(f"store for {src} absent at {d} — run with ECONDL_DATA pointing at the real "
                        f"clean_full root (the desktop) to execute the null ratchet")
        nulls, nostats, nullkeys = 0, 0, 0
        for f in files:
            md = pq.read_metadata(os.path.join(d, f))
            names = [md.schema.column(i).name for i in range(md.num_columns)]
            if "value" not in names:
                continue
            j = names.index("value")
            # A NULL series_key diverges too: the in-memory path writes a 'None' row, the stream's
            # WHERE key IS NOT NULL drops it (review of the insee_melodi csv_grain change, F4).
            kj = next((names.index(c) for c in ("series_key", "series_id") if c in names), None)
            for g in range(md.num_row_groups):
                for col, kind in ((j, "value"), (kj, "key")):
                    if col is None:
                        continue
                    stt = md.row_group(g).column(col).statistics
                    if stt is None:
                        nostats += 1
                    elif stt.null_count:
                        if kind == "value":
                            nulls += stt.null_count
                        else:
                            nullkeys += stt.null_count
            checked += 1
        assert nulls == 0, f"{src}: {nulls} null value(s) in a flow-grain store — the two derive paths diverge on them"
        assert nullkeys == 0, f"{src}: {nullkeys} null series key(s) in a flow-grain store — the two derive paths diverge on them"
        assert nostats == 0, f"{src}: {nostats} row group(s) without statistics — the ratchet cannot see them"
    assert checked > 0


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
