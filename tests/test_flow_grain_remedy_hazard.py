"""The bfs remedy hazard (2026-11-11): a source the resolver serves by key PREFIX must never be handed the bulk campaign.

`econdl._resolve._FLOW_GRAIN` serves eleven PxWeb-style sources by `key == id or key starts with id + ':'`. Before
this change only dst declared `csv_grain: table`, so for the other ten the health gate printed `run
tools/derive_csv_bulk.py --source <sid>` for an owed re-derive. That campaign writes one object per STORE key (bfs:
582 catalogue ids over 483,667 store keys), its --verify passes (the resolver's exact-equal arm matches a full store
key), and its zero-error end clears the owed row although no served table CSV was re-derived.

Pinned here, each with a negative control:
  1. RATCHET: every `_FLOW_GRAIN` member declares `csv_grain: table` in the real registry.
  2. The bulk tool refuses every campaign mode on a prefix-grain or flow-grain source - declared, OR undeclared but
     in the resolver's sets, OR when those sets cannot be read - and still runs `--clear-owed-only`.
  3. The health gate's remedy for an undeclared prefix-grain source is the per-id byte-compare, never the campaign.
  4. `table` changes nothing in the orchestrator: every `_csv_grain(...)` there is compared `== "flow"`, and the real
     retry drain calls derive exactly as for `series`.
"""
import ast
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
sys.path.insert(0, os.path.join(ROOT, "tools"))

from updater import config, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402

TEN = ["stat_latvia", "stat_estonia", "ssb", "bfs", "statfin", "hagstofa", "stat_slovenia", "scb", "unsdg", "cso"]


def _undeclared(reg: dict, members) -> list:
    """Members of the resolver's prefix set whose registry entry does not declare csv_grain: table."""
    by_id = {e["source_id"]: e for e in reg.get("sources", [])}
    return sorted(s for s in members if (by_id.get(s) or {}).get("csv_grain") != "table")


# ---------------------------------------------------------------- 1. the ratchet
def test_ratchet_every_flow_grain_member_declares_table():
    from econdl import _resolve
    reg = registry.load()
    assert set(TEN) | {"dst"} == set(_resolve._FLOW_GRAIN), "the resolver's set moved: re-measure, then update TEN"
    missing = _undeclared(reg, _resolve._FLOW_GRAIN)
    assert not missing, (f"_FLOW_GRAIN member(s) without csv_grain: table: {missing} - health would print the "
                         f"bulk campaign for them (the bfs remedy hazard)")
    assert not registry.validate(reg, expected_count=config.EXPECTED_SOURCE_COUNT)


def test_ratchet_negative_control_one_missing_declaration_is_caught():
    from econdl import _resolve
    reg = registry.load()
    for victim in ("bfs", "unsdg"):
        cut = {"sources": [({k: v for k, v in e.items() if k != "csv_grain"} if e["source_id"] == victim else e)
                           for e in reg["sources"]]}
        assert _undeclared(cut, _resolve._FLOW_GRAIN) == [victim]


# ---------------------------------------------------------------- 2. the bulk tool
def _bulk(monkeypatch, *args):
    import derive_csv_bulk as B
    monkeypatch.setattr(sys, "argv", ["derive_csv_bulk.py", *args])
    cleared = []
    monkeypatch.setattr(B, "_durable_clear", lambda src, **kw: cleared.append(src) or True)
    # past every refusal the tool imports duckdb first: make that the observable "the checks let it through"
    monkeypatch.setitem(sys.modules, "duckdb", None)
    return B, cleared


CAMPAIGNS = (["--dry-run"], [], ["--only-catalogued"], ["--only-catalogued", "--dry-run"])


@pytest.mark.parametrize("src", TEN)
def test_bulk_tool_refuses_every_campaign_mode_on_the_ten_and_allows_the_clear(monkeypatch, capsys, src):
    for extra in CAMPAIGNS:
        B, cleared = _bulk(monkeypatch, "--source", src, *extra)
        rc = B.main()
        out = capsys.readouterr().out
        assert rc == 2 and cleared == [] and "REFUSING" in out, (src, extra, out)
    B, cleared = _bulk(monkeypatch, "--source", src, "--clear-owed-only")
    assert B.main() == 0 and cleared == [src]


def test_bulk_tool_refuses_an_undeclared_prefix_source_on_the_resolvers_word(tmp_path, monkeypatch, capsys):
    """The registry alone is not trusted: bfs (a _FLOW_GRAIN member) and unctad_intratrade (_DOT_TABLE_GRAIN) with no
    declaration are still refused; a series source in the same registry passes the checks (negative control)."""
    p = tmp_path / "registry.yaml"
    p.write_text("- source_id: bfs\n- source_id: unctad_intratrade\n- source_id: zzs\n", encoding="utf-8")
    monkeypatch.setattr(orchestrate.config, "REGISTRY", str(p))
    for src in ("bfs", "unctad_intratrade"):
        for extra in CAMPAIGNS:
            B, cleared = _bulk(monkeypatch, "--source", src, *extra)
            rc = B.main()
            out = capsys.readouterr().out
            assert rc == 2 and cleared == [] and "served by key PREFIX" in out, (src, extra, out)
        B, cleared = _bulk(monkeypatch, "--source", src, "--clear-owed-only")
        assert B.main() == 0 and cleared == [src]
    B, cleared = _bulk(monkeypatch, "--source", "zzs", "--dry-run")
    with pytest.raises(ImportError):                       # reached `import duckdb`: no refusal fired
        B.main()
    assert "REFUSING" not in capsys.readouterr().out


def test_bulk_tool_refuses_a_flow_grain_campaign_and_allows_the_clear(monkeypatch, capsys):
    for extra in CAMPAIGNS:
        B, cleared = _bulk(monkeypatch, "--source", "eurostat", *extra)
        rc = B.main()
        out = capsys.readouterr().out
        assert rc == 2 and cleared == [] and "is csv_grain: flow" in out, (extra, out)
    B, cleared = _bulk(monkeypatch, "--source", "eurostat", "--clear-owed-only")
    assert B.main() == 0 and cleared == ["eurostat"]


def test_bulk_tool_series_source_still_passes_the_checks(monkeypatch, capsys):
    """Negative control on the REAL registry: a plain series-grain source is not refused by any of this."""
    B, cleared = _bulk(monkeypatch, "--source", "abs", "--dry-run")
    with pytest.raises(ImportError):
        B.main()
    assert "REFUSING" not in capsys.readouterr().out and cleared == []


def test_bulk_tool_refuses_campaigns_when_the_resolver_cannot_be_read(monkeypatch, capsys):
    import econdl
    monkeypatch.delattr(econdl, "_resolve", raising=False)
    monkeypatch.setitem(sys.modules, "econdl._resolve", None)
    B, cleared = _bulk(monkeypatch, "--source", "abs", "--dry-run")
    assert B.main() == 2 and "cannot read the resolver" in capsys.readouterr().out and cleared == []
    B, cleared = _bulk(monkeypatch, "--source", "abs", "--clear-owed-only")
    assert B.main() == 0 and cleared == ["abs"]


# ---------------------------------------------------------------- 3. the health gate
def _notes(monkeypatch, entries):
    from updater import health
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: {"sources": entries})
    st = StateStore(path=":memory:")
    for e in entries:
        st.note_full_rederive_owed(e["source_id"], vintage="v", note="csv coherence unmet: no cursors")
    return {r["source"]: r["attention"][0] for r in health.assess(store=st)["sources"]}


def _assert_table_remedy(sid, note):
    assert note.startswith("full re-derive OWED since ")
    assert "TABLE-grain" in note and "byte-compare" in note, note
    assert f"python -m core.derive_csv --bucket econ-data --source {sid} --only" in note, note
    i, n = 0, 0
    while (i := note.find(f"derive_csv_bulk.py --source {sid}", i)) != -1:
        assert note[i:].startswith(f"derive_csv_bulk.py --source {sid} --clear-owed-only"), note
        i, n = i + 1, n + 1
    assert n == 1, note


def test_health_remedy_for_the_ten_as_declared_is_the_byte_compare(monkeypatch):
    real = {e["source_id"]: dict(e) for e in registry.load()["sources"]}
    series = {"source_id": "zzseries", "strategy": "bulk_snapshot_if_changed", "cadence": "monthly", "live": True}
    notes = _notes(monkeypatch, [real[s] for s in TEN] + [series])
    for s in TEN:
        _assert_table_remedy(s, notes[s])
    assert "run tools/derive_csv_bulk.py --source zzseries; its zero-error" in notes["zzseries"], "negative control"


def test_health_remedy_for_an_undeclared_prefix_source_is_still_the_byte_compare(monkeypatch):
    base = {"strategy": "sdmx_delta", "cadence": "monthly", "live": True}
    notes = _notes(monkeypatch, [{"source_id": "bfs", **base}, {"source_id": "unctad_intratrade", **base},
                                 {"source_id": "zzseries", **base}])
    _assert_table_remedy("bfs", notes["bfs"])
    _assert_table_remedy("unctad_intratrade", notes["unctad_intratrade"])
    assert "run tools/derive_csv_bulk.py --source zzseries; its zero-error" in notes["zzseries"], "negative control"


# ---------------------------------------------------------------- 4. the orchestrator is unchanged by `table`
def test_every_orchestrator_use_of_csv_grain_compares_to_flow():
    src = open(os.path.join(ROOT, "updater", "orchestrate.py"), encoding="utf-8").read()
    tree = ast.parse(src)
    calls, ok = [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "_csv_grain":
            calls.append(node)
        if (isinstance(node, ast.Compare) and isinstance(node.left, ast.Call)
                and getattr(node.left.func, "id", None) == "_csv_grain" and len(node.ops) == 1
                and isinstance(node.ops[0], ast.Eq) and isinstance(node.comparators[0], ast.Constant)
                and node.comparators[0].value == "flow"):
            ok.append(node.left)
    assert calls, "the scan found nothing - it cannot be trusted"
    stray = [ast.unparse(c) for c in calls if not any(c is o for o in ok)]
    assert not stray, f"_csv_grain used other than `== 'flow'`: {stray} - re-audit what `table` does there"


def test_the_csv_grain_scan_can_fail():
    """Negative control for the scan above: a `!= 'series'` use is caught."""
    tree = ast.parse("a = _csv_grain(s) == 'flow'\nb = _csv_grain(s) != 'series'\n")
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_csv_grain"]
    ok = [n.left for n in ast.walk(tree) if isinstance(n, ast.Compare) and isinstance(n.ops[0], ast.Eq)
          and getattr(n.comparators[0], "value", None) == "flow"]
    assert len([c for c in calls if not any(c is o for o in ok)]) == 1


def test_real_csv_grain_reads_table_for_the_ten_and_series_by_default(monkeypatch):
    monkeypatch.setattr(orchestrate._csv_grain, "_cache", {}, raising=False)
    for s in TEN + ["dst"]:
        assert orchestrate._csv_grain(s) == "table", s
    assert orchestrate._csv_grain("eurostat") == "flow"
    assert orchestrate._csv_grain("abs") == "series" and orchestrate._csv_grain("no_such_source") == "series"


def test_the_real_retry_drain_derives_a_table_source_exactly_as_a_series_source(tmp_path, monkeypatch):
    """Through the REAL _csv_grain over a registry that declares one source `table` and leaves its twin undeclared:
    derive_and_put gets the same call (no flow_grain keyword) and the queue ends the same. A `flow` twin is the
    negative control: it DOES get flow_grain=True."""
    from updater import derive
    fake = {"sources": [{"source_id": s, "strategy": "sdmx_delta", "cadence": "monthly", "live": True, **g}
                        for s, g in (("zzt", {"csv_grain": "table"}), ("zzs", {}), ("zzf", {"csv_grain": "flow"}))]}
    monkeypatch.setattr(orchestrate.registry, "load", lambda *a, **k: fake)
    monkeypatch.setattr(orchestrate._csv_grain, "_cache", {}, raising=False)
    monkeypatch.setattr(orchestrate.config, "STATE_DIR", str(tmp_path / "_aqueduct"))
    monkeypatch.setattr(orchestrate, "_csv_desktop_excluded", lambda sid: set())
    seen = {}

    def fake_derive(ids, blob, **kw):
        seen[ids[0].split(":", 1)[0]] = (sorted(i.split(":", 1)[1] for i in ids), dict(kw))
        return {"put": len(ids), "put_ids": list(ids), "failed": []}
    monkeypatch.setattr(derive, "derive_and_put", fake_derive)
    queues = {}
    for sid in ("zzt", "zzs", "zzf"):
        st = StateStore(path=str(tmp_path / f"{sid}.db"))
        st.enqueue_csv_retry(sid, [f"{sid}:a", f"{sid}:b"], "earlier failure")
        unit = types.SimpleNamespace(key=f"{sid}/_all", source_id=sid, unit_id="_all")
        orchestrate._drain_csv_retry_queue(unit, object(), st)
        queues[sid] = sorted(r["series_id"].split(":", 1)[1] for r in st.csv_retries(sid))
    assert set(seen) == {"zzt", "zzs", "zzf"}, seen
    assert seen["zzt"] == seen["zzs"] and "flow_grain" not in seen["zzt"][1], seen
    assert queues["zzt"] == queues["zzs"] == [], queues
    assert seen["zzf"][1].get("flow_grain") is True, "negative control: a flow source is treated differently"
