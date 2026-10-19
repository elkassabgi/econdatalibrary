"""dst is TABLE grain (registry `csv_grain: table`): its owed-re-derive remedy must never name the bulk campaign.

One dst catalogue id (`dst:DST:<TABLE>`) serves every `DST:<TABLE>:...` key of a shared subject parquet. The bulk
campaign (tools/derive_csv_bulk.py without --clear-owed-only) writes one object per STORE key (R498; measured
2026-10-19: 959,014 distinct store keys against 2,264 catalogued table ids), so a reader who followed the old gate
text would have minted wrong-grain objects. Pinned in both places a reader meets it: the health gate's remedy and
the tool itself, with negative controls.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from updater import registry  # noqa: E402
from updater.state import StateStore  # noqa: E402


def test_dst_declares_table_grain_and_the_value_validates():
    reg = registry.load()
    by_id = {e["source_id"]: e for e in reg["sources"]}
    assert by_id["dst"].get("csv_grain") == "table"
    assert not [p for p in registry.validate(reg) if "csv_grain" in p]
    one = {"sources": [{"source_id": "zz", "strategy": "giant_changed_units", "cadence": "monthly",
                        "live": True, "csv_grain": "table"}]}
    assert not [p for p in registry.validate(one) if "csv_grain" in p]
    one["sources"][0]["csv_grain"] = "tabel"
    assert any("csv_grain" in p for p in registry.validate(one))


def _notes(monkeypatch, entries):
    from updater import health
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: {"sources": entries})
    st = StateStore(path=":memory:")
    for e in entries:
        st.note_full_rederive_owed(e["source_id"], vintage="date-tail", note="csv coherence unmet: no cursors")
    return {r["source"]: r for r in health.assess(store=st)["sources"]}


def test_the_dst_remedy_never_names_the_bulk_campaign(monkeypatch):
    dst = next(e for e in registry.load()["sources"] if e["source_id"] == "dst")
    series = {"source_id": "zzseries", "strategy": "bulk_snapshot_if_changed", "cadence": "monthly", "live": True}
    rows = _notes(monkeypatch, [dict(dst), series])
    note = rows["dst"]["attention"][0]
    assert note.startswith("full re-derive OWED since ")                 # the pinned prefix
    assert "TABLE-grain" in note and "byte-compare" in note and "LastModified" in note
    assert "python -m core.derive_csv --bucket econ-data --source dst --only" in note
    # every mention of the bulk tool is the clear, never a campaign
    i, n = 0, 0
    while (i := note.find("derive_csv_bulk.py --source dst", i)) != -1:
        assert note[i:].startswith("derive_csv_bulk.py --source dst --clear-owed-only"), note
        i, n = i + 1, n + 1
    assert n == 1 and "run tools/derive_csv_bulk.py --source dst;" not in note
    assert rows["dst"]["health"] not in ("OK", "ROTATING")
    # negative control: a series-grain source keeps the campaign text
    assert "run tools/derive_csv_bulk.py --source zzseries; its zero-error" in rows["zzseries"]["attention"][0]


def _bulk(monkeypatch, *args):
    import derive_csv_bulk as B
    monkeypatch.setattr(sys, "argv", ["derive_csv_bulk.py", *args])
    cleared = []
    monkeypatch.setattr(B, "_durable_clear", lambda src: cleared.append(src) or True)
    return B.main(), cleared


def test_the_bulk_tool_refuses_a_table_grain_campaign_but_allows_the_clear(monkeypatch, capsys):
    for args in (["--source", "dst"], ["--source", "dst", "--dry-run"], ["--source", "dst", "--only-catalogued"]):
        rc, cleared = _bulk(monkeypatch, *args)
        out = capsys.readouterr().out
        assert rc == 2 and cleared == [] and "is csv_grain: table" in out, (args, out)
    rc, cleared = _bulk(monkeypatch, "--source", "dst", "--clear-owed-only")
    assert rc == 0 and cleared == ["dst"]
    rc, cleared = _bulk(monkeypatch, "--source", "abs", "--clear-owed-only")
    assert rc == 0 and cleared == ["abs"], "negative control"


def test_the_bulk_tool_reads_table_grain_from_the_registry_it_is_given(tmp_path, monkeypatch, capsys):
    from updater import orchestrate
    p = tmp_path / "registry.yaml"
    p.write_text("- source_id: zzt\n  csv_grain: table\n- source_id: zzs\n", encoding="utf-8")
    monkeypatch.setattr(orchestrate.config, "REGISTRY", str(p))
    rc, cleared = _bulk(monkeypatch, "--source", "zzt", "--dry-run")
    assert rc == 2 and "is csv_grain: table" in capsys.readouterr().out
    rc, cleared = _bulk(monkeypatch, "--source", "zzs", "--clear-owed-only", "--dry-run")
    assert rc == 0 and "is csv_grain: table" not in capsys.readouterr().out, "negative control"
