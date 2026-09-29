"""statcan's ONE rule for a whole cube that grew past the derive cap, pinned at the cataloguer's main() (review
AR-166, 2026-09-29).

jobs/statcan_lane.serve_plan served such a cube WHOLE; tools/derive_statcan_tables.pinned_split REFUSED it; and
tools/catalog_statcan_tables.py refused the WHOLE catalogue when an over-cap cube had an object and no split. Now
all three keep it whole: the cap aims a NEW split, it is not a limit on a served object. Measured today: 0 whole
cubes over 3,000,000 rows; the largest, 43100031, holds 2,995,200 and the live worker serves it (84 MB, 1.3 s).

The unit tests of classify_absent pin the rule; THIS pins the wiring - that main() actually tells classify_absent
which tables the catalogue holds whole. The run is stopped right after the guard (sidecars() raises), so it
touches no store and writes nothing."""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "catalog_statcan_tables.py")
PID = "43100031"


def _load():
    spec = importlib.util.spec_from_file_location("_catalog_statcan_overcap_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class _PastTheGuard(Exception):
    pass


def _run(tmp_path, monkeypatch, capsys, *, catalogued_whole: bool, part_object: bool = False):
    m = _load()
    store = tmp_path / "store"
    store.mkdir()
    d = dt.date(2024, 1, 1)
    pq.write_table(pa.table({"series_key": ["v1", "v2"], "obs_date": [d, d], "value": [1.0, 2.0]}),
                   str(store / f"{PID}.parquet"))                  # 2 rows > the recorded cap of 1
    (store / "_split_map.json").write_text("{}")
    root = tmp_path / "root"
    (root / "logs").mkdir(parents=True)
    (root / "logs" / "statcan_tables_summary.json").write_text(json.dumps(
        {"max_rows": 1, "scope": "full", "refused": [], "considered": 1}))
    keys = [m.object_key(m.unit_id(PID), "series")]
    if part_object:
        keys.append(m.object_key(m.unit_id(PID, "x"), "series"))
    listing = tmp_path / "keys.txt"
    listing.write_text("\n".join(keys) + "\n")
    (tmp_path / "keys.txt.meta.json").write_text(json.dumps({
        "bucket": "econ-data", "prefix": m.key_prefix("series"), "count": len(keys),
        "listed_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}))
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, reservable INTEGER)")
    con.execute("INSERT INTO license VALUES ('statcan-open', 1)")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    if catalogued_whole:
        con.execute("INSERT INTO series VALUES (?)", (f"statcan:{PID}",))
    monkeypatch.setattr(m, "ROOT", str(root))
    monkeypatch.setattr(m, "STORE", str(store))
    monkeypatch.setattr(m.catalog_path, "connect", lambda *a, **k: con)

    def _stop():
        raise _PastTheGuard()
    monkeypatch.setattr(m, "sidecars", _stop)
    monkeypatch.setattr(sys, "argv", ["catalog_statcan_tables", "--r2-keys", str(listing)])
    try:
        rc = m.main()
    except _PastTheGuard:
        rc = "past the guard"
    return rc, capsys.readouterr().out


def test_a_whole_cube_over_the_cap_passes_the_guard_and_is_named(tmp_path, monkeypatch, capsys):
    rc, out = _run(tmp_path, monkeypatch, capsys, catalogued_whole=True)
    assert rc == "past the guard", out[-1500:]
    assert "catalogued WHOLE with their whole object in R2 - kept whole" in out and PID in out
    assert "have no split-map entry" not in out


def test_negative_control_the_same_cube_NOT_catalogued_whole_still_refuses(tmp_path, monkeypatch, capsys):
    """Nothing chose to serve it whole: a whole object above the cap is still the refusal it was."""
    rc, out = _run(tmp_path, monkeypatch, capsys, catalogued_whole=False)
    assert rc == 1 and "have no split-map entry" in out and "kept whole" not in out, out[-1500:]


def test_negative_control_a_whole_cube_with_part_objects_too_still_refuses(tmp_path, monkeypatch, capsys):
    rc, out = _run(tmp_path, monkeypatch, capsys, catalogued_whole=True, part_object=True)
    assert rc == 1 and "have no split-map entry" in out, out[-1500:]
