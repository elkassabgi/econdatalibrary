"""tools/audit_licence_disclosure.py after T0 (plan step 6d): the served licences are the catalogue BUILD's (the origin
serves its copies; D1 is frozen and is not asked)."""
import os
import sqlite3
import sys

import pytest

from core import catalog_path, cutover, d1_remote

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import audit_licence_disclosure as L  # noqa: E402


def test_after_t0_the_build_is_the_serving_catalogue(tmp_path, monkeypatch, capsys):
    build = tmp_path / "live" / "data" / "catalog.db"
    build.parent.mkdir(parents=True)
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, license_id TEXT)")
        c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, commercial_ok INT, no_modify INT, reservable INT)")
        c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT)")
        c.execute("INSERT INTO series VALUES ('zz:a', 'zz', 'cc-by-4.0')")
        c.execute("INSERT INTO license VALUES ('cc-by-4.0', 1, 0, 1)")
    c.close()
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(d1_remote, "rows", lambda *a, **k: pytest.fail("D1 asked after T0"))
    monkeypatch.setattr(L, "classifications", lambda: {"zz": "redistributable_attribution"})
    monkeypatch.setattr(L, "granted", lambda: {})
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(sys, "argv", ["audit_licence_disclosure.py"])
    L.main()
    assert "read from: the catalogue BUILD (what the origin serves since T0" in capsys.readouterr().out
