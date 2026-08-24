"""A source carries its licence TWICE, and the two copies can disagree.

`catalog.db` records `series.license_id` on every catalogued row AND `source.license_id` on
the parent row. Nothing local reads the parent, so a wrong value there is invisible - until
`core/sync_catalog_d1.py` pushes parent rows to D1 and the public `/v1/sources` starts
answering with it.

That is not hypothetical. On 2026-08-24 gus_dbw's 194 series all carried `gus-pl-open`
(Statistics Poland: attribution PLUS a PSI disclosure) while its source row said
`cc-by-4.0`, and the moment it was served the live API advertised plain CC BY for data that
carries an extra condition. Ledger R472.

This asserts the two agree for every source that has both, and that the id they name has a
row in the `license` table - a referenced licence with no row is how gus-pl-open was missing
from D1 entirely.

Skips when catalog.db is absent (CI runners that do not pull the 9 GB store); it still bites
on the workstation and anywhere the store is present, which is where publishing happens.
"""
from __future__ import annotations

import os
import sqlite3

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")


def _con():
    if not os.path.exists(CATALOG):
        pytest.skip("catalog.db not present here; this guard runs where the store is")
    return sqlite3.connect("file:%s?mode=ro" % CATALOG.replace("\\", "/"), uri=True, timeout=120)


def test_source_licence_matches_its_series_licence():
    con = _con()
    try:
        series = {s: lid for s, lid in con.execute(
            "SELECT source_id, license_id FROM series GROUP BY source_id "
            "HAVING COUNT(DISTINCT license_id) = 1")}
        parents = {s: lid for s, lid in con.execute(
            "SELECT source_id, license_id FROM source WHERE license_id IS NOT NULL")}
    finally:
        con.close()
    disagree = sorted(
        (s, parents[s], series[s]) for s in set(parents) & set(series)
        if parents[s] != series[s])
    assert not disagree, (
        "these source(s) name one licence on the parent row and another on every series row. "
        "The parent is what core/sync_catalog_d1.py publishes to D1 and what /v1/sources "
        "answers with, so a mismatch is a WRONG LICENCE on a public endpoint (R472): "
        + "; ".join(f"{s}: source={p!r} vs series={c!r}" for s, p, c in disagree))


def test_every_referenced_licence_id_has_a_license_row():
    con = _con()
    try:
        have = {r[0] for r in con.execute("SELECT license_id FROM license")}
        refs = {r[0] for r in con.execute(
            "SELECT DISTINCT license_id FROM source WHERE license_id IS NOT NULL")}
        refs |= {r[0] for r in con.execute(
            "SELECT DISTINCT license_id FROM series WHERE license_id IS NOT NULL")}
    finally:
        con.close()
    missing = sorted(refs - have)
    assert not missing, (
        f"{len(missing)} licence id(s) are referenced by source/series rows but have NO row in "
        f"the `license` table, so nothing can render their terms: {missing}")
