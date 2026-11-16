"""bis: the scheduled unit refreshes the CATALOGUED SDMX flow (WS_CBPOL), not only the CBS/LBS zips.

The only catalogued bis ids are bis:WS_CBPOL:<country>; their store file was written once (2026-06-30)
by jobs/ingest_bis_full.py and nothing refreshed it, while the unit reported success on the zips.
These tests run the real update(), the real merge and the real parser; only the HTTP calls are faked.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import config  # noqa: E402
from updater.strategies.fetchers import bis as B  # noqa: E402
from jobs import ingest_bis_full as F  # noqa: E402

HDR = ("STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE,UNIT_MEASURE,UNIT_MULT,"
       "TIME_FORMAT,COMPILATION,DECIMALS,SOURCE_REF,SUPP_INFO_BREAKS,TITLE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK")
TAIL = ",,,,,,,,,,,"                                   # the 11 empty attribute fields BIS ships


def _line(freq, cc, period, value, structure="BIS:WS_CBPOL(1.0)"):
    return f"dataflow,{structure},I,{freq},{cc},{period},{value}{TAIL}"


def _body(lines):
    return ("﻿" + HDR + "\n" + "\n".join(lines) + "\n").encode("utf-8")


def _store(tmp_path, rows):
    """rows: [(series_key, date, value)] -> WS_CBPOL.parquet (the first-pass ingest's schema)."""
    pq.write_table(pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date": pa.array([r[1] for r in rows], pa.date32()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
        "freq": pa.array([r[0].split(".")[0] for r in rows], pa.string())}),
        str(tmp_path / "WS_CBPOL.parquet"))


def _rows(tmp_path):
    t = pq.read_table(str(tmp_path / "WS_CBPOL.parquet"))
    return sorted(zip(t.column("series_key").to_pylist(), t.column("obs_date").to_pylist(),
                      t.column("value").to_pylist()))


STORE = [("M.CA", dt.date(2026, 4, 1), 2.75), ("M.US", dt.date(2026, 4, 1), 4.375),
         ("D.CA", dt.date(2026, 5, 26), 2.75), ("D.US", dt.date(2026, 5, 26), 4.375)]
M_NEW = _body([_line("M", "CA", "2026-04", 2.75), _line("M", "CA", "2026-10", 2.5),
               _line("M", "US", "2026-04", 4.375), _line("M", "US", "2026-10", 4.125)])
D_SAME = _body([_line("D", "CA", "2026-05-26", 2.75), _line("D", "US", "2026-05-26", 4.375)])
D_NEW = _body([_line("D", "CA", "2026-05-26", 2.75), _line("D", "CA", "2026-11-13", 2.25),
               _line("D", "US", "2026-05-26", 4.375)])


def _catalogue(tmp_path, ids):
    p = tmp_path / "root" / "data" / "catalog.db"          # <root>/data/catalog.db, as catalog_path.under
    p.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(p))
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?, 'bis')", [(i,) for i in ids])
    con.commit()
    con.close()
    return str(p)


CAT_IDS = ["bis:WS_CBPOL:CA", "bis:WS_CBPOL:US"]


def _run(tmp_path, monkeypatch, bodies, cat_ids=CAT_IDS, zips=None):
    """bodies: {FREQ: [body, ...]} answered in order (a list lets the confirming fetch differ)."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
    monkeypatch.setattr(B.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(B, "SDMX_PACE_S", 0)
    if cat_ids is None:
        monkeypatch.setattr(B.config, "ROOT", str(tmp_path / "absent"))
    else:
        monkeypatch.setattr(B.config, "ROOT", os.path.dirname(os.path.dirname(_catalogue(tmp_path, cat_ids))))
    monkeypatch.setattr(B.ig, "BULK", zips or {})
    calls = []

    def fake_get(sess, flow, freq):
        calls.append((flow, freq))
        q = bodies[freq]
        b = q.pop(0) if len(q) > 1 else q[0]
        if isinstance(b, Exception):
            raise b
        return (b if isinstance(b, tuple) else (200, b))

    monkeypatch.setattr(B, "_sdmx_get", fake_get)
    res = B.update(types.SimpleNamespace(config={}, key="bis/_all", source_id="bis"), None)
    return res, calls


def _vint(tmp_path):
    p = tmp_path / B.SDMX_SIDECAR
    return json.loads(p.read_text()) if p.exists() else {}


# ---- the fix ----------------------------------------------------------------------------------------

def test_a_changed_served_slice_merges_and_reports_catalogue_grain_keys(tmp_path, monkeypatch):
    _store(tmp_path, STORE)
    res, calls = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_NEW]})
    assert res.status == "ok", res
    # FOLDED: M.<cc> -> WS_CBPOL:<cc> (the exact tier's '<source>:<key>' == bis:WS_CBPOL:<cc>);
    # D.<cc> serves no catalogue id and is dropped.
    assert set(res.changed_keys) == {"WS_CBPOL:CA", "WS_CBPOL:US"}, res.changed_keys
    assert {str(v) for v in res.changed_keys.values()} == {"2026-10-01"}, res.changed_keys
    rows = _rows(tmp_path)
    assert ("M.CA", dt.date(2026, 10, 1), 2.5) in rows and ("D.CA", dt.date(2026, 11, 13), 2.25) in rows
    assert res.series_cursors["WS_CBPOL:CA"] == "2026-10-01", res.series_cursors
    assert not any(k.startswith("D.") or k.startswith("WS_CBPOL:D") for k in res.series_cursors)
    v = _vint(tmp_path)
    assert v["WS_CBPOL:M"]["sha256"] == hashlib.sha256(M_NEW).hexdigest()
    assert v["WS_CBPOL:D"]["sha256"] == hashlib.sha256(D_NEW).hexdigest()
    assert calls[0] == ("WS_CBPOL", "M"), "the served FREQ is fetched first"
    assert calls.count(("WS_CBPOL", "M")) == 2, "a changed body is confirmed by a second fetch"


def test_the_orchestrator_maps_the_folded_keys_to_the_served_ids(tmp_path, monkeypatch):
    from updater import orchestrate
    monkeypatch.setenv("ECONDL_CATALOG", _catalogue(tmp_path, CAT_IDS + ["bis:WS_CBPOL:JP"]))
    ids, unmapped = orchestrate._map_changed("bis", ["WS_CBPOL:CA", "WS_CBPOL:US"], merge_measured=True)
    assert sorted(ids) == ["bis:WS_CBPOL:CA", "bis:WS_CBPOL:US"] and unmapped == []


def test_the_fold_is_the_inverse_of_the_real_resolver(tmp_path):
    """_served_native must invert clients/python/econdl/_resolve.py::_resolve_bis exactly."""
    ds = pytest.importorskip("pyarrow.dataset")
    from econdl import _resolve
    (tmp_path / "bis").mkdir()
    _store(tmp_path / "bis", STORE)
    for cid in CAT_IDS:
        r = _resolve._resolve_bis(cid, str(tmp_path))
        keys = set(ds.dataset(r.parquet_path).to_table(filter=r.predicate).column("series_key").to_pylist())
        assert len(keys) == 1, keys
        (k,) = keys
        assert "bis:" + B._served_native("WS_CBPOL", k) == cid
    assert B._served_native("WS_CBPOL", "D.CA") is None
    assert B._served_native("WS_LBS_D_PUB", "M.CA") is None


def test_an_unchanged_body_costs_one_request_and_merges_nothing(tmp_path, monkeypatch):
    _store(tmp_path, STORE)
    (tmp_path / B.SDMX_SIDECAR).write_text(json.dumps({
        "WS_CBPOL:M": {"sha256": hashlib.sha256(M_NEW).hexdigest(), "pulled": "2026-11-01"},
        "WS_CBPOL:D": {"sha256": hashlib.sha256(D_SAME).hexdigest(), "pulled": "2026-11-01"}}))
    before = (tmp_path / "WS_CBPOL.parquet").stat().st_mtime_ns
    res, calls = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]})
    assert res.changed_keys == {} and res.status == "no_change", res
    assert calls == [("WS_CBPOL", "M"), ("WS_CBPOL", "D")]
    assert (tmp_path / "WS_CBPOL.parquet").stat().st_mtime_ns == before


def test_the_sdmx_phase_runs_when_the_zip_validators_are_unchanged(tmp_path, monkeypatch):
    """R2/C8: finalize stores new_vintage='date-tail', so only cadence gates bis. Whatever the zips
    say, update() must still refresh the served flow."""
    _store(tmp_path, STORE)
    for n in ("CBS", "LBS"):
        pq.write_table(pa.table({"series_key": ["Q.x"], "obs_date": pa.array([dt.date(2026, 1, 1)], pa.date32()),
                                 "value": [1.0], "freq": ["Q"]}), str(tmp_path / f"{n}.parquet"))
    today = dt.date.today().isoformat()
    (tmp_path / B.SIDECAR).write_text(json.dumps({"CBS": "len=1", "CBS__pulled": today,
                                                  "LBS": "len=1", "LBS__pulled": today}))
    monkeypatch.setattr(B, "_validator", lambda url, sess: "len=1")

    def no_download(*a, **k):
        raise AssertionError("an unchanged zip must not be downloaded")
    monkeypatch.setattr(B, "_download", no_download)
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]},
                  zips={"CBS": "https://x/CBS.zip", "LBS": "https://x/LBS.zip"})
    assert set(res.changed_keys) == {"WS_CBPOL:CA", "WS_CBPOL:US"}, res


def test_a_kill_during_the_zips_commits_nothing_of_the_served_flow(tmp_path, monkeypatch):
    """C1: the bodies are fetched first but merge LAST. A hard kill in the zip phase must leave the
    served parquet and its sidecar untouched, so the next pass re-merges and reports the change."""
    _store(tmp_path, STORE)
    monkeypatch.setattr(B, "_validator", lambda url, sess: "len=2")

    class Kill(BaseException):
        pass

    def killed(*a, **k):
        raise Kill()
    monkeypatch.setattr(B, "_download", killed)
    before = _rows(tmp_path)
    with pytest.raises(Kill):
        _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_NEW]}, zips={"CBS": "https://x/CBS.zip"})
    assert _rows(tmp_path) == before
    assert _vint(tmp_path) == {}


# ---- the guards ---------------------------------------------------------------------------------------

def test_a_body_cut_mid_line_never_merges(tmp_path, monkeypatch):
    _store(tmp_path, STORE)
    cut = M_NEW[:-9]                                   # the observed failure: a cut inside the last row
    res, _ = _run(tmp_path, monkeypatch, {"M": [cut], "D": [D_SAME]})
    assert res.status == "partial" and "WS_CBPOL:M" in res.error, res
    assert res.changed_keys == {}
    assert ("M.CA", dt.date(2026, 10, 1), 2.5) not in _rows(tmp_path)
    assert "WS_CBPOL:M" not in _vint(tmp_path)


STORE3 = STORE + [("M.JP", dt.date(2026, 4, 1), 0.5)]


def test_a_body_cut_at_a_line_boundary_that_drops_stored_series_never_merges(tmp_path, monkeypatch):
    """C2: a deterministic cut hashes the same twice and ends cleanly; it drops every series after
    the cut. More than MISSING_KEYS_TOLERATED absent stored series blocks the merge."""
    _store(tmp_path, STORE3)
    short = _body([_line("M", "CA", "2026-04", 2.75), _line("M", "CA", "2026-10", 2.5)])
    res, _ = _run(tmp_path, monkeypatch, {"M": [short], "D": [D_SAME]})
    assert res.status == "partial" and "absent from the body" in res.error, res
    assert res.changed_keys == {}
    assert "WS_CBPOL:M" not in _vint(tmp_path)


def test_a_body_whose_last_series_ends_before_the_store_never_merges(tmp_path, monkeypatch):
    """A cut at a line boundary INSIDE the last series sent: it ends before its own stored newest."""
    _store(tmp_path, STORE)
    cut = _body([_line("M", "CA", "2026-04", 2.75), _line("M", "CA", "2026-10", 2.5),
                 _line("M", "US", "2026-03", 4.375)])
    res, _ = _run(tmp_path, monkeypatch, {"M": [cut], "D": [D_SAME]})
    assert res.status == "partial" and "ends 2026-03-01, before the store's 2026-04-01" in res.error, res
    assert res.changed_keys == {}


def test_an_upstream_deletion_does_not_block_the_refresh_for_ever(tmp_path, monkeypatch):
    """Review F1: merge never deletes, so the store keeps a value BIS has deleted or blanked. A
    shorter body than the store must still merge, or the block could never clear."""
    _store(tmp_path, STORE + [("M.CA", dt.date(2026, 3, 1), 2.75)])
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]})
    assert res.status == "ok", res
    assert set(res.changed_keys) == {"WS_CBPOL:CA", "WS_CBPOL:US"}


def test_one_retired_series_is_named_and_the_rest_still_merge(tmp_path, monkeypatch, capsys):
    _store(tmp_path, STORE3)
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]})
    assert res.status == "ok", res
    assert set(res.changed_keys) == {"WS_CBPOL:CA", "WS_CBPOL:US"}
    assert "['M.JP'] absent from the body" in capsys.readouterr().out


def test_a_body_that_differs_between_two_fetches_never_merges(tmp_path, monkeypatch):
    _store(tmp_path, STORE)
    other = M_NEW.replace(b"2.5", b"2.4")
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW, other], "D": [D_SAME]})
    assert res.status == "partial" and "differed between two fetches" in res.error, res
    assert "WS_CBPOL:M" not in _vint(tmp_path)


def test_a_failed_daily_slice_never_blocks_the_served_monthly_one(tmp_path, monkeypatch):
    """C3: D serves no catalogue id; its failure must not hold back M."""
    _store(tmp_path, STORE)
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [ConnectionError("reset")]})
    assert res.status == "partial" and "WS_CBPOL:D" in res.error, res
    assert set(res.changed_keys) == {"WS_CBPOL:CA", "WS_CBPOL:US"}
    v = _vint(tmp_path)
    assert "WS_CBPOL:M" in v and "WS_CBPOL:D" not in v


def test_a_new_dsd_version_is_structural_and_the_served_change_survives_it(tmp_path, monkeypatch):
    """A STRUCTURE_ID other than the pinned DSD may be a re-grain (R22): refuse it, structurally.
    finalize then raises; the M ids this pass merged must still reach the CSV phase (Route B)."""
    _store(tmp_path, STORE)
    d_v2 = _body([_line("D", "CA", "2026-11-13", 2.25, structure="BIS:WS_CBPOL(2.0)"),
                  _line("D", "US", "2026-11-13", 4.0, structure="BIS:WS_CBPOL(2.0)")])
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [d_v2]})
    assert res.status == "partial" and "WS_CBPOL:D" in res.error, res
    assert set(res.changed_keys) == {"WS_CBPOL:CA", "WS_CBPOL:US"}


def test_a_structural_slice_with_nothing_served_changed_re_raises(tmp_path, monkeypatch):
    _store(tmp_path, STORE)
    m_v2 = M_NEW.replace(b"WS_CBPOL(1.0)", b"WS_CBPOL(2.0)")
    with pytest.raises(B.DefinitiveError) as ei:
        _run(tmp_path, monkeypatch, {"M": [m_v2], "D": [D_SAME]})
    assert "WS_CBPOL:M" in str(ei.value)


def test_a_newly_catalogued_flow_without_a_refresh_path_is_named(tmp_path, monkeypatch):
    """{} must never hide a catalogued flow nothing refreshes."""
    _store(tmp_path, STORE)
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]},
                  cat_ids=CAT_IDS + ["bis:WS_EER:XX"])
    assert res.status == "partial" and "WS_EER: catalogued but bis.py has no refresh path" in res.error, res


def test_an_unreadable_catalogue_announces_its_fallback(tmp_path, monkeypatch, capsys):
    _store(tmp_path, STORE)
    res, _ = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]}, cat_ids=None)
    assert "catalogue unreadable" in capsys.readouterr().out
    assert set(res.changed_keys) == {"WS_CBPOL:CA", "WS_CBPOL:US"}


def test_an_absent_store_file_is_refused_loudly(tmp_path, monkeypatch):
    res, calls = _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]})
    assert res.status == "partial" and "store file absent" in res.error, res
    assert calls == []


# ---- one parser ---------------------------------------------------------------------------------------

def test_the_ingest_and_the_fetcher_share_one_parser(tmp_path, monkeypatch):
    import gzip
    calls = []
    real = F.iter_rows

    def spy(fh, counts=None):
        calls.append(1)
        return real(fh, counts)
    monkeypatch.setattr(F, "iter_rows", spy)
    src = tmp_path / "b.csv.gz"
    with gzip.open(src, "wb") as g:
        g.write(M_NEW)
    st = F.parse_tmp_to_parquet(str(src), str(tmp_path / "o.parquet"))
    assert calls and st["n_obs"] == 4 and st["n_series"] == 2
    calls.clear()
    _store(tmp_path, STORE)
    _run(tmp_path, monkeypatch, {"M": [M_NEW], "D": [D_SAME]})
    assert calls, "the fetcher must parse with jobs/ingest_bis_full.iter_rows"


def test_iter_rows_keys_dates_and_values():
    import io
    rows = list(F.iter_rows(io.StringIO(M_NEW.decode("utf-8-sig"))))
    assert rows[1] == ("M.CA", dt.date(2026, 10, 1), 2.5, "M")
    with pytest.raises(F.NoObservations):
        list(F.iter_rows(io.StringIO("STRUCTURE,FOO\n")))


def test_served_flows_equal_the_catalogue():
    """The pin against the real catalogue (the defillama precedent). It skips where catalog.db is
    absent, which includes CI; update() repeats the check at run time against the live catalogue."""
    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    if not os.path.exists(cat):
        pytest.skip("catalog.db not present")
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
    try:
        ids = [r[0] for r in con.execute(
            "SELECT series_id FROM series WHERE series_id >= 'bis:' AND series_id < 'bis;'")]
    finally:
        con.close()
    assert {i.split(":")[1] for i in ids} == set(B.SERVED_SDMX_FLOWS)
