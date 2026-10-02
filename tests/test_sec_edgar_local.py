"""sec_edgar's LOCAL data_through and the gate that says whether the local rows may be believed.

Plan: "sec_edgar IS A T0 PREREQUISITE". Design review AR-194 (ledger R1356). Four pieces, each tested where it
can fail:
  core/sec_edgar_local.data_through       MAX(end_date) by primary-key range; RAISES on a forward row (no clamp)
  tools/selfhost/sec_edgar_local_check    READ-ONLY: store spans against catalogue rows, five counts, a receipt
  tools/selfhost/t0_ready.sec_edgar_local the receipt, RECOMPUTED; closed while proofs are still unbuilt
  tools/selfhost/origin_copies            after T0 the copy must carry the source's own source_state row
"""
import datetime as dt
import hashlib
import json
import os
import sqlite3
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core import catalog_path, cutover, sec_edgar_local, sync_state_d1

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import origin_copies as oc  # noqa: E402
import sec_edgar_local_check as C  # noqa: E402
import t0_ready as T  # noqa: E402
from tools import refresh_sec_edgar as R  # noqa: E402

TODAY = "2026-10-02"


# ---- A1: data_through ---------------------------------------------------------------------------------------------
def _series(rows):
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?, ?, ?)", rows)
    return con


def test_it_is_the_newest_end_date_of_the_source_s_own_rows():
    con = _series([("sec_edgar:AAPL", "sec_edgar", "2026-08-01"), ("sec_edgar:XOM", "sec_edgar", "2026-09-04"),
                   ("ecb:x", "ecb", "2026-10-01"),
                   # the 13F product's own key sorts outside the range, whatever its source_id column says
                   ("sec_edgar_13f:u", "sec_edgar", "2026-09-30"), ("sec_edgarx:y", "sec_edgar", "2026-09-29")])
    assert sec_edgar_local.data_through(con, today=TODAY) == "2026-09-04"


def test_no_row_is_none_not_an_error():
    assert sec_edgar_local.data_through(_series([("ecb:x", "ecb", "2026-10-01")]), today=TODAY) is None


def test_a_forward_row_fails_the_copy_and_is_never_clamped_away():
    """R737 / R1193: `MAX(end_date <= today)` returned the older date here and crept as the typo came round."""
    con = _series([("sec_edgar:AAPL", "sec_edgar", "2026-09-04"), ("sec_edgar:FUL", "sec_edgar", "2106-12-03")])
    with pytest.raises(sec_edgar_local.NotPublishable) as e:
        sec_edgar_local.data_through(con, today=TODAY)
    assert "sec_edgar:FUL" in str(e.value) and "2106-12-03" in str(e.value) and "1 row(s)" in str(e.value)
    assert "refresh_sec_edgar.py --ciks" in str(e.value), "the message says how to repair it"


@pytest.mark.parametrize("end, ok", [("2026-10-02", True), ("2026-10-03", False), ("2026-10-01", True)])
def test_the_boundary_is_today_utc(end, ok):
    con = _series([("sec_edgar:A", "sec_edgar", end)])
    if ok:
        assert sec_edgar_local.data_through(con, today=TODAY) == end
    else:
        with pytest.raises(sec_edgar_local.NotPublishable):
            sec_edgar_local.data_through(con, today=TODAY)


def test_the_default_clock_is_utc(monkeypatch):
    seen = []

    class _Clock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            seen.append(tz)
            return dt.datetime(2026, 10, 2, 23, 59, tzinfo=dt.timezone.utc)

    monkeypatch.setattr(sec_edgar_local.dt, "datetime", _Clock)
    con = _series([("sec_edgar:A", "sec_edgar", "2026-10-02")])
    assert sec_edgar_local.data_through(con) == "2026-10-02"
    assert seen == [dt.timezone.utc], "today is read in UTC, never the machine's zone"


@pytest.mark.parametrize("bad", [None, "", "2026-9-4", "20260904", "soon"])
def test_a_missing_or_malformed_end_date_fails_too(bad):
    con = _series([("sec_edgar:A", "sec_edgar", "2026-09-04"), ("sec_edgar:B", "sec_edgar", bad)])
    with pytest.raises(sec_edgar_local.NotPublishable, match="sec_edgar:B"):
        sec_edgar_local.data_through(con, today=TODAY)


def test_the_read_is_a_primary_key_range_never_a_scan():
    con = _series([("sec_edgar:A", "sec_edgar", "2026-09-04")])
    seen = []
    con.set_trace_callback(seen.append)
    sec_edgar_local.data_through(con, today=TODAY)
    con.set_trace_callback(None)
    assert seen, "the statements were traced"
    for sql in seen:
        plan = " ".join(r[3] for r in con.execute("EXPLAIN QUERY PLAN " + sql.replace("?", "'2026-10-02'")))
        assert "SEARCH" in plan and "sqlite_autoindex_series_1" in plan, f"{plan} <- {sql}"
        assert "SCAN" not in plan, plan


def test_importing_it_loads_nothing_heavy():
    """It is imported inside every origin-copy build."""
    code = "import sys; import core.sec_edgar_local; print(sorted(m for m in ('pyarrow','pandas','numpy','duckdb') if m in sys.modules))"
    out = subprocess.run([sys.executable, "-B", "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0 and out.stdout.strip() == "[]", out.stdout + out.stderr


def test_the_real_module_is_the_registered_writer_and_the_sync_still_skips_the_source():
    assert sync_state_d1.LOCAL_FRESHNESS_WRITERS == {"sec_edgar": "core.sec_edgar_local"}
    assert sync_state_d1.DATA_THROUGH_FROM_D1 == frozenset({"sec_edgar"})
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?, ?, ?)", [("sec_edgar:A", "sec_edgar", "2026-09-04"),
                                                            ("ecb:x", "ecb", "2026-06-30")])
    # the D1 sync's own statistic never stamps this source (before T0, D1's stamp is the truth - R737) ...
    assert dict(sync_state_d1.data_through_rows(con, set())) == {"ecb": "2026-06-30"}
    # ... and the origin copy takes it from the registered module
    assert sync_state_d1.local_writer_rows(con, set()) == [("sec_edgar", "2026-09-04")]
    assert sync_state_d1.local_writer_rows(con, {"sec_edgar"}) == [], "a gated source is not stamped"


# ---- the copy, built with the REAL registered writer --------------------------------------------------------------
def _catalogue(path, sec_rows):
    c = sqlite3.connect(path)
    c.executescript("""
      CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT);
      CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, license_id TEXT);
      CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT, license_id TEXT,
                           end_date TEXT);
      CREATE INDEX ix_series_source_id ON series(source_id);
      CREATE TABLE source_counts(source_id TEXT PRIMARY KEY, n INTEGER NOT NULL);
      CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
    """)
    c.executemany("INSERT INTO license VALUES (?, ?)", [("pd", "public")])
    c.executemany("INSERT INTO source VALUES (?, ?, 'pd')", [("ecb", "ECB"), ("sec_edgar", "SEC"), ("noaa", "NOAA")])
    rows = [("ecb:x", "ecb", "2026-06-30"), ("noaa:a", "noaa", "2026-06-30")] + \
           [(sid, "sec_edgar", end) for sid, end in sec_rows]
    c.executemany("INSERT INTO series VALUES (?, ?, 't', 'g', 'pd', ?)", rows)
    c.executemany("INSERT INTO series_fts VALUES (?, 't', 'g')", [(r[0],) for r in rows])
    c.commit()
    c.close()


def _state_db(path, rows):
    s = sqlite3.connect(path)
    s.executescript("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT, "
                    "last_success_utc TEXT);"
                    "CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, status TEXT, last_success_utc TEXT, "
                    "upstream_vintage TEXT, last_obs_date TEXT, obs_count INTEGER, PRIMARY KEY (source_id, unit_id));")
    for src, strategy, success in rows:
        s.execute("INSERT INTO source_state VALUES (?,?,?,?,?)", (src, strategy, "daily", "ok", success))
        s.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?,?)", (src, "_all", "ok", success, None, None, 1))
    s.commit()
    s.close()


OK_STATE = [("ecb", "x", "2026-09-01T00:00:00+00:00"), ("noaa", "x", "2026-09-01T00:00:00+00:00"),
            ("sec_edgar", "edgar_delta", "2026-09-01T00:00:00+00:00")]


def test_a_build_takes_the_value_from_the_real_writer(tmp_path):
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _catalogue(cat, [("sec_edgar:AAPL", "2026-09-04"), ("sec_edgar:XOM", "2026-08-01")])
    _state_db(st, OK_STATE)
    report = oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))
    con = sqlite3.connect(tmp_path / "out" / "primary.sqlite")
    try:
        got = dict(con.execute("SELECT source_id, data_through FROM source_data_through"))
    finally:
        con.close()
    assert got["sec_edgar"] == "2026-09-04"
    assert report["d1_only_state"] == "not checked before T0 (the row is D1's)"


def test_a_forward_row_leaves_no_copy(tmp_path):
    cat, st = tmp_path / "catalog.db", tmp_path / "state.db"
    _catalogue(cat, [("sec_edgar:AAPL", "2026-09-04"), ("sec_edgar:FUL", "2106-12-03")])
    _state_db(st, OK_STATE)
    with pytest.raises(sec_edgar_local.NotPublishable, match="sec_edgar:FUL"):
        oc.build(str(cat), str(tmp_path / "out"), state_db=str(st))
    assert not os.path.exists(tmp_path / "out" / "primary.sqlite"), "a refused build leaves nothing to serve"
    assert not os.path.exists(tmp_path / "out" / "climate.sqlite")


# ---- A5: after T0 the copy must carry the source's own freshness row ----------------------------------------------
def _primary(path, state_rows, with_strategy=True, served=True):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    if served:
        c.execute("INSERT INTO series VALUES ('sec_edgar:A', 'sec_edgar')")
    c.execute("CREATE TABLE source_state (source_id TEXT PRIMARY KEY, %scadence TEXT, last_success_utc TEXT)"
              % ("strategy TEXT, " if with_strategy else ""))
    for row in state_rows:
        c.execute("INSERT INTO source_state VALUES (%s)" % ",".join("?" * len(row)), row)
    c.commit()
    c.close()
    return str(path)


@pytest.fixture
def after_t0(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    assert cutover.is_cut_over()


def test_before_t0_the_freshness_row_is_not_demanded(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no-flag"))
    p = _primary(tmp_path / "p.sqlite", [])
    assert oc._d1_only_state(p, set()) == "not checked before T0 (the row is D1's)"


def test_after_t0_a_copy_without_the_row_is_refused(tmp_path, after_t0):
    """AR-194 B4: after the 13F rows moved, state.db has no sec_edgar row until the local refresher's first
    run; a first production build before that run served status / last_updated null."""
    p = _primary(tmp_path / "p.sqlite", [("ecb", "x", "daily", "2026-09-01")])
    with pytest.raises(RuntimeError, match="no source_state row for sec_edgar"):
        oc._d1_only_state(p, set())


@pytest.mark.parametrize("row", [("sec_edgar", "giant_changed_units", "quarterly", "2026-09-01"),   # the 13F row
                                 ("sec_edgar", "edgar_delta", "daily", None),                        # never whole
                                 ("sec_edgar", "edgar_delta", "daily", "")])
def test_after_t0_the_row_must_be_the_refreshers_own_with_a_success(tmp_path, after_t0, row):
    p = _primary(tmp_path / "p.sqlite", [row])
    with pytest.raises(RuntimeError, match="expected strategy 'edgar_delta'"):
        oc._d1_only_state(p, set())


def test_after_t0_the_refreshers_own_row_passes_and_a_gated_or_unserved_source_is_skipped(tmp_path, after_t0):
    good = ("sec_edgar", "edgar_delta", "daily", "2026-10-02T08:00:00+00:00")
    assert oc._d1_only_state(_primary(tmp_path / "a.sqlite", [good]), set()) == "checked"
    assert oc._d1_only_state(_primary(tmp_path / "b.sqlite", []), {"sec_edgar"}) == "checked"
    assert oc._d1_only_state(_primary(tmp_path / "c.sqlite", [], served=False), set()) == "checked"
    with pytest.raises(RuntimeError, match="no strategy column"):
        oc._d1_only_state(_primary(tmp_path / "d.sqlite", [], with_strategy=False), set())


# ---- A3: the read-only check ---------------------------------------------------------------------------------------
def _file(store, name, facts):
    """facts: [(obs_date, filed_or_None)]"""
    obs = [dt.date.fromisoformat(o) if o else None for o, _f in facts]
    vint = [dt.date.fromisoformat(f) if f else None for _o, f in facts]
    pq.write_table(pa.table({"metric": ["m"] * len(facts), "obs_date": pa.array(obs, pa.date32()),
                             "value": [1.0] * len(facts), "vintage_date": pa.array(vint, pa.date32())}),
                   os.path.join(store, name + ".parquet"))


def _check_world(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no-flag"))          # before T0: any catalogue path opens
    store = tmp_path / "store"
    store.mkdir()
    _file(store, "AAPL", [("2019-12-31", "2020-02-01"), ("2020-12-31", "2021-02-01")])
    # a filer typo far in the future, filed in 2026: the RULE excludes it (the span ends 2026-06-30, not 2106)
    _file(store, "FUL", [("2025-12-31", "2026-02-01"), ("2026-06-30", "2026-08-01"), ("2106-12-03", "2026-08-01")])
    _file(store, "BRK_B", [("2021-12-31", "2022-02-01")])
    cat = tmp_path / "catalog.db"
    c = sqlite3.connect(cat)
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, start_date TEXT, end_date TEXT)")
    c.executemany("INSERT INTO series VALUES (?, 'sec_edgar', ?, ?)", [
        ("sec_edgar:AAPL", "2019-12-31", "2020-12-31"), ("sec_edgar:FUL", "2025-12-31", "2026-06-30"),
        ("sec_edgar:BRK/B", "2021-12-31", "2021-12-31")])                     # "/" in the id, "_" in the file name
    c.execute("INSERT INTO series VALUES ('ecb:x', 'ecb', '2000-01-01', '2026-06-30')")
    c.commit()
    c.close()
    return str(cat), str(store), str(tmp_path / "receipts" / "r.json")


def _digest(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def test_a_store_and_catalogue_that_agree_are_clean_and_nothing_is_written_but_the_receipt(tmp_path, monkeypatch):
    cat, store, receipt = _check_world(tmp_path, monkeypatch)
    before = (_digest(cat), {n: _digest(os.path.join(store, n)) for n in os.listdir(store)},
              C.listing_sha256(C.store_listing(store)))
    out = C.run(cat, store, receipt, today=TODAY)
    assert out["counts"] == {k: 0 for k in C.COUNTS} and out["clean"] is True
    assert (out["catalogue_rows"], out["store_files"]) == (3, 3)
    assert json.load(open(receipt))["catalogue_sha256"] == out["catalogue_sha256"]
    after = (_digest(cat), {n: _digest(os.path.join(store, n)) for n in os.listdir(store)},
             C.listing_sha256(C.store_listing(store)))
    assert after == before, "the catalogue and the store are read, never written"
    assert sorted(os.listdir(tmp_path)) == ["catalog.db", "receipts", "store"], "one receipt, nothing else"


def test_the_span_is_the_refreshers_rule_not_the_newest_date_in_the_file(tmp_path, monkeypatch):
    """The mutant `max(obs_date)` reports FUL as differing AND forward (its raw maximum is 2106-12-03)."""
    cat, store, receipt = _check_world(tmp_path, monkeypatch)
    span = C.file_span(os.path.join(store, "FUL.parquet"), R.coverage_span)
    assert (str(span[0]), str(span[1])) == ("2025-12-31", "2026-06-30")
    assert C._refresher().coverage_span.__code__.co_code == R.coverage_span.__code__.co_code, "THE rule, not a copy"


def test_each_count_fires_on_a_planted_case(tmp_path, monkeypatch):
    cat, store, receipt = _check_world(tmp_path, monkeypatch)
    c = sqlite3.connect(cat)
    c.execute("UPDATE series SET end_date='2020-06-30' WHERE series_id='sec_edgar:AAPL'")        # differing
    c.execute("INSERT INTO series VALUES ('sec_edgar:GONE', 'sec_edgar', '2001-01-01', '2002-01-01')")   # catalogue_only
    c.execute("INSERT INTO series VALUES ('sec_edgar:TYPO', 'sec_edgar', '2999-01-01', '2999-01-01')")
    c.execute("INSERT INTO series VALUES ('sec_edgar:BAD', 'sec_edgar', '2001-01-01', '2002-01-01')")
    c.execute("INSERT INTO series VALUES ('sec_edgar:HOLE', 'sec_edgar', '2001-01-01', '2002-01-01')")
    c.commit()
    c.close()
    _file(store, "NEWCO", [("2026-06-30", "2026-08-01")])                                         # store_only
    _file(store, "TYPO", [("2999-01-01", None)])               # nothing ended, no filed date: the fallback -> forward
    open(os.path.join(store, "BAD.parquet"), "wb").write(b"not a parquet file")                   # unreadable
    _file(store, "HOLE", [("2001-01-01", "2001-02-01"), (None, "2002-02-01")])                    # NULL obs_date
    out = C.run(cat, store, receipt, today=TODAY)
    assert out["counts"] == {"differing": 1, "store_only": 1, "catalogue_only": 1, "forward": 1, "unreadable": 2}
    ex = out["examples"]
    assert ex["differing"][0][0] == "sec_edgar:AAPL" and ex["store_only"] == ["NEWCO.parquet"]
    assert ex["catalogue_only"] == ["sec_edgar:GONE"] and ex["forward"][0][0] == "sec_edgar:TYPO"
    assert sorted(u[0] for u in ex["unreadable"]) == ["sec_edgar:BAD", "sec_edgar:HOLE"]
    assert out["clean"] is False
    assert C.main(["--catalogue", cat, "--store", store, "--receipt", receipt]) == 1


def test_an_empty_catalogue_is_never_clean(tmp_path, monkeypatch):
    cat, store, receipt = _check_world(tmp_path, monkeypatch)
    c = sqlite3.connect(cat)
    c.execute("DELETE FROM series WHERE source_id='sec_edgar'")
    c.commit()
    c.close()
    for n in os.listdir(store):
        os.remove(os.path.join(store, n))
    out = C.run(cat, store, receipt, today=TODAY)
    assert out["counts"] == {k: 0 for k in C.COUNTS} and out["clean"] is False, "zero rows compared proves nothing"


def test_a_store_that_moves_during_the_read_is_not_clean(tmp_path, monkeypatch):
    cat, store, receipt = _check_world(tmp_path, monkeypatch)
    real = C.file_span

    def moving(path, rule):
        if path.endswith("FUL.parquet"):
            _file(store, "LATE", [("2026-06-30", "2026-08-01")])       # a writer lands a file mid-run
        return real(path, rule)

    monkeypatch.setattr(C, "file_span", moving)
    out = C.run(cat, store, receipt, today=TODAY, workers=1)
    assert out["store_stable_during_read"] is False and out["clean"] is False


def test_the_receipt_fingerprints_move_with_what_they_certify(tmp_path, monkeypatch):
    cat, store, receipt = _check_world(tmp_path, monkeypatch)
    a = C.run(cat, store, receipt, today=TODAY)
    c = sqlite3.connect(cat)
    c.execute("UPDATE series SET end_date='2021-12-30' WHERE series_id='sec_edgar:BRK/B'")
    c.commit()
    c.close()
    b = C.run(cat, store, receipt, today=TODAY)
    assert a["catalogue_sha256"] != b["catalogue_sha256"] and a["store_fingerprint"] == b["store_fingerprint"]
    _file(store, "AAPL", [("2019-12-31", "2020-02-01"), ("2020-12-31", "2021-02-01"), ("2021-12-31", "2022-02-01")])
    d = C.run(cat, store, receipt, today=TODAY)
    assert d["store_fingerprint"] != b["store_fingerprint"]


def test_a_receipt_under_the_mirrored_prefix_is_refused(tmp_path, monkeypatch):
    cat, store, _receipt = _check_world(tmp_path, monkeypatch)
    inside = os.path.join(C.ROOT, "data", "_aqueduct", "sec_edgar_local_check.json")
    with pytest.raises(SystemExit) as e:
        C.main(["--catalogue", cat, "--store", store, "--receipt", inside])
    assert e.value.code == 2 and not os.path.exists(inside)


def test_the_tool_has_no_write_mode():
    src = open(os.path.join(ROOT, "tools", "selfhost", "sec_edgar_local_check.py"), encoding="utf-8").read()
    code = src.split('"""', 2)[2]                                   # the code after the module docstring
    for word in ("--apply", "write=True", "mode=rw", "INSERT ", "UPDATE ", "DELETE ", "d1_remote", "r2_util",
                 "urllib", "requests", "put_atomic", "atomic_replace"):
        assert word not in code, f"{word!r} in a read-only tool"


def test_the_store_name_rule_is_the_refreshers():
    assert sec_edgar_local.store_name("BRK/B") == "BRK_B" and sec_edgar_local.store_name("A:B/C") == "A_B_C"
    src = open(os.path.join(ROOT, "tools", "refresh_sec_edgar.py"), encoding="utf-8").read()
    assert src.count("sec_edgar_local.store_name(ident)") == 3
    assert '.replace("/", "_")' not in src, "a second copy of the rule is back in the refresher"


# ---- A4: the gate ---------------------------------------------------------------------------------------------------
def _gate(tmp_path, monkeypatch):
    cat, store, receipt = _check_world(tmp_path, monkeypatch)
    C.run(cat, store, receipt, today=TODAY)
    return cat, store, receipt


def test_the_gate_is_closed_today_whatever_the_receipt_says(tmp_path, monkeypatch):
    """R1195: registering the writer made d1-only-sources pass on a name. This check must not."""
    cat, store, receipt = _gate(tmp_path, monkeypatch)
    assert T.SEC_EDGAR_OWED, "the unbuilt proofs are still listed"
    ok, detail = T.sec_edgar_local(receipt, cat, store)
    assert ok is False and "NOT BUILT YET" in detail and "D1-to-local" in detail
    assert ("sec-edgar-local", T.sec_edgar_local) in T.CHECKS
    names = [n for n, _f in T.CHECKS]
    assert names.index("sec-edgar-local") == names.index("d1-only-sources") + 1


def test_with_nothing_owed_a_clean_current_receipt_passes(tmp_path, monkeypatch):
    cat, store, receipt = _gate(tmp_path, monkeypatch)
    ok, detail = T.sec_edgar_local(receipt, cat, store, owed=())
    assert ok is True and "3 rows" in detail


def test_the_gate_fails_without_a_receipt_or_with_an_unreadable_one(tmp_path, monkeypatch):
    cat, store, receipt = _gate(tmp_path, monkeypatch)
    assert T.sec_edgar_local(str(tmp_path / "none.json"), cat, store, owed=())[0] is False
    open(receipt, "w").write("{ not json")
    ok, detail = T.sec_edgar_local(receipt, cat, store, owed=())
    assert ok is False and "unreadable" in detail


@pytest.mark.parametrize("change", ["count", "clean", "stable", "zero-rows", "no-counts", "other-catalogue", "other-store"])
def test_the_gate_fails_on_a_receipt_that_does_not_certify_this_state(tmp_path, monkeypatch, change):
    cat, store, receipt = _gate(tmp_path, monkeypatch)
    r = json.load(open(receipt))
    if change == "count":
        r["counts"]["differing"] = 1
    elif change == "clean":
        r["clean"] = False
    elif change == "stable":
        r["store_stable_during_read"] = False
    elif change == "zero-rows":
        r["catalogue_rows"] = 0
    elif change == "no-counts":
        del r["counts"]["forward"]
    elif change == "other-catalogue":
        r["catalogue_path"] = r["catalogue_path"] + ".copy"
    elif change == "other-store":
        r["store_path"] = r["store_path"] + "2"
    json.dump(r, open(receipt, "w"))
    assert T.sec_edgar_local(receipt, cat, store, owed=())[0] is False


def test_the_gate_fails_when_the_catalogue_or_the_store_moved_after_the_receipt(tmp_path, monkeypatch):
    cat, store, receipt = _gate(tmp_path, monkeypatch)
    c = sqlite3.connect(cat)
    c.execute("UPDATE series SET end_date='2020-06-30' WHERE series_id='sec_edgar:AAPL'")
    c.commit()
    c.close()
    ok, detail = T.sec_edgar_local(receipt, cat, store, owed=())
    assert ok is False and "catalogue's sec_edgar rows changed" in detail
    C.run(cat, store, receipt, today=TODAY)                          # a new receipt (now 1 differing: not clean)
    c = sqlite3.connect(cat)
    c.execute("UPDATE series SET end_date='2020-12-31' WHERE series_id='sec_edgar:AAPL'")
    c.commit()
    c.close()
    C.run(cat, store, receipt, today=TODAY)                          # clean again
    assert T.sec_edgar_local(receipt, cat, store, owed=())[0] is True
    _file(store, "NEWCO", [("2026-06-30", "2026-08-01")])
    ok, detail = T.sec_edgar_local(receipt, cat, store, owed=())
    assert ok is False and "store changed" in detail


# ---- A6: the refresher refuses to WRITE a forward span --------------------------------------------------------------
def test_coverage_span_can_produce_a_forward_end_which_is_why_the_writer_checks():
    """The fallback branch (no fact carries a filed date at or after its period end, and nothing has ended)."""
    lo, hi = R.coverage_span([dt.date(2999, 1, 1)], [None])
    assert str(hi) == "2999-01-01"
