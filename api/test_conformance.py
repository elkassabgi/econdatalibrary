#!/usr/bin/env python3
"""Conformance test for the /v1 public API contract (api/CONTRACT.md).

Boots the dev shim (``api/devserver.py``) on a free port and asserts each
endpoint's JSON shape / keys match the **v1.1 canonical pins** in CONTRACT.md
("Canonical response shapes (v1.1 — reconciled 2026-06-26)"). The dev shim is
the executable reference for the contract; the Cloudflare Worker is reconciled
to emit byte-for-byte identical shapes (asserted by review + this file's pins).

What it pins (one assertion group per Task in the reconciliation brief):
  1. /v1/sources           NESTED {source,name,homepage,license|null,freshness|null}
  2. /v1/bundle            Frictionless top-level keys + per-resource keys + citation
  3. /v1/series/{id}.metadata.json  has `category`; description/citation fallback;
                            last_updated falls back to unit_state('_all')
  4. /v1/last-updates       cadence map incl `annual`; others -> next_update_expected null
  5. status codes          501 not_migrated vs 502 data_unavailable vs 502 resolver_empty
                            vs 404; the data_unavailable/resolver_empty DISTINCTION
  6. /v1/series/{id}.csv    identity column == econdl._resolve.native_to_tidy key
                            (native key, NOT the catalog id), and a LOCAL bundle ==
                            an HTTP bundle row-for-row (series_id column included)

Stdlib + pytest only; it imports the shim's own backends (econdl resolver +
catalog) so the API and the client are checked against the SAME code.

Run:  python -m pytest api/test_conformance.py -q
"""
from __future__ import annotations

import io
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_THIS)
_DEVSERVER = os.path.join(_THIS, "devserver.py")
_CLIENTS = os.path.join(_REPO, "clients", "python")
sys.path.insert(0, _CLIENTS)

# Example ids used throughout. Both are 1:1 catalog-id -> single native curve:
#   * bls:CUUR0000SA0          native key 'CUUR0000SA0'        (key_col series_id)
#   * oecd:GDP_GROWTH_QOQ:USA  native key 'Q.Y.USA...T0102'    (key_col series_key)
# These exercise the Task #6 identity-column pin (native key != catalog id).
EX_BLS = "bls:CUUR0000SA0"
EX_OECD = "oecd:GDP_GROWTH_QOQ:USA"
# penn_world_table:rgdpe:USA has last_updated=NULL in the catalog but a
# unit_state('_all').last_success_utc -> exercises the metadata last_updated
# fallback pin (Task #3).
EX_PWT = "penn_world_table:rgdpe:USA"
# i18n example ids (metadata.titles loaded from OFFICIAL source labels):
#   * worldbank GDP for the Arab World carries an Arabic title -> localization hit.
#   * an ILOSTAT series carries es/fr ONLY (ILO has no Arabic) -> ?lang=ar must
#     fall back to English with NO title_en, exercising the graceful-fallback pin.
EX_WB_AR = "worldbank:NY.GDP.MKTP.CD:ARB"
EX_ILO_NOAR = "ilostat:UNE_DEAP_SEX_AGE_RT:AGE_YTHADULT_YGE15:AUS"


# --------------------------------------------------------------------------- #
# server fixture
# --------------------------------------------------------------------------- #

def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def base_url():
    """Boot devserver.py on a free port; tear it down at module end."""
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, _DEVSERVER, "--host", "127.0.0.1", "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        cwd=_REPO,
    )
    base = f"http://127.0.0.1:{port}"
    # Wait until /health answers (or the process dies).
    deadline = time.time() + 30
    last_err = None
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            raise RuntimeError(f"devserver exited early (code {proc.returncode}):\n{out}")
        try:
            with urllib.request.urlopen(base + "/health", timeout=2) as r:
                if r.status == 200:
                    break
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            last_err = e
            time.sleep(0.2)
    else:
        proc.terminate()
        raise RuntimeError(f"devserver did not come up on {base}: {last_err}")
    try:
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


# --------------------------------------------------------------------------- #
# tiny HTTP helpers
# --------------------------------------------------------------------------- #

def _enc(sid: str) -> str:
    return urllib.parse.quote(sid, safe="")


def _get(base: str, path: str) -> tuple[int, str, bytes]:
    url = base + path
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.headers.get("Content-Type", ""), resp.read()
    except urllib.error.HTTPError as e:
        ct = e.headers.get("Content-Type", "") if e.headers else ""
        return e.code, ct, e.read()


def _get_json(base: str, path: str) -> tuple[int, dict | list]:
    code, ct, body = _get(base, path)
    assert "application/json" in ct, f"{path}: expected JSON, got {ct!r}"
    return code, json.loads(body.decode("utf-8"))


# --------------------------------------------------------------------------- #
# Task 1 -- /v1/sources NESTED shape
# --------------------------------------------------------------------------- #

def test_sources_nested_shape(base_url):
    code, obj = _get_json(base_url, "/v1/sources")
    assert code == 200
    assert set(obj.keys()) == {"total", "sources"}
    assert obj["total"] == len(obj["sources"]) and obj["total"] > 0

    for s in obj["sources"]:
        # Exactly the canonical nested keys -- flat source_id/cadence/status/etc.
        # must NOT leak (the old shim shape).
        assert set(s.keys()) == {"source", "name", "homepage", "license", "freshness"}, s
        assert "source_id" not in s and "cadence" not in s and "status" not in s
        lic = s["license"]
        if lic is not None:
            assert set(lic.keys()) == {
                "id", "name", "url", "reservable", "commercial_ok",
                "attribution_required", "no_modify",
            }, lic
            for b in ("reservable", "commercial_ok", "attribution_required", "no_modify"):
                assert isinstance(lic[b], bool)
        fr = s["freshness"]
        if fr is not None:
            assert set(fr.keys()) == {"status", "last_updated", "cadence"}, fr

    # at least one source carries a non-null nested license + a non-null freshness
    assert any(s["license"] is not None for s in obj["sources"])
    assert any(s["freshness"] is not None for s in obj["sources"])


# --------------------------------------------------------------------------- #
# Task 2 -- /v1/bundle Frictionless manifest
# --------------------------------------------------------------------------- #

def test_bundle_manifest_shape(base_url):
    path = f"/v1/bundle?ids={_enc(EX_BLS)},{_enc(EX_OECD)}"
    code, dp = _get_json(base_url, path)
    assert code == 200

    expected_top = [
        "name", "profile", "econdl:schema_version", "econdl:client",
        "econdl:snapshot_date", "econdl:series_requested",
        "econdl:resource_url_count", "econdl:fanout_note", "licenses",
        "resources", "econdl:unresolved",
    ]
    assert list(dp.keys()) == expected_top, list(dp.keys())
    assert dp["name"] == "econdl-bundle"
    assert dp["profile"] == "tabular-data-package"
    assert dp["econdl:series_requested"] == sorted([EX_BLS, EX_OECD])
    assert dp["econdl:unresolved"] == []
    assert isinstance(dp["licenses"], list) and len(dp["licenses"]) >= 1
    for licm in dp["licenses"]:
        assert set(licm.keys()) == {"name", "title", "path"}

    assert len(dp["resources"]) == 2  # bls + oecd
    url_count = 0
    for r in dp["resources"]:
        assert set(r.keys()) == {
            "name", "profile", "format", "mediatype", "path",
            "econdl:series_ids", "econdl:provenance",
        }, r
        assert r["profile"] == "tabular-data-resource"
        assert r["format"] == "csv" and r["mediatype"] == "text/csv"
        assert isinstance(r["path"], list) and len(r["path"]) == len(r["econdl:series_ids"])
        # path entries are the stable per-series .csv URLs
        for u in r["path"]:
            assert u.startswith("/v1/series/") and u.endswith(".csv")
        url_count += len(r["path"])
        prov = r["econdl:provenance"]
        assert "citation" in prov and isinstance(prov["citation"], str) and prov["citation"]
        assert set(prov.keys()) == {
            "source_id", "name", "homepage", "attribution", "terms_url",
            "license", "citation",
        }, prov
    assert dp["econdl:resource_url_count"] == url_count


# --------------------------------------------------------------------------- #
# Task 3 -- /v1/series/{id}.metadata.json
# --------------------------------------------------------------------------- #

def test_metadata_has_category_and_csv_url(base_url):
    code, m = _get_json(base_url, f"/v1/series/{_enc(EX_OECD)}.metadata.json")
    assert code == 200
    # canonical core keys always present (category is the Task #3 addition).
    for k in ("series_id", "source", "title", "frequency", "unit", "geography",
              "category", "start_date", "end_date", "license", "attribution",
              "homepage", "terms_url", "last_updated", "csv_url"):
        assert k in m, f"metadata missing {k!r}: {sorted(m)}"
    assert m["series_id"] == EX_OECD
    assert m["source"] == "oecd"
    assert m["category"] == "macro"  # exact value from catalog.db
    assert m["csv_url"] == f"/v1/series/{_enc(EX_OECD)}.csv"


def test_metadata_task5_keys_present(base_url):
    # Task #5 is APPLIED: curated sources carry description_key + producer-first
    # citation_short/long + description_processing (core/build_series_metadata.py).
    # OWID is curated, so the metadata endpoint must emit the Task#5 path here.
    ex = "owid:annual-co2-emissions-per-country:USA"
    code, m = _get_json(base_url, f"/v1/series/{_enc(ex)}.metadata.json")
    assert code == 200
    assert isinstance(m.get("description_key"), list) and m["description_key"], m
    assert m["citation_short"] == "Our World in Data."          # PRODUCER first
    assert m["citation_long"] and "Elkassabgi Data Library" in m["citation_long"]
    assert m.get("description_processing")
    # producer-first citation is universal (every series got one); never fabricated empty.
    code2, m2 = _get_json(base_url, f"/v1/series/{_enc(EX_OECD)}.metadata.json")
    assert m2["citation_short"] == "OECD."


def test_metadata_description_citation_fallback():
    # The defensive fallback: a series carrying a bare `description` + `citation`
    # but NO Task#5 keys must surface `description` (not description_key) and derive
    # citation_short/long. No real series exercises this post-Task#5, so synthesise
    # one in a temp catalog (same pattern as the data_unavailable test).
    import shutil
    import sqlite3
    import tempfile
    src_cat = os.path.join(_REPO, "data", "catalog.db")
    tmpdir = tempfile.mkdtemp(prefix="econdl_conf_md_")
    tmp_cat = os.path.join(tmpdir, "catalog.db")
    shutil.copy(src_cat, tmp_cat)
    conn = sqlite3.connect(tmp_cat)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO series (series_id, source_id, title, frequency, "
            "category, license_id, last_updated, metadata) VALUES (?,?,?,?,?,?,?,?)",
            ("bls:FALLBACK_TEST", "bls", "synthetic fallback", "M", "macro",
             "us-public-domain", None,
             json.dumps({"description": "raw desc here", "citation": "Producer X (2026)"})))
        conn.commit()
    finally:
        conn.close()
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, _DEVSERVER, "--host", "127.0.0.1", "--port", str(port),
         "--catalog", tmp_cat],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=_REPO)
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError("temp devserver died:\n" + (proc.stdout.read() if proc.stdout else ""))
            try:
                with urllib.request.urlopen(base + "/health", timeout=2) as r:
                    if r.status == 200:
                        break
            except (urllib.error.URLError, OSError):
                time.sleep(0.2)
        code, m = _get_json(base, f"/v1/series/{_enc('bls:FALLBACK_TEST')}.metadata.json")
        assert code == 200
        assert "description_key" not in m            # fallback path, not Task#5
        assert m["description"] == "raw desc here"
        assert m["citation_short"] == "Producer X (2026)"
        assert m["citation_long"] == "Producer X (2026). Compiled by Elkassabgi Data Library."
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_metadata_last_updated_fallback_to_unit_state(base_url):
    # penn_world_table:rgdpe:USA has last_updated=NULL in the catalog; the contract
    # requires falling back to the source's unit_state('_all').last_success_utc.
    code, m = _get_json(base_url, f"/v1/series/{_enc(EX_PWT)}.metadata.json")
    assert code == 200
    # The fallback must produce a real timestamp, not null and not fabricated.
    assert m["last_updated"], "expected unit_state('_all') fallback, got null"
    # cross-check against state.db directly: it must EQUAL the _all last_success.
    import sqlite3
    state = os.path.join(_REPO, "data", "_aqueduct", "state.db")
    conn = sqlite3.connect(f"file:{state}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT unit_id, last_success_utc FROM unit_state WHERE source_id=? "
            "ORDER BY unit_id", ("penn_world_table",)).fetchall()
    finally:
        conn.close()
    all_row = next((r for r in rows if r["unit_id"] == "_all"), None)
    expected = all_row["last_success_utc"] if all_row else (rows[0]["last_success_utc"] if rows else None)
    assert m["last_updated"] == expected


# --------------------------------------------------------------------------- #
# Task 4 -- /v1/last-updates cadence (annual + null for everything else)
# --------------------------------------------------------------------------- #

def test_last_updates_cadence_annual_and_null(base_url):
    code, obj = _get_json(base_url, "/v1/last-updates")
    assert code == 200
    assert set(obj.keys()) == {"generated", "datasets"}
    cadence_days = {"daily": 1, "weekly": 7, "monthly": 30, "quarterly": 91, "annual": 365}
    from datetime import datetime, timedelta

    saw_annual = False
    saw_null_cadence = False
    for d in obj["datasets"]:
        assert set(d.keys()) == {
            "source", "unit", "status", "last_updated", "source_date_accessed",
            "source_version", "last_obs_date", "next_update_expected", "obs_count",
        }, d
        last = d["last_updated"]
        cad = None
        # recompute the expected next_update from last + cadence and compare.
        nxt = d["next_update_expected"]
        # We don't have cadence in the response; assert the invariant indirectly:
        # if next_update_expected is non-null, it must be a valid ISO date AFTER
        # last_updated by one of the allowed deltas.
        if nxt is not None:
            assert last, "next_update_expected non-null but last_updated null (fabricated!)"
            ld = datetime.fromisoformat(last.replace("Z", "+00:00")).date()
            nd = datetime.fromisoformat(nxt).date()
            delta = (nd - ld).days
            assert delta in cadence_days.values(), (d, delta)
            if delta == 365:
                saw_annual = True
        else:
            saw_null_cadence = True
    # state.db has 17 annual rows (with last_success) -> annual:365 must appear,
    # and irregular/static/None cadences -> next_update_expected null must appear.
    assert saw_annual, "expected at least one annual (365d) next_update_expected"
    assert saw_null_cadence, "expected at least one null next_update_expected"


# --------------------------------------------------------------------------- #
# Task 5 -- status codes (501 / 502 data_unavailable / 502 resolver_empty / 404)
# --------------------------------------------------------------------------- #

def test_status_404_unknown_id(base_url):
    for suffix in (".csv", ".metadata.json"):
        code, ct, body = _get(base_url, f"/v1/series/{_enc('bls:NOPE_NOT_A_REAL_ID_999')}{suffix}")
        assert code == 404, (suffix, code, body[:200])
        assert json.loads(body)["error"] == "not_found"


def test_status_resolver_empty_zero_rows_in_window(base_url):
    # bls:CUUR0000SA0 exists and resolves, but a far-future window selects zero
    # rows -> 502 resolver_empty (present file/window, ZERO rows). NOT data_unavailable.
    code, ct, body = _get(base_url, f"/v1/series/{_enc(EX_BLS)}.csv?from=2099-01-01")
    assert code == 502, (code, body[:200])
    assert json.loads(body)["error"] == "resolver_empty"


def test_status_data_unavailable_vs_resolver_empty_distinct(base_url):
    # The DISTINCTION pin: a supported source whose at-rest FILE is absent must be
    # 502 data_unavailable (NOT resolver_empty, NOT 501). We synthesise this by
    # adding a catalog row for a supported source (bls) whose at-rest file cannot
    # exist (a bogus BLS code -> stem 'zz' -> bls/zz.parquet absent), pointing the
    # shim at a temp catalog that includes it.
    import sqlite3
    import tempfile
    src_cat = os.path.join(_REPO, "data", "catalog.db")
    tmpdir = tempfile.mkdtemp(prefix="econdl_conf_")
    tmp_cat = os.path.join(tmpdir, "catalog.db")
    import shutil
    shutil.copy(src_cat, tmp_cat)
    conn = sqlite3.connect(tmp_cat)
    try:
        # A bls id whose 2-char stem 'zz' has no on-disk file -> resolve() raises
        # "expected BLS file .../bls/zz.parquet not found" for a SUPPORTED source.
        conn.execute(
            "INSERT OR REPLACE INTO series (series_id, source_id, title, frequency, "
            "category, license_id, last_updated, metadata) VALUES "
            "(?,?,?,?,?,?,?,?)",
            ("bls:ZZ_NO_FILE_TEST", "bls", "synthetic absent-file test", "M",
             "macro", "us-public-domain", None, "{}"))
        conn.commit()
    finally:
        conn.close()

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, _DEVSERVER, "--host", "127.0.0.1", "--port", str(port),
         "--catalog", tmp_cat],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=_REPO)
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                raise RuntimeError(f"temp devserver died:\n{out}")
            try:
                with urllib.request.urlopen(base + "/health", timeout=2) as r:
                    if r.status == 200:
                        break
            except (urllib.error.URLError, OSError):
                time.sleep(0.2)
        # supported source (bls IS migrated) but the at-rest file is absent:
        code, ct, body = _get(base, f"/v1/series/{_enc('bls:ZZ_NO_FILE_TEST')}.csv")
        assert code == 502, (code, body[:300])
        assert json.loads(body)["error"] == "data_unavailable", body[:300]
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmpdir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Task 6 -- .csv identity column == native_to_tidy key (NOT the catalog id)
# --------------------------------------------------------------------------- #

def test_csv_identity_column_is_native_key(base_url):
    import pandas as pd
    from econdl import _resolve

    for sid in (EX_BLS, EX_OECD):
        code, ct, body = _get(base_url, f"/v1/series/{_enc(sid)}.csv")
        assert code == 200, (sid, code, body[:200])
        assert "text/csv" in ct
        df = pd.read_csv(io.BytesIO(body))
        assert list(df.columns) == ["series_id", "obs_date", "value"]
        http_keys = set(df["series_id"].astype(str).unique())

        # the LOCAL resolver's native_to_tidy is the source of truth for the key.
        res = _resolve.resolve(sid, root=_resolve.default_data_root())
        tidy = _resolve.native_to_tidy(res, _resolve.read_native(res))
        local_keys = set(tidy["series_id"].astype(str).unique())

        assert http_keys == local_keys, (sid, sorted(http_keys)[:3], sorted(local_keys)[:3])
        # and the key must be the NATIVE key, NOT the requested catalog id (the
        # whole point of Task #6): bls -> 'CUUR0000SA0', oecd -> 'Q.Y.USA...'.
        assert sid not in http_keys, (
            f"{sid}: .csv mislabeled the series with the catalog id; "
            f"expected the native key {sorted(local_keys)}")


def test_local_and_http_bundle_row_for_row_identical(base_url, tmp_path):
    import pandas as pd
    import econdl

    ids = [EX_BLS, EX_OECD]
    http_zip = str(tmp_path / "http.zip")
    local_zip = str(tmp_path / "local.zip")
    df_http = econdl.bundle(ids, out=http_zip, api=base_url, snapshot_date="2026-06-26")
    df_local = econdl.bundle(ids, out=local_zip, snapshot_date="2026-06-26")

    def canon(df: "pd.DataFrame") -> "pd.DataFrame":
        d = df.copy()
        d["obs_date"] = pd.to_datetime(d["obs_date"]).dt.strftime("%Y-%m-%d")
        d["value"] = pd.to_numeric(d["value"], errors="coerce").round(9)
        d["series_id"] = d["series_id"].astype(str)
        return (d[["series_id", "obs_date", "value"]]
                .sort_values(["series_id", "obs_date", "value"])
                .reset_index(drop=True))

    ch, cl = canon(df_http), canon(df_local)
    # row-for-row identical INCLUDING the series_id (identity) column.
    assert ch.equals(cl), (
        f"http rows={len(ch)} local rows={len(cl)}; "
        f"http ids={sorted(ch['series_id'].unique())} "
        f"local ids={sorted(cl['series_id'].unique())}")
    # belt-and-braces: the identity column must be the native keys, not catalog ids.
    assert set(ch["series_id"]) == {"CUUR0000SA0", "Q.Y.USA.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102"}


# --------------------------------------------------------------------------- #
# i18n -- ?lang= localization (official titles; English byte-identical default)
# --------------------------------------------------------------------------- #

def test_lang_default_is_byte_identical(base_url):
    # The contract guarantee: NO ?lang= (and ?lang=en) is byte-for-byte the
    # pre-i18n response -- no `lang`/`title_en` keys -> all other pins still hold.
    _, _, plain = _get(base_url, f"/v1/series/{_enc(EX_WB_AR)}.metadata.json")
    _, _, en = _get(base_url, f"/v1/series/{_enc(EX_WB_AR)}.metadata.json?lang=en")
    assert plain == en, "?lang=en must be byte-identical to no lang"
    m = json.loads(plain)
    assert "lang" not in m and "title_en" not in m, m


def test_lang_metadata_localized(base_url):
    code, m = _get_json(base_url, f"/v1/series/{_enc(EX_WB_AR)}.metadata.json?lang=ar")
    assert code == 200
    assert m["lang"] == "ar"
    # title is now the OFFICIAL Arabic label; the English label is preserved.
    assert m["title_en"] and m["title_en"] != m["title"]
    assert m["title"] != m["title_en"]
    # contains Arabic-script characters (U+0600..U+06FF) -> real localization.
    assert any("؀" <= ch <= "ۿ" for ch in m["title"]), m["title"]


def test_lang_metadata_graceful_fallback(base_url):
    # ILOSTAT has es/fr only. Asking for ar must fall back to the English title
    # (no fabricated Arabic) and, since no translation applied, NOT add title_en.
    code, m = _get_json(base_url, f"/v1/series/{_enc(EX_ILO_NOAR)}.metadata.json?lang=ar")
    assert code == 200
    assert m["lang"] == "ar"
    assert "title_en" not in m            # nothing was localized -> no title_en
    assert not any("؀" <= ch <= "ۿ" for ch in (m["title"] or "")), m["title"]
    # but es IS available -> that one localizes.
    code2, m2 = _get_json(base_url, f"/v1/series/{_enc(EX_ILO_NOAR)}.metadata.json?lang=es")
    assert m2["lang"] == "es" and m2.get("title_en") and m2["title"] != m2["title_en"]


def test_lang_unsupported_is_400(base_url):
    code, ct, body = _get(base_url, f"/v1/series/{_enc(EX_WB_AR)}.metadata.json?lang=sw")
    assert code == 400, (code, body[:200])
    err = json.loads(body)
    assert err["error"] == "unsupported_language"
    assert err["parameter"] == "lang" and err["value"] == "sw"
    assert set(err["supported"]) == {"en", "ar", "es", "fr", "ru", "zh"}


def test_lang_catalog_localized_and_en_clean(base_url):
    # ?lang=ar: results localize where available, response carries `lang`, and the
    # internal `metadata` column never leaks into a result row.
    code, c = _get_json(base_url, f"/v1/catalog?q=GDP&source=worldbank&limit=5&lang=ar")
    assert code == 200 and c["lang"] == "ar"
    assert c["results"], c
    assert all("metadata" not in r for r in c["results"])
    assert any(any("؀" <= ch <= "ۿ" for ch in (r["title"] or ""))
               for r in c["results"]), "expected >=1 Arabic title in worldbank GDP page"
    # the broad term that used to blow SQLite's variable limit must now 200.
    code2, c2 = _get_json(base_url, "/v1/catalog?q=GDP&source=worldbank&limit=3")
    assert code2 == 200 and "lang" not in c2
    assert all("metadata" not in r for r in c2["results"])


def test_catalog_q_and_source_combine(base_url):
    # q + source MUST combine (filter the search to that source), not ignore source.
    # Regression guard for the Worker drift where `source` was dropped whenever `q`
    # was present. worldbank GDP is a stable, large slice to assert against.
    code, c = _get_json(base_url, "/v1/catalog?q=GDP&source=worldbank&limit=10")
    assert code == 200 and c["results"], c
    assert all(r["source"] == "worldbank" for r in c["results"]), \
        {r["source"] for r in c["results"]}
    # a source filter that excludes all q-hits yields an honest empty set, not a leak.
    code2, c2 = _get_json(base_url, "/v1/catalog?q=GDP&source=bls&limit=10")
    assert code2 == 200
    assert all(r["source"] == "bls" for r in c2["results"])


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
