"""cepii_baci: a new CEPII release must actually reach the store, and nothing else may move it.

Before 2026-11-19 download() returned any cached <HS>.zip and ingest_zip() returned early when the
parquet existed, so a run that asked for V202601 could re-book whatever was cached as success. These tests
drive the REAL fetcher update() and the REAL ingest on tiny zips through a fake HTTP session (no
network, no localhost server). Each guard has a test that fails when the guard is removed (R346).
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config                                  # noqa: E402
from updater.errors import DefinitiveError                       # noqa: E402
from updater.strategies.fetchers import cepii_baci as F          # noqa: E402
from jobs import ingest_cepii_baci as J                          # noqa: E402

OLD, NEW = "V202401b", "V202601"
N_CODES = 210                    # extract_country_codes refuses fewer than 200


# ---- fixtures -----------------------------------------------------------------------------------

def _zip_bytes(hs, release, years, *, header="t,i,j,k,v,q", stamp=True, extra_rows=()):
    codes = "country_code,country_name,country_iso2,country_iso3\n" + "\n".join(
        f"{c},Name{c},X{c % 10},I{c:02d}" for c in range(1, N_CODES + 1))
    suf = f"_{release}" if stamp else ""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(f"country_codes_{release}.csv", codes)
        z.writestr(f"product_codes_{hs}_{release}.csv", "code,description\n010101,x\n")
        z.writestr("Readme.txt", "fixture")
        for y in years:
            body = [header]
            for exp, imp, v in ((1, 2, 1.5), (1, 2, 2.5), (2, 1, 10.0), (3, 1, float(y))):
                body.append(f"{y},{exp},{imp},010101,{v},3.0")
            body.extend(extra_rows)
            z.writestr(f"BACI_{hs}_Y{y}{suf}.csv", "\n".join(body) + "\n")
    return buf.getvalue()


class _Resp:
    def __init__(self, body, status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise IOError(f"HTTP {self.status_code}")

    def iter_content(self, n):
        for i in range(0, len(self.body), n):
            yield self.body[i:i + n]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeSession:
    """URL -> bytes. Honours Range like CEPII (Accept-Ranges: bytes). Records every call."""

    def __init__(self, files):
        self.files, self.calls = files, []

    def head(self, url, **kw):
        self.calls.append(("HEAD", url))
        b = self.files[url]
        return _Resp(b"", headers={"Content-Length": str(len(b)), "ETag": f'"{len(b)}"',
                                   "Accept-Ranges": "bytes"})

    def get(self, url, headers=None, **kw):
        self.calls.append(("GET", url, (headers or {}).get("Range")))
        b = self.files[url]
        rng = (headers or {}).get("Range")
        if rng:
            start = int(rng.split("=")[1].rstrip("-"))
            return _Resp(b[start:], 206, {"Content-Range": f"bytes {start}-{len(b) - 1}/{len(b)}"})
        return _Resp(b, 200, {"Content-Length": str(len(b))})


class NoNetwork(FakeSession):
    def __init__(self):
        super().__init__({})

    def head(self, url, **kw):
        raise AssertionError(f"no download expected, HEAD {url}")

    def get(self, url, **kw):
        raise AssertionError(f"no download expected, GET {url}")


class Env:
    pass


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    e = Env()
    e.mp = monkeypatch
    e.data = tmp_path / "data"
    e.data_root = e.data / "clean_full"
    e.out = e.data_root / "cepii_baci"
    e.out.mkdir(parents=True)
    monkeypatch.setattr(config, "DATA_ROOT", str(e.data_root))
    monkeypatch.setattr(config, "source_dir", lambda s: str(e.data_root / s))
    e.stage = e.data / "_staging_cepii_baci"
    e.raw = e.data / "raw" / "cepii_baci"
    e.files = {}
    e.release = {"HS17": NEW, "HS96": NEW}
    monkeypatch.setattr(F, "_releases", lambda sess=None: dict(e.release))
    e.sess = None
    monkeypatch.setattr(F.requests, "Session", lambda: e.sess)
    return e


def _publish_release(e, release, years, **kw):
    for hs in ("HS17", "HS96"):
        e.files[J.release_url(hs, release)] = _zip_bytes(hs, release, years, **kw)
    e.sess = FakeSession(e.files)


def _sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def _years(p):
    return sorted(set(pq.read_table(p, columns=["year"]).column("year").to_pylist()))


def _install_old(e):
    """A pre-sidecar store: parquets with no release sidecar, V202401b codes, legacy zips."""
    _publish_release(e, OLD, [2020, 2021])
    e.release = {"HS17": OLD, "HS96": OLD}
    res = F.update(None, None)
    assert res.status == "ok", res
    for hs in ("HS17", "HS96"):                      # strip the sidecar: legacy shape
        os.remove(J.sidecar_path(hs, str(e.out)))
    e.raw.mkdir(parents=True, exist_ok=True)
    (e.raw / "HS96.zip").write_bytes(b"legacy V202401b zip, must never be deleted" * 50_000)
    e.release = {"HS17": NEW, "HS96": NEW}


# ---- the defect ---------------------------------------------------------------------------------

def test_new_release_is_downloaded_ingested_projected_and_stamped(env):
    _install_old(env)
    _publish_release(env, NEW, [2020, 2021, 2022])
    res = F.update(None, None)
    assert res.status == "ok", res
    got = [c[1] for c in env.sess.calls if c[0] == "GET"]
    assert got == [J.release_url("HS17", NEW), J.release_url("HS96", NEW)]
    for hs in ("HS17", "HS96"):
        p = J.parquet_path(hs, str(env.out))
        assert _years(p) == [2020, 2021, 2022], "the new release's rows must be in the store"
        assert J.installed_release(hs, str(env.out)) == NEW
        assert (env.raw / NEW / f"{hs}.zip").exists()
    side = json.loads((env.out / F.CODES_SIDECAR).read_text(encoding="utf-8"))
    assert side["release"] == NEW
    pairs = pq.read_table(str(env.out / F.PAIRS_BASENAME))
    assert max(pairs.column("obs_date").to_pylist()).year == 2022
    assert F._pairs_release(str(env.out)) == NEW
    assert res.new_vintage == F.current_vintage(None) == F._token([("HS17", NEW), ("HS96", NEW)])
    assert res.cursor_cap_hit is True and res.series_cursors, "served CSVs must be re-derived"
    assert (env.raw / "HS96.zip").exists(), "old zips are not auto-deleted"


def test_same_release_skips_without_touching_network_or_files(env):
    _publish_release(env, NEW, [2020, 2021])
    assert F.update(None, None).status == "ok"
    before = {hs: (os.stat(J.parquet_path(hs, str(env.out))).st_mtime_ns,
                   _sha(J.parquet_path(hs, str(env.out)))) for hs in ("HS17", "HS96")}
    pairs_m = os.stat(env.out / F.PAIRS_BASENAME).st_mtime_ns
    env.sess = NoNetwork()
    res = F.update(None, None)
    # Nothing is rebuilt or fetched. The run still RE-SIGNALS the full re-derive the projection
    # owes (the flag is durable so a kill before the orchestrator books it cannot lose it), so a
    # forced re-run reads `ok` with a revision, never a silent no_change over stale CSVs.
    assert res.status == "ok", res
    assert "revised" in (res.error or ""), res.error
    assert res.cursor_cap_hit is True
    assert res.new_vintage == F.current_vintage(None)
    for hs, (m, s) in before.items():
        p = J.parquet_path(hs, str(env.out))
        assert (os.stat(p).st_mtime_ns, _sha(p)) == (m, s)
    assert os.stat(env.out / F.PAIRS_BASENAME).st_mtime_ns == pairs_m


def test_a_parquet_swapped_under_its_sidecar_is_rebuilt(env):
    """mirror_sync can copy an older object over the local file; the sidecar alone must not seal it."""
    _publish_release(env, NEW, [2020, 2021])
    F.update(None, None)
    p = J.parquet_path("HS96", str(env.out))
    t = pq.read_table(p)
    pq.write_table(t.slice(0, 3), p)                     # same schema, other rows
    res = F.update(None, None)
    assert res.status == "ok", res
    assert pq.read_metadata(p).num_rows == t.num_rows


def test_a_sidecar_naming_another_release_is_rebuilt(env):
    _publish_release(env, NEW, [2020, 2021])
    F.update(None, None)
    sc = J.sidecar_path("HS17", str(env.out))
    d = json.loads(open(sc, encoding="utf-8").read())
    d["release"] = "V202501"
    open(sc, "w", encoding="utf-8").write(json.dumps(d))
    res = F.update(None, None)
    assert res.status == "ok" and J.installed_release("HS17", str(env.out)) == NEW


# ---- crash safety -------------------------------------------------------------------------------

class _Killed(RuntimeError):
    pass


def test_a_kill_mid_ingest_leaves_the_old_parquet_and_sidecar_byte_identical(env):
    _publish_release(env, OLD, [2020, 2021])
    env.release = {"HS17": OLD, "HS96": OLD}
    F.update(None, None)
    paths = [J.parquet_path(h, str(env.out)) for h in ("HS17", "HS96")]
    scs = [J.sidecar_path(h, str(env.out)) for h in ("HS17", "HS96")]
    old_sha = [_sha(p) for p in paths]
    old_sc = [open(s, encoding="utf-8").read() for s in scs]

    _publish_release(env, NEW, [2020, 2021, 2022])
    env.release = {"HS17": NEW, "HS96": NEW}
    env.mp.setattr(J, "BATCH", 2)
    real = J.pq.ParquetWriter
    n = {"batches": 0}

    class Dies(real):
        def write_batch(self, b, *a, **k):
            super().write_batch(b, *a, **k)
            n["batches"] += 1
            if n["batches"] % 2 == 0:                   # after real bytes reached the part
                raise _Killed("killed mid-ingest")
    env.mp.setattr(J.pq, "ParquetWriter", Dies)
    res = F.update(None, None)
    assert n["batches"] >= 4                            # both classifications died mid-parse
    assert res.status == "partial" and res.new_vintage is None, res
    assert [_sha(p) for p in paths] == old_sha
    assert [open(s, encoding="utf-8").read() for s in scs] == old_sc
    assert not [f for f in os.listdir(env.stage) if f.endswith(".part")], "no half part left"


def test_a_hard_kill_leftover_part_is_discarded_not_installed(env):
    _publish_release(env, NEW, [2020, 2021])
    env.stage.mkdir(parents=True)
    junk = env.stage / "baci_hs96.parquet.part"
    pq.write_table(pa.table({"x": [1]}), str(junk))      # a SIGKILL leaves a part, no sidecar
    res = F.update(None, None)
    assert res.status == "ok"
    assert _years(J.parquet_path("HS96", str(env.out))) == [2020, 2021]


def test_a_held_file_at_replace_keeps_old_and_the_verified_part_then_swaps_next_run(env):
    _publish_release(env, OLD, [2020])
    env.release = {"HS17": OLD, "HS96": OLD}
    F.update(None, None)
    p = J.parquet_path("HS96", str(env.out))
    old_sha = _sha(p)

    _publish_release(env, NEW, [2020, 2021])
    env.release = {"HS17": NEW, "HS96": NEW}
    real = J.atomic_replace

    def held(tmp, dest, *a, **k):
        if dest == p:
            raise PermissionError(5, "a reader holds the file")
        return real(tmp, dest, *a, **k)
    env.mp.setattr(J, "atomic_replace", held)
    res = F.update(None, None)
    assert res.status == "partial" and res.new_vintage is None, res
    assert _sha(p) == old_sha
    assert J.installed_release("HS96", str(env.out)) == OLD
    assert F._codes_release(str(env.out)) == OLD, "codes must not move before HS96 does"
    assert F._pairs_release(str(env.out)) == OLD, "projection must not move before HS96 does"
    assert (env.stage / "baci_hs96.parquet.part").exists()
    assert (env.stage / "baci_hs96.parquet.part.json").exists()

    env.mp.setattr(J, "atomic_replace", real)
    env.mp.setattr(J, "_parse_to", lambda *a, **k: pytest.fail("verified part must be reused"))
    res = F.update(None, None)
    assert res.status == "ok", res
    assert _years(p) == [2020, 2021] and J.installed_release("HS96", str(env.out)) == NEW
    assert res.new_vintage == F.current_vintage(None)


# ---- verification refusals (old store kept, loud) -----------------------------------------------

@pytest.mark.parametrize("kw", [{"header": "year,exporter,importer,product,value,quantity"},
                                {"stamp": False},
                                {"extra_rows": ["2020,1,2,010101,notanumber,1"] * 50}])
def test_a_release_that_fails_verification_is_refused_and_old_kept(env, kw):
    _publish_release(env, OLD, [2020, 2021])
    env.release = {"HS17": OLD, "HS96": OLD}
    F.update(None, None)
    p = J.parquet_path("HS96", str(env.out))
    old_sha = _sha(p)
    _publish_release(env, NEW, [2020, 2021, 2022], **kw)
    env.release = {"HS17": NEW, "HS96": NEW}
    with pytest.raises(DefinitiveError):
        F.update(None, None)
    assert _sha(p) == old_sha and J.installed_release("HS96", str(env.out)) == OLD


def test_a_release_with_fewer_rows_or_an_earlier_last_year_is_refused(env):
    _publish_release(env, OLD, [2020, 2021, 2022])
    env.release = {"HS17": OLD, "HS96": OLD}
    F.update(None, None)
    _publish_release(env, NEW, [2020, 2021])
    env.release = {"HS17": NEW, "HS96": NEW}
    with pytest.raises(DefinitiveError):
        F.update(None, None)
    assert _years(J.parquet_path("HS96", str(env.out))) == [2020, 2021, 2022]


def test_an_older_release_never_replaces_a_newer_one(env, tmp_path):
    _publish_release(env, NEW, [2020, 2021])
    F.update(None, None)
    zp = tmp_path / "old.zip"
    zp.write_bytes(_zip_bytes("HS96", OLD, [2020, 2021, 2022]))
    with pytest.raises(J.ReleaseRefused):
        J.ingest_zip("HS96", str(zp), release=OLD, out_dir=str(env.out), stage_dir=str(env.stage))
    assert J.installed_release("HS96", str(env.out)) == NEW


def test_stale_country_codes_are_re_extracted_and_projection_rebuilt_without_reparse(env):
    _publish_release(env, NEW, [2020, 2021])
    F.update(None, None)
    cp = env.out / F.CODES_SIDECAR
    d = json.loads(cp.read_text(encoding="utf-8"))
    d["release"] = OLD
    cp.write_text(json.dumps(d), encoding="utf-8")
    env.mp.setattr(J, "_parse_to", lambda *a, **k: pytest.fail("HS96 is current; no re-parse"))
    res = F.update(None, None)
    assert res.status == "ok", res
    assert F._codes_release(str(env.out)) == NEW and res.cursor_cap_hit


def test_download_resumes_with_range_from_a_matching_part(env):
    _publish_release(env, NEW, [2020])
    url = J.release_url("HS96", NEW)
    body = env.files[url]
    d = env.raw / NEW
    d.mkdir(parents=True)
    (d / "HS96.zip.part").write_bytes(body[:100])
    (d / "HS96.zip.part.json").write_text(json.dumps(
        {"url": url, "length": len(body), "etag": f'"{len(body)}"', "release": NEW}))
    p = J.download("HS96", url, release=NEW, raw_dir=str(env.raw), sess=env.sess)
    assert open(p, "rb").read() == body
    assert ("GET", url, "bytes=100-") in env.sess.calls


def test_releases_takes_the_highest_version_not_the_last_listed(monkeypatch):
    page = ("BACI_HS96_V202601.zip BACI_HS17_V202601.zip "
            "previous: BACI_HS96_V202401b.zip BACI_HS17_V202401b.zip")

    class S:
        def get(self, *a, **k):
            return type("R", (), {"text": page, "raise_for_status": lambda self: None})()
    assert F._releases(S()) == {"HS96": NEW, "HS17": NEW}


# ---- staging never inside a tree a reader walks (R1949) -----------------------------------------

def test_temp_files_never_appear_in_the_store_tree_while_a_rebuild_runs(env):
    _publish_release(env, NEW, [2020, 2021])
    seen = []
    real = J.atomic_replace

    def spy(tmp, dest, *a, **k):
        # the moment of the swap: every staged file exists; ask the REAL readers what they see
        seen.append((tmp, blob.list_parquets(str(env.out), recursive=True),
                     sorted(os.listdir(env.out))))
        return real(tmp, dest, *a, **k)
    env.mp.setattr(J, "atomic_replace", spy)
    F.update(None, None)
    parts = [t for t, _l, _d in seen if t.endswith(".part")]
    assert parts, "control: the rebuild must have staged something"
    for t, listed, names in seen:
        assert not any("part" in os.path.basename(x) or "staging" in x for x in listed), listed
        # only a release-sidecar .json.tmp may pass through the store dir (no reader lists it)
        assert not any(n.endswith(".part") or ".parquet." in n for n in names), names
    for t in parts:
        assert not J._inside(t, str(env.data_root)), t


def test_a_staging_dir_inside_the_store_root_is_refused(env, tmp_path):
    zp = tmp_path / "z.zip"
    zp.write_bytes(_zip_bytes("HS96", NEW, [2020]))
    with pytest.raises(ValueError):
        J.ingest_zip("HS96", str(zp), release=NEW, out_dir=str(env.out),
                     stage_dir=str(env.data_root / "_staging_cepii_baci"))


def test_the_real_staging_and_raw_dirs_are_outside_the_real_store_root():
    """The fixture patches DATA_ROOT; the unpatched functions are pinned here."""
    data_root = os.path.normcase(os.path.abspath(config.DATA_ROOT))
    for d in (F.staging_dir(), F.raw_dir(), J.default_stage_dir(J.OUT)):
        d = os.path.normcase(os.path.abspath(d))
        assert os.path.commonpath([d, data_root]) != data_root, (d, data_root)


# ---- second review (AR, 2026-11-19): the paths the first test set did not reach -----------------

def test_a_refused_hs17_does_not_move_the_projection_and_the_fix_signals_the_rederive(env):
    """D1: HS17 refused + HS96 installed used to rebuild the projection, then finalize() raised, so
    the cursors were lost and the next run found the projection current with nothing signalled."""
    _publish_release(env, OLD, [2020, 2021])
    env.release = {"HS17": OLD, "HS96": OLD}
    F.update(None, None)
    env.release = {"HS17": NEW, "HS96": NEW}
    env.files[J.release_url("HS17", NEW)] = _zip_bytes("HS17", NEW, [2020, 2021, 2022], stamp=False)
    env.files[J.release_url("HS96", NEW)] = _zip_bytes("HS96", NEW, [2020, 2021, 2022])
    env.sess = FakeSession(env.files)
    with pytest.raises(DefinitiveError):
        F.update(None, None)
    assert F._pairs_release(str(env.out)) == OLD, "nothing may move the projection on a refusal"
    env.files[J.release_url("HS17", NEW)] = _zip_bytes("HS17", NEW, [2020, 2021, 2022])
    res = F.update(None, None)                 # re-issued under the same name: HEAD sees new bytes
    assert res.status == "ok", res
    assert res.cursor_cap_hit is True and F._pairs_release(str(env.out)) == NEW
    assert res.new_vintage == F.current_vintage(None)


def test_a_projection_not_at_the_release_forces_partial_and_no_stamp(env):
    """The not-in-place guard: ok/no_change must never stand over a store behind the page."""
    _publish_release(env, NEW, [2020, 2021])
    env.mp.setattr(F, "_pairs_release", lambda out_dir: OLD)
    res = F.update(None, None)
    assert res.status == "partial" and res.new_vintage is None, res
    assert "pairs projection" in res.error


def test_published_hs96_of_the_wrong_size_blocks_the_projection(env):
    """Under r2 the projection reads the PUBLISHED object; a size mismatch must stop it."""
    _publish_release(env, NEW, [2020, 2021])
    env.mp.setattr(F.blob, "stored_size", lambda p: 1)
    res = F.update(None, None)
    assert res.status == "partial" and res.new_vintage is None, res
    assert F._pairs_release(str(env.out)) is None


def test_a_pairs_file_swapped_under_its_sidecar_is_rebuilt(env):
    _publish_release(env, NEW, [2020, 2021])
    F.update(None, None)
    p = env.out / F.PAIRS_BASENAME
    pq.write_table(pq.read_table(str(p)).slice(0, 1), str(p))
    assert F._pairs_release(str(env.out)) is None
    env.sess = NoNetwork()
    res = F.update(None, None)
    assert res.status == "ok" and F._pairs_release(str(env.out)) == NEW


def test_a_server_ignoring_range_restarts_the_download_cleanly(env):
    _publish_release(env, NEW, [2020])
    url = J.release_url("HS96", NEW)
    body = env.files[url]
    d = env.raw / NEW
    d.mkdir(parents=True)
    (d / "HS96.zip.part").write_bytes(b"X" * 100)        # garbage that a splice would keep
    (d / "HS96.zip.part.json").write_text(json.dumps(
        {"url": url, "length": len(body), "etag": f'"{len(body)}"', "release": NEW}))

    class NoRange(FakeSession):
        def get(self, url, headers=None, **kw):
            self.calls.append(("GET", url, (headers or {}).get("Range")))
            return _Resp(self.files[url], 200)
    s = NoRange(env.files)
    p = J.download("HS96", url, release=NEW, raw_dir=str(env.raw), sess=s)
    assert open(p, "rb").read() == body


def test_a_part_from_another_etag_is_not_resumed(env):
    _publish_release(env, NEW, [2020])
    url = J.release_url("HS96", NEW)
    body = env.files[url]
    d = env.raw / NEW
    d.mkdir(parents=True)
    (d / "HS96.zip.part").write_bytes(b"X" * 100)
    (d / "HS96.zip.part.json").write_text(json.dumps(
        {"url": url, "length": len(body), "etag": '"other"', "release": NEW}))
    p = J.download("HS96", url, release=NEW, raw_dir=str(env.raw), sess=env.sess)
    assert open(p, "rb").read() == body
    assert all(c[2] is None for c in env.sess.calls if c[0] == "GET"), env.sess.calls


def test_the_cache_is_used_offline_and_refetched_when_cepii_reissues(env):
    _publish_release(env, NEW, [2020])
    url = J.release_url("HS96", NEW)
    p = J.download("HS96", url, release=NEW, raw_dir=str(env.raw), sess=env.sess)

    class Offline(FakeSession):
        def head(self, url, **kw):
            raise IOError("offline")
    assert J.download("HS96", url, release=NEW, raw_dir=str(env.raw),
                      sess=Offline(env.files)) == p
    env.files[url] = _zip_bytes("HS96", NEW, [2020, 2021])        # same name, new bytes
    s = FakeSession(env.files)
    J.download("HS96", url, release=NEW, raw_dir=str(env.raw), sess=s)
    assert open(p, "rb").read() == env.files[url]
    assert any(c[0] == "GET" for c in s.calls)


def test_a_corrupt_cached_zip_is_quarantined_not_reused_forever(env, tmp_path):
    zp = tmp_path / "raw" / NEW / "HS96.zip"
    zp.parent.mkdir(parents=True)
    good = bytearray(_zip_bytes("HS96", NEW, [2020, 2021]))
    i = good.find(b"2020,1,2")
    good[i] = ord("9")                                             # flips a CRC, not the dir
    zp.write_bytes(bytes(good))
    import zipfile as _z
    with pytest.raises(_z.BadZipFile):
        J.ingest_zip("HS96", str(zp), release=NEW, out_dir=str(env.out), stage_dir=str(env.stage))
    assert not zp.exists() and [f for f in os.listdir(zp.parent) if ".bad-" in f]


def test_a_legacy_store_without_sidecars_still_refuses_a_downgrade(env, tmp_path):
    (env.out / F.CODES_SIDECAR).write_text(json.dumps(
        {"from": f"country_codes_{NEW}.csv", "codes": {}}), encoding="utf-8")
    zp = tmp_path / "z.zip"
    zp.write_bytes(_zip_bytes("HS96", OLD, [2020]))
    with pytest.raises(J.ReleaseRefused):
        J.ingest_zip("HS96", str(zp), release=OLD, out_dir=str(env.out), stage_dir=str(env.stage))


def test_a_lettered_reissue_is_not_accepted_as_the_plain_release(env, tmp_path):
    zp = tmp_path / "z.zip"
    zp.write_bytes(_zip_bytes("HS96", NEW + "b", [2020]))
    with pytest.raises(J.ReleaseRefused):
        J.ingest_zip("HS96", str(zp), release=NEW, out_dir=str(env.out), stage_dir=str(env.stage))


def test_main_takes_the_release_from_the_page_and_never_downgrades(env, monkeypatch):
    _publish_release(env, NEW, [2020, 2021])
    F.update(None, None)
    monkeypatch.setattr(J, "OUT", str(env.out))
    monkeypatch.setattr(J, "TMP", str(env.raw))
    with pytest.raises(J.ReleaseRefused):
        _publish_release(env, OLD, [2020, 2021, 2022])
        monkeypatch.setattr(J.requests, "Session", lambda: env.sess)
        J.main(["--release", OLD])
    assert J.installed_release("HS96", str(env.out)) == NEW
