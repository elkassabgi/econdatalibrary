"""tools/publish_ilostat_split_map.py --source census against a fake R2 and a temporary catalogue.

Each guard is shown to FIRE on a planted bad input and to STAND DOWN on its control (review AR-408: the
census split map goes to R2 only through a reviewed tool - absent-only, coverage against the catalogue's
'#part' ids, a byte-equal read-back, refused after T0). Never the network.
"""
import hashlib
import json
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import publish_ilostat_split_map as T  # noqa: E402

KEY = "clean_full/census/_split_map.json"
MAP = {"intltrade__exports__hs": {"dims": ["E_COMMODITY:3"], "parts": 2, "rows": 10, "sep": "~"},
       "eits__m3": {"dims": ["seasonally_adj"], "parts": 1, "rows": 5, "sep": "~"}}
IDS = ["census:intltrade__exports__hs#010", "census:intltrade__exports__hs#020", "census:eits__m3#no",
       "census:eits__marts", "census:advm3:DXD:NO:sa", "ilostat:EMP_TEMP_Q#p1"]


class FakeR2:
    def __init__(self, have=None, *, refuse_put=False, back=None, etag=None):
        self.objs = {} if have is None else {KEY: have}
        self.refuse_put, self.back, self.etag = refuse_put, back, etag
        self.put_ifs, self.put_atomics, self.gets = [], 0, 0

    def get(self, key):
        self.gets += 1
        if self.back is not None and self.put_ifs:
            return self.back
        return self.objs.get(key)

    def put_if(self, key, data, *, if_match=None, if_none_match=False):
        self.put_ifs.append((key, if_none_match, if_match))
        if self.refuse_put or (if_none_match and key in self.objs):
            return None
        self.objs[key] = data
        return self.etag if self.etag is not None else hashlib.md5(data).hexdigest()

    def put_atomic(self, key, data, **kw):          # must never be used for census (unconditional PUT)
        self.put_atomics += 1
        self.objs[key] = data


def _setup(tmp_path, monkeypatch, *, smap=MAP, ids=IDS, body=None, local_rel=("census",)):
    from core import cutover
    from updater import blob
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER"))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")              # restored after the test (the tool sets r2)
    local = tmp_path.joinpath("data", "clean_full", *local_rel, "_split_map.json")
    local.parent.mkdir(parents=True)
    local.write_bytes(body if body is not None else json.dumps(smap, indent=1, sort_keys=True).encode())
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, i.split(':')[0]) for i in ids])
    con.commit()
    con.close()
    built = []
    return local, cat, built, blob


def _run(tmp_path, monkeypatch, fake, *args, no_md5=False, **kw):
    """--expect-md5 is REQUIRED for census: pass the written file's own md5 unless the test names one."""
    local, cat, built, blob = _setup(tmp_path, monkeypatch, **kw)
    monkeypatch.setattr(blob, "_r2_routed", lambda: built.append(1) or fake)
    if "--expect-md5" not in args and not no_md5:
        args = (*args, "--expect-md5", hashlib.md5(local.read_bytes()).hexdigest())
    rc = T.main(["--source", "census", "--local", str(local), "--catalog", str(cat), *args])
    return rc, built, local


# ---- controls ------------------------------------------------------------------------------------------
def test_control_absent_map_dry_run_writes_nothing(tmp_path, monkeypatch, capsys):
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake)
    out = capsys.readouterr().out
    assert rc == 0 and fake.put_ifs == [] and fake.put_atomics == 0 and built == [1]
    assert "dry run - nothing written" in out and "problems: 0" in out


def test_control_absent_map_is_published_create_only_and_read_back(tmp_path, monkeypatch, capsys):
    fake = FakeR2()
    rc, _, local = _run(tmp_path, monkeypatch, fake, "--apply")
    out = capsys.readouterr().out
    assert rc == 0 and "read back IDENTICAL" in out
    assert fake.put_ifs == [(KEY, True, None)] and fake.put_atomics == 0
    assert fake.objs[KEY] == local.read_bytes()


def test_control_identical_map_on_r2_is_nothing_to_do(tmp_path, monkeypatch, capsys):
    body = json.dumps(MAP, indent=1, sort_keys=True).encode()
    fake = FakeR2(have=body)
    rc, _, _ = _run(tmp_path, monkeypatch, fake, "--apply")
    assert rc == 0 and "identical - nothing to do" in capsys.readouterr().out and fake.put_ifs == []


# ---- planted refusals ----------------------------------------------------------------------------------
def test_a_different_map_already_in_r2_is_refused(tmp_path, monkeypatch, capsys):
    fake = FakeR2(have=b'{"other": {"dims": ["X"], "parts": 1}}')
    rc, _, _ = _run(tmp_path, monkeypatch, fake, "--apply")
    assert rc == 2 and fake.put_ifs == [] and fake.put_atomics == 0
    assert "absent-only" in capsys.readouterr().out


def test_replace_is_refused_for_census(tmp_path, monkeypatch, capsys):
    fake = FakeR2(have=b'{"other": {}}')
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", "--replace")
    assert rc == 2 and built == [] and fake.put_ifs == [] and fake.put_atomics == 0


def test_a_map_created_after_the_check_is_never_overwritten(tmp_path, monkeypatch, capsys):
    """The race (AR-409 P3): R2 refuses the create-only PUT; the tool must not fall back to a plain PUT."""
    fake = FakeR2(refuse_put=True)
    rc, _, _ = _run(tmp_path, monkeypatch, fake, "--apply")
    assert rc == 2 and len(fake.put_ifs) == 1 and fake.put_atomics == 0 and KEY not in fake.objs
    assert "appeared after the check" in capsys.readouterr().out


def test_a_read_back_mismatch_fails(tmp_path, monkeypatch, capsys):
    fake = FakeR2(back=b'{"truncated": 1}')
    rc, _, _ = _run(tmp_path, monkeypatch, fake, "--apply")
    assert rc == 1 and "read back DIFFERENT" in capsys.readouterr().out


def test_a_put_etag_that_is_not_the_local_md5_fails(tmp_path, monkeypatch, capsys):
    fake = FakeR2(etag="0" * 32)
    rc, _, _ = _run(tmp_path, monkeypatch, fake, "--apply")
    assert rc == 1 and "is not the local md5" in capsys.readouterr().out


def test_after_t0_it_is_refused_before_anything(tmp_path, monkeypatch):
    from core import cutover
    fake = FakeR2()
    local, cat, built, blob = _setup(tmp_path, monkeypatch)
    flag = tmp_path / "CUTOVER"
    flag.write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    monkeypatch.setattr(blob, "_r2_routed", lambda: built.append(1) or fake)
    with pytest.raises(cutover.CutoverRefused):
        T.main(["--source", "census", "--local", str(local), "--catalog", str(cat), "--apply"])
    assert built == [] and fake.put_ifs == [] and fake.put_atomics == 0


@pytest.mark.parametrize("ids,what", [
    (IDS + ["census:intltrade__imports__hs#01"], "and the map has no entry"),          # table not in map
    ([i for i in IDS if not i.startswith("census:eits__m3#")], "serves no '#part' id"),  # map entry not served
    (IDS + ["census:intltrade__exports__hs#030"], "the map says 2 part(s), the catalogue serves 3"),
    ([i for i in IDS if i != "census:intltrade__exports__hs#020"], "the map says 2 part(s), the catalogue serves 1"),
])
def test_a_coverage_gap_is_refused_before_r2(tmp_path, monkeypatch, capsys, ids, what):
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", ids=ids)
    out = capsys.readouterr().out
    assert rc == 2 and built == [] and fake.put_ifs == [] and what in out, out


@pytest.mark.parametrize("entry,what", [
    ({"parts": 1}, "names no split dimension"),
    ({"dims": [], "parts": 1}, "names no split dimension"),
    ({"dims": ["seasonally_adj:0"], "parts": 1}, "not NAME or NAME:<n>"),
    ({"dims": ["a=b|c"], "parts": 1}, "not NAME or NAME:<n>"),
    ({"dims": ["seasonally_adj"], "parts": 1, "sep": ""}, "sep is not a non-empty string"),
    ({"dims": ["seasonally_adj"], "parts": "1"}, "no integer part count"),
    ({"dims": ["seasonally_adj"], "parts": True}, "no integer part count"),
    ("not an object", "the entry is not an object"),
])
def test_an_entry_the_resolver_cannot_use_is_refused(tmp_path, monkeypatch, capsys, entry, what):
    smap = dict(MAP, eits__m3=entry)
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", smap=smap)
    out = capsys.readouterr().out
    assert rc == 2 and built == [] and what in out, out


def test_control_a_legacy_dim_entry_is_accepted(tmp_path, monkeypatch):
    smap = dict(MAP, eits__m3={"dim": "seasonally_adj", "parts": 1})
    rc, _, _ = _run(tmp_path, monkeypatch, FakeR2(), smap=smap)
    assert rc == 0


def test_expect_md5_refuses_a_different_file(tmp_path, monkeypatch, capsys):
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", "--expect-md5", "345d9dbbf87d80a04a6ec38b97590210")
    assert rc == 2 and built == [] and fake.put_ifs == [] and "not the expected" in capsys.readouterr().out


def test_control_expect_md5_of_the_file_passes(tmp_path, monkeypatch):
    body = json.dumps(MAP, indent=1, sort_keys=True).encode()
    rc, _, _ = _run(tmp_path, monkeypatch, FakeR2(), "--apply", "--expect-md5", hashlib.md5(body).hexdigest().upper())
    assert rc == 0


def test_a_map_from_another_directory_is_refused(tmp_path, monkeypatch, capsys):
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", local_rel=("ilostat",))
    assert rc == 2 and built == [] and "does not end in data/clean_full/census/_split_map.json" in capsys.readouterr().out


@pytest.mark.parametrize("body,what", [(b"not json", "is not JSON"), (b"[]", "not a non-empty JSON object"),
                                       (b"{}", "not a non-empty JSON object")])
def test_a_map_that_is_not_a_non_empty_object_is_refused(tmp_path, monkeypatch, capsys, body, what):
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", body=body)
    assert rc == 2 and built == [] and fake.put_ifs == [] and what in capsys.readouterr().out


def test_a_census_publish_without_expect_md5_is_refused(tmp_path, monkeypatch, capsys):
    """AR-409 r2: the coverage check cannot see a changed cut with equal part counts, so the md5 is required."""
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", no_md5=True)
    assert rc == 2 and built == [] and fake.put_ifs == [] and "needs --expect-md5" in capsys.readouterr().out


@pytest.mark.parametrize("cut", [{"sep": "|"}, {"dims": ["STATE"]}, {"dims": ["E_COMMODITY:4"]}])
def test_a_changed_cut_with_equal_counts_passes_coverage_so_the_md5_must_stop_it(tmp_path, monkeypatch, capsys, cut):
    """The planted cut changes read 0 problems (the reason the md5 is required) and the md5 refuses them."""
    smap = dict(MAP, intltrade__exports__hs=dict(MAP["intltrade__exports__hs"], **cut))
    con_parts = {"intltrade__exports__hs": 2, "eits__m3": 1}
    assert T.census_map_problems(smap, con_parts) == []
    reviewed = hashlib.md5(json.dumps(MAP, indent=1, sort_keys=True).encode()).hexdigest()
    fake = FakeR2()
    rc, built, _ = _run(tmp_path, monkeypatch, fake, "--apply", "--expect-md5", reviewed, smap=smap)
    assert rc == 2 and built == [] and fake.put_ifs == [] and "not the expected" in capsys.readouterr().out


PLANT = "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"


def _boom():
    from botocore.exceptions import EndpointConnectionError
    return EndpointConnectionError(endpoint_url=f"{PLANT}/econ-data/{KEY}")


@pytest.mark.parametrize("where,rc_want,what", [
    ("build", 2, "could not be built (EndpointConnectionError)"),
    ("get", 2, "GET of clean_full/census/_split_map.json failed (EndpointConnectionError); nothing written"),
    ("put", 1, "raised (EndpointConnectionError); whether it landed is UNKNOWN"),
    ("back", 1, "read-back GET failed (EndpointConnectionError)"),
])
def test_an_r2_error_is_reduced_to_a_code(tmp_path, monkeypatch, capsys, where, rc_want, what):
    """K2: botocore errors carry the endpoint URL; the tool prints only a code, and never a traceback."""
    assert PLANT in str(_boom())                                  # the plant is real
    fake = FakeR2()
    if where == "get":
        fake.get = lambda key: (_ for _ in ()).throw(_boom())
    elif where == "put":
        def put_if(key, data, **kw):
            raise _boom()
        fake.put_if = put_if
    elif where == "back":
        orig_put, calls = fake.put_if, []

        def put_if(key, data, **kw):
            calls.append(1)
            return orig_put(key, data, **kw)

        def get(key):
            if calls:
                raise _boom()
            return None
        fake.put_if, fake.get = put_if, get
    if where == "build":
        local, cat, built, blob = _setup(tmp_path, monkeypatch)

        def bad():
            raise _boom()
        monkeypatch.setattr(blob, "_r2_routed", bad)
        rc = T.main(["--source", "census", "--local", str(local), "--catalog", str(cat), "--apply",
                     "--expect-md5", hashlib.md5(local.read_bytes()).hexdigest()])
    else:
        rc, _, _ = _run(tmp_path, monkeypatch, fake, "--apply")
    out = capsys.readouterr()
    assert rc == rc_want and what in out.out, out.out
    assert "cloudflarestorage" not in out.out + out.err


def test_an_unreadable_or_empty_catalogue_is_refused(tmp_path, monkeypatch, capsys):
    fake = FakeR2()
    local, cat, built, blob = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(blob, "_r2_routed", lambda: built.append(1) or fake)
    assert T.main(["--source", "census", "--local", str(local), "--catalog", str(tmp_path / "nope.db"),
                   "--apply", "--expect-md5", hashlib.md5(local.read_bytes()).hexdigest()]) == 2
    assert "cannot be read to check the map's coverage" in capsys.readouterr().out
    rc, built2, _ = _run(tmp_path / "b", monkeypatch, fake, "--apply", ids=["ilostat:EMP_TEMP_Q#p1"])
    assert rc == 2 and built == [] and built2 == [] and fake.put_ifs == []


def test_a_catalogue_with_no_census_part_ids_says_so(tmp_path, monkeypatch, capsys):
    rc, built, _ = _run(tmp_path, monkeypatch, FakeR2(), "--apply", ids=["ilostat:EMP_TEMP_Q#p1"])
    assert rc == 2 and built == [] and "holds no census '#part' id" in capsys.readouterr().out


def test_catalogued_parts_counts_only_census_part_ids():
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    con.executemany("INSERT INTO series VALUES (?)", [(i,) for i in IDS + ["censusx:a#b", "census;z#q"]])
    assert T.catalogued_parts(con, "census") == {"intltrade__exports__hs": 2, "eits__m3": 1}
