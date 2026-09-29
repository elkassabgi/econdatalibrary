"""unsdg rotation-cycle edge cases: the review's probes (R1284), kept as tests, plus the release-token,
outage and broken-stream paths added in round 2."""
import datetime as dt, json, os, sys, types  # noqa: E401
import pytest
import requests
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from updater.strategies.fetchers import unsdg as U  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402

def _unit(b): return types.SimpleNamespace(config={"max_series": b})

@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(U.config, "source_dir", lambda s: str(tmp_path / s))
    monkeypatch.setattr(U, "RATE", 0)
    st = {"codes": ["A1","B2","C3"], "missing": set(), "transient": set(), "release": None, "broken": set()}
    monkeypatch.setattr(U, "_series_list", lambda: ([{"code": c, "release": st["release"]} for c in st["codes"]],
                                                    "ok"))
    fetched = []
    def fetch(code):
        fetched.append(code)
        if code in st["broken"]: raise requests.exceptions.ChunkedEncodingError("stream broke")
        if code in st["transient"]: return [], [], [], "transient"
        if code in st["missing"]: return [], [], [], "missing"
        return [f"{code}:4"], [dt.date(2025,12,31)], [1.0], "ok"
    monkeypatch.setattr(U, "_fetch_series", fetch)
    return types.SimpleNamespace(dir=tmp_path/"unsdg", st=st, fetched=fetched)

def _cyc(w): return json.loads((w.dir/"_cycle.json").read_text())

def test_closing_pass_of_many_empty_codes(world):
    """3 data codes then 12 missing codes; pass1 takes the data, pass2 only the 12 empty ones."""
    world.st["codes"] = ["A1","B2","C3"] + [f"M{i:02d}" for i in range(12)]
    world.st["missing"] = {f"M{i:02d}" for i in range(12)}
    r1 = U.update(_unit(3), None); assert r1.status == "partial"
    try:
        r2 = U.update(_unit(50), None)
        print("R2", r2.status, r2.error)
        outcome = r2.status
    except DefinitiveError as e:
        print("R2 RAISED", e); outcome = "raised"
    print("cycle after", _cyc(world))
    assert outcome != "raised", "closing pass of >10 empty codes raises all-empty structural error (red) and has already reset the cycle"

def test_chunk_of_all_empty_codes_is_visited(world):
    """55 codes: the first 50 all missing (chunk flush with no keys) then 5 data codes."""
    world.st["codes"] = [f"M{i:02d}" for i in range(50)] + ["A1","B2","C3","D4","E5"]
    world.st["missing"] = {f"M{i:02d}" for i in range(50)}
    r = U.update(_unit(0), None)
    assert r.status == "ok", (r.status, r.error)

def test_code_dropped_mid_cycle(world):
    world.st["codes"] = ["A1","B2","C3","D4"]
    U.update(_unit(2), None)
    world.st["codes"] = ["A1","B2","C3"]     # D4 retired, C3 owed
    r = U.update(_unit(5), None)
    assert r.status == "ok" and world.fetched == ["A1","B2","C3"], (r.status, world.fetched)

def test_code_added_mid_cycle(world):
    world.st["codes"] = ["A1","B2","C3"]
    U.update(_unit(2), None)
    world.st["codes"] = ["A1","B2","C3","Z9"]
    r = U.update(_unit(5), None)
    assert r.status == "ok" and sorted(world.fetched[2:]) == ["C3","Z9"], (r.status, world.fetched)

def test_quarantine_two_transients_closes(world):
    world.st["transient"] = {"B2"}
    r1 = U.update(_unit(5), None); r2 = U.update(_unit(5), None)
    print(r1.status, r2.status, _cyc(world))
    assert r2.status == "partial"
    assert _cyc(world)["visited"] == [], "second consecutive failure quarantines B2 and closes the cycle"

def test_budget_zero_all_codes(world):
    r = U.update(_unit(0), None)
    assert r.status == "ok" and world.fetched == ["A1","B2","C3"]


# --- round 2: the cycle's release token, a first-pass outage, a broken stream ---------------------------
def test_a_cycle_swept_under_one_release_stamps_its_token_on_close(world):
    world.st["release"] = "2026.Q2.G.02"
    tok = U.current_token([{"code": c, "release": "2026.Q2.G.02"} for c in world.st["codes"]])
    r1 = U.update(_unit(2), None)
    assert r1.status == "partial" and r1.new_vintage != tok
    r2 = U.update(_unit(2), None)
    assert r2.status == "ok" and r2.new_vintage == tok, (r2.status, r2.new_vintage)
    assert tok == U.current_vintage(None), "the stamp is the probe's own token"


def test_a_release_that_moves_mid_cycle_is_not_claimed(world):
    world.st["release"] = "2026.Q2.G.02"
    U.update(_unit(2), None)
    world.st["release"] = "2026.Q3.G.01"                 # UNSD releases while the cycle is half done
    r2 = U.update(_unit(2), None)
    assert r2.status == "ok" and r2.new_vintage == "date-tail", r2.new_vintage
    r3 = U.update(_unit(5), None)                       # a fresh cycle, wholly under the new release
    new = U.current_token([{"code": c, "release": "2026.Q3.G.01"} for c in world.st["codes"]])
    assert r3.status == "ok" and r3.new_vintage == new, r3.new_vintage


def test_codes_without_release_tags_never_seal_the_unit(world):
    assert U.current_token([{"code": "A1"}, {"code": "B2", "release": ""}]) is None
    r = U.update(_unit(5), None)
    assert r.status == "ok" and r.new_vintage == "date-tail"


def test_a_first_pass_outage_raises_and_leaves_every_code_owed(world):
    world.st["codes"] = [f"M{i:02d}" for i in range(12)]
    world.st["missing"] = set(world.st["codes"])
    with pytest.raises(DefinitiveError, match="all 12 stored series code"):
        U.update(_unit(50), None)
    assert _cyc(world)["visited"] == [], "an outage refreshed nothing: no code may count as visited"


def test_a_broken_stream_is_a_transient_failure_of_that_code_not_a_frozen_rotation(world):
    world.st["broken"] = {"B2"}
    r = U.update(_unit(5), None)
    assert r.status == "partial" and "B2" in (r.error or ""), (r.status, r.error)
    assert set(_cyc(world)["visited"]) == {"A1", "C3"}
    world.st["broken"] = set()
    assert U.update(_unit(5), None).status == "ok"


# --- round 3 (review R1286): an outage that starts mid-cycle; a parser raise -----------------------------
def test_an_outage_mid_cycle_over_stored_codes_raises_and_visits_nothing(world):
    """Production shape: the store holds every code. A new cycle's first pass is healthy, then Series/Data
    answers nothing while Series/List still works - the vanished codes make it an outage."""
    world.st["codes"] = [f"C{i:02d}" for i in range(36)]
    world.st["release"] = "2026.Q2.G.02"
    assert U.update(_unit(0), None).status == "ok"                 # all 36 stored; the cycle closes
    assert U.update(_unit(12), None).status == "partial"           # a new cycle: 12 visited
    before = set(_cyc(world)["visited"])
    world.st["missing"] = set(world.st["codes"])                   # the outage
    with pytest.raises(DefinitiveError, match="wholesale outage"):
        U.update(_unit(12), None)
    assert set(_cyc(world)["visited"]) == before, "an outage pass visits nothing"


def test_a_first_sweep_outage_cannot_seal_the_release(world):
    """The reviewer's probe: the store holds only the first pass's codes, so the later ones cannot vanish.
    The cycle may close, but a pass of >10 codes that added nothing marks the token MIXED - no seal."""
    world.st["codes"] = [f"C{i:02d}" for i in range(36)]
    world.st["release"] = "2026.Q2"
    assert U.update(_unit(12), None).status == "partial"
    world.st["missing"] = set(world.st["codes"])
    out = []
    for _ in range(2):
        try:
            r = U.update(_unit(12), None)
            out.append((r.status, r.new_vintage))
        except DefinitiveError as e:
            out.append(("RAISED", str(e)[:60]))
    token = U.current_token([{"code": c, "release": "2026.Q2"} for c in world.st["codes"]])
    assert out[-1][1] != token, f"24 of 36 codes returned nothing, yet the release was claimed: {out}"


def test_a_parser_raise_is_a_failure_of_that_code_not_a_frozen_rotation(world, monkeypatch):
    real = U._fetch_series

    def fetch(code):
        if code == "B2":
            raise AttributeError("'str' object has no attribute 'get'")
        return real(code)
    monkeypatch.setattr(U, "_fetch_series", fetch)
    r = U.update(_unit(0), None)
    assert r.status == "partial" and set(_cyc(world)["visited"]) == {"A1", "C3"}
    r2 = U.update(_unit(0), None)
    assert world.fetched.count("A1") == 1, "visited codes are not re-asked"
